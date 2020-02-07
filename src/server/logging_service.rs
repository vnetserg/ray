use super::config::{LoggingConfig, LoggingTarget};
use crate::{
    errors::*,
    util::{do_and_die, ProfiledUnboundedReceiver, ProfiledUnboundedSender},
};

use chrono::{DateTime, Utc};
use crossbeam::channel::{unbounded, Receiver, Sender};
use lazy_static::lazy_static;
use uuid::Uuid;

use libc::STDERR_FILENO;
use nix::unistd::dup;

use log::{Level, LevelFilter, Log, Metadata, Record};
use metrics::gauge;

use tokio::sync::mpsc::error::TryRecvError;

use std::{
    fmt::{self, Display},
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    os::unix::io::FromRawFd,
    thread,
};

lazy_static! {
    static ref FASTLOG_CHANNEL: (Sender<FastlogRecord>, Receiver<FastlogRecord>) = unbounded();
    pub static ref FASTLOG_SENDER: Sender<FastlogRecord> = FASTLOG_CHANNEL.0.clone();
    static ref FASTLOG_RECEIVER: Receiver<FastlogRecord> = FASTLOG_CHANNEL.1.clone();
}

const DATETIME_FORMAT: &str = "%F %T%.3f";

pub struct LoggingService {
    receiver: ProfiledUnboundedReceiver<(String, Level)>,
    writers: Vec<(BufWriter<File>, LevelFilter)>,
}

impl LoggingService {
    pub fn new(
        receiver: ProfiledUnboundedReceiver<(String, Level)>,
        config: &LoggingConfig,
    ) -> Result<Self> {
        let mut writers = vec![];
        for target_config in &config.targets {
            let file = match &target_config.target {
                LoggingTarget::Stderr => {
                    let stderr_fd =
                        dup(STDERR_FILENO).chain_err(|| "failed to dup stderr file descriptor")?;
                    unsafe { File::from_raw_fd(stderr_fd) }
                }
                LoggingTarget::File { path } => {
                    let maybe_file = OpenOptions::new().append(true).create(true).open(path);
                    maybe_file.chain_err(|| format!("failed to open {}", path))?
                }
            };
            let writer = BufWriter::with_capacity(config.buffer_size, file);
            writers.push((writer, target_config.level.into()));
        }

        Ok(Self { receiver, writers })
    }

    pub async fn serve(&mut self) -> Result<()> {
        loop {
            gauge!(
                "rayd.logging_service.queue_size",
                self.receiver.approx_len()
            );
            let (message, level) = match self.receiver.try_recv() {
                Ok(result) => result,
                Err(TryRecvError::Empty) => {
                    self.flush().chain_err(|| "failed to flush writers")?;
                    self.receiver.recv().await.chain_err(|| "receiver failed")?
                }
                Err(TryRecvError::Closed) => {
                    bail!("receiver is closed");
                }
            };
            for (writer, filter) in self.writers.iter_mut() {
                if level <= *filter {
                    writer
                        .write(message.as_bytes())
                        .chain_err(|| format!("failed to write message '{}'", message))?;
                }
            }
        }
    }

    fn flush(&mut self) -> Result<()> {
        for (writer, _) in self.writers.iter_mut() {
            writer.flush()?;
        }
        Ok(())
    }
}

pub struct LoggingServiceFacade {
    sender: ProfiledUnboundedSender<(String, Level)>,
    modules: Vec<String>,
    max_level: LevelFilter,
}

impl Log for LoggingServiceFacade {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= self.max_level
            && self
                .modules
                .iter()
                .any(|module| metadata.target().starts_with(module))
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            let message = format!(
                "{} [{}] {}: {}\n",
                Utc::now().format(DATETIME_FORMAT),
                record.level(),
                record.module_path().unwrap_or("unknown"),
                record.args(),
            );
            self.sender
                .send((message, record.level()))
                .expect("logging service is dead");
        }
    }

    fn flush(&self) {}
}

impl LoggingServiceFacade {
    pub fn init(
        sender: ProfiledUnboundedSender<(String, Level)>,
        config: &LoggingConfig,
    ) -> Result<()> {
        let max_level = config
            .targets
            .iter()
            .map(|target| LevelFilter::from(target.level))
            .max()
            .unwrap_or(LevelFilter::Off);
        let modules = config.modules.clone();
        let facade = Box::new(LoggingServiceFacade {
            sender,
            max_level,
            modules,
        });
        log::set_boxed_logger(facade)
            .map(|_| log::set_max_level(max_level))
            .chain_err(|| "failed to set logger")
    }
}

pub struct FastlogRecord {
    pub datetime: DateTime<Utc>,
    pub module: &'static str,
    pub message: FastlogMessage,
}

impl ToString for FastlogRecord {
    fn to_string(&self) -> String {
        format!(
            "{} [DEBUG] {}: {}\n",
            self.datetime.format(DATETIME_FORMAT),
            self.module,
            self.message,
        )
    }
}

pub enum FastlogMessage {
    ApplyingMutation { epoch: u64, id: Uuid },
    ServingQuery { epoch: u64, id: Uuid },
    PersistedMutation { epoch: u64, id: Uuid },
    RecoveredMutation { epoch: u64, id: Uuid },
}

impl Display for FastlogMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ApplyingMutation { epoch, id } => {
                write!(f, "Applying mutation (id: {}, new epoch: {})", id, epoch)
            }
            Self::ServingQuery { epoch, id } => {
                write!(f, "Serving query (id: {}, epoch: {})", id, epoch)
            }
            Self::PersistedMutation { epoch, id } => {
                write!(f, "Persisted mutation (id: {}, epoch: {})", id, epoch)
            }
            Self::RecoveredMutation { epoch, id } => {
                write!(f, "Recovered mutation (id: {}, epoch: {})", id, epoch)
            }
        }
    }
}

pub struct FastlogService {
    receiver: Receiver<FastlogRecord>,
    sender: ProfiledUnboundedSender<(String, Level)>,
}

impl FastlogService {
    pub fn init(sender: ProfiledUnboundedSender<(String, Level)>, threads: u16) -> Result<()> {
        let threads = if threads == 0 {
            num_cpus::get()
        } else {
            threads as usize
        };
        for _ in 0..threads {
            let thread_sender = sender.clone();
            let thread = thread::Builder::new()
                .name("rayd-fastlog".to_string())
                .spawn(move || {
                    let receiver = FASTLOG_RECEIVER.clone();
                    let mut worker = FastlogService {
                        receiver,
                        sender: thread_sender,
                    };
                    do_and_die(move || worker.run());
                });
            thread.chain_err(|| "failed to spawn thread")?;
        }
        Ok(())
    }

    fn run(&mut self) -> Result<()> {
        for record in self.receiver.iter() {
            let message = record.to_string();
            self.sender
                .send((message, Level::Debug))
                .chain_err(|| "sender failed")?;
        }
        bail!("receiver terminated");
    }
}

pub fn fastlog_queue_size() -> usize {
    FASTLOG_SENDER.len()
}

#[macro_export]
macro_rules! fastlog {
    ($message:expr) => {
        crate::server::logging_service::FASTLOG_SENDER
            .send(crate::server::logging_service::FastlogRecord {
                datetime: ::chrono::Utc::now(),
                module: ::std::module_path!(),
                message: $message,
            })
            .expect("fastlog sender failed")
    };
    (now: $now:expr, $message:expr) => {
        crate::server::logging_service::FASTLOG_SENDER
            .send(crate::server::logging_service::FastlogRecord {
                datetime: $now,
                module: ::std::module_path!(),
                message: $message,
            })
            .expect("fastlog sender failed")
    };
}

use super::config::{LoggingConfig, LoggingTarget};
use crate::{
    errors::*,
    util::{ProfiledUnboundedReceiver, ProfiledUnboundedSender},
};

use log::{Level, LevelFilter, Log, Metadata, Record};
use metrics::gauge;

use chrono::Utc;

use libc::STDERR_FILENO;
use nix::unistd::dup;

use tokio::sync::mpsc::error::TryRecvError;

use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    os::unix::io::FromRawFd,
};

pub struct LoggingService {
    receiver: ProfiledUnboundedReceiver<(String, Level)>,
    writers: Vec<(BufWriter<File>, LevelFilter)>,
}

impl LoggingService {
    pub fn new(
        receiver: ProfiledUnboundedReceiver<(String, Level)>,
        configs: &[LoggingConfig],
    ) -> Result<Self> {
        let mut writers = vec![];
        for config in configs {
            let file = match &config.target {
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
            let writer = BufWriter::new(file);
            writers.push((writer, config.level.into()));
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
    max_level: LevelFilter,
}

impl Log for LoggingServiceFacade {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= self.max_level
            && (metadata.target().starts_with("ray") || metadata.target().starts_with("panic"))
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            let message = format!(
                "{} [{}] {}: {}\n",
                Utc::now().format("%F %T%.3f"),
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
        configs: &[LoggingConfig],
    ) -> Result<()> {
        let max_level = configs
            .iter()
            .map(|config| LevelFilter::from(config.level))
            .max()
            .unwrap_or(LevelFilter::Off);
        let facade = Box::new(LoggingServiceFacade { sender, max_level });
        log::set_boxed_logger(facade)
            .map(|_| log::set_max_level(max_level))
            .chain_err(|| "failed to set logger")
    }
}

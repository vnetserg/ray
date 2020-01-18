mod config;
mod file_mutation_log;
mod log_service;
mod machine_service;
mod rpc;
mod storage_machine;

pub use config::{Config, LoggingConfig, LoggingTarget};

use super::proto::storage_server::StorageServer;
use config::PsmConfig;
use file_mutation_log::FileMutationLog;
use log_service::{LogService, PersistentLog};
use machine_service::{Machine, MachineService, MachineServiceHandle};
use rpc::RayStorageService;

use futures::channel::mpsc;
use tokio::runtime;
use tonic::transport::Server;

use log::LevelFilter;
use simplelog::{
    CombinedLogger, LevelPadding, SharedLogger, TermLogger, TerminalMode, WriteLogger,
};

use std::{fs, future::Future, net::SocketAddr, process::exit, thread};

pub fn serve_forever(config: Config) -> ! {
    init_logging(&config.logging);

    let ip_address = config.rpc.address.parse().unwrap_or_else(|_| {
        error!("'{}' is not a valid IP address", config.rpc.address);
        exit(1);
    });
    let socket_address = SocketAddr::new(ip_address, config.rpc.port);

    let log = FileMutationLog::new(&config.mutation_log).unwrap_or_else(|err| {
        error!("Failed to open '{}': {}", &config.mutation_log.path, err);
        exit(1);
    });

    let handle = run_psm(log, &config.psm);
    let storage = RayStorageService::new(handle);
    let server = Server::builder()
        .add_service(StorageServer::new(storage))
        .serve(socket_address);

    let num_threads = if config.rpc.threads > 0 {
        config.rpc.threads as usize
    } else {
        num_cpus::get()
    };
    let mut runtime = runtime::Builder::new()
        .threaded_scheduler()
        .core_threads(num_threads)
        .thread_name("rayd-rpc-worker")
        .enable_all()
        .build()
        .expect("Failed to build Tokio runtime");

    info!("Serving rayd on {}", socket_address);

    match runtime.block_on(server) {
        Ok(()) => exit(0),
        Err(err) => {
            eprintln!("Error: {}", err);
            exit(1);
        }
    }
}

fn init_logging(configs: &[LoggingConfig]) {
    let sl_config = simplelog::ConfigBuilder::new()
        .add_filter_allow_str("ray")
        .add_filter_allow_str("log_panics")
        .set_time_format_str("%F %T%.3f")
        .set_target_level(LevelFilter::Error)
        .set_thread_level(LevelFilter::Off)
        .set_level_padding(LevelPadding::Off)
        .build();

    let loggers = configs
        .iter()
        .map(|config| {
            let logger: Box<dyn SharedLogger> = match &config.target {
                LoggingTarget::Stderr => {
                    TermLogger::new(config.level.into(), sl_config.clone(), TerminalMode::Mixed)
                        .unwrap_or_else(|| {
                            eprintln!("Failed to create terminal logger");
                            exit(1);
                        })
                }
                LoggingTarget::File { path } => {
                    let maybe_file = fs::OpenOptions::new().append(true).create(true).open(path);
                    let file = maybe_file.unwrap_or_else(|err| {
                        eprintln!("Failed to open '{}': {}", path, err);
                        exit(1);
                    });
                    WriteLogger::new(config.level.into(), sl_config.clone(), file)
                }
            };
            logger
        })
        .collect();

    CombinedLogger::init(loggers).unwrap_or_else(|err| {
        eprintln!("Failed to initialize combined logger: {}", err);
        exit(1);
    });

    log_panics::init();
}

fn run_psm<M: Machine, L: PersistentLog>(log: L, config: &PsmConfig) -> MachineServiceHandle<M> {
    let log_config = &config.log_service;
    let machine_config = &config.machine_service;

    let (log_sender, log_receiver) = mpsc::channel(log_config.request_queue_size);
    let (machine_sender, machine_receiver) = mpsc::channel(machine_config.request_queue_size);
    let (mutation_sender, mutation_receiver) = mpsc::channel(machine_config.mutation_queue_size);

    let log_batch_size = log_config.batch_size;
    run_in_dedicated_thread("rayd-log", async move {
        let mut log_service =
            LogService::<L, M::Mutation>::new(log, mutation_sender, log_receiver, log_batch_size);
        log_service.recover().await;
        log_service.serve().await;
    });
    run_in_dedicated_thread("rayd-machine", async move {
        let mut machine_service = MachineService::new(mutation_receiver, machine_receiver);
        machine_service.serve().await;
    });
    MachineServiceHandle::new(log_sender, machine_sender)
}

fn run_in_dedicated_thread<T: Future + Send + 'static>(thread_name: &'static str, task: T) {
    let thread = thread::Builder::new()
        .name(thread_name.to_string())
        .spawn(move || {
            let mut runtime = runtime::Builder::new()
                .basic_scheduler()
                .build()
                .expect("Failed to build Tokio runtime");
            runtime.block_on(task);
            panic!("Thread '{}' terminated unexpectedly", thread_name);
        });
    thread.expect("Failed to spawn thread");
}

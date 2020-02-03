mod config;
mod directory_journal;
mod directory_snapshot_storage;
mod journal_service;
mod machine_service;
mod rpc;
mod snapshot_service;
mod storage_machine;

pub use config::Config;

use config::{LoggingConfig, LoggingTarget, MetricsConfig, PsmConfig};
use directory_journal::DirectoryJournalReader;
use directory_snapshot_storage::DirectorySnapshotStorage;
use journal_service::{JournalReader, JournalServiceRestorer};
use machine_service::{Machine, MachineService, MachineServiceHandle};
use rpc::RayStorageService;
use snapshot_service::{read_snapshot, SnapshotService, SnapshotStorage};

use crate::{
    errors::*,
    proto::storage_server::StorageServer,
    util::{profiled_channel, profiled_unbounded_channel},
};

use tokio::runtime;
use tonic::transport::Server;

use log::LevelFilter;
use simplelog::{
    CombinedLogger, LevelPadding, SharedLogger, TermLogger, TerminalMode, WriteLogger,
};

use metrics_runtime::{exporters::HttpExporter, observers::PrometheusBuilder, Receiver};

use std::{
    fs,
    future::Future,
    net::SocketAddr,
    panic::{catch_unwind, AssertUnwindSafe},
    process::exit,
    thread,
};

pub fn serve_forever(config: Config) -> ! {
    init_logging(&config.logging).unwrap_or_else(|err| {
        eprintln!(
            "Failed to initialize logging (error chain below)\n{}",
            err.display_fancy_chain()
        );
        exit(1);
    });

    init_metrics(&config.metrics).unwrap_or_else(|err| {
        eprintln!(
            "Failed to initialize metrics (error chain below)\n{}",
            err.display_fancy_chain()
        );
        exit(1);
    });

    start_server(config).unwrap_or_else(|err| {
        error!(
            "Failed to start server (error chain below)\n{}",
            err.display_fancy_chain()
        );
        exit(1);
    });

    exit(0);
}

fn init_logging(configs: &[LoggingConfig]) -> Result<()> {
    let sl_config = simplelog::ConfigBuilder::new()
        .add_filter_allow_str("ray")
        .add_filter_allow_str("log_panics")
        .set_time_format_str("%F %T%.3f")
        .set_target_level(LevelFilter::Error)
        .set_thread_level(LevelFilter::Off)
        .set_level_padding(LevelPadding::Off)
        .build();

    let mut loggers = vec![];
    for config in configs {
        let logger: Box<dyn SharedLogger> = match &config.target {
            LoggingTarget::Stderr => {
                TermLogger::new(config.level.into(), sl_config.clone(), TerminalMode::Mixed)
                    .chain_err(|| "failed to initialize terminal logger")?
            }
            LoggingTarget::File { path } => {
                let maybe_file = fs::OpenOptions::new().append(true).create(true).open(path);
                let file = maybe_file.chain_err(|| format!("failed to open {}", path))?;
                WriteLogger::new(config.level.into(), sl_config.clone(), file)
            }
        };
        loggers.push(logger);
    }

    CombinedLogger::init(loggers).chain_err(|| "failed to initialize combined lobber")?;

    log_panics::init();

    Ok(())
}

fn init_metrics(config: &MetricsConfig) -> Result<()> {
    if !config.enable {
        return Ok(());
    }

    let receiver = Receiver::builder()
        .build()
        .chain_err(|| "failed to create metrics receiver")?;

    let address = config
        .address
        .parse()
        .chain_err(|| format!("not a valid IP address: {}", config.address))?;
    let server = HttpExporter::new(
        receiver.controller(),
        PrometheusBuilder::new(),
        SocketAddr::new(address, config.port),
    );

    receiver.install();

    run_in_dedicated_thread("rayd-metrics", RuntimeKind::WithIo, async move {
        server
            .async_run()
            .await
            .chain_err(|| "failed to run metrics server")
    })?;

    Ok(())
}

fn start_server(config: Config) -> Result<()> {
    let ip_address = config
        .rpc
        .address
        .parse()
        .chain_err(|| format!("not a valid IP address: {}", config.rpc.address))?;
    let socket_address = SocketAddr::new(ip_address, config.rpc.port);

    let journal_reader = DirectoryJournalReader::new(&config.journal_storage)
        .chain_err(|| "failed to initialize journal reader")?;

    let snapshot_storage = DirectorySnapshotStorage::new(&config.snapshot_storage.path)
        .chain_err(|| "failed to initialize snapshot storage")?;

    let handle = run_psm(journal_reader, snapshot_storage, &config.psm)
        .chain_err(|| "failed to initialize PSM services")?;

    let storage_service = RayStorageService::new(handle);
    let server = Server::builder()
        .add_service(StorageServer::new(storage_service))
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
        .chain_err(|| "failed to start Tokio runtime")?;

    info!("Serving rayd on {}", socket_address);

    runtime.block_on(server).chain_err(|| "RPC service failed")
}

fn run_psm<M: Machine, R: JournalReader, S: SnapshotStorage>(
    journal_reader: R,
    storage: S,
    config: &PsmConfig,
) -> Result<MachineServiceHandle<M>> {
    let journal_config = &config.journal_service;
    let machine_config = &config.machine_service;
    let snapshot_config = &config.snapshot_service;

    let (journal_sender, journal_receiver) = profiled_channel(journal_config.request_queue_size);
    let (machine_sender, machine_receiver) = profiled_channel(machine_config.request_queue_size);
    let (snapshot_sender, snapshot_receiver) = profiled_unbounded_channel();
    let (min_epoch_sender, min_epoch_receiver) = profiled_unbounded_channel();

    let handle = MachineServiceHandle::new(journal_sender, machine_sender.clone());
    let snapshot = storage
        .open_last_snapshot()
        .chain_err(|| "failed to open the last snapshot")?;

    let (machine, epoch) = match snapshot {
        Some(mut reader) => {
            let (machine, epoch) =
                read_snapshot(&mut reader).chain_err(|| "failed to read snapshot")?;
            info!("Recovered state from snapshot (epoch: {})", epoch);
            (machine, epoch)
        }
        None => {
            info!("No snapshots found, starting fresh");
            (M::default(), 0)
        }
    };

    let journal_batch_size = journal_config.batch_size;
    run_in_dedicated_thread("rayd-journal", RuntimeKind::Basic, async move {
        let restorer = JournalServiceRestorer::<R, M>::new(
            journal_reader,
            machine_sender,
            snapshot_sender,
            journal_receiver,
            min_epoch_receiver,
            journal_batch_size,
            epoch,
        );
        let mut journal_service = restorer.restore().await?;
        journal_service.serve().await
    })?;

    let snapshot_machine = machine.clone();
    let snapshot_interval = snapshot_config.snapshot_interval;
    let snapshot_batch_size = snapshot_config.batch_size;
    run_in_dedicated_thread("rayd-snapshot", RuntimeKind::Basic, async move {
        let mut snapshot_service = SnapshotService::<S, M>::new(
            storage,
            snapshot_machine,
            snapshot_receiver,
            min_epoch_sender,
            epoch,
            snapshot_interval,
            snapshot_batch_size,
        );
        snapshot_service.serve().await
    })?;

    run_in_dedicated_thread("rayd-machine", RuntimeKind::Basic, async move {
        let mut machine_service = MachineService::new(machine, machine_receiver, epoch);
        machine_service.serve().await
    })?;

    Ok(handle)
}

enum RuntimeKind {
    Basic,
    WithIo,
}

fn run_in_dedicated_thread<T: Future<Output = Result<()>> + Send + 'static>(
    thread_name: &'static str,
    kind: RuntimeKind,
    task: T,
) -> Result<()> {
    let thread = thread::Builder::new()
        .name(thread_name.to_string())
        .spawn(move || {
            let mut builder = runtime::Builder::new();
            builder.basic_scheduler();
            if let RuntimeKind::WithIo = kind {
                builder.enable_io();
            }

            let mut runtime = builder.build().unwrap_or_else(|err| {
                error!("Failed to build Tokio runtime: {}", err);
                exit(1);
            });

            let result = catch_unwind(AssertUnwindSafe(move || runtime.block_on(task)));

            match result {
                Ok(Ok(())) => error!("Thread '{}' finished unexpectedly", thread_name),
                Ok(Err(err)) => error!(
                    "Thread '{}' failed (error chain below)\n{}",
                    thread_name,
                    err.display_fancy_chain()
                ),
                Err(_) => (), // panic occured, error is already logged by the panic hook.
            }

            exit(1);
        });

    thread.chain_err(|| "failed to spawn thread")?;
    Ok(())
}

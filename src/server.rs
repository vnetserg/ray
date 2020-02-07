mod config;
mod directory_journal;
mod directory_snapshot_storage;
mod journal_service;
mod logging_service;
mod machine_service;
mod rpc;
mod snapshot_service;
mod storage_machine;

pub use config::Config;

use config::{LoggingConfig, MetricsConfig, PsmConfig};
use directory_journal::DirectoryJournalReader;
use directory_snapshot_storage::DirectorySnapshotStorage;
use journal_service::{JournalReader, JournalServiceRestorer};
use logging_service::{FastlogService, LoggingService, LoggingServiceFacade};
use machine_service::{Machine, MachineService, MachineServiceHandle};
use rpc::RayStorageService;
use snapshot_service::{read_snapshot, SnapshotService, SnapshotStorage};

use crate::{
    errors::*,
    proto::storage_server::StorageServer,
    util::{
        do_and_die, get_children_pids, get_process_cpu_time, get_process_name, profiled_channel,
        profiled_unbounded_channel,
    },
};

use tokio::{runtime, sync::oneshot};
use tonic::transport::Server;

use metrics::{labels, Key};
use metrics_runtime::{
    exporters::HttpExporter, observers::PrometheusBuilder, Measurement, Receiver,
};

use std::{
    future::Future,
    net::SocketAddr,
    process::exit,
    sync::{atomic::AtomicU64, Arc},
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
        error!(
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

fn init_logging(config: &LoggingConfig) -> Result<()> {
    let (log_sender, log_receiver) = profiled_unbounded_channel();

    let mut logging_service = LoggingService::new(log_receiver, config)
        .chain_err(|| "failed to create logging service")?;
    run_in_dedicated_thread("rayd-logging", RuntimeKind::Basic, async move {
        logging_service.serve().await
    })?;

    LoggingServiceFacade::init(log_sender.clone(), config)?;
    FastlogService::init(log_sender, config.fastlog_threads)?;
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

    let main_pid = std::process::id();
    receiver.sink().proxy("rayd", move || {
        let pids = match get_children_pids(main_pid) {
            Ok(pids) => pids,
            Err(err) => {
                warn!("Failed to get child pids:\n{}", err.display_fancy_chain());
                return Vec::new();
            }
        };
        pids.into_iter()
            .filter_map(|pid| {
                let name = match get_process_name(pid) {
                    Ok(name) => name,
                    Err(err) => {
                        warn!(
                            "Failed to get process name (pid {}):\n{}",
                            pid,
                            err.display_fancy_chain()
                        );
                        return None;
                    }
                };
                let cpu_time = match get_process_cpu_time(pid) {
                    Ok(cpu_time) => cpu_time,
                    Err(err) => {
                        warn!(
                            "Failed to get process cpu time (pid {}):\n{}",
                            pid,
                            err.display_fancy_chain()
                        );
                        return None;
                    }
                };
                let labels = labels!("pid" => pid.to_string(), "name" => name);
                let key = Key::from_name_and_labels("cpu_time", labels);
                let value = Measurement::Gauge(cpu_time as i64);
                Some((key, value))
            })
            .collect()
    });

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

    let (handle, ready) = run_psm(journal_reader, snapshot_storage, &config.psm)
        .chain_err(|| "failed to run PSM services")?;

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

    // Wait for PSM services to become initialized.
    runtime
        .block_on(ready)
        .chain_err(|| "wait on PSM initialization failed")?;

    info!("Serving rayd on {}", socket_address);

    runtime.block_on(server).chain_err(|| "RPC service failed")
}

fn run_psm<M: Machine, R: JournalReader, S: SnapshotStorage>(
    journal_reader: R,
    storage: S,
    config: &PsmConfig,
) -> Result<(MachineServiceHandle<M>, oneshot::Receiver<()>)> {
    let journal_config = &config.journal_service;
    let machine_config = &config.machine_service;
    let snapshot_config = &config.snapshot_service;

    let (journal_sender, journal_receiver) = profiled_channel(journal_config.request_queue_size);
    let (machine_sender, machine_receiver) = profiled_channel(machine_config.request_queue_size);
    let (snapshot_sender, snapshot_receiver) = profiled_unbounded_channel();
    let (min_epoch_sender, min_epoch_receiver) = profiled_unbounded_channel();
    let persisted_epoch = Arc::new(AtomicU64::new(0));

    let handle = MachineServiceHandle::new(
        journal_sender,
        machine_sender.clone(),
        persisted_epoch.clone(),
    );
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

    let (ready_sender, ready_receiver) = oneshot::channel();
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
            persisted_epoch,
        );
        let mut journal_service = restorer.restore().await?;
        ready_sender.send(()).ok();
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

    Ok((handle, ready_receiver))
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

            do_and_die(move || runtime.block_on(task));
        });

    thread.chain_err(|| "failed to spawn thread")?;
    Ok(())
}

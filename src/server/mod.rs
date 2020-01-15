mod machine_service;
mod storage_machine;
mod log_service;
mod file_mutation_log;

pub use file_mutation_log::FileMutationLog;

use machine_service::{
    Machine,
    MachineService,
    MachineServiceHandle,
};

use storage_machine::StorageMachine;

use log_service::{
    PersistentLog,
    LogService,
};

use tokio::runtime;

use futures::channel::mpsc;

use std::{
    thread,
    future::Future,
};


pub type StorageHandle = MachineServiceHandle<StorageMachine>;

pub fn run_storage(log: FileMutationLog) -> StorageHandle {
    run_psm::<StorageMachine, FileMutationLog>(log)
}


fn run_psm<M: Machine, L: PersistentLog>(log: L) -> MachineServiceHandle<M> {
    let (log_sender, log_receiver) = mpsc::channel(1000);
    let (machine_sender, machine_receiver) = mpsc::channel(1000);
    let (mutation_sender, mutation_receiver) = mpsc::channel(1000);
    run_in_dedicated_thread("PersistentLogService",
        async move {
            let mut log_service = LogService::<L, M::Mutation>::new(
                log,
                mutation_sender,
                log_receiver,
            );
            log_service.recover().await;
            log_service.serve().await;
        }
    );
    run_in_dedicated_thread("MachineService",
        async move {
            let mut machine_service = MachineService::new(
                mutation_receiver,
                machine_receiver,
            );
            machine_service.serve().await;
        }
    );
    MachineServiceHandle::new(log_sender, machine_sender)
}

fn run_in_dedicated_thread<T: Future + Send + 'static>(service_name: &'static str, task: T) {
    thread::spawn(move || {
        let mut rt = runtime::Builder::new()
            .basic_scheduler()
            .build()
            .expect("runtime creation failed");
        rt.block_on(task);
        panic!("Service '{}' terminated (run in dedicated thread)", service_name);
    });
}

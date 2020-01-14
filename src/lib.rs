pub mod client;

pub mod proto {
    tonic::include_proto!("ray");
}

pub mod config {
    pub const DEFAULT_PORT: u16 = 39781;
}

mod psm;
mod storage_machine;
mod mutation_log;

pub mod server {
    pub use super::psm::{
        run as run_persistent_state_machine,
        PersistentMachineHandle,
    };
    pub use super::storage_machine::StorageMachine;
    pub use super::mutation_log::MutationLog;
}

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const AUTHORS: &str = env!("CARGO_PKG_AUTHORS");

pub mod client;

pub mod proto {
    tonic::include_proto!("ray");
}

pub mod config {
    pub const DEFAULT_PORT: u16 = 39781;
}

pub mod psm;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const AUTHORS: &str = env!("CARGO_PKG_AUTHORS");

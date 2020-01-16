pub mod proto {
    tonic::include_proto!("ray");
}

pub mod client;
pub mod server;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const AUTHORS: &str = env!("CARGO_PKG_AUTHORS");

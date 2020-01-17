#[macro_use]
extern crate log;

pub mod client;
pub mod proto;
pub mod server;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const AUTHORS: &str = env!("CARGO_PKG_AUTHORS");

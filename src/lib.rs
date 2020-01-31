#[macro_use]
extern crate log;

pub mod benchmark;
pub mod client;
pub mod proto;
pub mod server;

mod errors;
mod util;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const AUTHORS: &str = env!("CARGO_PKG_AUTHORS");

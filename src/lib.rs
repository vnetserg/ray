pub mod client;

pub mod proto {
    tonic::include_proto!("ray");

    impl From<Value> for Box<[u8]> {
        fn from(value: Value) -> Box<[u8]> {
            value.value.into_boxed_slice()
        }
    }

    impl From<Box<[u8]>> for Value {
        fn from(bx: Box<[u8]>) -> Value {
            Value { value: bx.into() }
        }
    }
}

pub mod config {
    pub const DEFAULT_PORT: u16 = 39781;
}

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const AUTHORS: &str = env!("CARGO_PKG_AUTHORS");

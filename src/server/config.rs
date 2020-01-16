use serde::{Serialize, Deserialize};

#[derive(Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub rpc: RpcConfig,
    pub psm: PsmConfig,
    pub mutation_log: MutationLogConfig,
}

#[derive(Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RpcConfig {
    pub threads: u16,
    pub address: String,
    pub port: u16,
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            threads: 0,
            address: String::from("127.0.0.1"),
            port: 39172,
        }
    }
}

#[derive(Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PsmConfig {
    pub machine_service: MachineServiceConfig,
    pub log_service: LogServiceConfig,
}

#[derive(Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MachineServiceConfig {
    pub request_queue_size: usize,
    pub mutation_queue_size: usize,
}

impl Default for MachineServiceConfig {
    fn default() -> Self {
        Self {
            request_queue_size: 10000,
            mutation_queue_size: 10000,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LogServiceConfig {
    pub request_queue_size: usize,
    pub batch_size: usize,
}

impl Default for LogServiceConfig {
    fn default() -> Self {
        Self {
            request_queue_size: 10000,
            batch_size: 100,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MutationLogConfig {
    pub path: String,
}

impl Default for MutationLogConfig {
    fn default() -> Self {
        Self {
            path: String::from("./rayd-log.bin"),
        }
    }
}

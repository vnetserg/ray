use log::LevelFilter;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub rpc: RpcConfig,
    pub psm: PsmConfig,
    pub mutation_log: MutationLogConfig,
    pub snapshot_storage: SnapshotStorageConfig,
    pub logging: Vec<LoggingConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rpc: RpcConfig::default(),
            psm: PsmConfig::default(),
            mutation_log: MutationLogConfig::default(),
            snapshot_storage: SnapshotStorageConfig::default(),
            logging: vec![LoggingConfig {
                target: LoggingTarget::Stderr,
                level: LogLevel::Info,
            }],
        }
    }
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
    pub snapshot_service: SnapshotServiceConfig,
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
pub struct SnapshotServiceConfig {
    pub snapshot_interval: u64,
    pub batch_size: usize,
}

impl Default for SnapshotServiceConfig {
    fn default() -> Self {
        Self {
            snapshot_interval: 10000,
            batch_size: 100_000,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MutationLogConfig {
    pub path: String,
    pub soft_file_size_limit: usize,
}

impl Default for MutationLogConfig {
    fn default() -> Self {
        Self {
            path: String::from("./journal"),
            soft_file_size_limit: 100_000_000,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SnapshotStorageConfig {
    pub path: String,
}

impl Default for SnapshotStorageConfig {
    fn default() -> Self {
        Self {
            path: String::from("./snapshots"),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    pub target: LoggingTarget,
    pub level: LogLevel,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum LoggingTarget {
    #[serde(rename = "stderr")]
    Stderr,
    #[serde(rename = "file")]
    File { path: String },
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum LogLevel {
    #[serde(rename = "debug")]
    Debug,
    #[serde(rename = "info")]
    Info,
    #[serde(rename = "warn")]
    Warn,
    #[serde(rename = "error")]
    Error,
}

impl From<LogLevel> for LevelFilter {
    fn from(level: LogLevel) -> log::LevelFilter {
        match level {
            LogLevel::Debug => LevelFilter::Debug,
            LogLevel::Info => LevelFilter::Info,
            LogLevel::Warn => LevelFilter::Warn,
            LogLevel::Error => LevelFilter::Error,
        }
    }
}

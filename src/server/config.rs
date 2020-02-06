use log::LevelFilter;
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub rpc: RpcConfig,
    pub psm: PsmConfig,
    pub journal_storage: JournalStorageConfig,
    pub snapshot_storage: SnapshotStorageConfig,
    pub logging: LoggingConfig,
    pub metrics: MetricsConfig,
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
            address: "127.0.0.1".into(),
            port: 39172,
        }
    }
}

#[derive(Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PsmConfig {
    pub machine_service: MachineServiceConfig,
    pub journal_service: JournalServiceConfig,
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
pub struct JournalServiceConfig {
    pub request_queue_size: usize,
    pub batch_size: usize,
}

impl Default for JournalServiceConfig {
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
pub struct JournalStorageConfig {
    pub path: String,
    pub file_size_soft_limit: usize,
}

impl Default for JournalStorageConfig {
    fn default() -> Self {
        Self {
            path: String::from("./journal"),
            file_size_soft_limit: 100_000_000,
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
    pub buffer_size: usize,
    pub modules: Vec<String>,
    pub targets: Vec<LoggingTargetConfig>,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            buffer_size: 1_000_000,
            modules: vec!["ray".to_string(), "panic".to_string()],
            targets: vec![LoggingTargetConfig {
                target: LoggingTarget::Stderr,
                level: LogLevel::Info,
            }],
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingTargetConfig {
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

#[derive(Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetricsConfig {
    pub enable: bool,
    pub address: String,
    pub port: u16,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enable: true,
            address: "127.0.0.1".into(),
            port: 40000,
        }
    }
}

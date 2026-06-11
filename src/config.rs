use std::{env, net::SocketAddr, time::Duration};

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub dht: DhtConfig,
    pub metadata: MetadataConfig,
    pub pipeline: PipelineConfig,
    pub storage: StorageConfig,
    pub search: SearchConfig,
}

#[derive(Debug, Clone)]
pub struct DhtConfig {
    pub listen_addr: SocketAddr,
    pub routing_table_max_nodes: usize,
    pub hash_queue_size: usize,
}

#[derive(Debug, Clone)]
pub struct MetadataConfig {
    pub max_concurrent_fetches: usize,
    pub timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub print_jsonl: bool,
    pub stats_interval: Duration,
}

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub enabled: bool,
    pub database_url: Option<String>,
    pub auth_token: Option<String>,
    pub max_files_per_torrent: usize,
    pub max_file_path_len: usize,
}

#[derive(Debug, Clone)]
pub struct SearchConfig {
    pub max_name_ngram_len: usize,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        let listen_addr = env::var("DHT_LISTEN_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:6881".to_string())
            .parse()
            .context("invalid DHT_LISTEN_ADDR")?;

        Ok(Self {
            dht: DhtConfig {
                listen_addr,
                routing_table_max_nodes: env_usize("DHT_ROUTING_TABLE_MAX_NODES", 200_000)
                    .clamp(1_000, 1_000_000),
                hash_queue_size: env_usize("INFO_HASH_QUEUE_SIZE", 50_000).clamp(1_000, 1_000_000),
            },
            metadata: MetadataConfig {
                max_concurrent_fetches: env_usize("METADATA_MAX_CONCURRENT_FETCHES", 1_000)
                    .clamp(16, 8_192),
                timeout: Duration::from_secs(env_u64("METADATA_TIMEOUT_SECS", 15).clamp(1, 120)),
            },
            pipeline: PipelineConfig {
                print_jsonl: env_bool("PRINT_JSONL", false),
                stats_interval: Duration::from_secs(
                    env_u64("STATS_INTERVAL_SECS", 60).clamp(5, 600),
                ),
            },
            storage: StorageConfig {
                enabled: env_bool("STORAGE_ENABLED", true),
                database_url: env::var("LIBSQL_DATABASE_URL").ok(),
                auth_token: env::var("LIBSQL_AUTH_TOKEN").ok(),
                max_files_per_torrent: env_usize("MAX_FILES_PER_TORRENT", 2_000).clamp(1, 20_000),
                max_file_path_len: env_usize("MAX_FILE_PATH_LEN", 1_024).clamp(32, 8_192),
            },
            search: SearchConfig {
                max_name_ngram_len: env_usize("MAX_NAME_NGRAM_LEN", 4_096).clamp(128, 100_000),
            },
        })
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    env::var(key)
        .ok()
        .and_then(|value| match value.as_str() {
            "1" | "true" | "TRUE" | "yes" | "YES" => Some(true),
            "0" | "false" | "FALSE" | "no" | "NO" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

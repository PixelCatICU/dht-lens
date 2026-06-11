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
    pub listen_addr_v6: Option<SocketAddr>,
    pub bootstrap_nodes: Vec<String>,
    pub bootstrap_query_limit: usize,
    pub get_peers_probe_count: usize,
    pub routing_table_max_nodes: usize,
    pub virtual_nodes: usize,
}

#[derive(Debug, Clone)]
pub struct MetadataConfig {
    pub max_concurrent_fetches: usize,
    pub max_peers_per_hash: usize,
    pub connect_timeout: Duration,
    pub metadata_timeout: Duration,
    pub max_metadata_size: usize,
}

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub info_hash_queue_size: usize,
    pub result_queue_size: usize,
    pub peer_collect_window: Duration,
    pub print_jsonl: bool,
}

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub enabled: bool,
    pub database_url: Option<String>,
    pub auth_token: Option<String>,
    pub write_concurrency: usize,
    pub batch_size: usize,
    pub flush_interval: Duration,
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
        let listen_addr_v6 = match env::var("DHT_LISTEN_ADDR_V6") {
            Ok(value) => Some(value.parse().context("invalid DHT_LISTEN_ADDR_V6")?),
            Err(_) => Some(
                "[::]:6881"
                    .parse()
                    .context("invalid default IPv6 DHT address")?,
            ),
        };

        let bootstrap_nodes = env::var("DHT_BOOTSTRAP_NODES")
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToOwned::to_owned)
                    .collect()
            })
            .unwrap_or_else(|_| {
                vec![
                    "router.bittorrent.com:6881".to_string(),
                    "dht.transmissionbt.com:6881".to_string(),
                    "router.utorrent.com:6881".to_string(),
                ]
            });

        Ok(Self {
            dht: DhtConfig {
                listen_addr,
                listen_addr_v6,
                bootstrap_nodes,
                bootstrap_query_limit: env_usize("DHT_BOOTSTRAP_QUERY_LIMIT", 1024)
                    .clamp(16, 8_192),
                get_peers_probe_count: env_usize("DHT_GET_PEERS_PROBE_COUNT", 4).clamp(0, 64),
                routing_table_max_nodes: env_usize("DHT_ROUTING_TABLE_MAX_NODES", 200_000),
                virtual_nodes: env_usize("DHT_VIRTUAL_NODES", 512).clamp(1, 4_096),
            },
            metadata: MetadataConfig {
                max_concurrent_fetches: env_usize("METADATA_MAX_CONCURRENT_FETCHES", 4_096)
                    .clamp(64, 8_192),
                max_peers_per_hash: env_usize("METADATA_MAX_PEERS_PER_HASH", 32).clamp(1, 128),
                connect_timeout: Duration::from_secs(env_u64("METADATA_CONNECT_TIMEOUT_SECS", 2)),
                metadata_timeout: Duration::from_secs(env_u64("METADATA_TIMEOUT_SECS", 2)),
                max_metadata_size: env_usize("METADATA_MAX_SIZE_MB", 8) * 1024 * 1024,
            },
            pipeline: PipelineConfig {
                info_hash_queue_size: env_usize("INFO_HASH_QUEUE_SIZE", 50_000),
                result_queue_size: env_usize("RESULT_QUEUE_SIZE", 100_000),
                peer_collect_window: Duration::from_millis(env_u64("PEER_COLLECT_WINDOW_MS", 0)),
                print_jsonl: env_bool("PRINT_JSONL", true),
            },
            storage: StorageConfig {
                enabled: env_bool("STORAGE_ENABLED", true),
                database_url: env::var("LIBSQL_DATABASE_URL").ok(),
                auth_token: env::var("LIBSQL_AUTH_TOKEN").ok(),
                write_concurrency: env_usize("DB_WRITE_CONCURRENCY", 4).clamp(1, 64),
                batch_size: env_usize("DB_BATCH_SIZE", 100),
                flush_interval: Duration::from_millis(env_u64("DB_FLUSH_INTERVAL_MS", 1_000)),
                max_files_per_torrent: env_usize("MAX_FILES_PER_TORRENT", 2_000),
                max_file_path_len: env_usize("MAX_FILE_PATH_LEN", 1_024),
            },
            search: SearchConfig {
                max_name_ngram_len: env_usize("MAX_NAME_NGRAM_LEN", 4_096),
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

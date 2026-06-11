use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use anyhow::{Context, Result, bail};
use tracing::{error, info, warn};

use crate::{
    ady_dht::{
        Config as DhtConfig, Dht, Mode,
        bencode::{self, BVal},
        types::Peer,
        wire::WireRunner,
    },
    config::AppConfig,
    model::{Source, TorrentFile, TorrentRecord, now_ts},
    storage::LibsqlStore,
};

pub async fn run(config: AppConfig, store: Option<Arc<LibsqlStore>>) -> Result<()> {
    let mut dht_config = DhtConfig::default();
    dht_config.mode = Mode::Crawl;
    dht_config.network = "udp4".to_string();
    dht_config.address = config.dht.listen_addr.to_string();
    dht_config.max_nodes = config.dht.routing_table_max_nodes;
    dht_config.refresh_node_num = 8;

    let mut dht = Dht::new(dht_config.clone()).await?;
    let stats = Arc::new(CrawlerStats::default());

    let wire_workers = config.metadata.max_concurrent_fetches.max(1);
    let wire_queue_size = config.dht.hash_queue_size.max(1);
    let (wire_runner, wire_handle) = WireRunner::new_with_timeout(
        65_536,
        wire_queue_size,
        wire_workers,
        config.metadata.timeout,
    );

    let mut metadata_rx = wire_handle.subscribe();
    let store_for_metadata = store.clone();
    let config_for_metadata = config.clone();
    let metadata_stats = stats.clone();
    tokio::spawn(async move {
        loop {
            match metadata_rx.recv().await {
                Ok(response) => {
                    metadata_stats.fetched.fetch_add(1, Ordering::Relaxed);
                    let stats = metadata_stats.clone();
                    let store = store_for_metadata.clone();
                    let config = config_for_metadata.clone();
                    tokio::spawn(async move {
                        match build_record(
                            response.request.info_hash,
                            &response.metadata_info,
                            &config,
                        ) {
                            Ok(record) => {
                                if config.pipeline.print_jsonl {
                                    println!("{}", record.name);
                                }

                                if let Some(store) = store {
                                    match store
                                        .insert_torrent(&record, config.search.max_name_ngram_len)
                                        .await
                                    {
                                        Ok(()) => {
                                            stats.stored.fetch_add(1, Ordering::Relaxed);
                                        }
                                        Err(err) => {
                                            stats.store_errors.fetch_add(1, Ordering::Relaxed);
                                            error!(info_hash = %record.info_hash, error = %err, "failed to store torrent");
                                        }
                                    }
                                }
                            }
                            Err(err) => {
                                stats.parse_errors.fetch_add(1, Ordering::Relaxed);
                                warn!(error = %err, "failed to parse metadata");
                            }
                        }
                    });
                }
                Err(err) => {
                    warn!(error = %err, "metadata receiver lagged");
                }
            }
        }
    });

    let announce_wire = wire_handle.clone_handle();
    let announce_stats = stats.clone();
    dht.callbacks.on_announce_peer = Some(Arc::new(move |info_hash, ip, port| {
        announce_stats
            .announce_peers
            .fetch_add(1, Ordering::Relaxed);
        let handle = announce_wire.clone_handle();
        tokio::spawn(async move {
            match decode_info_hash_hex(&info_hash) {
                Ok(info_hash_bytes) => {
                    handle.request(&info_hash_bytes, &ip, port).await;
                }
                Err(err) => {
                    warn!(info_hash = %info_hash, error = %err, "invalid announce_peer info_hash");
                }
            }
        });
    }));

    let response_wire = wire_handle.clone_handle();
    let response_stats = stats.clone();
    dht.callbacks.on_get_peers_response = Some(Arc::new(move |info_hash, peer: &Peer| {
        response_stats
            .peer_responses
            .fetch_add(1, Ordering::Relaxed);
        let handle = response_wire.clone_handle();
        let ip = peer.ip.to_string();
        let port = peer.port;
        tokio::spawn(async move {
            match decode_info_hash_hex(&info_hash) {
                Ok(info_hash_bytes) => {
                    handle.request(&info_hash_bytes, &ip, port).await;
                }
                Err(err) => {
                    warn!(info_hash = %info_hash, error = %err, "invalid get_peers info_hash");
                }
            }
        });
    }));

    let node_stats = stats.clone();
    dht.callbacks.on_node = Some(Arc::new(move |_node_id, _ip, _port| {
        node_stats.nodes.fetch_add(1, Ordering::Relaxed);
    }));

    tokio::spawn(async move {
        wire_runner.run().await;
    });

    let _dht_handle = dht.start();
    spawn_stats(stats, config.pipeline.stats_interval);

    info!(
        listen_addr = %dht_config.address,
        max_nodes = dht_config.max_nodes,
        metadata_timeout_secs = config.metadata.timeout.as_secs(),
        metadata_workers = wire_workers,
        metadata_queue_size = wire_queue_size,
        mode = "crawl",
        "adysec dht-spider crawler started"
    );

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
    }
}

fn build_record(
    info_hash_bytes: [u8; 20],
    metadata_info: &[u8],
    config: &AppConfig,
) -> Result<TorrentRecord> {
    let value = bencode::decode(metadata_info).context("invalid metadata bencode")?;
    let BVal::Dict(info) = value else {
        bail!("metadata info is not a dict");
    };

    let name = info
        .get("name.utf-8")
        .or_else(|| info.get("name"))
        .and_then(bytes_value)
        .map(decode_lossy)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| hex::encode(info_hash_bytes));

    let mut files = if let Some(BVal::List(entries)) = info.get("files") {
        parse_multi_files(entries, config)?
    } else {
        let size = int_value(info.get("length")).context("missing single-file length")?;
        vec![TorrentFile {
            path: truncate(name.clone(), config.storage.max_file_path_len),
            size,
        }]
    };

    if files.is_empty() {
        bail!("metadata has no files");
    }

    let file_count = files.len();
    let total_size = files.iter().map(|file| file.size).sum();
    files.truncate(config.storage.max_files_per_torrent);

    let now = now_ts();
    Ok(TorrentRecord {
        info_hash: hex::encode(info_hash_bytes),
        info_hash_bytes,
        name,
        total_size,
        file_count,
        files_stored_count: files.len(),
        files,
        peer_count: 1,
        source: Source::DhtAnnouncePeer,
        hot_score: 1,
        first_seen_at: now,
        last_seen_at: now,
        metadata_fetched_at: now,
    })
}

fn parse_multi_files(entries: &[BVal], config: &AppConfig) -> Result<Vec<TorrentFile>> {
    let mut files = Vec::with_capacity(entries.len());
    for entry in entries {
        let BVal::Dict(file) = entry else {
            continue;
        };
        let Some(size) = int_value(file.get("length")) else {
            continue;
        };
        let path = file
            .get("path.utf-8")
            .or_else(|| file.get("path"))
            .and_then(path_value)
            .unwrap_or_else(|| "unknown".to_string());
        files.push(TorrentFile {
            path: truncate(path, config.storage.max_file_path_len),
            size,
        });
    }
    Ok(files)
}

fn path_value(value: &BVal) -> Option<String> {
    match value {
        BVal::List(parts) => {
            let path = parts
                .iter()
                .filter_map(bytes_value)
                .map(decode_lossy)
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>()
                .join("/");
            if path.is_empty() { None } else { Some(path) }
        }
        BVal::Bytes(bytes) => Some(decode_lossy(bytes)),
        _ => None,
    }
}

fn bytes_value(value: &BVal) -> Option<&[u8]> {
    match value {
        BVal::Bytes(bytes) => Some(bytes),
        _ => None,
    }
}

fn int_value(value: Option<&BVal>) -> Option<u64> {
    match value {
        Some(BVal::Int(value)) if *value >= 0 => Some(*value as u64),
        _ => None,
    }
}

fn decode_info_hash_hex(info_hash: &str) -> Result<[u8; 20]> {
    let bytes = hex::decode(info_hash).context("invalid info_hash hex")?;
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("info_hash is not 20 bytes"))
}

fn decode_lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes)
        .trim_matches(char::from(0))
        .trim()
        .to_string()
}

fn truncate(mut value: String, max_len: usize) -> String {
    if value.len() <= max_len {
        return value;
    }
    while !value.is_char_boundary(max_len) {
        value.pop();
        if value.len() <= max_len {
            return value;
        }
    }
    value.truncate(max_len);
    value
}

#[derive(Default)]
struct CrawlerStats {
    fetched: AtomicU64,
    stored: AtomicU64,
    store_errors: AtomicU64,
    parse_errors: AtomicU64,
    announce_peers: AtomicU64,
    peer_responses: AtomicU64,
    nodes: AtomicU64,
}

fn spawn_stats(stats: Arc<CrawlerStats>, interval: std::time::Duration) {
    tokio::spawn(async move {
        let started_at = Instant::now();
        let mut timer = tokio::time::interval(interval);
        loop {
            timer.tick().await;
            let uptime_secs = started_at.elapsed().as_secs().max(1);
            let fetched = stats.fetched.load(Ordering::Relaxed);
            let speed = fetched as f64 / (uptime_secs as f64 / 60.0);
            info!(
                uptime_secs,
                fetched,
                stored = stats.stored.load(Ordering::Relaxed),
                store_errors = stats.store_errors.load(Ordering::Relaxed),
                parse_errors = stats.parse_errors.load(Ordering::Relaxed),
                announce_peers = stats.announce_peers.load(Ordering::Relaxed),
                peer_responses = stats.peer_responses.load(Ordering::Relaxed),
                nodes = stats.nodes.load(Ordering::Relaxed),
                speed_per_min = speed,
                "crawler stats"
            );
        }
    });
}

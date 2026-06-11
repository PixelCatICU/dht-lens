use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use anyhow::{Context, Result};
use dht_crawler::prelude::{DHTOptions, DHTServer, NetMode, TorrentInfo};
use tracing::{error, info, warn};

use crate::{
    config::AppConfig,
    model::{Source, TorrentFile, TorrentRecord, now_ts},
    storage::LibsqlStore,
};

pub async fn run(config: AppConfig, store: Option<Arc<LibsqlStore>>) -> Result<()> {
    let port = config.dht.listen_addr.port();
    let options = DHTOptions {
        port,
        metadata_timeout: config.metadata.timeout.as_secs(),
        max_metadata_queue_size: config.dht.hash_queue_size,
        max_metadata_worker_count: config.metadata.max_concurrent_fetches,
        netmode: NetMode::Ipv4Only,
        node_queue_capacity: config.dht.routing_table_max_nodes,
        hash_queue_capacity: config.dht.hash_queue_size,
    };

    let server = DHTServer::new(options.clone()).await?;
    let stats = Arc::new(CrawlerStats::default());

    server.on_error(|err| {
        warn!(error = %err, "dht-crawler runtime error");
    });

    let torrent_stats = stats.clone();
    let store_for_callback = store.clone();
    let config_for_callback = config.clone();
    server.on_torrent(move |torrent| {
        torrent_stats.fetched.fetch_add(1, Ordering::Relaxed);
        let stats = torrent_stats.clone();
        let store = store_for_callback.clone();
        let config = config_for_callback.clone();
        tokio::spawn(async move {
            match build_record(torrent, &config) {
                Ok(record) => {
                    if config.pipeline.print_jsonl {
                        println!("{}", record.name);
                    }
                    info!(
                        info_hash = %record.info_hash,
                        name = %record.name,
                        total_size = record.total_size,
                        file_count = record.file_count,
                        "metadata fetched"
                    );

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
                    warn!(error = %err, "failed to build torrent record");
                }
            }
        });
    });

    server.on_metadata_fetch(|_hash| async move { true });

    spawn_stats(stats, config.pipeline.stats_interval);

    info!(
        port = options.port,
        metadata_timeout_secs = options.metadata_timeout,
        metadata_workers = options.max_metadata_worker_count,
        metadata_queue_size = options.max_metadata_queue_size,
        node_queue_capacity = options.node_queue_capacity,
        hash_queue_capacity = options.hash_queue_capacity,
        "dht-crawler started"
    );

    server.start().await?;
    Ok(())
}

fn build_record(torrent: TorrentInfo, config: &AppConfig) -> Result<TorrentRecord> {
    let info_hash = torrent.info_hash.to_ascii_lowercase();
    let bytes = hex::decode(&info_hash).context("invalid info_hash hex from dht-crawler")?;
    let info_hash_bytes: [u8; 20] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("info_hash is not 20 bytes"))?;

    let mut files = torrent
        .files
        .into_iter()
        .map(|file| TorrentFile {
            path: truncate(file.path, config.storage.max_file_path_len),
            size: file.size,
        })
        .collect::<Vec<_>>();
    let file_count = files.len();
    files.truncate(config.storage.max_files_per_torrent);

    let now = now_ts();
    Ok(TorrentRecord {
        info_hash,
        info_hash_bytes,
        name: torrent.name,
        total_size: torrent.total_size,
        file_count,
        files_stored_count: files.len(),
        files,
        peer_count: torrent.peers.len() as u32,
        source: Source::DhtAnnouncePeer,
        hot_score: 1,
        first_seen_at: now,
        last_seen_at: now,
        metadata_fetched_at: now,
    })
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
                speed_per_min = speed,
                "crawler stats"
            );
        }
    });
}

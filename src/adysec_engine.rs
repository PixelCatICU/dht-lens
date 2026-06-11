use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use dht_spider::{
    Config as SpiderConfig, Dht, Mode,
    wire::{WireHandle, WireRunner},
};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::{
    config::AppConfig,
    metadata::parser::parse_info_metadata,
    model::{Source, TorrentRecord, now_ts},
    storage::LibsqlStore,
};

#[derive(Debug, Clone)]
struct MetadataJob {
    info_hash: [u8; 20],
    ip: String,
    port: u16,
    source: Source,
}

pub async fn run_crawl(config: AppConfig, store: Option<Arc<LibsqlStore>>) -> Result<()> {
    let mut spider_cfg = SpiderConfig::default();
    spider_cfg.mode = if config.dht.crawl_mode {
        Mode::Crawl
    } else {
        Mode::Standard
    };
    spider_cfg.address = config.dht.listen_addr.to_string();
    spider_cfg.prime_nodes = config.dht.bootstrap_nodes.clone();
    spider_cfg.max_nodes = config.dht.routing_table_max_nodes;
    spider_cfg.refresh_node_num = config.dht.bootstrap_query_limit.clamp(8, 256);
    spider_cfg.try_times = config.dht.get_peers_probe_depth.clamp(1, 8);
    spider_cfg.check_kbucket_period = Duration::from_secs(10);

    let mut dht = Dht::new(spider_cfg.clone())
        .await
        .with_context(|| format!("failed to bind DHT spider at {}", spider_cfg.address))?;

    let worker_count = config.metadata.max_concurrent_fetches.clamp(64, 8_192);
    let (runner, wire_handle) = WireRunner::new(
        config.storage.max_files_per_torrent.max(65_536),
        config.pipeline.info_hash_queue_size.max(1_024),
        worker_count,
    );
    let (metadata_tx, metadata_rx) =
        mpsc::channel::<MetadataJob>(config.pipeline.info_hash_queue_size.max(1_024));

    tokio::spawn(async move {
        runner.run().await;
    });

    spawn_metadata_responses(wire_handle.clone_handle(), store.clone(), config.clone());
    spawn_pex_forwarder(wire_handle.clone_handle(), metadata_tx.clone());
    spawn_metadata_scheduler(wire_handle.clone_handle(), metadata_rx);
    spawn_stats();

    dht.callbacks.on_get_peers = None;
    dht.callbacks.on_node = Some(Arc::new(|id, ip, port| {
        tracing::debug!(node_id = id, %ip, port, "dht node discovered");
    }));

    let announce_tx = metadata_tx.clone();
    dht.callbacks.on_announce_peer =
        Some(Arc::new(
            move |info_hash_hex, ip, port| match decode_info_hash_hex(&info_hash_hex) {
                Ok(info_hash) => {
                    let tx = announce_tx.clone();
                    tokio::spawn(async move {
                        let _ = tx
                            .send(MetadataJob {
                                info_hash,
                                ip,
                                port,
                                source: Source::DhtAnnouncePeer,
                            })
                            .await;
                    });
                }
                Err(err) => warn!(%info_hash_hex, error = %err, "invalid announced info_hash"),
            },
        ));

    let response_tx = metadata_tx;
    dht.callbacks.on_get_peers_response =
        Some(Arc::new(
            move |info_hash_hex, peer| match decode_info_hash_hex(&info_hash_hex) {
                Ok(info_hash) => {
                    let tx = response_tx.clone();
                    let ip = peer.ip.to_string();
                    let port = peer.port;
                    tokio::spawn(async move {
                        let _ = tx
                            .send(MetadataJob {
                                info_hash,
                                ip,
                                port,
                                source: Source::DhtGetPeers,
                            })
                            .await;
                    });
                }
                Err(err) => warn!(%info_hash_hex, error = %err, "invalid get_peers info_hash"),
            },
        ));

    let _handle = dht.start();
    info!(
        addr = %spider_cfg.address,
        mode = ?spider_cfg.mode,
        max_nodes = spider_cfg.max_nodes,
        refresh_node_num = spider_cfg.refresh_node_num,
        metadata_workers = worker_count,
        metadata_queue_size = config.pipeline.info_hash_queue_size,
        "adysec dht-spider crawler started"
    );

    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}

fn spawn_metadata_scheduler(wire: WireHandle, mut rx: mpsc::Receiver<MetadataJob>) {
    tokio::spawn(async move {
        while let Some(job) = rx.recv().await {
            let source = job.source;
            wire.request(&job.info_hash, &job.ip, job.port).await;
            tracing::debug!(
                info_hash = %hex::encode(job.info_hash),
                ip = %job.ip,
                port = job.port,
                ?source,
                "metadata request queued"
            );
        }
    });
}

fn spawn_metadata_responses(wire: WireHandle, store: Option<Arc<LibsqlStore>>, config: AppConfig) {
    let mut sub = wire.subscribe();
    tokio::spawn(async move {
        while let Ok(resp) = sub.recv().await {
            let info_hash = resp.request.info_hash;
            let metadata = resp.metadata_info.clone();
            let store = store.clone();
            let config = config.clone();
            tokio::spawn(async move {
                match parse_info_metadata(&info_hash, &metadata) {
                    Ok(parsed) => {
                        let mut files = parsed.files;
                        let file_count = files.len();
                        truncate_files(
                            &mut files,
                            config.storage.max_files_per_torrent,
                            config.storage.max_file_path_len,
                        );
                        let now = now_ts();
                        let record = TorrentRecord {
                            info_hash: hex::encode(info_hash),
                            info_hash_bytes: info_hash,
                            name: parsed.name,
                            total_size: parsed.total_size,
                            file_count,
                            files_stored_count: files.len(),
                            files,
                            peer_count: 1,
                            source: Source::DhtAnnouncePeer,
                            hot_score: 1,
                            first_seen_at: now,
                            last_seen_at: now,
                            metadata_fetched_at: now,
                        };

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
                            if let Err(err) = store
                                .insert_torrent(&record, config.search.max_name_ngram_len)
                                .await
                            {
                                error!(info_hash = %record.info_hash, error = %err, "failed to store torrent");
                            }
                        }
                    }
                    Err(err) => {
                        warn!(
                            info_hash = %hex::encode(info_hash),
                            error = %err,
                            "failed to parse metadata"
                        );
                    }
                }
            });
        }
    });
}

fn spawn_pex_forwarder(wire: WireHandle, tx: mpsc::Sender<MetadataJob>) {
    let mut sub = wire.subscribe_peers();
    tokio::spawn(async move {
        while let Ok(peer) = sub.recv().await {
            let _ = tx
                .send(MetadataJob {
                    info_hash: peer.info_hash,
                    ip: peer.ip,
                    port: peer.port,
                    source: Source::DhtGetPeers,
                })
                .await;
        }
    });
}

fn spawn_stats() {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
            info!("adysec crawler alive");
        }
    });
}

fn truncate_files(
    files: &mut Vec<crate::model::TorrentFile>,
    max_files: usize,
    max_path_len: usize,
) {
    files.truncate(max_files);
    for file in files {
        if file.path.len() > max_path_len {
            file.path.truncate(max_path_len);
        }
    }
}

fn decode_info_hash_hex(value: &str) -> Result<[u8; 20]> {
    let bytes = hex::decode(value).context("info_hash is not valid hex")?;
    let bytes: [u8; 20] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("info_hash must be 20 bytes"))?;
    Ok(bytes)
}

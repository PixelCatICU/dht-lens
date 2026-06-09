use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use anyhow::Result;
use tokio::sync::{Semaphore, mpsc};
use tracing::{debug, info, warn};

use crate::{
    config::AppConfig,
    metadata::{fetcher::fetch_from_peer, parser::ParsedMetadata},
    model::{InfoHashEvent, Source, TorrentRecord, now_ts},
    storage::LibsqlStore,
};

pub async fn run_crawl(config: AppConfig, store: Option<Arc<LibsqlStore>>) -> Result<()> {
    let (hash_tx, mut hash_rx) =
        mpsc::channel::<InfoHashEvent>(config.pipeline.info_hash_queue_size);
    let dht_config = config.dht.clone();
    tokio::spawn(async move {
        if let Err(err) = crate::dht::run(dht_config, hash_tx).await {
            warn!(error = %err, "dht task stopped");
        }
    });

    let semaphore = Arc::new(Semaphore::new(config.metadata.max_concurrent_fetches));
    let mut short_seen: HashMap<[u8; 20], i64> = HashMap::new();

    info!(
        result_queue_size = config.pipeline.result_queue_size,
        db_batch_size = config.storage.batch_size,
        db_flush_interval_ms = config.storage.flush_interval.as_millis(),
        "crawler started"
    );
    while let Some(event) = hash_rx.recv().await {
        let now = now_ts();
        if short_seen
            .get(&event.info_hash)
            .is_some_and(|seen_at| now - seen_at < 1_800)
        {
            continue;
        }
        short_seen.insert(event.info_hash, now);
        if short_seen.len() > 500_000 {
            short_seen.retain(|_, seen_at| now - *seen_at < 1_800);
        }

        let permit = semaphore.clone().acquire_owned().await?;
        let config = config.clone();
        let store = store.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(err) = process_hash(event, config, store).await {
                debug!(error = %err, "info_hash dropped");
            }
        });
    }
    Ok(())
}

async fn process_hash(
    mut event: InfoHashEvent,
    config: AppConfig,
    store: Option<Arc<LibsqlStore>>,
) -> Result<()> {
    let peers = crate::dht::get_peers(event.info_hash, &config.dht).await?;
    event.peer_count = peers.len() as u32;
    let metadata = fetch_from_first_peer(&peers, event.info_hash, &config).await?;
    let record = build_record(event, metadata, &config);

    if config.pipeline.print_jsonl {
        println!("{}", serde_json::to_string(&record)?);
    }
    if let Some(store) = store {
        store
            .insert_torrent(&record, config.search.max_name_ngram_len)
            .await?;
    }
    Ok(())
}

async fn fetch_from_first_peer(
    peers: &[SocketAddr],
    info_hash: [u8; 20],
    config: &AppConfig,
) -> Result<ParsedMetadata> {
    for peer in peers {
        match fetch_from_peer(*peer, info_hash, &config.metadata).await {
            Ok(metadata) => return Ok(metadata),
            Err(err) => debug!(%peer, error = %err, "metadata peer failed"),
        }
    }
    anyhow::bail!("no usable peers for metadata")
}

fn build_record(
    event: InfoHashEvent,
    metadata: ParsedMetadata,
    config: &AppConfig,
) -> TorrentRecord {
    let now = now_ts();
    let file_count = metadata.files.len();
    let files: Vec<_> = metadata
        .files
        .into_iter()
        .take(config.storage.max_files_per_torrent)
        .map(|mut file| {
            if file.path.len() > config.storage.max_file_path_len {
                file.path.truncate(config.storage.max_file_path_len);
            }
            file
        })
        .collect();

    TorrentRecord {
        info_hash: hex::encode(event.info_hash),
        info_hash_bytes: event.info_hash,
        name: metadata.name,
        total_size: metadata.total_size,
        file_count,
        files_stored_count: files.len(),
        files,
        peer_count: event.peer_count,
        source: match event.source {
            Source::DhtGetPeers => Source::DhtGetPeers,
            Source::DhtAnnouncePeer => Source::DhtAnnouncePeer,
            Source::ManualMagnet => Source::ManualMagnet,
        },
        hot_score: 1,
        first_seen_at: event.seen_at,
        last_seen_at: now,
        metadata_fetched_at: now,
    }
}

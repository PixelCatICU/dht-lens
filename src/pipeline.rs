use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::Arc,
};

use anyhow::Result;
use rand::seq::SliceRandom;
use tokio::{
    sync::{Semaphore, mpsc},
    task::JoinSet,
};
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
    let mut short_seen: HashMap<[u8; 20], SeenState> = HashMap::new();

    info!(
        result_queue_size = config.pipeline.result_queue_size,
        db_batch_size = config.storage.batch_size,
        db_flush_interval_ms = config.storage.flush_interval.as_millis(),
        "crawler started"
    );
    while let Some(event) = hash_rx.recv().await {
        let now = now_ts();
        if should_skip_event(&mut short_seen, &event, now) {
            continue;
        }
        if event.peer.is_none() {
            continue;
        }
        if short_seen.len() > 500_000 {
            short_seen.retain(|_, state| now - state.last_seen_at < 1_800);
        }

        let permit = semaphore.clone().acquire_owned().await?;
        let config = config.clone();
        let store = store.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let info_hash = hex::encode(event.info_hash);
            if let Err(err) = process_hash(event, config, store).await {
                if err.to_string() == "no usable peers for metadata" {
                    debug!(%info_hash, error = %err, "info_hash dropped");
                } else {
                    warn!(%info_hash, error = %err, "info_hash processing failed");
                }
            }
        });
    }
    Ok(())
}

#[derive(Debug, Default)]
struct SeenState {
    last_seen_at: i64,
    recent_peers: VecDeque<(SocketAddr, i64)>,
}

fn should_skip_event(
    short_seen: &mut HashMap<[u8; 20], SeenState>,
    event: &InfoHashEvent,
    now: i64,
) -> bool {
    let state = short_seen.entry(event.info_hash).or_default();

    let Some(peer) = event.peer else {
        if now - state.last_seen_at < 1_800 {
            return true;
        }
        state.last_seen_at = now;
        return false;
    };

    state.last_seen_at = now;
    state
        .recent_peers
        .retain(|(_, seen_at)| now - *seen_at < 300);
    if state
        .recent_peers
        .iter()
        .any(|(recent_peer, _)| *recent_peer == peer)
    {
        return true;
    }

    state.recent_peers.push_back((peer, now));
    while state.recent_peers.len() > 32 {
        state.recent_peers.pop_front();
    }
    false
}

async fn process_hash(
    mut event: InfoHashEvent,
    config: AppConfig,
    store: Option<Arc<LibsqlStore>>,
) -> Result<()> {
    let info_hash_hex = hex::encode(event.info_hash);
    let Some(primary_peer) = event.peer else {
        debug!(
            info_hash = %info_hash_hex,
            source = ?event.source,
            seed_nodes = event.seed_nodes.len(),
            "info_hash observed without peer; skipping metadata fetch"
        );
        return Ok(());
    };

    let mut peers = vec![primary_peer];
    peers.sort_unstable();
    peers.dedup();
    peers.shuffle(&mut rand::thread_rng());
    event.peer_count = peers.len() as u32;
    debug!(
        info_hash = %info_hash_hex,
        peer_count = event.peer_count,
        source = ?event.source,
        "processing info_hash"
    );
    let metadata = fetch_from_first_peer(&peers, event.info_hash, &config).await?;
    let record = build_record(event, metadata, &config);
    info!(
        info_hash = %record.info_hash,
        name = %record.name,
        total_size = record.total_size,
        file_count = record.file_count,
        "metadata fetched"
    );

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
    let max_attempts = peers.len().min(config.metadata.max_peers_per_hash);
    let mut tasks = JoinSet::new();
    for peer in peers.iter().take(max_attempts).copied() {
        let metadata_config = config.metadata.clone();
        tasks.spawn(async move {
            (
                peer,
                fetch_from_peer(peer, info_hash, &metadata_config).await,
            )
        });
    }

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok((_peer, Ok(metadata))) => {
                tasks.abort_all();
                return Ok(metadata);
            }
            Ok((peer, Err(err))) => debug!(%peer, error = %err, "metadata peer failed"),
            Err(err) => {
                debug!(error = %err, "metadata peer task failed");
            }
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

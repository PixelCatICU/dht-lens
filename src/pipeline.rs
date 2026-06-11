use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use anyhow::Result;
use tokio::{
    sync::{Semaphore, mpsc},
    task::JoinSet,
    time::{Duration, interval},
};
use tracing::{debug, info, warn};

use crate::{
    config::AppConfig,
    metadata::{fetcher::fetch_from_peer_with_pex, parser::ParsedMetadata},
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
    let db_semaphore = Arc::new(Semaphore::new(config.storage.write_concurrency));
    let stats = Arc::new(PipelineStats::default());
    tokio::spawn(pipeline_stats_loop(stats.clone()));
    let mut short_seen: HashMap<[u8; 20], SeenState> = HashMap::new();
    let mut pending: HashMap<[u8; 20], PendingFetch> = HashMap::new();
    let mut flush_tick = interval(Duration::from_millis(100));

    info!(
        result_queue_size = config.pipeline.result_queue_size,
        db_batch_size = config.storage.batch_size,
        db_flush_interval_ms = config.storage.flush_interval.as_millis(),
        db_write_concurrency = config.storage.write_concurrency,
        peer_collect_window_ms = config.pipeline.peer_collect_window.as_millis(),
        "crawler started"
    );
    loop {
        tokio::select! {
            event = hash_rx.recv() => {
                let Some(event) = event else {
                    break;
                };
                stats.events_received.fetch_add(1, Ordering::Relaxed);
                if event.peer.is_some() {
                    stats.peer_events.fetch_add(1, Ordering::Relaxed);
                }
                let now = now_ts();
                if should_skip_event(&mut short_seen, &event, now) {
                    stats.events_skipped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                if event.peer.is_none() {
                    continue;
                }
                if short_seen.len() > 500_000 {
                    short_seen.retain(|_, state| now - state.last_seen_at < 1_800);
                }

                let peers = recent_peers_for(
                    &short_seen,
                    event.info_hash,
                    config.metadata.max_peers_per_hash,
                );
                if let Some(info_hash) =
                    add_pending_peers(&mut pending, event, peers, config.metadata.max_peers_per_hash)
                {
                    flush_pending_hash(
                        &mut pending,
                        info_hash,
                        &semaphore,
                        &db_semaphore,
                        &stats,
                        &config,
                        &store,
                    )
                    .await?;
                }
            }
            _ = flush_tick.tick() => {
                flush_ready_pending(&mut pending, &semaphore, &db_semaphore, &stats, &config, &store).await?;
            }
        }
    }
    flush_all_pending(
        &mut pending,
        &semaphore,
        &db_semaphore,
        &stats,
        &config,
        &store,
    )
    .await?;
    Ok(())
}

#[derive(Debug, Default)]
struct PipelineStats {
    events_received: AtomicU64,
    events_skipped: AtomicU64,
    peer_events: AtomicU64,
    fetch_jobs_started: AtomicU64,
    peer_fetch_attempts: AtomicU64,
    peer_fetch_failures: AtomicU64,
    peer_fetch_connect_failures: AtomicU64,
    peer_fetch_extension_failures: AtomicU64,
    peer_fetch_timeout_failures: AtomicU64,
    peer_fetch_hash_failures: AtomicU64,
    peer_fetch_other_failures: AtomicU64,
    metadata_success: AtomicU64,
    metadata_failures: AtomicU64,
    no_usable_peers: AtomicU64,
    db_success: AtomicU64,
    db_failures: AtomicU64,
}

async fn pipeline_stats_loop(stats: Arc<PipelineStats>) {
    let mut ticker = interval(Duration::from_secs(60));
    loop {
        ticker.tick().await;
        info!(
            events_received = stats.events_received.swap(0, Ordering::Relaxed),
            events_skipped = stats.events_skipped.swap(0, Ordering::Relaxed),
            peer_events = stats.peer_events.swap(0, Ordering::Relaxed),
            fetch_jobs_started = stats.fetch_jobs_started.swap(0, Ordering::Relaxed),
            peer_fetch_attempts = stats.peer_fetch_attempts.swap(0, Ordering::Relaxed),
            peer_fetch_failures = stats.peer_fetch_failures.swap(0, Ordering::Relaxed),
            peer_fetch_connect_failures =
                stats.peer_fetch_connect_failures.swap(0, Ordering::Relaxed),
            peer_fetch_extension_failures = stats
                .peer_fetch_extension_failures
                .swap(0, Ordering::Relaxed),
            peer_fetch_timeout_failures =
                stats.peer_fetch_timeout_failures.swap(0, Ordering::Relaxed),
            peer_fetch_hash_failures = stats.peer_fetch_hash_failures.swap(0, Ordering::Relaxed),
            peer_fetch_other_failures = stats.peer_fetch_other_failures.swap(0, Ordering::Relaxed),
            metadata_success = stats.metadata_success.swap(0, Ordering::Relaxed),
            metadata_failures = stats.metadata_failures.swap(0, Ordering::Relaxed),
            no_usable_peers = stats.no_usable_peers.swap(0, Ordering::Relaxed),
            db_success = stats.db_success.swap(0, Ordering::Relaxed),
            db_failures = stats.db_failures.swap(0, Ordering::Relaxed),
            "pipeline stats"
        );
    }
}

#[derive(Debug, Default)]
struct SeenState {
    last_seen_at: i64,
    recent_peers: VecDeque<(SocketAddr, i64)>,
}

#[derive(Debug)]
struct PendingFetch {
    event: InfoHashEvent,
    peers: Vec<SocketAddr>,
    created_at: Instant,
}

fn add_pending_peers(
    pending: &mut HashMap<[u8; 20], PendingFetch>,
    mut event: InfoHashEvent,
    peers: Vec<SocketAddr>,
    max_peers_per_hash: usize,
) -> Option<[u8; 20]> {
    let current_peer = event.peer;
    let current_source = event.source;
    let entry = pending.entry(event.info_hash).or_insert_with(|| {
        event.peer_count = 0;
        PendingFetch {
            event,
            peers: Vec::new(),
            created_at: Instant::now(),
        }
    });

    if matches!(current_source, Source::DhtAnnouncePeer)
        && let Some(peer) = current_peer
    {
        push_peer_front(&mut entry.peers, peer, max_peers_per_hash);
    }

    for peer in peers {
        push_peer_back(&mut entry.peers, peer, max_peers_per_hash);
    }

    (entry.peers.len() >= max_peers_per_hash).then_some(entry.event.info_hash)
}

fn push_peer_front(peers: &mut Vec<SocketAddr>, peer: SocketAddr, limit: usize) {
    peers.retain(|candidate| *candidate != peer);
    peers.insert(0, peer);
    peers.truncate(limit);
}

fn push_peer_back(peers: &mut Vec<SocketAddr>, peer: SocketAddr, limit: usize) {
    if peers.len() >= limit || peers.contains(&peer) {
        return;
    }
    peers.push(peer);
}

async fn flush_ready_pending(
    pending: &mut HashMap<[u8; 20], PendingFetch>,
    semaphore: &Arc<Semaphore>,
    db_semaphore: &Arc<Semaphore>,
    stats: &Arc<PipelineStats>,
    config: &AppConfig,
    store: &Option<Arc<LibsqlStore>>,
) -> Result<()> {
    let ready: Vec<_> = pending
        .iter()
        .filter_map(|(info_hash, fetch)| {
            (fetch.created_at.elapsed() >= config.pipeline.peer_collect_window)
                .then_some(*info_hash)
        })
        .collect();

    for info_hash in ready {
        flush_pending_hash(
            pending,
            info_hash,
            semaphore,
            db_semaphore,
            stats,
            config,
            store,
        )
        .await?;
    }
    Ok(())
}

async fn flush_all_pending(
    pending: &mut HashMap<[u8; 20], PendingFetch>,
    semaphore: &Arc<Semaphore>,
    db_semaphore: &Arc<Semaphore>,
    stats: &Arc<PipelineStats>,
    config: &AppConfig,
    store: &Option<Arc<LibsqlStore>>,
) -> Result<()> {
    let hashes: Vec<_> = pending.keys().copied().collect();
    for info_hash in hashes {
        flush_pending_hash(
            pending,
            info_hash,
            semaphore,
            db_semaphore,
            stats,
            config,
            store,
        )
        .await?;
    }
    Ok(())
}

async fn flush_pending_hash(
    pending: &mut HashMap<[u8; 20], PendingFetch>,
    info_hash: [u8; 20],
    semaphore: &Arc<Semaphore>,
    db_semaphore: &Arc<Semaphore>,
    stats: &Arc<PipelineStats>,
    config: &AppConfig,
    store: &Option<Arc<LibsqlStore>>,
) -> Result<()> {
    let Some(fetch) = pending.remove(&info_hash) else {
        return Ok(());
    };
    if fetch.peers.is_empty() {
        return Ok(());
    }

    let permit = semaphore.clone().acquire_owned().await?;
    let db_semaphore = db_semaphore.clone();
    let stats = stats.clone();
    let config = config.clone();
    let store = store.clone();
    tokio::spawn(async move {
        stats.fetch_jobs_started.fetch_add(1, Ordering::Relaxed);
        let info_hash = hex::encode(fetch.event.info_hash);
        let result = fetch_record(fetch.event, fetch.peers, &config, &stats).await;
        drop(permit);

        let record = match result {
            Ok(record) => {
                stats.metadata_success.fetch_add(1, Ordering::Relaxed);
                record
            }
            Err(err) if err.to_string() == "no usable peers for metadata" => {
                stats.no_usable_peers.fetch_add(1, Ordering::Relaxed);
                debug!(%info_hash, error = %err, "info_hash dropped");
                return;
            }
            Err(err) => {
                stats.metadata_failures.fetch_add(1, Ordering::Relaxed);
                warn!(%info_hash, error = %err, "info_hash processing failed");
                return;
            }
        };

        log_record(&record, &config);
        if let Some(store) = store {
            match db_semaphore.acquire_owned().await {
                Ok(_db_permit) => {
                    if let Err(err) = store
                        .insert_torrent(&record, config.search.max_name_ngram_len)
                        .await
                    {
                        stats.db_failures.fetch_add(1, Ordering::Relaxed);
                        warn!(info_hash = %record.info_hash, error = %err, "torrent storage failed");
                    } else {
                        stats.db_success.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(err) => {
                    stats.db_failures.fetch_add(1, Ordering::Relaxed);
                    warn!(info_hash = %record.info_hash, error = %err, "db semaphore closed")
                }
            }
        }
    });
    Ok(())
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

fn recent_peers_for(
    short_seen: &HashMap<[u8; 20], SeenState>,
    info_hash: [u8; 20],
    limit: usize,
) -> Vec<SocketAddr> {
    short_seen
        .get(&info_hash)
        .map(|state| {
            state
                .recent_peers
                .iter()
                .rev()
                .take(limit)
                .map(|(peer, _)| *peer)
                .collect()
        })
        .unwrap_or_default()
}

async fn fetch_record(
    mut event: InfoHashEvent,
    mut peers: Vec<SocketAddr>,
    config: &AppConfig,
    stats: &PipelineStats,
) -> Result<TorrentRecord> {
    let info_hash_hex = hex::encode(event.info_hash);
    if peers.is_empty() {
        debug!(
            info_hash = %info_hash_hex,
            source = ?event.source,
            seed_nodes = event.seed_nodes.len(),
            "info_hash observed without peer; skipping metadata fetch"
        );
        anyhow::bail!("no usable peers for metadata");
    }

    let mut seen = HashSet::with_capacity(peers.len());
    peers.retain(|peer| seen.insert(*peer));
    event.peer_count = peers.len() as u32;
    debug!(
        info_hash = %info_hash_hex,
        peer_count = event.peer_count,
        source = ?event.source,
        "processing info_hash"
    );
    let metadata = fetch_from_first_peer(&peers, event.info_hash, config, stats).await?;
    Ok(build_record(event, metadata, config))
}

fn log_record(record: &TorrentRecord, config: &AppConfig) {
    info!(
        name = %record.name,
        "metadata fetched"
    );

    if config.pipeline.print_jsonl {
        println!("{}", record.name);
    }
}

async fn fetch_from_first_peer(
    peers: &[SocketAddr],
    info_hash: [u8; 20],
    config: &AppConfig,
    stats: &PipelineStats,
) -> Result<ParsedMetadata> {
    let max_attempts = peers.len().min(config.metadata.max_peers_per_hash);
    let mut tasks = JoinSet::new();
    let (pex_tx, mut pex_rx) = mpsc::unbounded_channel::<Vec<SocketAddr>>();
    let mut spawned_peers = Vec::with_capacity(config.metadata.max_peers_per_hash);
    let mut pex_open = true;

    for peer in peers.iter().take(max_attempts).copied() {
        let metadata_config = config.metadata.clone();
        let pex_tx = pex_tx.clone();
        spawned_peers.push(peer);
        stats.peer_fetch_attempts.fetch_add(1, Ordering::Relaxed);
        tasks.spawn(async move {
            (
                peer,
                fetch_from_peer_with_pex(peer, info_hash, &metadata_config, pex_tx).await,
            )
        });
    }
    drop(pex_tx);

    loop {
        tokio::select! {
            result = tasks.join_next(), if !tasks.is_empty() => {
                match result {
                    Some(Ok((_peer, Ok(outcome)))) => {
                        tasks.abort_all();
                        return Ok(outcome.metadata);
                    }
                    Some(Ok((peer, Err(err)))) => {
                        stats.record_peer_fetch_failure(&err);
                        debug!(%peer, error = %err, "metadata peer failed");
                    }
                    Some(Err(err)) => {
                        stats.peer_fetch_failures.fetch_add(1, Ordering::Relaxed);
                        stats.peer_fetch_other_failures.fetch_add(1, Ordering::Relaxed);
                        debug!(error = %err, "metadata peer task failed");
                    }
                    None => {}
                }
            }
            pex_peers = pex_rx.recv(), if pex_open && spawned_peers.len() < config.metadata.max_peers_per_hash => {
                let Some(pex_peers) = pex_peers else {
                    pex_open = false;
                    continue;
                };
                for peer in pex_peers {
                    if spawned_peers.len() >= config.metadata.max_peers_per_hash {
                        break;
                    }
                    if spawned_peers.contains(&peer) {
                        continue;
                    }
                    spawned_peers.push(peer);
                    let metadata_config = config.metadata.clone();
                    let (peer_pex_tx, _) = mpsc::unbounded_channel::<Vec<SocketAddr>>();
                    stats.peer_fetch_attempts.fetch_add(1, Ordering::Relaxed);
                    tasks.spawn(async move {
                        (
                            peer,
                            fetch_from_peer_with_pex(peer, info_hash, &metadata_config, peer_pex_tx).await,
                        )
                    });
                }
            }
            else => break,
        }
    }
    anyhow::bail!("no usable peers for metadata")
}

impl PipelineStats {
    fn record_peer_fetch_failure(&self, err: &anyhow::Error) {
        self.peer_fetch_failures.fetch_add(1, Ordering::Relaxed);
        let message = err.to_string();
        if message.contains("connect timeout")
            || message.contains("connection refused")
            || message.contains("connection reset")
            || message.contains("network unreachable")
            || message.contains("host unreachable")
            || message.contains("No route to host")
            || message.contains("timed out")
        {
            self.peer_fetch_connect_failures
                .fetch_add(1, Ordering::Relaxed);
        } else if message.contains("extension protocol")
            || message.contains("ut_metadata")
            || message.contains("missing msg_type")
            || message.contains("invalid msg_type")
        {
            self.peer_fetch_extension_failures
                .fetch_add(1, Ordering::Relaxed);
        } else if message.contains("metadata fetch timeout") {
            self.peer_fetch_timeout_failures
                .fetch_add(1, Ordering::Relaxed);
        } else if message.contains("sha1 mismatch") || message.contains("info_hash mismatch") {
            self.peer_fetch_hash_failures
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.peer_fetch_other_failures
                .fetch_add(1, Ordering::Relaxed);
        }
    }
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

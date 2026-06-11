use std::{
    collections::{BTreeMap, VecDeque},
    hash::{Hash, Hasher},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::Result;
use rand::{Rng, RngCore};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{
    net::UdpSocket,
    sync::mpsc,
    time::{Duration, interval},
};
use tracing::{debug, info, warn};

use crate::{
    bencode::{Value, as_bytes, as_int, dict_get, encode, parse},
    config::DhtConfig,
    model::{InfoHashEvent, Source, now_ts},
};

#[derive(Debug)]
struct NodeTable {
    shards: Vec<Mutex<VecDeque<SocketAddr>>>,
    max_nodes: usize,
    max_nodes_per_shard: usize,
}

impl NodeTable {
    fn new(max_nodes: usize, shard_count: usize) -> Self {
        let shard_count = shard_count.max(1);
        let max_nodes_per_shard = (max_nodes / shard_count).max(1);
        Self {
            shards: (0..shard_count)
                .map(|_| Mutex::new(VecDeque::new()))
                .collect(),
            max_nodes,
            max_nodes_per_shard,
        }
    }

    fn add(&self, addr: SocketAddr) {
        if addr.port() == 0 || addr.ip().is_unspecified() {
            return;
        }

        let mut nodes = self.shards[self.shard_index(&addr)]
            .lock()
            .expect("node table mutex poisoned");
        if let Some(index) = nodes.iter().position(|node| *node == addr) {
            nodes.remove(index);
            nodes.push_back(addr);
            return;
        }
        nodes.push_back(addr);
        while nodes.len() > self.max_nodes_per_shard {
            nodes.pop_front();
        }
    }

    fn add_many(&self, addrs: impl IntoIterator<Item = SocketAddr>) {
        for addr in addrs {
            self.add(addr);
        }
    }

    fn sample(&self, family_addr: SocketAddr, limit: usize) -> Vec<SocketAddr> {
        let mut family_nodes = Vec::with_capacity(limit.saturating_mul(2).max(64));
        let start = rand::thread_rng().gen_range(0..self.shards.len());
        for shard in self
            .shards
            .iter()
            .cycle()
            .skip(start)
            .take(self.shards.len())
        {
            let nodes = shard.lock().expect("node table mutex poisoned");
            family_nodes.extend(
                nodes
                    .iter()
                    .filter(|addr| addr.is_ipv4() == family_addr.is_ipv4())
                    .copied(),
            );
            if family_nodes.len() >= limit.saturating_mul(4).max(limit) {
                break;
            }
        }
        if family_nodes.len() <= limit {
            return family_nodes;
        }

        let start = rand::thread_rng().gen_range(0..family_nodes.len());
        family_nodes
            .iter()
            .cycle()
            .skip(start)
            .take(limit)
            .filter(|addr| addr.is_ipv4() == family_addr.is_ipv4())
            .copied()
            .collect()
    }

    fn compact_nodes(&self, family_addr: SocketAddr, limit: usize) -> Vec<u8> {
        let mut out = Vec::new();
        for addr in self.sample(family_addr, limit) {
            if let SocketAddr::V4(addr) = addr {
                out.extend_from_slice(&random_id());
                out.extend_from_slice(&addr.ip().octets());
                out.extend_from_slice(&addr.port().to_be_bytes());
            }
        }
        out
    }

    fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.lock().expect("node table mutex poisoned").len())
            .sum::<usize>()
            .min(self.max_nodes)
    }

    fn shard_index(&self, addr: &SocketAddr) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        addr.hash(&mut hasher);
        (hasher.finish() as usize) % self.shards.len()
    }
}

#[derive(Debug)]
struct DhtPacket {
    addr: SocketAddr,
    bytes: Vec<u8>,
}

#[derive(Debug, Default)]
struct DhtStats {
    packets_received: AtomicU64,
    packets_dropped: AtomicU64,
    get_peers_queries: AtomicU64,
    announce_peer_queries: AtomicU64,
    peer_events: AtomicU64,
    hash_events: AtomicU64,
    events_dropped: AtomicU64,
    active_probes_sent: AtomicU64,
    active_probe_peers: AtomicU64,
}

pub async fn run(config: DhtConfig, tx: mpsc::Sender<InfoHashEvent>) -> Result<()> {
    let mut tasks = Vec::new();
    let nodes = Arc::new(NodeTable::new(
        config.routing_table_max_nodes,
        config.node_shards,
    ));

    for bootstrap in &config.bootstrap_nodes {
        nodes.add_many(resolve_addrs(bootstrap).await);
    }

    let v4_config = config.clone();
    let v4_tx = tx.clone();
    let v4_nodes = nodes.clone();
    let v4_virtual_node_count = config.virtual_nodes;
    let v4_bootstrap_query_limit = config.bootstrap_query_limit;
    let v4_get_peers_probe_count = config.get_peers_probe_count;
    let v4_get_peers_probe_depth = config.get_peers_probe_depth;
    let v4_packet_workers = config.packet_workers;
    let v4_packet_queue_size = config.packet_queue_size;
    let v4_crawl_mode = config.crawl_mode;
    let v4_crawl_response_nodes = config.crawl_response_nodes;
    tasks.push(tokio::spawn(async move {
        if let Err(err) = run_listener(
            v4_config.listen_addr,
            v4_config.bootstrap_nodes,
            v4_virtual_node_count,
            v4_bootstrap_query_limit,
            v4_get_peers_probe_count,
            v4_get_peers_probe_depth,
            v4_packet_workers,
            v4_packet_queue_size,
            v4_crawl_mode,
            v4_crawl_response_nodes,
            v4_nodes,
            v4_tx,
        )
        .await
        {
            warn!(addr = %v4_config.listen_addr, error = %err, "dht listener stopped");
        }
    }));

    if let Some(listen_addr_v6) = config.listen_addr_v6 {
        let v6_tx = tx.clone();
        let bootstrap_nodes = config.bootstrap_nodes.clone();
        let v6_nodes = nodes.clone();
        let v6_virtual_node_count = config.virtual_nodes;
        let v6_bootstrap_query_limit = config.bootstrap_query_limit;
        let v6_get_peers_probe_count = config.get_peers_probe_count;
        let v6_get_peers_probe_depth = config.get_peers_probe_depth;
        let v6_packet_workers = config.packet_workers;
        let v6_packet_queue_size = config.packet_queue_size;
        let v6_crawl_mode = config.crawl_mode;
        let v6_crawl_response_nodes = config.crawl_response_nodes;
        tasks.push(tokio::spawn(async move {
            if let Err(err) = run_listener(
                listen_addr_v6,
                bootstrap_nodes,
                v6_virtual_node_count,
                v6_bootstrap_query_limit,
                v6_get_peers_probe_count,
                v6_get_peers_probe_depth,
                v6_packet_workers,
                v6_packet_queue_size,
                v6_crawl_mode,
                v6_crawl_response_nodes,
                v6_nodes,
                v6_tx,
            )
            .await
            {
                warn!(addr = %listen_addr_v6, error = %err, "dht listener stopped");
            }
        }));
    }

    for task in tasks {
        let _ = task.await;
    }

    Ok(())
}

async fn run_listener(
    listen_addr: SocketAddr,
    bootstrap_nodes: Vec<String>,
    virtual_node_count: usize,
    bootstrap_query_limit: usize,
    get_peers_probe_count: usize,
    get_peers_probe_depth: u8,
    packet_workers: usize,
    packet_queue_size: usize,
    crawl_mode: bool,
    crawl_response_nodes: usize,
    nodes: Arc<NodeTable>,
    tx: mpsc::Sender<InfoHashEvent>,
) -> Result<()> {
    let socket = Arc::new(bind_udp_socket(listen_addr)?);
    let stats = Arc::new(DhtStats::default());
    let node_ids: Arc<[[u8; 20]]> = (0..virtual_node_count)
        .map(|_| random_id())
        .collect::<Vec<_>>()
        .into();

    info!(
        addr = %listen_addr,
        virtual_nodes = node_ids.len(),
        packet_workers,
        packet_queue_size,
        crawl_mode,
        crawl_response_nodes,
        "dht listener bound"
    );
    tokio::spawn(bootstrap_loop(
        socket.clone(),
        node_ids.clone(),
        bootstrap_nodes,
        bootstrap_query_limit,
        nodes.clone(),
    ));
    tokio::spawn(stats_loop(listen_addr, nodes.clone(), stats.clone()));

    let mut packet_senders = Vec::with_capacity(packet_workers);
    for worker_id in 0..packet_workers {
        let (packet_tx, mut packet_rx) = mpsc::channel::<DhtPacket>(packet_queue_size);
        packet_senders.push(packet_tx);
        let worker_socket = socket.clone();
        let worker_node_ids = node_ids.clone();
        let worker_nodes = nodes.clone();
        let worker_tx = tx.clone();
        let worker_stats = stats.clone();
        tokio::spawn(async move {
            while let Some(packet) = packet_rx.recv().await {
                if let Err(err) = handle_packet(
                    worker_socket.clone(),
                    worker_node_ids.clone(),
                    worker_nodes.clone(),
                    packet.addr,
                    &packet.bytes,
                    get_peers_probe_count,
                    get_peers_probe_depth,
                    crawl_mode,
                    crawl_response_nodes,
                    worker_stats.clone(),
                    &worker_tx,
                )
                .await
                {
                    debug!(worker_id, addr = %packet.addr, error = %err, "ignored dht packet");
                }
            }
        });
    }

    let mut buf = vec![0u8; 4096];
    let mut worker_index = 0usize;
    let mut dropped_packets = 0u64;
    loop {
        let (len, addr) = socket.recv_from(&mut buf).await?;
        stats.packets_received.fetch_add(1, Ordering::Relaxed);
        nodes.add(addr);
        let packet = DhtPacket {
            addr,
            bytes: buf[..len].to_vec(),
        };
        let target = worker_index % packet_senders.len();
        worker_index = worker_index.wrapping_add(1);
        if packet_senders[target].try_send(packet).is_err() {
            dropped_packets = dropped_packets.wrapping_add(1);
            stats.packets_dropped.fetch_add(1, Ordering::Relaxed);
            if dropped_packets % 10_000 == 0 {
                warn!(
                    addr = %listen_addr,
                    dropped_packets,
                    "dht packet worker queues full; dropping packets"
                );
            }
        }
    }
}

async fn stats_loop(listen_addr: SocketAddr, nodes: Arc<NodeTable>, stats: Arc<DhtStats>) {
    let mut ticker = interval(Duration::from_secs(60));
    loop {
        ticker.tick().await;
        info!(
            local_addr = %listen_addr,
            known_nodes = nodes.len(),
            packets_received = stats.packets_received.swap(0, Ordering::Relaxed),
            packets_dropped = stats.packets_dropped.swap(0, Ordering::Relaxed),
            get_peers_queries = stats.get_peers_queries.swap(0, Ordering::Relaxed),
            announce_peer_queries = stats.announce_peer_queries.swap(0, Ordering::Relaxed),
            hash_events = stats.hash_events.swap(0, Ordering::Relaxed),
            peer_events = stats.peer_events.swap(0, Ordering::Relaxed),
            events_dropped = stats.events_dropped.swap(0, Ordering::Relaxed),
            active_probes_sent = stats.active_probes_sent.swap(0, Ordering::Relaxed),
            active_probe_peers = stats.active_probe_peers.swap(0, Ordering::Relaxed),
            "dht stats"
        );
    }
}

fn bind_udp_socket(addr: SocketAddr) -> Result<UdpSocket> {
    if addr.is_ipv6() {
        let socket = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))?;
        socket.set_only_v6(true)?;
        socket.set_reuse_address(true)?;
        socket.bind(&addr.into())?;
        socket.set_nonblocking(true)?;
        Ok(UdpSocket::from_std(socket.into())?)
    } else {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        socket.set_reuse_address(true)?;
        socket.bind(&addr.into())?;
        socket.set_nonblocking(true)?;
        Ok(UdpSocket::from_std(socket.into())?)
    }
}

fn event_seed_nodes(nodes: &NodeTable, addr: SocketAddr) -> Vec<SocketAddr> {
    let mut seed_nodes = nodes.sample(addr, 256);
    if is_global_address(&addr) {
        seed_nodes.push(addr);
    }
    unique(
        seed_nodes
            .into_iter()
            .filter(|node| node.is_ipv4() && is_global_address(node))
            .collect(),
    )
}

async fn bootstrap_loop(
    socket: Arc<UdpSocket>,
    node_ids: Arc<[[u8; 20]]>,
    bootstrap_nodes: Vec<String>,
    bootstrap_query_limit: usize,
    nodes: Arc<NodeTable>,
) {
    let mut ticker = interval(Duration::from_secs(5));
    let local_addr = match socket.local_addr() {
        Ok(addr) => addr,
        Err(err) => {
            warn!(error = %err, "failed to read dht socket local addr");
            return;
        }
    };

    let mut round = 0u64;
    let mut node_index = 0usize;
    loop {
        round += 1;
        ticker.tick().await;
        let mut targets = nodes.sample(local_addr, bootstrap_query_limit);
        for node in &bootstrap_nodes {
            targets.extend(resolve_addrs(node).await);
        }
        targets = unique(targets);
        let target_count = targets.len();

        for addr in targets {
            if addr.is_ipv4() != local_addr.is_ipv4() {
                continue;
            }

            let target = random_id();
            let node_id = node_ids[node_index % node_ids.len()];
            node_index = node_index.wrapping_add(1);
            let request = find_node_request(&node_id, &target);
            if let Err(err) = socket.send_to(&request, addr).await {
                if err.raw_os_error() == Some(101) {
                    warn!(
                        local_addr = %local_addr,
                        node = %addr,
                        error = %err,
                        "network unreachable; disabling dht bootstrap for this listener"
                    );
                    return;
                }

                debug!(
                    local_addr = %local_addr,
                    node = %addr,
                    error = %err,
                    "failed to send dht bootstrap request"
                );
            }
        }

        if round % 4 == 0 {
            info!(
                local_addr = %local_addr,
                known_nodes = nodes.len(),
                virtual_nodes = node_ids.len(),
                target_count,
                "dht bootstrap round complete"
            );
        } else {
            debug!(
                local_addr = %local_addr,
                known_nodes = nodes.len(),
                virtual_nodes = node_ids.len(),
                target_count,
                "dht bootstrap round complete"
            );
        }
    }
}

async fn handle_packet(
    socket: Arc<UdpSocket>,
    node_ids: Arc<[[u8; 20]]>,
    nodes: Arc<NodeTable>,
    addr: SocketAddr,
    packet: &[u8],
    get_peers_probe_count: usize,
    get_peers_probe_depth: u8,
    crawl_mode: bool,
    crawl_response_nodes: usize,
    stats: Arc<DhtStats>,
    tx: &mpsc::Sender<InfoHashEvent>,
) -> Result<()> {
    let value = parse(packet)?;
    let Value::Dict(dict) = value else {
        return Ok(());
    };

    let transaction = dict_get(&dict, b"t").and_then(as_bytes).unwrap_or(b"");
    let y = dict_get(&dict, b"y").and_then(as_bytes).unwrap_or(b"");
    if y == b"r" {
        if let Some(Value::Dict(response)) = dict_get(&dict, b"r") {
            let mut response_nodes = Vec::new();
            if let Some(bytes) = dict_get(response, b"nodes").and_then(as_bytes) {
                response_nodes.extend(parse_compact_nodes(bytes));
            }
            if let Some(bytes) = dict_get(response, b"nodes6").and_then(as_bytes) {
                response_nodes.extend(parse_compact_nodes6(bytes));
            }
            nodes.add_many(response_nodes.iter().copied());
            if let Some((hash, remaining_depth)) = active_get_peers_hash(transaction) {
                for peer in parse_compact_peer_values(response) {
                    stats.active_probe_peers.fetch_add(1, Ordering::Relaxed);
                    emit_event(
                        tx,
                        InfoHashEvent {
                            info_hash: hash,
                            source: Source::DhtGetPeers,
                            peer_count: 1,
                            peer: Some(peer),
                            seed_nodes: Vec::new(),
                            seen_at: now_ts(),
                        },
                        &stats,
                    );
                }
                if remaining_depth > 0 && get_peers_probe_count > 0 {
                    probe_get_peers_targets(
                        socket.clone(),
                        node_ids.clone(),
                        response_nodes,
                        addr,
                        hash,
                        get_peers_probe_count,
                        remaining_depth - 1,
                        stats.clone(),
                    )
                    .await;
                }
            }
        }
        return Ok(());
    }

    if y != b"q" {
        return Ok(());
    }

    let query = dict_get(&dict, b"q").and_then(as_bytes).unwrap_or(b"");
    let args = match dict_get(&dict, b"a") {
        Some(Value::Dict(args)) => args,
        _ => return Ok(()),
    };

    match query {
        b"ping" => {
            let node_id = node_ids[0];
            let response = response(transaction, node_id, BTreeMap::new());
            socket.send_to(&response, addr).await?;
        }
        b"find_node" => {
            let node_id = dict_get(args, b"target")
                .and_then(as_bytes)
                .and_then(to_hash)
                .map(|target| closest_node_id(&node_ids, &target))
                .unwrap_or(node_ids[0]);
            let mut extra = BTreeMap::new();
            extra.insert(
                b"nodes".to_vec(),
                Value::Bytes(nodes.compact_nodes(addr, 8)),
            );
            let response = response(transaction, node_id, extra);
            socket.send_to(&response, addr).await?;
        }
        b"get_peers" => {
            stats.get_peers_queries.fetch_add(1, Ordering::Relaxed);
            if let Some(hash) = dict_get(args, b"info_hash")
                .and_then(as_bytes)
                .and_then(to_hash)
            {
                debug!(info_hash = %hex::encode(hash), source = "get_peers", "discovered info_hash");
                emit_event(
                    tx,
                    InfoHashEvent {
                        info_hash: hash,
                        source: Source::DhtGetPeers,
                        peer_count: 0,
                        peer: None,
                        seed_nodes: event_seed_nodes(&nodes, addr),
                        seen_at: now_ts(),
                    },
                    &stats,
                );
                if get_peers_probe_count > 0 {
                    probe_get_peers(
                        socket.clone(),
                        node_ids.clone(),
                        nodes.clone(),
                        addr,
                        hash,
                        get_peers_probe_count,
                        get_peers_probe_depth,
                        stats.clone(),
                    )
                    .await;
                }
            }
            let node_id = dict_get(args, b"info_hash")
                .and_then(as_bytes)
                .and_then(to_hash)
                .map(|hash| closest_node_id(&node_ids, &hash))
                .unwrap_or(node_ids[0]);
            let mut extra = BTreeMap::new();
            extra.insert(b"token".to_vec(), Value::Bytes(b"dht-lens".to_vec()));
            let node_limit = if crawl_mode {
                crawl_response_nodes
            } else {
                crawl_response_nodes.max(8)
            };
            extra.insert(
                b"nodes".to_vec(),
                Value::Bytes(nodes.compact_nodes(addr, node_limit)),
            );
            let response = response(transaction, node_id, extra);
            socket.send_to(&response, addr).await?;
        }
        b"announce_peer" => {
            stats.announce_peer_queries.fetch_add(1, Ordering::Relaxed);
            if let Some(hash) = dict_get(args, b"info_hash")
                .and_then(as_bytes)
                .and_then(to_hash)
            {
                let node_id = closest_node_id(&node_ids, &hash);
                let peer = announce_peer_addr(args, addr);
                debug!(info_hash = %hex::encode(hash), source = "announce_peer", ?peer, "discovered info_hash");
                emit_event(
                    tx,
                    InfoHashEvent {
                        info_hash: hash,
                        source: Source::DhtAnnouncePeer,
                        peer_count: peer.is_some() as u32,
                        peer,
                        seed_nodes: event_seed_nodes(&nodes, addr),
                        seen_at: now_ts(),
                    },
                    &stats,
                );
                let response = response(transaction, node_id, BTreeMap::new());
                socket.send_to(&response, addr).await?;
                return Ok(());
            }
            let node_id = node_ids[0];
            let response = response(transaction, node_id, BTreeMap::new());
            socket.send_to(&response, addr).await?;
        }
        _ => {}
    }
    Ok(())
}

fn emit_event(tx: &mpsc::Sender<InfoHashEvent>, event: InfoHashEvent, stats: &DhtStats) {
    if event.peer.is_some() {
        stats.peer_events.fetch_add(1, Ordering::Relaxed);
    } else {
        stats.hash_events.fetch_add(1, Ordering::Relaxed);
    }
    if let Err(err) = tx.try_send(event) {
        stats.events_dropped.fetch_add(1, Ordering::Relaxed);
        debug!(error = %err, "dropping dht event because pipeline queue is full");
    }
}

fn find_node_request(node_id: &[u8; 20], target: &[u8; 20]) -> Vec<u8> {
    let mut args = BTreeMap::new();
    args.insert(b"id".to_vec(), Value::Bytes(node_id.to_vec()));
    args.insert(b"target".to_vec(), Value::Bytes(target.to_vec()));

    let mut root = BTreeMap::new();
    root.insert(b"t".to_vec(), Value::Bytes(random_transaction()));
    root.insert(b"y".to_vec(), Value::Bytes(b"q".to_vec()));
    root.insert(b"q".to_vec(), Value::Bytes(b"find_node".to_vec()));
    root.insert(b"a".to_vec(), Value::Dict(args));

    let mut out = Vec::new();
    encode(&Value::Dict(root), &mut out);
    out
}

async fn probe_get_peers(
    socket: Arc<UdpSocket>,
    node_ids: Arc<[[u8; 20]]>,
    nodes: Arc<NodeTable>,
    addr: SocketAddr,
    info_hash: [u8; 20],
    probe_count: usize,
    remaining_depth: u8,
    stats: Arc<DhtStats>,
) {
    let mut targets = Vec::with_capacity(probe_count.max(1));
    targets.push(addr);
    if probe_count > 1 {
        targets.extend(nodes.sample(addr, probe_count - 1));
    }
    probe_get_peers_targets(
        socket,
        node_ids,
        targets,
        addr,
        info_hash,
        probe_count,
        remaining_depth,
        stats,
    )
    .await;
}

async fn probe_get_peers_targets(
    socket: Arc<UdpSocket>,
    node_ids: Arc<[[u8; 20]]>,
    targets: Vec<SocketAddr>,
    family_addr: SocketAddr,
    info_hash: [u8; 20],
    probe_count: usize,
    remaining_depth: u8,
    stats: Arc<DhtStats>,
) {
    for target in unique(targets)
        .into_iter()
        .filter(|target| target.is_ipv4() == family_addr.is_ipv4() && is_global_address(target))
        .take(probe_count)
    {
        let node_id = closest_node_id(&node_ids, &info_hash);
        let request = get_peers_request(&node_id, &info_hash, remaining_depth);
        match socket.send_to(&request, target).await {
            Ok(_) => {
                stats.active_probes_sent.fetch_add(1, Ordering::Relaxed);
            }
            Err(err) => {
                debug!(%target, error = %err, "failed to send active get_peers request");
            }
        }
    }
}

fn get_peers_request(node_id: &[u8; 20], info_hash: &[u8; 20], remaining_depth: u8) -> Vec<u8> {
    let mut args = BTreeMap::new();
    args.insert(b"id".to_vec(), Value::Bytes(node_id.to_vec()));
    args.insert(b"info_hash".to_vec(), Value::Bytes(info_hash.to_vec()));

    let mut root = BTreeMap::new();
    root.insert(
        b"t".to_vec(),
        Value::Bytes(active_get_peers_transaction(info_hash, remaining_depth)),
    );
    root.insert(b"y".to_vec(), Value::Bytes(b"q".to_vec()));
    root.insert(b"q".to_vec(), Value::Bytes(b"get_peers".to_vec()));
    root.insert(b"a".to_vec(), Value::Dict(args));

    let mut out = Vec::new();
    encode(&Value::Dict(root), &mut out);
    out
}

fn active_get_peers_transaction(info_hash: &[u8; 20], remaining_depth: u8) -> Vec<u8> {
    let mut transaction = Vec::with_capacity(22);
    transaction.push(b'g');
    transaction.push(remaining_depth);
    transaction.extend_from_slice(info_hash);
    transaction
}

fn active_get_peers_hash(transaction: &[u8]) -> Option<([u8; 20], u8)> {
    if transaction.first() != Some(&b'g') {
        return None;
    }
    if transaction.len() == 22 {
        return to_hash(&transaction[2..]).map(|hash| (hash, transaction[1]));
    }
    if transaction.len() == 21 {
        return to_hash(&transaction[1..]).map(|hash| (hash, 0));
    }
    None
}

fn parse_compact_nodes(bytes: &[u8]) -> Vec<SocketAddr> {
    bytes
        .chunks_exact(26)
        .filter_map(|chunk| {
            let ip = std::net::Ipv4Addr::new(chunk[20], chunk[21], chunk[22], chunk[23]);
            let port = u16::from_be_bytes([chunk[24], chunk[25]]);
            (port != 0).then_some(SocketAddr::new(ip.into(), port))
        })
        .collect()
}

fn parse_compact_nodes6(bytes: &[u8]) -> Vec<SocketAddr> {
    bytes
        .chunks_exact(38)
        .filter_map(|chunk| {
            let ip = Ipv6Addr::new(
                u16::from_be_bytes([chunk[20], chunk[21]]),
                u16::from_be_bytes([chunk[22], chunk[23]]),
                u16::from_be_bytes([chunk[24], chunk[25]]),
                u16::from_be_bytes([chunk[26], chunk[27]]),
                u16::from_be_bytes([chunk[28], chunk[29]]),
                u16::from_be_bytes([chunk[30], chunk[31]]),
                u16::from_be_bytes([chunk[32], chunk[33]]),
                u16::from_be_bytes([chunk[34], chunk[35]]),
            );
            let port = u16::from_be_bytes([chunk[36], chunk[37]]);
            (port != 0).then_some(SocketAddr::new(ip.into(), port))
        })
        .collect()
}

fn parse_compact_peer_values(response: &BTreeMap<Vec<u8>, Value>) -> Vec<SocketAddr> {
    let Some(Value::List(values)) = dict_get(response, b"values") else {
        return Vec::new();
    };

    values
        .iter()
        .filter_map(as_bytes)
        .flat_map(parse_compact_peers)
        .filter(is_global_address)
        .collect()
}

fn parse_compact_peers(bytes: &[u8]) -> Vec<SocketAddr> {
    bytes
        .chunks_exact(6)
        .filter_map(|chunk| {
            let ip = std::net::Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
            let port = u16::from_be_bytes([chunk[4], chunk[5]]);
            (port != 0).then_some(SocketAddr::new(ip.into(), port))
        })
        .collect()
}

fn is_global_address(addr: &SocketAddr) -> bool {
    match addr.ip() {
        IpAddr::V4(ip) => is_public_v4(ip),
        IpAddr::V6(ip) => is_public_v6(ip),
    }
}

fn is_public_v4(ip: Ipv4Addr) -> bool {
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_multicast()
        || ip.is_link_local()
        || ip.is_broadcast())
}

fn is_public_v6(ip: Ipv6Addr) -> bool {
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_multicast()
        || ip.is_unicast_link_local()
        || ip.is_unique_local())
}

async fn resolve_addrs(addr: &str) -> Vec<SocketAddr> {
    match tokio::net::lookup_host(addr).await {
        Ok(addrs) => addrs.collect(),
        Err(err) => {
            debug!(%addr, error = %err, "failed to resolve dht node");
            Vec::new()
        }
    }
}

fn unique(mut addrs: Vec<SocketAddr>) -> Vec<SocketAddr> {
    addrs.sort_unstable();
    addrs.dedup();
    addrs
}

fn response(transaction: &[u8], node_id: [u8; 20], mut extra: BTreeMap<Vec<u8>, Value>) -> Vec<u8> {
    extra.insert(b"id".to_vec(), Value::Bytes(node_id.to_vec()));

    let mut root = BTreeMap::new();
    root.insert(b"t".to_vec(), Value::Bytes(transaction.to_vec()));
    root.insert(b"y".to_vec(), Value::Bytes(b"r".to_vec()));
    root.insert(b"r".to_vec(), Value::Dict(extra));

    let mut out = Vec::new();
    encode(&Value::Dict(root), &mut out);
    out
}

fn random_id() -> [u8; 20] {
    let mut id = [0u8; 20];
    rand::thread_rng().fill_bytes(&mut id);
    id
}

fn random_transaction() -> Vec<u8> {
    let mut tx = [0u8; 4];
    rand::thread_rng().fill_bytes(&mut tx);
    tx.to_vec()
}

fn closest_node_id(node_ids: &[[u8; 20]], target: &[u8; 20]) -> [u8; 20] {
    node_ids
        .iter()
        .min_by(|left, right| xor_distance(left, target).cmp(&xor_distance(right, target)))
        .copied()
        .unwrap_or_else(random_id)
}

fn xor_distance(left: &[u8; 20], right: &[u8; 20]) -> [u8; 20] {
    let mut distance = [0u8; 20];
    for (index, byte) in distance.iter_mut().enumerate() {
        *byte = left[index] ^ right[index];
    }
    distance
}

fn announce_peer_addr(args: &BTreeMap<Vec<u8>, Value>, addr: SocketAddr) -> Option<SocketAddr> {
    let port = match dict_get(args, b"implied_port").and_then(as_int) {
        Some(1) => addr.port(),
        _ => {
            let port = dict_get(args, b"port").and_then(as_int)?;
            u16::try_from(port).ok()?
        }
    };
    (port != 0)
        .then_some(SocketAddr::new(addr.ip(), port))
        .filter(|addr| is_global_address(addr))
}

fn to_hash(bytes: &[u8]) -> Option<[u8; 20]> {
    if bytes.len() != 20 {
        return None;
    }
    let mut hash = [0u8; 20];
    hash.copy_from_slice(bytes);
    Some(hash)
}

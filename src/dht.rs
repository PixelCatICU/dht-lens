use std::{
    collections::{BTreeMap, VecDeque},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{Arc, Mutex},
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
    nodes: Mutex<VecDeque<SocketAddr>>,
    max_nodes: usize,
}

impl NodeTable {
    fn new(max_nodes: usize) -> Self {
        Self {
            nodes: Mutex::new(VecDeque::new()),
            max_nodes,
        }
    }

    fn add(&self, addr: SocketAddr) {
        if addr.port() == 0 || addr.ip().is_unspecified() {
            return;
        }

        let mut nodes = self.nodes.lock().expect("node table mutex poisoned");
        if let Some(index) = nodes.iter().position(|node| *node == addr) {
            nodes.remove(index);
            nodes.push_back(addr);
            return;
        }
        nodes.push_back(addr);
        while nodes.len() > self.max_nodes {
            nodes.pop_front();
        }
    }

    fn add_many(&self, addrs: impl IntoIterator<Item = SocketAddr>) {
        for addr in addrs {
            self.add(addr);
        }
    }

    fn sample(&self, family_addr: SocketAddr, limit: usize) -> Vec<SocketAddr> {
        let nodes = self.nodes.lock().expect("node table mutex poisoned");
        let family_nodes: Vec<_> = nodes
            .iter()
            .filter(|addr| addr.is_ipv4() == family_addr.is_ipv4())
            .copied()
            .collect();
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
        self.nodes.lock().expect("node table mutex poisoned").len()
    }
}

pub async fn run(config: DhtConfig, tx: mpsc::Sender<InfoHashEvent>) -> Result<()> {
    let mut tasks = Vec::new();
    let nodes = Arc::new(NodeTable::new(config.routing_table_max_nodes));

    for bootstrap in &config.bootstrap_nodes {
        nodes.add_many(resolve_addrs(bootstrap).await);
    }

    let v4_config = config.clone();
    let v4_tx = tx.clone();
    let v4_nodes = nodes.clone();
    let v4_virtual_node_count = config.virtual_nodes;
    let v4_bootstrap_query_limit = config.bootstrap_query_limit;
    tasks.push(tokio::spawn(async move {
        if let Err(err) = run_listener(
            v4_config.listen_addr,
            v4_config.bootstrap_nodes,
            v4_virtual_node_count,
            v4_bootstrap_query_limit,
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
        tasks.push(tokio::spawn(async move {
            if let Err(err) = run_listener(
                listen_addr_v6,
                bootstrap_nodes,
                v6_virtual_node_count,
                v6_bootstrap_query_limit,
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
    nodes: Arc<NodeTable>,
    tx: mpsc::Sender<InfoHashEvent>,
) -> Result<()> {
    let socket = Arc::new(bind_udp_socket(listen_addr)?);
    let node_ids: Arc<[[u8; 20]]> = (0..virtual_node_count)
        .map(|_| random_id())
        .collect::<Vec<_>>()
        .into();

    info!(addr = %listen_addr, virtual_nodes = node_ids.len(), "dht listener bound");
    tokio::spawn(bootstrap_loop(
        socket.clone(),
        node_ids.clone(),
        bootstrap_nodes,
        bootstrap_query_limit,
        nodes.clone(),
    ));

    let mut buf = vec![0u8; 4096];
    loop {
        let (len, addr) = socket.recv_from(&mut buf).await?;
        let packet = &buf[..len];
        nodes.add(addr);
        if let Err(err) = handle_packet(
            socket.clone(),
            node_ids.clone(),
            nodes.clone(),
            addr,
            packet,
            &tx,
        )
        .await
        {
            debug!(%addr, error = %err, "ignored dht packet");
        }
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
    let mut ticker = interval(Duration::from_secs(15));
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

                warn!(
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
    tx: &mpsc::Sender<InfoHashEvent>,
) -> Result<()> {
    let value = parse(packet)?;
    let Value::Dict(dict) = value else {
        return Ok(());
    };

    let y = dict_get(&dict, b"y").and_then(as_bytes).unwrap_or(b"");
    if y == b"r" {
        if let Some(Value::Dict(response)) = dict_get(&dict, b"r") {
            if let Some(bytes) = dict_get(response, b"nodes").and_then(as_bytes) {
                nodes.add_many(parse_compact_nodes(bytes));
            }
            if let Some(bytes) = dict_get(response, b"nodes6").and_then(as_bytes) {
                nodes.add_many(parse_compact_nodes6(bytes));
            }
        }
        return Ok(());
    }

    let transaction = dict_get(&dict, b"t").and_then(as_bytes).unwrap_or(b"");
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
            if let Some(hash) = dict_get(args, b"info_hash")
                .and_then(as_bytes)
                .and_then(to_hash)
            {
                debug!(info_hash = %hex::encode(hash), source = "get_peers", "discovered info_hash");
                let _ = tx
                    .send(InfoHashEvent {
                        info_hash: hash,
                        source: Source::DhtGetPeers,
                        peer_count: 0,
                        peer: None,
                        seed_nodes: event_seed_nodes(&nodes, addr),
                        seen_at: now_ts(),
                    })
                    .await;
            }
            let node_id = dict_get(args, b"info_hash")
                .and_then(as_bytes)
                .and_then(to_hash)
                .map(|hash| closest_node_id(&node_ids, &hash))
                .unwrap_or(node_ids[0]);
            let mut extra = BTreeMap::new();
            extra.insert(b"token".to_vec(), Value::Bytes(b"dht-lens".to_vec()));
            extra.insert(
                b"nodes".to_vec(),
                Value::Bytes(nodes.compact_nodes(addr, 8)),
            );
            let response = response(transaction, node_id, extra);
            socket.send_to(&response, addr).await?;
        }
        b"announce_peer" => {
            if let Some(hash) = dict_get(args, b"info_hash")
                .and_then(as_bytes)
                .and_then(to_hash)
            {
                let node_id = closest_node_id(&node_ids, &hash);
                let peer = announce_peer_addr(args, addr);
                info!(info_hash = %hex::encode(hash), source = "announce_peer", ?peer, "discovered info_hash");
                let _ = tx
                    .send(InfoHashEvent {
                        info_hash: hash,
                        source: Source::DhtAnnouncePeer,
                        peer_count: peer.is_some() as u32,
                        peer,
                        seed_nodes: event_seed_nodes(&nodes, addr),
                        seen_at: now_ts(),
                    })
                    .await;
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

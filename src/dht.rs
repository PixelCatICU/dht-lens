use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};

use anyhow::Result;
use rand::RngCore;
use tokio::{
    net::UdpSocket,
    sync::mpsc,
    time::{Duration, Instant, interval, timeout},
};
use tracing::{debug, warn};

use crate::{
    bencode::{Value, as_bytes, dict_get, encode, parse},
    config::DhtConfig,
    model::{InfoHashEvent, Source, now_ts},
};

pub async fn run(config: DhtConfig, tx: mpsc::Sender<InfoHashEvent>) -> Result<()> {
    let socket = Arc::new(UdpSocket::bind(config.listen_addr).await?);
    let node_id = random_id();
    let bootstrap_nodes = config.bootstrap_nodes.clone();

    tokio::spawn(bootstrap_loop(socket.clone(), node_id, bootstrap_nodes));

    let mut buf = vec![0u8; 4096];
    loop {
        let (len, addr) = socket.recv_from(&mut buf).await?;
        let packet = &buf[..len];
        if let Err(err) = handle_packet(socket.clone(), node_id, addr, packet, &tx).await {
            debug!(%addr, error = %err, "ignored dht packet");
        }
    }
}

pub async fn get_peers(info_hash: [u8; 20], config: &DhtConfig) -> Result<Vec<SocketAddr>> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    let node_id = random_id();
    let mut peers = Vec::new();
    let mut nodes = Vec::new();

    for bootstrap in &config.bootstrap_nodes {
        nodes.extend(resolve_addrs(bootstrap).await);
    }

    let deadline = Instant::now() + Duration::from_secs(6);
    let mut queried = 0usize;
    let mut buf = vec![0u8; 4096];

    while Instant::now() < deadline && queried < config.max_inflight_queries.min(256) {
        let batch: Vec<_> = nodes.drain(..nodes.len().min(32)).collect();
        if batch.is_empty() {
            break;
        }

        for node in &batch {
            let request = get_peers_request(&node_id, &info_hash);
            let _ = socket.send_to(&request, node).await;
            queried += 1;
        }

        let read_until = Instant::now() + Duration::from_millis(700);
        while Instant::now() < read_until {
            match timeout(Duration::from_millis(120), socket.recv_from(&mut buf)).await {
                Ok(Ok((len, _))) => {
                    if let Ok((mut found_peers, mut found_nodes)) =
                        parse_get_peers_response(&buf[..len])
                    {
                        peers.append(&mut found_peers);
                        if peers.len() >= 64 {
                            peers.truncate(64);
                            return Ok(unique(peers));
                        }
                        nodes.append(&mut found_nodes);
                        nodes.truncate(config.routing_table_max_nodes.min(2_000));
                    }
                }
                _ => break,
            }
        }
    }

    Ok(unique(peers))
}

async fn bootstrap_loop(socket: Arc<UdpSocket>, node_id: [u8; 20], nodes: Vec<String>) {
    let mut ticker = interval(Duration::from_secs(15));
    loop {
        ticker.tick().await;
        for node in &nodes {
            let target = random_id();
            let request = find_node_request(&node_id, &target);
            if let Err(err) = socket.send_to(&request, node).await {
                warn!(node, error = %err, "failed to send dht bootstrap request");
            }
        }
    }
}

async fn handle_packet(
    socket: Arc<UdpSocket>,
    node_id: [u8; 20],
    addr: SocketAddr,
    packet: &[u8],
    tx: &mpsc::Sender<InfoHashEvent>,
) -> Result<()> {
    let value = parse(packet)?;
    let Value::Dict(dict) = value else {
        return Ok(());
    };

    let transaction = dict_get(&dict, b"t").and_then(as_bytes).unwrap_or(b"");
    let y = dict_get(&dict, b"y").and_then(as_bytes).unwrap_or(b"");
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
            let response = response(transaction, node_id, BTreeMap::new());
            socket.send_to(&response, addr).await?;
        }
        b"find_node" => {
            let mut extra = BTreeMap::new();
            extra.insert(b"nodes".to_vec(), Value::Bytes(Vec::new()));
            let response = response(transaction, node_id, extra);
            socket.send_to(&response, addr).await?;
        }
        b"get_peers" => {
            if let Some(hash) = dict_get(args, b"info_hash")
                .and_then(as_bytes)
                .and_then(to_hash)
            {
                let _ = tx
                    .send(InfoHashEvent {
                        info_hash: hash,
                        source: Source::DhtGetPeers,
                        peer_count: 0,
                        seen_at: now_ts(),
                    })
                    .await;
            }
            let mut extra = BTreeMap::new();
            extra.insert(b"token".to_vec(), Value::Bytes(b"dht-lens".to_vec()));
            extra.insert(b"nodes".to_vec(), Value::Bytes(Vec::new()));
            let response = response(transaction, node_id, extra);
            socket.send_to(&response, addr).await?;
        }
        b"announce_peer" => {
            if let Some(hash) = dict_get(args, b"info_hash")
                .and_then(as_bytes)
                .and_then(to_hash)
            {
                let _ = tx
                    .send(InfoHashEvent {
                        info_hash: hash,
                        source: Source::DhtAnnouncePeer,
                        peer_count: 1,
                        seen_at: now_ts(),
                    })
                    .await;
            }
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

fn get_peers_request(node_id: &[u8; 20], info_hash: &[u8; 20]) -> Vec<u8> {
    let mut args = BTreeMap::new();
    args.insert(b"id".to_vec(), Value::Bytes(node_id.to_vec()));
    args.insert(b"info_hash".to_vec(), Value::Bytes(info_hash.to_vec()));

    let mut root = BTreeMap::new();
    root.insert(b"t".to_vec(), Value::Bytes(random_transaction()));
    root.insert(b"y".to_vec(), Value::Bytes(b"q".to_vec()));
    root.insert(b"q".to_vec(), Value::Bytes(b"get_peers".to_vec()));
    root.insert(b"a".to_vec(), Value::Dict(args));

    let mut out = Vec::new();
    encode(&Value::Dict(root), &mut out);
    out
}

fn parse_get_peers_response(input: &[u8]) -> Result<(Vec<SocketAddr>, Vec<SocketAddr>)> {
    let value = parse(input)?;
    let Value::Dict(root) = value else {
        return Ok((Vec::new(), Vec::new()));
    };
    if dict_get(&root, b"y").and_then(as_bytes) != Some(b"r") {
        return Ok((Vec::new(), Vec::new()));
    }

    let response = match dict_get(&root, b"r") {
        Some(Value::Dict(response)) => response,
        _ => return Ok((Vec::new(), Vec::new())),
    };

    let mut peers = Vec::new();
    if let Some(Value::List(values)) = dict_get(response, b"values") {
        for value in values {
            if let Some(bytes) = as_bytes(value) {
                peers.extend(parse_compact_peers(bytes));
            }
        }
    }

    let nodes = dict_get(response, b"nodes")
        .and_then(as_bytes)
        .map(parse_compact_nodes)
        .unwrap_or_default();

    Ok((peers, nodes))
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

fn to_hash(bytes: &[u8]) -> Option<[u8; 20]> {
    if bytes.len() != 20 {
        return None;
    }
    let mut hash = [0u8; 20];
    hash.copy_from_slice(bytes);
    Some(hash)
}

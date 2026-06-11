use std::{collections::BTreeMap, net::SocketAddr};

use anyhow::{Context, Result, bail};
use bytes::{BufMut, BytesMut};
use rand::RngCore;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

use crate::{
    bencode::{Value, as_int, dict_get, encode, parse, parse_prefix},
    config::MetadataConfig,
    metadata::parser::{ParsedMetadata, parse_info_metadata},
};

const BT_PROTOCOL: &[u8] = b"BitTorrent protocol";
const MSG_EXTENDED: u8 = 20;
const EXT_HANDSHAKE: u8 = 0;

pub async fn fetch_from_peer(
    peer: SocketAddr,
    info_hash: [u8; 20],
    config: &MetadataConfig,
) -> Result<ParsedMetadata> {
    let fut = async {
        let mut stream = timeout(config.connect_timeout, TcpStream::connect(peer))
            .await
            .context("connect timeout")??;
        handshake(&mut stream, &info_hash).await?;

        send_extended_handshake(&mut stream).await?;
        let (metadata_ext_id, metadata_size) = read_extended_handshake(&mut stream).await?;
        if metadata_size == 0 || metadata_size > config.max_metadata_size {
            bail!("invalid metadata size {metadata_size}");
        }

        let metadata = fetch_metadata_pieces(&mut stream, metadata_ext_id, metadata_size).await?;
        parse_info_metadata(&info_hash, &metadata)
    };

    timeout(config.metadata_timeout, fut)
        .await
        .context("metadata fetch timeout")?
}

async fn handshake(stream: &mut TcpStream, info_hash: &[u8; 20]) -> Result<()> {
    let mut peer_id = [0u8; 20];
    peer_id[..8].copy_from_slice(b"-DL0001-");
    rand::thread_rng().fill_bytes(&mut peer_id[8..]);

    let mut request = Vec::with_capacity(68);
    request.push(BT_PROTOCOL.len() as u8);
    request.extend_from_slice(BT_PROTOCOL);
    let mut reserved = [0u8; 8];
    reserved[5] |= 0x10;
    request.extend_from_slice(&reserved);
    request.extend_from_slice(info_hash);
    request.extend_from_slice(&peer_id);
    stream.write_all(&request).await?;

    let mut response = [0u8; 68];
    stream.read_exact(&mut response).await?;
    if response[0] != BT_PROTOCOL.len() as u8 || &response[1..20] != BT_PROTOCOL {
        bail!("invalid bittorrent handshake");
    }
    if &response[28..48] != info_hash {
        bail!("peer returned different info_hash");
    }
    if response[25] & 0x10 == 0 {
        bail!("peer does not support extension protocol");
    }
    Ok(())
}

async fn send_extended_handshake(stream: &mut TcpStream) -> Result<()> {
    let mut m = BTreeMap::new();
    m.insert(b"ut_metadata".to_vec(), Value::Int(1));
    let mut dict = BTreeMap::new();
    dict.insert(b"m".to_vec(), Value::Dict(m));
    let payload = encode_to_vec(&Value::Dict(dict));

    let mut frame = BytesMut::new();
    frame.put_u32((2 + payload.len()) as u32);
    frame.put_u8(MSG_EXTENDED);
    frame.put_u8(EXT_HANDSHAKE);
    frame.extend_from_slice(&payload);
    stream.write_all(&frame).await?;
    Ok(())
}

async fn read_extended_handshake(stream: &mut TcpStream) -> Result<(u8, usize)> {
    loop {
        let payload = read_message(stream).await?;
        if payload.is_empty() {
            continue;
        }
        if payload[0] != MSG_EXTENDED || payload.len() < 2 || payload[1] != EXT_HANDSHAKE {
            continue;
        }
        let value = parse(&payload[2..])?;
        let Value::Dict(dict) = value else {
            bail!("extended handshake is not dict");
        };
        let metadata_id = match dict_get(&dict, b"m") {
            Some(Value::Dict(m)) => dict_get(m, b"ut_metadata")
                .and_then(as_int)
                .unwrap_or_default(),
            _ => 0,
        };
        let metadata_size = dict_get(&dict, b"metadata_size")
            .and_then(as_int)
            .unwrap_or_default();
        if metadata_id <= 0 || metadata_id > u8::MAX as i64 {
            bail!("peer did not advertise ut_metadata");
        }
        if metadata_size <= 0 {
            bail!("peer did not advertise metadata_size");
        }
        return Ok((metadata_id as u8, metadata_size as usize));
    }
}

async fn fetch_metadata_pieces(
    stream: &mut TcpStream,
    metadata_ext_id: u8,
    metadata_size: usize,
) -> Result<Vec<u8>> {
    let piece_count = metadata_size.div_ceil(16 * 1024);
    let mut pieces: Vec<Option<Vec<u8>>> = vec![None; piece_count];

    for piece in 0..piece_count {
        request_piece(stream, metadata_ext_id, piece).await?;
    }

    while pieces.iter().any(Option::is_none) {
        let payload = read_message(stream).await?;
        if payload.len() < 2 || payload[0] != MSG_EXTENDED || payload[1] != metadata_ext_id {
            continue;
        }
        let body = &payload[2..];
        let (header, data_offset) = parse_prefix(body)?;
        let Value::Dict(dict) = header else {
            continue;
        };
        let msg_type = dict_get(&dict, b"msg_type")
            .and_then(as_int)
            .unwrap_or_default();
        let piece = dict_get(&dict, b"piece").and_then(as_int).unwrap_or(-1);
        if msg_type != 1 || piece < 0 || piece as usize >= piece_count {
            continue;
        }
        pieces[piece as usize] = Some(body[data_offset..].to_vec());
    }

    let mut metadata = Vec::with_capacity(metadata_size);
    for piece in pieces.into_iter().flatten() {
        metadata.extend_from_slice(&piece);
    }
    metadata.truncate(metadata_size);
    Ok(metadata)
}

async fn request_piece(stream: &mut TcpStream, metadata_ext_id: u8, piece: usize) -> Result<()> {
    let mut dict = BTreeMap::new();
    dict.insert(b"msg_type".to_vec(), Value::Int(0));
    dict.insert(b"piece".to_vec(), Value::Int(piece as i64));
    let payload = encode_to_vec(&Value::Dict(dict));

    let mut frame = BytesMut::new();
    frame.put_u32((2 + payload.len()) as u32);
    frame.put_u8(MSG_EXTENDED);
    frame.put_u8(metadata_ext_id);
    frame.extend_from_slice(&payload);
    stream.write_all(&frame).await?;
    Ok(())
}

async fn read_message(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await?;
    let len = u32::from_be_bytes(len) as usize;
    if len == 0 {
        return Ok(Vec::new());
    }
    if len > 2 * 1024 * 1024 {
        bail!("peer message too large");
    }
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    Ok(payload)
}

fn encode_to_vec(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    encode(value, &mut out);
    out
}

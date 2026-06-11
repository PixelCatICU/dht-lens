use std::{collections::BTreeMap, net::SocketAddr};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use rbit::{
    ExtensionHandshake, Message, MetadataMessage, MetadataMessageType, PeerConnection, PeerId,
    metadata_piece_count, peer::ExtensionMessage,
};
use sha1::{Digest, Sha1};
use tokio::time::timeout;

use crate::{
    config::MetadataConfig,
    metadata::parser::{ParsedMetadata, parse_info_metadata},
};

pub async fn fetch_from_peer(
    peer: SocketAddr,
    info_hash: [u8; 20],
    config: &MetadataConfig,
) -> Result<ParsedMetadata> {
    let metadata = fetch_metadata_bytes(peer, info_hash, config).await?;
    parse_info_metadata(&info_hash, &metadata)
}

async fn fetch_metadata_bytes(
    peer: SocketAddr,
    info_hash: [u8; 20],
    config: &MetadataConfig,
) -> Result<Vec<u8>> {
    let peer_id = PeerId::generate();
    let mut conn = timeout(
        config.connect_timeout,
        PeerConnection::connect(peer, info_hash, *peer_id.as_bytes()),
    )
    .await
    .context("connect timeout")??;

    if !conn.supports_extension {
        bail!("peer does not support extension protocol");
    }

    let local_metadata_id = 1;
    let handshake = ExtensionHandshake::with_extensions(&[("ut_metadata", local_metadata_id)])
        .encode()
        .context("encode extension handshake")?;
    conn.send(Message::Extended {
        id: 0,
        payload: handshake,
    })
    .await?;

    let fut = async {
        let mut metadata_size = 0usize;
        let mut remote_metadata_id = 0u8;
        let mut request_sent = false;
        let mut pieces: BTreeMap<u32, Bytes> = BTreeMap::new();

        loop {
            let msg = conn.receive().await?;
            let Message::Extended { id, payload } = msg else {
                continue;
            };

            if id == 0 {
                if let Ok(ExtensionMessage::Handshake(remote)) =
                    ExtensionMessage::decode(id, &payload)
                {
                    if let Some(size) = remote.metadata_size {
                        metadata_size = size as usize;
                    }
                    if let Some(ext_id) = remote.get_extension_id("ut_metadata") {
                        remote_metadata_id = ext_id;
                    }
                }

                if metadata_size == 0 || remote_metadata_id == 0 || request_sent {
                    continue;
                }
                if metadata_size > config.max_metadata_size {
                    bail!("metadata too large {metadata_size}");
                }

                for piece in 0..metadata_piece_count(metadata_size) {
                    let request = MetadataMessage::request(piece as u32)
                        .encode()
                        .context("encode metadata request")?;
                    conn.send(Message::Extended {
                        id: remote_metadata_id,
                        payload: request,
                    })
                    .await?;
                }
                request_sent = true;
                continue;
            }

            if id != local_metadata_id {
                continue;
            }

            let message = match MetadataMessage::decode(&payload) {
                Ok(message) => message,
                Err(_) => continue,
            };
            if message.msg_type != MetadataMessageType::Data {
                continue;
            }
            let Some(data) = message.data else {
                continue;
            };
            pieces.insert(message.piece, data);

            if metadata_size == 0 {
                continue;
            }
            let received: usize = pieces.values().map(Bytes::len).sum();
            if received < metadata_size {
                continue;
            }

            let count = metadata_piece_count(metadata_size);
            let mut metadata = Vec::with_capacity(metadata_size);
            for piece in 0..count {
                let Some(data) = pieces.get(&(piece as u32)) else {
                    bail!("missing metadata piece {piece}");
                };
                metadata.extend_from_slice(data);
            }
            metadata.truncate(metadata_size);

            let mut hasher = Sha1::new();
            hasher.update(&metadata);
            let digest: [u8; 20] = hasher.finalize().into();
            if digest != info_hash {
                bail!("metadata sha1 mismatch");
            }
            return Ok(metadata);
        }
    };

    timeout(config.metadata_timeout, fut)
        .await
        .context("metadata fetch timeout")?
}

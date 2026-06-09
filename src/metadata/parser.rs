use anyhow::{Context, Result, bail};
use sha1::{Digest, Sha1};

use crate::{
    bencode::{Value, as_bytes, as_int, dict_get, encode, parse},
    metadata::decode::decode_text,
    model::TorrentFile,
};

#[derive(Debug, Clone)]
pub struct ParsedMetadata {
    pub name: String,
    pub total_size: u64,
    pub files: Vec<TorrentFile>,
}

pub fn parse_info_metadata(
    expected_info_hash: &[u8; 20],
    metadata: &[u8],
) -> Result<ParsedMetadata> {
    let mut hasher = Sha1::new();
    hasher.update(metadata);
    let actual = hasher.finalize();
    if actual.as_slice() != expected_info_hash {
        bail!("metadata info_hash mismatch");
    }

    let value = parse(metadata)?;
    parse_info_value(&value, None)
}

pub fn parse_torrent_metainfo(input: &[u8]) -> Result<([u8; 20], ParsedMetadata)> {
    let root = parse(input)?;
    let Value::Dict(root_dict) = &root else {
        bail!("torrent metainfo root is not a dict");
    };
    let info = dict_get(root_dict, b"info").context("missing info dict")?;
    let encoding = dict_get(root_dict, b"encoding").and_then(as_bytes);

    let mut encoded_info = Vec::new();
    encode(info, &mut encoded_info);
    let mut hasher = Sha1::new();
    hasher.update(&encoded_info);
    let hash = hasher.finalize();
    let mut info_hash = [0u8; 20];
    info_hash.copy_from_slice(&hash);

    Ok((info_hash, parse_info_value(info, encoding)?))
}

fn parse_info_value(value: &Value, encoding: Option<&[u8]>) -> Result<ParsedMetadata> {
    let Value::Dict(info) = value else {
        bail!("metadata info is not a dict");
    };

    let name = dict_get(info, b"name")
        .and_then(as_bytes)
        .map(|bytes| decode_text(bytes, encoding))
        .context("missing torrent name")?;

    let mut files = Vec::new();
    if let Some(Value::List(items)) = dict_get(info, b"files") {
        for item in items {
            let Value::Dict(file) = item else {
                continue;
            };
            let size = dict_get(file, b"length")
                .and_then(as_int)
                .unwrap_or_default()
                .max(0) as u64;
            let path = match dict_get(file, b"path") {
                Some(Value::List(parts)) => parts
                    .iter()
                    .filter_map(as_bytes)
                    .map(|part| decode_text(part, encoding))
                    .collect::<Vec<_>>()
                    .join("/"),
                _ => String::new(),
            };
            if !path.is_empty() {
                files.push(TorrentFile { path, size });
            }
        }
    } else {
        let size = dict_get(info, b"length")
            .and_then(as_int)
            .unwrap_or_default()
            .max(0) as u64;
        files.push(TorrentFile {
            path: name.clone(),
            size,
        });
    }

    let total_size = files.iter().map(|file| file.size).sum();
    Ok(ParsedMetadata {
        name,
        total_size,
        files,
    })
}

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct TorrentFile {
    pub path: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TorrentRecord {
    pub info_hash: String,
    #[serde(skip_serializing)]
    pub info_hash_bytes: [u8; 20],
    pub name: String,
    pub total_size: u64,
    pub file_count: usize,
    pub files_stored_count: usize,
    pub files: Vec<TorrentFile>,
    pub peer_count: u32,
    pub source: Source,
    pub hot_score: u64,
    pub first_seen_at: i64,
    pub last_seen_at: i64,
    pub metadata_fetched_at: i64,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    DhtGetPeers = 1,
    DhtAnnouncePeer = 2,
    ManualMagnet = 3,
}

impl Source {
    pub fn as_i64(self) -> i64 {
        self as i64
    }
}

#[derive(Debug, Clone)]
pub struct InfoHashEvent {
    pub info_hash: [u8; 20],
    pub source: Source,
    pub peer_count: u32,
    pub peer: Option<std::net::SocketAddr>,
    pub seed_nodes: Vec<std::net::SocketAddr>,
    pub seen_at: i64,
}

pub fn now_ts() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_secs() as i64)
        .unwrap_or_default()
}

use anyhow::{Context, Result};
use libsql::{Builder, Connection, Database, params};

use crate::{model::TorrentRecord, search::build_name_ngram};

pub struct LibsqlStore {
    db: Database,
}

impl LibsqlStore {
    pub async fn connect(url: &str, auth_token: Option<&str>) -> Result<Self> {
        let db = if url.starts_with("http://")
            || url.starts_with("https://")
            || url.starts_with("libsql://")
        {
            let token = auth_token.context("LIBSQL_AUTH_TOKEN is required for remote libSQL")?;
            Builder::new_remote(url.to_string(), token.to_string())
                .build()
                .await?
        } else {
            Builder::new_local(url).build().await?
        };
        Ok(Self { db })
    }

    pub async fn conn(&self) -> Result<Connection> {
        Ok(self.db.connect()?)
    }

    pub async fn migrate(&self) -> Result<()> {
        let conn = self.conn().await?;
        for statement in SCHEMA {
            conn.execute(statement, ()).await?;
        }
        Ok(())
    }

    pub async fn insert_torrent(&self, record: &TorrentRecord, max_ngram_len: usize) -> Result<()> {
        let conn = self.conn().await?;
        conn.execute("BEGIN", ()).await?;

        let result = async {
            conn.execute(
                r#"
                INSERT INTO torrents (
                  info_hash, info_hash_hex, name, total_size, file_count, files_stored_count,
                  peer_count, hot_score, source, first_seen_at, last_seen_at, metadata_fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(info_hash) DO UPDATE SET
                  name = excluded.name,
                  total_size = excluded.total_size,
                  file_count = excluded.file_count,
                  files_stored_count = excluded.files_stored_count,
                  peer_count = MAX(peer_count, excluded.peer_count),
                  hot_score = hot_score + 1,
                  source = excluded.source,
                  last_seen_at = excluded.last_seen_at,
                  metadata_fetched_at = excluded.metadata_fetched_at
                "#,
                params![
                    record.info_hash_bytes.to_vec(),
                    record.info_hash.clone(),
                    record.name.clone(),
                    record.total_size as i64,
                    record.file_count as i64,
                    record.files_stored_count as i64,
                    record.peer_count as i64,
                    record.hot_score as i64,
                    record.source.as_i64(),
                    record.first_seen_at,
                    record.last_seen_at,
                    record.metadata_fetched_at,
                ],
            )
            .await?;

            conn.execute("DELETE FROM torrent_files WHERE info_hash = ?", params![record.info_hash_bytes.to_vec()])
                .await?;
            for (idx, file) in record.files.iter().enumerate() {
                conn.execute(
                    "INSERT INTO torrent_files (info_hash, file_index, path, size) VALUES (?, ?, ?, ?)",
                    params![
                        record.info_hash_bytes.to_vec(),
                        idx as i64,
                        file.path.clone(),
                        file.size as i64
                    ],
                )
                .await?;
            }

            let name_ngram = build_name_ngram(&record.name, max_ngram_len);
            conn.execute(
                "DELETE FROM torrent_search WHERE info_hash_hex = ?",
                params![record.info_hash.clone()],
            )
            .await?;
            conn.execute(
                "INSERT INTO torrent_search (info_hash_hex, name_ngram) VALUES (?, ?)",
                params![record.info_hash.clone(), name_ngram],
            )
            .await?;

            upsert_observation(&conn, "torrent_observation_5m", record, bucket(record.last_seen_at, 300)).await?;
            upsert_observation(
                &conn,
                "torrent_observation_hourly",
                record,
                bucket(record.last_seen_at, 3_600),
            )
            .await?;

            anyhow::Ok(())
        }
        .await;

        match result {
            Ok(()) => {
                conn.execute("COMMIT", ()).await?;
                Ok(())
            }
            Err(err) => {
                let _ = conn.execute("ROLLBACK", ()).await;
                Err(err)
            }
        }
    }

    pub async fn search(
        &self,
        query: &str,
        limit: u32,
        max_ngram_len: usize,
    ) -> Result<Vec<SearchRow>> {
        let conn = self.conn().await?;
        let query = build_name_ngram(query, max_ngram_len);
        let mut rows = conn
            .query(
                r#"
                SELECT
                  t.info_hash_hex,
                  t.name,
                  t.total_size,
                  t.file_count,
                  t.peer_count,
                  t.hot_score,
                  t.last_seen_at
                FROM torrent_search
                JOIN torrents t ON t.info_hash_hex = torrent_search.info_hash_hex
                WHERE torrent_search MATCH ?
                ORDER BY bm25(torrent_search), t.hot_score DESC
                LIMIT ?
                "#,
                params![query, limit as i64],
            )
            .await?;

        let mut output = Vec::new();
        while let Some(row) = rows.next().await? {
            output.push(SearchRow {
                info_hash: row.get(0)?,
                name: row.get(1)?,
                total_size: row.get::<i64>(2)? as u64,
                file_count: row.get::<i64>(3)? as u64,
                peer_count: row.get::<i64>(4)? as u64,
                hot_score: row.get::<i64>(5)? as u64,
                last_seen_at: row.get(6)?,
            });
        }
        Ok(output)
    }
}

#[derive(Debug, serde::Serialize)]
pub struct SearchRow {
    pub info_hash: String,
    pub name: String,
    pub total_size: u64,
    pub file_count: u64,
    pub peer_count: u64,
    pub hot_score: u64,
    pub last_seen_at: i64,
}

async fn upsert_observation(
    conn: &Connection,
    table: &str,
    record: &TorrentRecord,
    bucket_start: i64,
) -> Result<()> {
    let sql = format!(
        r#"
        INSERT INTO {table} (
          info_hash, bucket_start, seen_count, max_peer_count, last_source, updated_at
        ) VALUES (?, ?, 1, ?, ?, ?)
        ON CONFLICT(info_hash, bucket_start) DO UPDATE SET
          seen_count = seen_count + 1,
          max_peer_count = MAX(max_peer_count, excluded.max_peer_count),
          last_source = excluded.last_source,
          updated_at = excluded.updated_at
        "#
    );
    conn.execute(
        &sql,
        params![
            record.info_hash_bytes.to_vec(),
            bucket_start,
            record.peer_count as i64,
            record.source.as_i64(),
            record.last_seen_at
        ],
    )
    .await?;
    Ok(())
}

fn bucket(ts: i64, size: i64) -> i64 {
    ts - ts.rem_euclid(size)
}

const SCHEMA: &[&str] = &[
    r#"
    CREATE TABLE IF NOT EXISTS torrents (
      info_hash BLOB PRIMARY KEY,
      info_hash_hex TEXT NOT NULL UNIQUE,
      name TEXT NOT NULL,
      total_size INTEGER NOT NULL DEFAULT 0,
      file_count INTEGER NOT NULL DEFAULT 0,
      files_stored_count INTEGER NOT NULL DEFAULT 0,
      peer_count INTEGER NOT NULL DEFAULT 0,
      hot_score INTEGER NOT NULL DEFAULT 0,
      source INTEGER NOT NULL DEFAULT 0,
      first_seen_at INTEGER NOT NULL,
      last_seen_at INTEGER NOT NULL,
      metadata_fetched_at INTEGER
    )
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS torrent_files (
      info_hash BLOB NOT NULL,
      file_index INTEGER NOT NULL,
      path TEXT NOT NULL,
      size INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (info_hash, file_index)
    )
    "#,
    r#"
    CREATE VIRTUAL TABLE IF NOT EXISTS torrent_search
    USING fts5(
      info_hash_hex UNINDEXED,
      name_ngram,
      tokenize = 'unicode61'
    )
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS torrent_observation_5m (
      info_hash BLOB NOT NULL,
      bucket_start INTEGER NOT NULL,
      seen_count INTEGER NOT NULL DEFAULT 0,
      max_peer_count INTEGER NOT NULL DEFAULT 0,
      last_source INTEGER NOT NULL DEFAULT 0,
      updated_at INTEGER NOT NULL,
      PRIMARY KEY (info_hash, bucket_start)
    )
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS torrent_observation_hourly (
      info_hash BLOB NOT NULL,
      bucket_start INTEGER NOT NULL,
      seen_count INTEGER NOT NULL DEFAULT 0,
      max_peer_count INTEGER NOT NULL DEFAULT 0,
      last_source INTEGER NOT NULL DEFAULT 0,
      updated_at INTEGER NOT NULL,
      PRIMARY KEY (info_hash, bucket_start)
    )
    "#,
    "CREATE INDEX IF NOT EXISTS idx_torrents_hot_score ON torrents(hot_score DESC)",
    "CREATE INDEX IF NOT EXISTS idx_torrents_last_seen_at ON torrents(last_seen_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_torrents_metadata_fetched_at ON torrents(metadata_fetched_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_obs_5m_bucket ON torrent_observation_5m(bucket_start DESC)",
    "CREATE INDEX IF NOT EXISTS idx_obs_hourly_bucket ON torrent_observation_hourly(bucket_start DESC)",
];

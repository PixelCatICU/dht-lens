import { createClient } from '@libsql/client';
import { buildNameNgram } from './search.mjs';

export class LibsqlStore {
  constructor(config) {
    if (!config.databaseUrl) throw new Error('LIBSQL_DATABASE_URL is required');
    if (/^https?:|^libsql:/.test(config.databaseUrl) && !config.authToken) {
      throw new Error('LIBSQL_AUTH_TOKEN is required for remote libSQL');
    }
    this.client = createClient({
      url: config.databaseUrl,
      authToken: config.authToken,
    });
  }

  async migrate() {
    for (const statement of SCHEMA) {
      await this.client.execute(statement);
    }
  }

  async insertTorrent(record, maxNameNgramLen) {
    const infoHash = Buffer.from(record.infoHash, 'hex');
    const tx = await this.client.transaction('write');
    try {
      await tx.execute({
        sql: `
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
        `,
        args: [
          infoHash,
          record.infoHash,
          record.name,
          record.totalSize,
          record.fileCount,
          record.filesStoredCount,
          record.peerCount,
          record.hotScore,
          record.source,
          record.firstSeenAt,
          record.lastSeenAt,
          record.metadataFetchedAt,
        ],
      });

      await tx.execute({ sql: 'DELETE FROM torrent_files WHERE info_hash = ?', args: [infoHash] });
      for (let idx = 0; idx < record.files.length; idx += 1) {
        const file = record.files[idx];
        await tx.execute({
          sql: 'INSERT INTO torrent_files (info_hash, file_index, path, size) VALUES (?, ?, ?, ?)',
          args: [infoHash, idx, file.path, file.size],
        });
      }

      await tx.execute({ sql: 'DELETE FROM torrent_search WHERE info_hash_hex = ?', args: [record.infoHash] });
      await tx.execute({
        sql: 'INSERT INTO torrent_search (info_hash_hex, name_ngram) VALUES (?, ?)',
        args: [record.infoHash, buildNameNgram(record.name, maxNameNgramLen)],
      });

      await upsertObservation(tx, 'torrent_observation_5m', record, bucket(record.lastSeenAt, 300));
      await upsertObservation(tx, 'torrent_observation_hourly', record, bucket(record.lastSeenAt, 3600));
      await tx.commit();
    } catch (error) {
      await tx.rollback();
      throw error;
    }
  }

  async search(query, limit, maxNameNgramLen) {
    const result = await this.client.execute({
      sql: `
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
      `,
      args: [buildNameNgram(query, maxNameNgramLen), limit],
    });
    return result.rows.map((row) => ({
      info_hash: row.info_hash_hex,
      name: row.name,
      total_size: Number(row.total_size),
      file_count: Number(row.file_count),
      peer_count: Number(row.peer_count),
      hot_score: Number(row.hot_score),
      last_seen_at: Number(row.last_seen_at),
    }));
  }
}

async function upsertObservation(tx, table, record, bucketStart) {
  await tx.execute({
    sql: `
      INSERT INTO ${table} (
        info_hash, bucket_start, seen_count, max_peer_count, last_source, updated_at
      ) VALUES (?, ?, 1, ?, ?, ?)
      ON CONFLICT(info_hash, bucket_start) DO UPDATE SET
        seen_count = seen_count + 1,
        max_peer_count = MAX(max_peer_count, excluded.max_peer_count),
        last_source = excluded.last_source,
        updated_at = excluded.updated_at
    `,
    args: [
      Buffer.from(record.infoHash, 'hex'),
      bucketStart,
      record.peerCount,
      record.source,
      record.lastSeenAt,
    ],
  });
}

function bucket(ts, size) {
  return ts - (ts % size);
}

const SCHEMA = [
  `
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
  `,
  `
  CREATE TABLE IF NOT EXISTS torrent_files (
    info_hash BLOB NOT NULL,
    file_index INTEGER NOT NULL,
    path TEXT NOT NULL,
    size INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (info_hash, file_index)
  )
  `,
  `
  CREATE VIRTUAL TABLE IF NOT EXISTS torrent_search
  USING fts5(
    info_hash_hex UNINDEXED,
    name_ngram,
    tokenize = 'unicode61'
  )
  `,
  `
  CREATE TABLE IF NOT EXISTS torrent_observation_5m (
    info_hash BLOB NOT NULL,
    bucket_start INTEGER NOT NULL,
    seen_count INTEGER NOT NULL DEFAULT 0,
    max_peer_count INTEGER NOT NULL DEFAULT 0,
    last_source INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (info_hash, bucket_start)
  )
  `,
  `
  CREATE TABLE IF NOT EXISTS torrent_observation_hourly (
    info_hash BLOB NOT NULL,
    bucket_start INTEGER NOT NULL,
    seen_count INTEGER NOT NULL DEFAULT 0,
    max_peer_count INTEGER NOT NULL DEFAULT 0,
    last_source INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (info_hash, bucket_start)
  )
  `,
  'CREATE INDEX IF NOT EXISTS idx_torrents_hot_score ON torrents(hot_score DESC)',
  'CREATE INDEX IF NOT EXISTS idx_torrents_last_seen_at ON torrents(last_seen_at DESC)',
  'CREATE INDEX IF NOT EXISTS idx_torrents_metadata_fetched_at ON torrents(metadata_fetched_at DESC)',
  'CREATE INDEX IF NOT EXISTS idx_obs_5m_bucket ON torrent_observation_5m(bucket_start DESC)',
  'CREATE INDEX IF NOT EXISTS idx_obs_hourly_bucket ON torrent_observation_hourly(bucket_start DESC)',
];

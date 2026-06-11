import { createClient } from '@libsql/client';

const FIVE_MINUTES = 5 * 60;
const ONE_HOUR = 60 * 60;

function toUnixSeconds(date = new Date()) {
  return Math.floor(date.getTime() / 1000);
}

function bucketStart(timestamp, bucketSize) {
  return Math.floor(timestamp / bucketSize) * bucketSize;
}

function normalizeFiles(files = [], fallbackName, fallbackSize) {
  if (Array.isArray(files) && files.length > 0) {
    return files.map((file, index) => ({
      index,
      path: String(file.path || fallbackName || ''),
      size: Number(file.size || 0)
    }));
  }

  return [{
    index: 0,
    path: String(fallbackName || ''),
    size: Number(fallbackSize || 0)
  }];
}

export function createTorrentStorage(options = {}) {
  const url = options.url || process.env.LIBSQL_DATABASE_URL;
  const authToken = options.authToken || process.env.LIBSQL_AUTH_TOKEN;

  if (!url) {
    console.log('libSQL storage disabled: LIBSQL_DATABASE_URL is not set');
    return null;
  }

  const client = createClient({ url, authToken });
  const source = Number(process.env.DHT_LENS_SOURCE || 0);

  return {
    async save(metadata) {
      const infoHashHex = String(metadata.infohash || '').toLowerCase();
      if (!/^[0-9a-f]{40}$/.test(infoHashHex)) {
        return;
      }

      const infoHash = Buffer.from(infoHashHex, 'hex');
      const now = toUnixSeconds();
      const files = normalizeFiles(metadata.files, metadata.name, metadata.size);
      const totalSize = Number(metadata.size || files.reduce((sum, file) => sum + file.size, 0));
      const peerCount = Number(metadata.peerCount || metadata.peer_count || 1);
      const bucket5m = bucketStart(now, FIVE_MINUTES);
      const bucketHourly = bucketStart(now, ONE_HOUR);

      const statements = [
        {
          sql: `
            INSERT INTO torrents (
              info_hash, info_hash_hex, name, total_size, file_count,
              files_stored_count, peer_count, hot_score, source,
              first_seen_at, last_seen_at, metadata_fetched_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
            ON CONFLICT(info_hash) DO UPDATE SET
              name = excluded.name,
              total_size = excluded.total_size,
              file_count = excluded.file_count,
              files_stored_count = excluded.files_stored_count,
              peer_count = max(torrents.peer_count, excluded.peer_count),
              hot_score = torrents.hot_score + 1,
              source = excluded.source,
              last_seen_at = excluded.last_seen_at,
              metadata_fetched_at = excluded.metadata_fetched_at
          `,
          args: [
            infoHash,
            infoHashHex,
            metadata.name || '',
            totalSize,
            files.length,
            files.length,
            peerCount,
            source,
            now,
            now,
            now
          ]
        },
        {
          sql: 'DELETE FROM torrent_files WHERE info_hash = ?',
          args: [infoHash]
        },
        {
          sql: 'DELETE FROM torrent_search WHERE info_hash_hex = ?',
          args: [infoHashHex]
        },
        {
          sql: 'INSERT INTO torrent_search (info_hash_hex, name_ngram) VALUES (?, ?)',
          args: [infoHashHex, metadata.name || '']
        },
        {
          sql: `
            INSERT INTO torrent_observation_5m (
              info_hash, bucket_start, seen_count, max_peer_count, last_source, updated_at
            )
            VALUES (?, ?, 1, ?, ?, ?)
            ON CONFLICT(info_hash, bucket_start) DO UPDATE SET
              seen_count = torrent_observation_5m.seen_count + 1,
              max_peer_count = max(torrent_observation_5m.max_peer_count, excluded.max_peer_count),
              last_source = excluded.last_source,
              updated_at = excluded.updated_at
          `,
          args: [infoHash, bucket5m, peerCount, source, now]
        },
        {
          sql: `
            INSERT INTO torrent_observation_hourly (
              info_hash, bucket_start, seen_count, max_peer_count, last_source, updated_at
            )
            VALUES (?, ?, 1, ?, ?, ?)
            ON CONFLICT(info_hash, bucket_start) DO UPDATE SET
              seen_count = torrent_observation_hourly.seen_count + 1,
              max_peer_count = max(torrent_observation_hourly.max_peer_count, excluded.max_peer_count),
              last_source = excluded.last_source,
              updated_at = excluded.updated_at
          `,
          args: [infoHash, bucketHourly, peerCount, source, now]
        }
      ];

      files.forEach(file => {
        statements.push({
          sql: `
            INSERT INTO torrent_files (info_hash, file_index, path, size)
            VALUES (?, ?, ?, ?)
          `,
          args: [infoHash, file.index, file.path, file.size]
        });
      });

      await client.batch(statements, 'write');
    },

    close() {
      client.close();
    }
  };
}

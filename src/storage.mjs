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

function isReadableName(name) {
  const value = String(name || '').trim();
  if (value.length < 2) {
    return false;
  }

  const codePoints = Array.from(value);
  const replacementCount = (value.match(/\uFFFD/g) || []).length;
  const controlCount = (value.match(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g) || []).length;
  const readableCount = codePoints.filter(char => /[\p{L}\p{N}\p{Script=Han}\p{P}\p{S}\p{Zs}]/u.test(char)).length;

  if (replacementCount > 0 || controlCount > 0) {
    return false;
  }

  return readableCount / codePoints.length >= 0.8;
}

function normalizeInfoHash(infohash) {
  const infoHashHex = String(infohash || '').toLowerCase();
  if (!/^[0-9a-f]{40}$/.test(infoHashHex)) {
    return null;
  }

  return {
    infoHashHex,
    infoHash: Buffer.from(infoHashHex, 'hex')
  };
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
    async observe(data) {
      const normalized = normalizeInfoHash(data.infohash);
      if (!normalized) {
        return;
      }

      const { infoHash, infoHashHex } = normalized;
      const now = toUnixSeconds();
      const peerCount = Number(data.peerCount || data.peer_count || 1);
      const bucket5m = bucketStart(now, FIVE_MINUTES);
      const bucketHourly = bucketStart(now, ONE_HOUR);

      await client.batch([
        {
          sql: `
            INSERT INTO torrents (
              info_hash, info_hash_hex, name, total_size, file_count,
              files_stored_count, peer_count, hot_score, source,
              first_seen_at, last_seen_at, metadata_fetched_at
            )
            VALUES (?, ?, '', 0, 0, 0, ?, 1, ?, ?, ?, NULL)
            ON CONFLICT(info_hash) DO UPDATE SET
              peer_count = max(torrents.peer_count, excluded.peer_count),
              hot_score = torrents.hot_score + 1,
              source = excluded.source,
              last_seen_at = excluded.last_seen_at
          `,
          args: [
            infoHash,
            infoHashHex,
            peerCount,
            source,
            now,
            now
          ]
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
      ], 'write');
    },

    async save(metadata) {
      if (!isReadableName(metadata.name)) {
        return;
      }

      const normalized = normalizeInfoHash(metadata.infohash);
      if (!normalized) {
        return;
      }

      const { infoHash, infoHashHex } = normalized;
      const now = toUnixSeconds();
      const files = normalizeFiles(metadata.files, metadata.name, metadata.size);
      const totalSize = Number(metadata.size || files.reduce((sum, file) => sum + file.size, 0));

      const statements = [
        {
          sql: `
            INSERT INTO torrents (
              info_hash, info_hash_hex, name, total_size, file_count,
              files_stored_count, peer_count, hot_score, source,
              first_seen_at, last_seen_at, metadata_fetched_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 1, 0, ?, ?, ?, ?)
            ON CONFLICT(info_hash) DO UPDATE SET
              name = excluded.name,
              total_size = excluded.total_size,
              file_count = excluded.file_count,
              files_stored_count = excluded.files_stored_count,
              source = excluded.source,
              metadata_fetched_at = excluded.metadata_fetched_at
          `,
          args: [
            infoHash,
            infoHashHex,
            metadata.name || '',
            totalSize,
            files.length,
            files.length,
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

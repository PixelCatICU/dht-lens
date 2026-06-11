# dht-lens

Node.js 26 DHT magnet metadata crawler.

The first version runs as a single-node long-lived service:

- listens on public Mainline DHT
- uses the JS `p2pspider` DHT crawler approach
- returns empty `get_peers` nodes with a token to bias peers toward `announce_peer`
- discovers `info_hash` from public `announce_peer` traffic
- uses recursive `find_node` traffic for node table growth
- fetches metadata from public `announce_peer` addresses
- fetches BEP 9 torrent metadata from peers
- parses `name`, `total_size`, and file list
- prints fetched metadata names
- writes successful metadata only to remote libSQL
- indexes `name_ngram` with libSQL FTS5
- stores 5-minute and hourly trend buckets

It does not store failed metadata fetches.

## Setup

Create `.env`:

```env
LIBSQL_DATABASE_URL=https://your-libsql-host.example.com
LIBSQL_AUTH_TOKEN=replace-with-token
```

Optional runtime settings can be provided through environment variables:

```env
DHT_LISTEN_ADDR=0.0.0.0:6881
DHT_JOIN_INTERVAL_MS=1000
DHT_NEIGHBOR_INTERVAL_MS=1000
DHT_ROUTING_TABLE_MAX_NODES=100000
METADATA_MAX_CONCURRENT_FETCHES=512
METADATA_TIMEOUT_MS=8000
INFO_HASH_QUEUE_SIZE=10000
PRINT_JSONL=true
STORAGE_ENABLED=true
MAX_FILES_PER_TORRENT=2000
MAX_FILE_PATH_LEN=1024
MAX_NAME_NGRAM_LEN=4096
```

## Commands

Run database migrations:

```bash
npm run migrate
```

Start crawler:

```bash
npm start
```

Search by name:

```bash
npm run search -- "周杰伦" --limit 20
```

## CapRover Deploy

This repo includes:

- `captain-definition`
- `Dockerfile`
- `.github/workflows/deploy.yml`
- `scripts/caprover-start.sh`

Set these CapRover app environment variables:

```env
LIBSQL_DATABASE_URL=https://your-libsql-host.example.com
LIBSQL_AUTH_TOKEN=replace-with-token
DHT_LISTEN_ADDR=0.0.0.0:6881
METADATA_MAX_CONCURRENT_FETCHES=512
PRINT_JSONL=true
STORAGE_ENABLED=true
```

The deployment path is server-side Docker build:

1. `caprover deploy` uploads this source tree.
2. CapRover reads `captain-definition`.
3. The server builds `Dockerfile`, runs `npm ci --omit=dev`, and starts Node.

GitHub Actions only runs Node checks; it does not build or push a GHCR image.

The container starts with:

```bash
node /app/js/app.mjs migrate
node /app/js/app.mjs crawl --print
```

CapRover's normal HTTP routing does not automatically publish UDP DHT traffic.
For best DHT listener performance, expose UDP `6881` on the host or run this
service on a host/network where inbound UDP is reachable. This JS crawler relies
heavily on inbound public DHT traffic and recursive `find_node` expansion.

## Database

The schema uses `BLOB(20)` for the primary `info_hash` and keeps `info_hash_hex`
for API output and FTS joins.

Tables:

- `torrents`: successful metadata records
- `torrent_files`: per-file rows, keyed by `(info_hash, file_index)`
- `torrent_search`: FTS5 table for `name_ngram`
- `torrent_observation_5m`: 5-minute trend buckets
- `torrent_observation_hourly`: hourly trend buckets

## Search

`name_ngram` is generated in the application:

- CJK text uses 2-gram and 3-gram tokens
- ASCII words and numbers are lowercased and kept as tokens
- files are not indexed in v1

Example:

```text
周杰伦演唱会.2024.1080p
=> 周杰 杰伦 伦演 演唱 唱会 周杰伦 杰伦演 伦演唱 演唱会 2024 1080p
```

## Current Boundary

This version uses a Node.js crawler based on the supplied `p2pspider` code while
keeping the existing libSQL schema, trend buckets, and `name_ngram` search. The
active knobs that matter most now are `DHT_LISTEN_ADDR`, `DHT_BOOTSTRAP_NODES`,
`DHT_ROUTING_TABLE_MAX_NODES`, `DHT_JOIN_INTERVAL_MS`,
`DHT_NEIGHBOR_INTERVAL_MS`, `METADATA_TIMEOUT_MS`, and
`METADATA_MAX_CONCURRENT_FETCHES`.

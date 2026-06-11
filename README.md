# dht-lens

Rust DHT magnet metadata crawler.

The first version runs as a single-node long-lived service:

- listens on public Mainline DHT
- discovers `info_hash` from `get_peers` and `announce_peer`
- uses `get_peers` traffic for discovery and node table growth
- fetches metadata from public `announce_peer` addresses
- fetches BEP 9 torrent metadata from peers
- parses `name`, `total_size`, and file list
- prints JSONL
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
DHT_LISTEN_ADDR_V6=[::]:6881
DHT_BOOTSTRAP_QUERY_LIMIT=512
DHT_GET_PEERS_PROBE_COUNT=2
DHT_GET_PEERS_PROBE_DEPTH=1
DHT_PACKET_WORKERS=8
DHT_PACKET_QUEUE_SIZE=65536
DHT_NODE_SHARDS=64
DHT_CRAWL_MODE=true
DHT_CRAWL_RESPONSE_NODES=8
DHT_VIRTUAL_NODES=512
DHT_ROUTING_TABLE_MAX_NODES=100000
METADATA_MAX_CONCURRENT_FETCHES=1000
METADATA_MAX_PEERS_PER_HASH=64
METADATA_CONNECT_TIMEOUT_SECS=4
METADATA_TIMEOUT_SECS=8
METADATA_MAX_SIZE_MB=8
INFO_HASH_QUEUE_SIZE=10000
PEER_COLLECT_WINDOW_MS=2000
PRINT_JSONL=true
STORAGE_ENABLED=true
DB_BATCH_SIZE=100
DB_FLUSH_INTERVAL_MS=1000
MAX_FILES_PER_TORRENT=2000
MAX_FILE_PATH_LEN=1024
MAX_NAME_NGRAM_LEN=4096
```

## Commands

Run database migrations:

```bash
cargo run -- migrate
```

Start crawler:

```bash
cargo run -- crawl --print
```

Search by name:

```bash
cargo run -- search "周杰伦" --limit 20
```

Parse a local `.torrent` file for parser verification:

```bash
cargo run -- parse-torrent ./sample.torrent
```

## CapRover Deploy

This repo includes:

- `captain-definition`
- `Dockerfile`
- `.github/workflows/deploy.yml`
- `scripts/caprover-start.sh`
- `scripts/deploy-caprover.sh`

Set these CapRover app environment variables:

```env
LIBSQL_DATABASE_URL=https://your-libsql-host.example.com
LIBSQL_AUTH_TOKEN=replace-with-token
RUST_LOG=dht_lens=info
DHT_LISTEN_ADDR=0.0.0.0:6881
DHT_LISTEN_ADDR_V6=[::]:6881
DHT_BOOTSTRAP_QUERY_LIMIT=512
DHT_GET_PEERS_PROBE_COUNT=2
DHT_GET_PEERS_PROBE_DEPTH=1
DHT_PACKET_WORKERS=8
DHT_PACKET_QUEUE_SIZE=65536
DHT_NODE_SHARDS=64
DHT_CRAWL_MODE=true
DHT_CRAWL_RESPONSE_NODES=8
DHT_VIRTUAL_NODES=512
PRINT_JSONL=true
STORAGE_ENABLED=true
```

Deploy with CapRover CLI:

```bash
export CAPROVER_APP=dht-lens
./scripts/deploy-caprover.sh
```

The preferred deployment path is GitHub Actions:

1. GitHub Actions builds and pushes `ghcr.io/pixelcaticu/dht-lens:latest`.
2. CapRover reads `captain-definition`.
3. CapRover pulls the prebuilt image instead of building Rust on the server.

Set this GitHub Actions secret to auto-deploy after image push:

```text
CAPROVER_DEPLOY_WEBHOOK
```

The GHCR package must be public, or CapRover must be configured with registry
credentials that can pull `ghcr.io/pixelcaticu/dht-lens:latest`.

The container starts with:

```bash
dht-lens migrate
dht-lens crawl --print
```

## CapRover recovery when panel is unreachable

If `https://captain.vlist.cyou` returns 502, run once on the CapRover node:

```bash
export CAPROVER_SERVICE_NAME=captain-captain
export CAPROVER_PANEL_HOST=captain.vlist.cyou
./scripts/fix-caprover-access.sh
```

This sets:

- `DOCKER_API_VERSION=1.40` for the CapRover container (compatible with newer Docker API).
- endpoint mode to `dnsrr` so nginx can resolve `captain-captain` to task IPs instead of failing VIP route.
- a short health check of `/checkhealth`.

CapRover's normal HTTP routing does not automatically publish UDP DHT traffic.
For best DHT listener performance, expose UDP `6881` on the host or run this
service on a host/network where inbound UDP is reachable. The crawler still
performs active `get_peers` queries, but passive DHT discovery benefits from
inbound UDP.

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

This version implements the crawler pipeline and protocol primitives directly.
The DHT side uses a dedicated UDP reader, packet worker queues, a sharded node
table, Crawl-mode `get_peers` responses, optional active `get_peers` probes, and
BEP 11 PeX peer discovery during metadata fetches. A production crawler should
next add stricter token validation, better node scoring, and live metrics for
packet drops, queue depth, metadata success rate, and storage latency.

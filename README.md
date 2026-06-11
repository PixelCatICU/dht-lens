# dht-lens

Rust DHT magnet metadata crawler.

The first version runs as a single-node long-lived service:

- listens on public Mainline DHT
- uses `adysec/dht-spider` as the DHT crawler engine
- runs `adysec` Crawl mode to bias peers toward `announce_peer`
- discovers `info_hash` from `announce_peer`, `get_peers` values, and PeX peers
- uses recursive `find_node` / `get_peers` traffic for node table growth
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
DHT_BOOTSTRAP_QUERY_LIMIT=512
DHT_GET_PEERS_PROBE_DEPTH=1
DHT_CRAWL_MODE=true
DHT_ROUTING_TABLE_MAX_NODES=100000
METADATA_MAX_CONCURRENT_FETCHES=1000
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
DHT_BOOTSTRAP_QUERY_LIMIT=512
DHT_GET_PEERS_PROBE_DEPTH=1
DHT_CRAWL_MODE=true
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
performs recursive DHT queries through `adysec/dht-spider`, but passive DHT
discovery benefits from inbound UDP.

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

This version delegates DHT crawl behavior and BEP 9/10/11 metadata fetching to
`adysec/dht-spider`, while keeping dht-lens responsible for metadata parsing,
libSQL writes, trend buckets, and name search. The active knobs that matter most
now are `DHT_LISTEN_ADDR`, `DHT_BOOTSTRAP_NODES`, `DHT_BOOTSTRAP_QUERY_LIMIT`,
`DHT_CRAWL_MODE`, `DHT_ROUTING_TABLE_MAX_NODES`,
`METADATA_MAX_CONCURRENT_FETCHES`, and `INFO_HASH_QUEUE_SIZE`.

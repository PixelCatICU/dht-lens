# dht-lens

Rust DHT magnet metadata crawler.

The first version runs as a single-node long-lived service:

- listens on public Mainline DHT
- uses vendored crawler code adapted from `adysec/dht-spider`
- discovers public DHT torrent metadata through DHT crawl mode
- fetches BEP 9 torrent metadata from peers
- fetches metadata from public `announce_peer` addresses
- parses `name`, `total_size`, and file list
- logs crawler stats and error/warn events without printing metadata names by default
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
DHT_ROUTING_TABLE_MAX_NODES=100000
METADATA_MAX_CONCURRENT_FETCHES=1000
METADATA_TIMEOUT_SECS=15
INFO_HASH_QUEUE_SIZE=10000
PRINT_JSONL=false
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
cargo run -- crawl
```

Search by name:

```bash
cargo run -- search "周杰伦" --limit 20
```

## Deploy

This repo includes:

- `captain-definition`
- `Dockerfile`
- `.github/workflows/deploy.yml`
- `entrypoint.sh`

GitHub Actions builds the Linux executable on every `main` push and uploads it
to the rolling GitHub Release tag `latest`:

```text
dht-lens-linux-amd64
SHA256SUMS
```

Set these service environment variables:

```env
LIBSQL_DATABASE_URL=https://your-libsql-host.example.com
LIBSQL_AUTH_TOKEN=replace-with-token
DHT_LISTEN_ADDR=0.0.0.0:6881
METADATA_MAX_CONCURRENT_FETCHES=1000
METADATA_TIMEOUT_SECS=15
PRINT_JSONL=false
STORAGE_ENABLED=true
```

CapRover still builds the Docker image, but the Dockerfile does not compile
Rust on the server. It downloads the executable from the `latest` GitHub
Release, verifies `SHA256SUMS`, and copies it into a slim Debian runtime image.

```bash
caprover deploy
```

To make GitHub release first and CapRover deploy second, set this GitHub Actions
secret to a CapRover deployment webhook URL:

```text
CAPROVER_DEPLOY_WEBHOOK_URL
```

If the secret is set, the workflow triggers CapRover only after the `latest`
Release has been published.

For Docker Swarm host-mode UDP deployment, stop the old replica before updating
the image so `6881/udp` is not reserved by both old and new tasks at the same
time.

The container starts with:

```bash
dht-lens migrate
dht-lens crawl
```

CapRover's normal HTTP routing does not automatically publish UDP DHT traffic.
For best DHT listener performance, expose UDP `6881` on the host or run this
service on a host/network where inbound UDP is reachable. This crawler relies
heavily on inbound public DHT traffic and BEP 9 metadata workers.

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

This version uses crawler code adapted from `adysec/dht-spider` for DHT crawl
behavior and BEP 9 metadata fetching, while keeping dht-lens responsible for
libSQL writes, trend buckets, and `name_ngram` search. The active knobs that
matter most now are `DHT_LISTEN_ADDR`, `DHT_ROUTING_TABLE_MAX_NODES`,
`INFO_HASH_QUEUE_SIZE`, `METADATA_TIMEOUT_SECS`, and
`METADATA_MAX_CONCURRENT_FETCHES`.

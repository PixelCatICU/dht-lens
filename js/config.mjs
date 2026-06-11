export function loadConfig() {
  return {
    dht: {
      address: hostFromAddr(env('DHT_LISTEN_ADDR', '0.0.0.0:6881')),
      port: portFromAddr(env('DHT_LISTEN_ADDR', '0.0.0.0:6881')),
      bootstrapNodes: envList('DHT_BOOTSTRAP_NODES', [
        'router.bittorrent.com:6881',
        'dht.transmissionbt.com:6881',
        'router.utorrent.com:6881',
        'router.bitcomet.com:6881',
        'dht.aelitis.com:6881',
        'dht.libtorrent.org:25401',
        'router.bittorrentcloud.com:6881',
        'dht.vuze.com:6881',
        'router.silotis.us:6881',
        'router.ktorrent.com:6881',
        'router.tribler.org:6881',
      ]).map(parseHostPort),
      nodesMaxSize: envInt('DHT_ROUTING_TABLE_MAX_NODES', 200000, 1000, 1000000),
      joinIntervalMs: envInt('DHT_JOIN_INTERVAL_MS', 1000, 200, 60000),
      neighborIntervalMs: envInt('DHT_NEIGHBOR_INTERVAL_MS', 1000, 200, 60000),
    },
    metadata: {
      timeoutMs: envInt('METADATA_TIMEOUT_MS', envInt('METADATA_TIMEOUT_SECS', 8) * 1000, 1000, 120000),
      maxConcurrentFetches: envInt('METADATA_MAX_CONCURRENT_FETCHES', 512, 16, 8192),
      maxMetadataSize: envInt('METADATA_MAX_SIZE_MB', 8, 1, 64) * 1024 * 1024,
    },
    pipeline: {
      printJsonl: envBool('PRINT_JSONL', true),
      statsIntervalMs: envInt('STATS_INTERVAL_MS', 60000, 10000, 600000),
    },
    storage: {
      enabled: envBool('STORAGE_ENABLED', true),
      databaseUrl: process.env.LIBSQL_DATABASE_URL,
      authToken: process.env.LIBSQL_AUTH_TOKEN,
      maxFilesPerTorrent: envInt('MAX_FILES_PER_TORRENT', 2000, 1, 20000),
      maxFilePathLen: envInt('MAX_FILE_PATH_LEN', 1024, 32, 8192),
    },
    search: {
      maxNameNgramLen: envInt('MAX_NAME_NGRAM_LEN', 4096, 128, 100000),
    },
  };
}

function env(key, fallback) {
  return process.env[key] ?? fallback;
}

function envBool(key, fallback) {
  const value = process.env[key];
  if (value === undefined) return fallback;
  return ['1', 'true', 'TRUE', 'yes', 'YES'].includes(value);
}

function envInt(key, fallback, min = Number.MIN_SAFE_INTEGER, max = Number.MAX_SAFE_INTEGER) {
  const parsed = Number.parseInt(process.env[key] ?? `${fallback}`, 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(Math.max(parsed, min), max);
}

function envList(key, fallback) {
  const value = process.env[key];
  if (!value) return fallback;
  return value.split(',').map((item) => item.trim()).filter(Boolean);
}

function hostFromAddr(addr) {
  const idx = addr.lastIndexOf(':');
  return idx === -1 ? addr : addr.slice(0, idx);
}

function portFromAddr(addr) {
  const idx = addr.lastIndexOf(':');
  const value = idx === -1 ? '6881' : addr.slice(idx + 1);
  return Number.parseInt(value, 10);
}

function parseHostPort(value) {
  const idx = value.lastIndexOf(':');
  return {
    address: value.slice(0, idx),
    port: Number.parseInt(value.slice(idx + 1), 10),
  };
}

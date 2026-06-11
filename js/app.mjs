import { DHTSpider } from './p2p/dht.mjs';
import { BTClient } from './p2p/btclient.mjs';
import { loadConfig } from './config.mjs';
import { Source, nowTs } from './model.mjs';
import { LibsqlStore } from './storage.mjs';

const command = process.argv[2] ?? 'crawl';

try {
  if (command === 'migrate') {
    const config = loadConfig();
    const store = new LibsqlStore(config.storage);
    await store.migrate();
    console.log('migration complete');
  } else if (command === 'crawl') {
    await crawl();
  } else if (command === 'search') {
    await search();
  } else {
    throw new Error(`unknown command: ${command}`);
  }
} catch (error) {
  console.error(error);
  process.exitCode = 1;
}

async function crawl() {
  const config = loadConfig();
  const print = process.argv.includes('--print');
  let store = null;
  if (config.storage.enabled) {
    store = new LibsqlStore(config.storage);
    await store.migrate();
    console.log('migration complete');
  }

  const btclient = new BTClient({
    timeoutMs: config.metadata.timeoutMs,
    maxConcurrent: config.metadata.maxConcurrentFetches,
    maxMetadataSize: config.metadata.maxMetadataSize,
  });

  const dht = new DHTSpider(config.dht);
  const stats = {
    metadata: 0,
    stored: 0,
    storeErrors: 0,
  };

  dht.on('listening', (addr) => {
    console.log(JSON.stringify({
      level: 'info',
      event: 'crawler_started',
      engine: 'p2pspider-js',
      address: addr.address,
      port: addr.port,
      nodes_max_size: config.dht.nodesMaxSize,
      metadata_workers: config.metadata.maxConcurrentFetches,
    }));
  });
  dht.on('error', (error) => {
    console.error('dht error', error);
    process.exit(1);
  });
  dht.on('announcePeer', ({ infoHash, address, port }) => {
    btclient.download({ address, port, source: Source.DhtAnnouncePeer }, infoHash);
  });

  btclient.on('complete', async (metadata, infoHash, peer) => {
    stats.metadata += 1;
    const record = buildRecord(metadata, infoHash, peer.source, config);
    if (print || config.pipeline.printJsonl) console.log(record.name);
    console.log(JSON.stringify({
      level: 'info',
      event: 'metadata_fetched',
      info_hash: record.infoHash,
      name: record.name,
      total_size: record.totalSize,
      file_count: record.fileCount,
    }));

    if (!store) return;
    try {
      await store.insertTorrent(record, config.search.maxNameNgramLen);
      stats.stored += 1;
    } catch (error) {
      stats.storeErrors += 1;
      console.error(JSON.stringify({
        level: 'error',
        event: 'store_failed',
        info_hash: record.infoHash,
        error: error.message,
      }));
    }
  });

  setInterval(() => {
    console.log(JSON.stringify({
      level: 'info',
      event: 'crawler_stats',
      dht: dht.stats,
      metadata: btclient.stats,
      stored: stats.stored,
      store_errors: stats.storeErrors,
      metadata_fetched: stats.metadata,
      active_fetches: btclient.active,
      queued_fetches: btclient.queue.length,
    }));
  }, config.pipeline.statsIntervalMs);

  dht.start();
}

async function search() {
  const config = loadConfig();
  const query = process.argv[3];
  const limitArgIndex = process.argv.indexOf('--limit');
  const limit = limitArgIndex === -1 ? 20 : Number.parseInt(process.argv[limitArgIndex + 1], 10);
  if (!query) throw new Error('search query is required');
  const store = new LibsqlStore(config.storage);
  const rows = await store.search(query, limit, config.search.maxNameNgramLen);
  for (const row of rows) console.log(JSON.stringify(row));
}

function buildRecord(metadata, infoHash, source, config) {
  const files = metadata.files
    .slice(0, config.storage.maxFilesPerTorrent)
    .map((file) => ({
      path: truncate(file.path, config.storage.maxFilePathLen),
      size: Number(file.size ?? 0),
    }));
  const now = nowTs();
  return {
    infoHash: infoHash.toString('hex'),
    name: metadata.name,
    totalSize: Number(metadata.totalSize ?? 0),
    fileCount: metadata.files.length,
    filesStoredCount: files.length,
    files,
    peerCount: 1,
    source,
    hotScore: 1,
    firstSeenAt: now,
    lastSeenAt: now,
    metadataFetchedAt: now,
  };
}

function truncate(value, maxLen) {
  return value.length > maxLen ? value.slice(0, maxLen) : value;
}

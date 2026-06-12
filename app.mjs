import dhtLens from './src/index.mjs';
import { createTorrentStorage } from './src/storage.mjs';

const storage = createTorrentStorage();

dhtLens({
  address: '0.0.0.0',
  port: 6881,
  nodesMaxSize: Number(process.env.DHT_LENS_NODES_MAX_SIZE || 1000),
  joinIntervalMs: Number(process.env.DHT_LENS_JOIN_INTERVAL_MS || 5000),
  makeNeighboursIntervalMs: Number(process.env.DHT_LENS_MAKE_NEIGHBOURS_INTERVAL_MS || 3000),
  udpRecvBufferSize: Number(process.env.DHT_LENS_UDP_RECV_BUFFER_SIZE || 4 * 1024 * 1024),
  udpSendBufferSize: Number(process.env.DHT_LENS_UDP_SEND_BUFFER_SIZE || 4 * 1024 * 1024),
  onAnnouncePeer: async data => {
    if (!storage) {
      return;
    }

    try {
      await storage.observe(data);
    } catch (error) {
      console.error('failed to save torrent observation:', error.message);
    }
  }
}, async data => {
  console.log(new Date().getTimezoneOffset() + ' ' + data.name);

  if (!storage) {
    return;
  }

  try {
    await storage.save(data);
  } catch (error) {
    console.error('failed to save torrent metadata:', error.message);
  }
});

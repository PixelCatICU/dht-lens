import dhtLens from './src/index.mjs';
import { createTorrentStorage } from './src/storage.mjs';

const storage = createTorrentStorage();

dhtLens({
  address: '0.0.0.0',
  port: 6881,
  nodesMaxSize: 4000
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

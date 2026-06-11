import EventEmitter from 'node:events';
import net from 'node:net';
import { Wire } from './wire.mjs';
import { parseFiles } from './utils.mjs';

export class BTClient extends EventEmitter {
  constructor(options = {}) {
    super();
    this.timeoutMs = options.timeoutMs ?? 8000;
    this.maxConcurrent = options.maxConcurrent ?? 512;
    this.maxMetadataSize = options.maxMetadataSize ?? 8 * 1024 * 1024;
    this.queue = [];
    this.active = 0;
    this.seen = new Map();
    this.seenTtlMs = 10 * 60 * 1000;
    this.stats = {
      queued: 0,
      started: 0,
      completed: 0,
      failed: 0,
      skipped: 0,
    };
  }

  download(peer, infoHash) {
    const infoHashHex = infoHash.toString('hex');
    const now = Date.now();
    const seenAt = this.seen.get(infoHashHex);
    if (seenAt && now - seenAt < this.seenTtlMs) {
      this.stats.skipped += 1;
      return;
    }
    this.seen.set(infoHashHex, now);
    this.queue.push({ peer, infoHash });
    this.stats.queued += 1;
    this.pump();
  }

  pump() {
    while (this.active < this.maxConcurrent && this.queue.length > 0) {
      const job = this.queue.shift();
      this.active += 1;
      this.stats.started += 1;
      this.fetch(job.peer, job.infoHash)
        .catch(() => {
          this.stats.failed += 1;
        })
        .finally(() => {
          this.active -= 1;
          this.pump();
        });
    }
  }

  async fetch(peer, infoHash) {
    await new Promise((resolve, reject) => {
      const socket = new net.Socket();
      let done = false;
      const finish = (error) => {
        if (done) return;
        done = true;
        socket.destroy();
        if (error) reject(error);
        else resolve();
      };

      socket.setTimeout(this.timeoutMs);
      socket.connect(peer.port, peer.address, () => {
        const wire = new Wire(infoHash, this.maxMetadataSize);
        socket.pipe(wire).pipe(socket);
        wire.on('metadata', (info) => {
          const parsed = parseFiles(info);
          if (parsed) {
            this.stats.completed += 1;
            this.emit('complete', parsed, infoHash, peer);
          }
          finish();
        });
        wire.on('error', finish);
        wire.sendHandshake();
      });
      socket.on('error', finish);
      socket.on('timeout', () => finish(new Error('metadata timeout')));
    });
  }
}

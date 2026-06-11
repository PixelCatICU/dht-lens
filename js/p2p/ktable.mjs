import { randomId } from './utils.mjs';

export class KTable {
  constructor(maxSize = 1000) {
    this.nid = randomId();
    this.nodes = [];
    this.maxSize = maxSize;
    this.seen = new Set();
  }

  push(node) {
    if (this.nodes.length >= this.maxSize) return;
    const key = `${node.address}:${node.port}`;
    if (this.seen.has(key)) return;
    this.seen.add(key);
    this.nodes.push(node);
  }

  drain() {
    const nodes = this.nodes;
    this.nodes = [];
    this.seen.clear();
    return nodes;
  }
}

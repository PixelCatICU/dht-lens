import dgram from 'node:dgram';
import EventEmitter from 'node:events';
import bencode from 'bencode';
import { KTable } from './ktable.mjs';
import { decodeNodes, genNeighborId, isPublicIpv4, randomId } from './utils.mjs';

const TID_LENGTH = 4;
const TOKEN_LENGTH = 2;

export class DHTSpider extends EventEmitter {
  constructor(options = {}) {
    super();
    this.address = options.address ?? '0.0.0.0';
    this.port = options.port ?? 6881;
    this.udp = dgram.createSocket('udp4');
    this.ktable = new KTable(options.nodesMaxSize ?? 1000);
    this.bootstrapNodes = options.bootstrapNodes ?? [];
    this.joinIntervalMs = options.joinIntervalMs ?? 1000;
    this.neighborIntervalMs = options.neighborIntervalMs ?? 1000;
    this.stats = {
      packets: 0,
      nodes: 0,
      getPeers: 0,
      announcePeer: 0,
    };
  }

  start() {
    this.udp.bind(this.port, this.address);
    this.udp.on('listening', () => {
      const addr = this.udp.address();
      this.emit('listening', addr);
      this.joinTimer = setInterval(() => this.joinDHTNetwork(), this.joinIntervalMs);
      this.neighborTimer = setInterval(() => this.makeNeighbours(), this.neighborIntervalMs);
    });
    this.udp.on('message', (msg, rinfo) => this.onMessage(msg, rinfo));
    this.udp.on('error', (error) => this.emit('error', error));
  }

  stop() {
    clearInterval(this.joinTimer);
    clearInterval(this.neighborTimer);
    this.udp.close();
  }

  sendKRPC(msg, rinfo = {}) {
    if (!rinfo.address || rinfo.port >= 65536 || rinfo.port <= 0) return;
    const buf = bencode.encode(msg);
    this.udp.send(buf, 0, buf.length, rinfo.port, rinfo.address);
  }

  onFindNodeResponse(nodes) {
    for (const node of decodeNodes(nodes)) {
      if (
        node.address !== this.address &&
        !node.nid.equals(this.ktable.nid) &&
        node.port < 65536 &&
        node.port > 0 &&
        isPublicIpv4(node.address)
      ) {
        this.ktable.push(node);
        this.stats.nodes += 1;
        this.emit('node', node);
      }
    }
  }

  sendFindNodeRequest(rinfo, nid) {
    const id = nid ? genNeighborId(nid, this.ktable.nid) : this.ktable.nid;
    this.sendKRPC({
      t: randomId().subarray(0, TID_LENGTH),
      y: 'q',
      q: 'find_node',
      a: {
        id,
        target: randomId(),
      },
    }, rinfo);
  }

  joinDHTNetwork() {
    for (const node of this.bootstrapNodes) {
      this.sendFindNodeRequest(node);
    }
  }

  makeNeighbours() {
    for (const node of this.ktable.drain()) {
      this.sendFindNodeRequest({
        address: node.address,
        port: node.port,
      }, node.nid);
    }
  }

  onGetPeersRequest(msg, rinfo) {
    const infohash = msg.a?.info_hash;
    const tid = msg.t;
    const nid = msg.a?.id;
    if (!tid || !Buffer.isBuffer(infohash) || infohash.length !== 20 || !Buffer.isBuffer(nid) || nid.length !== 20) return;

    const token = infohash.subarray(0, TOKEN_LENGTH);
    this.stats.getPeers += 1;
    this.sendKRPC({
      t: tid,
      y: 'r',
      r: {
        id: genNeighborId(infohash, this.ktable.nid),
        nodes: Buffer.alloc(0),
        token,
      },
    }, rinfo);
  }

  onAnnouncePeerRequest(msg, rinfo) {
    const infohash = msg.a?.info_hash;
    const token = msg.a?.token;
    const nid = msg.a?.id;
    const tid = msg.t;
    if (!tid || !Buffer.isBuffer(infohash) || infohash.length !== 20 || !Buffer.isBuffer(token) || !Buffer.isBuffer(nid)) return;
    if (!infohash.subarray(0, TOKEN_LENGTH).equals(token)) return;

    let port = Number(msg.a?.port ?? 0);
    if (msg.a?.implied_port !== undefined && msg.a.implied_port !== 0) port = rinfo.port;
    if (port >= 65536 || port <= 0 || !isPublicIpv4(rinfo.address)) return;

    this.stats.announcePeer += 1;
    this.sendKRPC({
      t: tid,
      y: 'r',
      r: {
        id: genNeighborId(nid, this.ktable.nid),
      },
    }, rinfo);
    this.emit('announcePeer', {
      infoHash: infohash,
      address: rinfo.address,
      port,
    });
  }

  onMessage(raw, rinfo) {
    this.stats.packets += 1;
    let msg;
    try {
      msg = bencode.decode(raw);
    } catch {
      return;
    }
    const y = msg.y?.toString();
    const q = msg.q?.toString();
    if (y === 'r' && msg.r?.nodes) {
      this.onFindNodeResponse(msg.r.nodes);
    } else if (y === 'q' && q === 'get_peers') {
      this.onGetPeersRequest(msg, rinfo);
    } else if (y === 'q' && q === 'announce_peer') {
      this.onAnnouncePeerRequest(msg, rinfo);
    }
  }
}

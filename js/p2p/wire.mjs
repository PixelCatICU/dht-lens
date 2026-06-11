import { Duplex } from 'node:stream';
import crypto from 'node:crypto';
import bencode from 'bencode';
import { randomId } from './utils.mjs';

const BT_RESERVED = Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x01]);
const BT_PROTOCOL = Buffer.from('BitTorrent protocol');
const PIECE_LENGTH = 2 ** 14;
const EXT_HANDSHAKE_ID = 0;
const BT_MSG_ID = 20;

export class Wire extends Duplex {
  constructor(infoHash, maxMetadataSize) {
    super();
    this.infoHash = infoHash;
    this.maxMetadataSize = maxMetadataSize;
    this.buffers = [];
    this.bufferSize = 0;
    this.next = null;
    this.nextSize = 0;
    this.metadata = null;
    this.metadataSize = 0;
    this.numPieces = 0;
    this.utMetadata = null;
    this.havePieces = new Set();
    this.register(1, (buffer) => this.onHandshakeLength(buffer));
  }

  sendHandshake() {
    this.push(Buffer.concat([
      Buffer.from([BT_PROTOCOL.length]),
      BT_PROTOCOL,
      BT_RESERVED,
      this.infoHash,
      randomId(),
    ]));
  }

  onHandshakeLength(buffer) {
    const pstrlen = buffer.readUInt8(0);
    this.register(pstrlen + 48, (handshake) => {
      const protocol = handshake.subarray(0, pstrlen);
      if (!protocol.equals(BT_PROTOCOL)) {
        this.end();
        return;
      }
      const rest = handshake.subarray(pstrlen);
      if (rest[5] & 0x10) this.sendExtHandshake();
      this.register(4, (next) => this.onMessageLength(next));
    });
  }

  onMessageLength(buffer) {
    const length = buffer.readUInt32BE(0);
    if (length > 0) this.register(length, (next) => this.onMessage(next));
    else this.register(4, (next) => this.onMessageLength(next));
  }

  onMessage(buffer) {
    this.register(4, (next) => this.onMessageLength(next));
    if (buffer[0] === BT_MSG_ID) this.onExtended(buffer.readUInt8(1), buffer.subarray(2));
  }

  onExtended(ext, payload) {
    if (ext === 0) {
      try {
        this.onExtHandshake(bencode.decode(payload));
      } catch {
        // Ignore malformed extension handshakes.
      }
    } else {
      this.onPiece(payload);
    }
  }

  onExtHandshake(extHandshake) {
    const metadataSize = Number(extHandshake.metadata_size ?? 0);
    const utMetadata = Number(extHandshake.m?.ut_metadata ?? 0);
    if (!metadataSize || !utMetadata || metadataSize > this.maxMetadataSize) return;

    this.metadataSize = metadataSize;
    this.numPieces = Math.ceil(metadataSize / PIECE_LENGTH);
    this.utMetadata = utMetadata;
    this.metadata = Buffer.alloc(metadataSize);
    for (let piece = 0; piece < this.numPieces; piece += 1) {
      this.requestPiece(piece);
    }
  }

  requestPiece(piece) {
    this.sendMessage(Buffer.concat([
      Buffer.from([BT_MSG_ID]),
      Buffer.from([this.utMetadata]),
      bencode.encode({ msg_type: 0, piece }),
    ]));
  }

  sendExtHandshake() {
    this.sendMessage(Buffer.concat([
      Buffer.from([BT_MSG_ID]),
      Buffer.from([EXT_HANDSHAKE_ID]),
      bencode.encode({ m: { ut_metadata: 1 } }),
    ]));
  }

  sendMessage(msg) {
    const len = Buffer.allocUnsafe(4);
    len.writeUInt32BE(msg.length, 0);
    this.push(Buffer.concat([len, msg]));
  }

  onPiece(piece) {
    let dict;
    let trailerIndex;
    try {
      trailerIndex = findBencodeEnd(piece);
      dict = bencode.decode(piece.subarray(0, trailerIndex));
    } catch {
      return;
    }
    if (Number(dict.msg_type) !== 1) return;
    const pieceNo = Number(dict.piece);
    const trailer = piece.subarray(trailerIndex);
    if (!this.metadata || !Number.isInteger(pieceNo) || pieceNo < 0 || pieceNo >= this.numPieces) return;
    if (trailer.length > PIECE_LENGTH) return;

    trailer.copy(this.metadata, pieceNo * PIECE_LENGTH);
    this.havePieces.add(pieceNo);
    if (this.havePieces.size === this.numPieces) this.onDone();
  }

  onDone() {
    const digest = crypto.createHash('sha1').update(this.metadata).digest();
    if (!digest.equals(this.infoHash)) return;
    let info;
    try {
      info = bencode.decode(this.metadata);
    } catch {
      return;
    }
    this.emit('metadata', info, this.infoHash);
  }

  register(size, next) {
    this.nextSize = size;
    this.next = next;
  }

  _write(buf, _encoding, next) {
    this.bufferSize += buf.length;
    this.buffers.push(buf);
    while (this.bufferSize >= this.nextSize) {
      const buffer = Buffer.concat(this.buffers);
      const current = buffer.subarray(0, this.nextSize);
      const rest = buffer.subarray(this.nextSize);
      this.bufferSize -= this.nextSize;
      this.buffers = rest.length ? [rest] : [];
      this.next(current);
    }
    next();
  }

  _read() {}
}

function findBencodeEnd(buffer) {
  let i = 0;
  const parse = () => {
    const ch = String.fromCharCode(buffer[i]);
    if (ch === 'i') {
      const end = buffer.indexOf(0x65, i);
      if (end === -1) throw new Error('invalid int');
      i = end + 1;
      return;
    }
    if (ch === 'l') {
      i += 1;
      while (buffer[i] !== 0x65) parse();
      i += 1;
      return;
    }
    if (ch === 'd') {
      i += 1;
      while (buffer[i] !== 0x65) {
        parse();
        parse();
      }
      i += 1;
      return;
    }
    if (/[0-9]/.test(ch)) {
      const colon = buffer.indexOf(0x3a, i);
      if (colon === -1) throw new Error('invalid bytes');
      const len = Number.parseInt(buffer.subarray(i, colon).toString(), 10);
      i = colon + 1 + len;
      return;
    }
    throw new Error('invalid bencode');
  };
  parse();
  return i;
}

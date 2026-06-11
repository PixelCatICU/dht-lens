import crypto from 'node:crypto';
import iconv from 'iconv-lite';

export function randomId() {
  return crypto.createHash('sha1').update(crypto.randomBytes(20)).digest();
}

export function decodeNodes(data) {
  const nodes = [];
  for (let i = 0; i + 26 <= data.length; i += 26) {
    nodes.push({
      nid: data.subarray(i, i + 20),
      address: `${data[i + 20]}.${data[i + 21]}.${data[i + 22]}.${data[i + 23]}`,
      port: data.readUInt16BE(i + 24),
    });
  }
  return nodes;
}

export function genNeighborId(target, nid) {
  return Buffer.concat([target.subarray(0, 10), nid.subarray(10)]);
}

export function decodeText(value) {
  if (!Buffer.isBuffer(value)) return String(value ?? '');
  const encodings = ['utf8', 'gb18030', 'big5', 'shift_jis', 'euc-kr', 'windows-1251', 'latin1'];
  for (const encoding of encodings) {
    try {
      const decoded = iconv.decode(value, encoding);
      if (decoded && !decoded.includes('\uFFFD')) return decoded;
    } catch {
      // Try the next encoding.
    }
  }
  return value.toString('utf8');
}

export function isPublicIpv4(address) {
  const parts = address.split('.').map((part) => Number.parseInt(part, 10));
  if (parts.length !== 4 || parts.some((part) => !Number.isInteger(part) || part < 0 || part > 255)) return false;
  const [a, b] = parts;
  if (a === 0 || a === 10 || a === 127 || a >= 224) return false;
  if (a === 100 && b >= 64 && b <= 127) return false;
  if (a === 169 && b === 254) return false;
  if (a === 172 && b >= 16 && b <= 31) return false;
  if (a === 192 && b === 168) return false;
  return true;
}

export function parseFiles(info) {
  const name = decodeText(info['utf-8.name'] ?? info.name);
  if (!name) return null;

  if (Array.isArray(info.files)) {
    let totalSize = 0;
    const files = [];
    for (const item of info.files) {
      const size = Number(item.length ?? 0);
      const pathParts = Array.isArray(item.path) ? item.path : [];
      const path = pathParts.map(decodeText).filter(Boolean).join('/');
      if (!path) continue;
      totalSize += size;
      files.push({ path, size });
    }
    return { name, totalSize, files };
  }

  const totalSize = Number(info.length ?? 0);
  return {
    name,
    totalSize,
    files: [{ path: name, size: totalSize }],
  };
}

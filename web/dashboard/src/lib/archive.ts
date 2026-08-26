const blockSize = 512;
const encoder = new TextEncoder();

export function buildEvidenceBundle(evidence: ArrayBuffer, sha256: string): ArrayBuffer {
  if (!/^[0-9a-f]{64}$/.test(sha256)) {
    throw new Error('A valid SHA-256 checksum is required for the evidence bundle.');
  }
  const entries = [
    { name: 'agentos-aims-evidence.json', body: new Uint8Array(evidence) },
    { name: 'agentos-aims-evidence.json.sha256', body: encoder.encode(`${sha256}  agentos-aims-evidence.json\n`) }
  ];
  const size = entries.reduce((total, entry) => total + blockSize + paddedSize(entry.body.byteLength), blockSize * 2);
  const archive = new ArrayBuffer(size);
  const output = new Uint8Array(archive);
  let offset = 0;
  for (const entry of entries) {
    output.set(tarHeader(entry.name, entry.body.byteLength), offset);
    offset += blockSize;
    output.set(entry.body, offset);
    offset += paddedSize(entry.body.byteLength);
  }
  return archive;
}

function tarHeader(name: string, size: number): Uint8Array {
  const nameBytes = encoder.encode(name);
  if (nameBytes.byteLength === 0 || nameBytes.byteLength > 100 || size < 0 || size > 0o77777777777) {
    throw new Error('Evidence bundle entry is outside the supported tar bounds.');
  }
  const header = new Uint8Array(blockSize);
  header.set(nameBytes, 0);
  writeOctal(header, 100, 8, 0o644);
  writeOctal(header, 108, 8, 0);
  writeOctal(header, 116, 8, 0);
  writeOctal(header, 124, 12, size);
  writeOctal(header, 136, 12, 0);
  header.fill(0x20, 148, 156);
  header[156] = 0x30;
  header.set(encoder.encode('ustar\0'), 257);
  header.set(encoder.encode('00'), 263);
  const checksum = header.reduce((sum, value) => sum + value, 0);
  header.set(encoder.encode(checksum.toString(8).padStart(6, '0')), 148);
  header[154] = 0;
  header[155] = 0x20;
  return header;
}

function writeOctal(target: Uint8Array, offset: number, width: number, value: number): void {
  const encoded = value.toString(8).padStart(width - 1, '0');
  if (encoded.length >= width) throw new Error('Evidence bundle value exceeds its tar field.');
  target.set(encoder.encode(encoded), offset);
  target[offset + width - 1] = 0;
}

function paddedSize(size: number): number {
  return Math.ceil(size / blockSize) * blockSize;
}

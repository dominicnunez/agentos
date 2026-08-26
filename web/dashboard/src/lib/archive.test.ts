import assert from 'node:assert/strict';
import test from 'node:test';

import { buildEvidenceBundle } from './archive.ts';

const decoder = new TextDecoder();
const evidence = new TextEncoder().encode('{"schema_version":"agentos.aims.evidence.v1"}\n');
const checksum = '7d0afe91bda5a5fecb0cb23901f0f8af0b891eb24ca1807035cda122e68bd539';

test('bundles exact evidence bytes and their detached checksum in one tar', () => {
  const archive = new Uint8Array(buildEvidenceBundle(evidence.buffer, checksum));
  const first = entryAt(archive, 0);
  assert.equal(first.name, 'agentos-aims-evidence.json');
  assert.deepEqual(first.body, evidence);
  assertHeaderChecksum(archive.subarray(0, 512));

  const secondOffset = 512 + Math.ceil(evidence.byteLength / 512) * 512;
  const second = entryAt(archive, secondOffset);
  assert.equal(second.name, 'agentos-aims-evidence.json.sha256');
  assert.equal(decoder.decode(second.body), `${checksum}  agentos-aims-evidence.json\n`);
  assertHeaderChecksum(archive.subarray(secondOffset, secondOffset + 512));
  assert.ok(archive.subarray(secondOffset + 1024).every((value) => value === 0));
});

test('rejects a bundle without a canonical checksum', () => {
  assert.throws(() => buildEvidenceBundle(evidence.buffer, ''), /valid SHA-256 checksum/);
  assert.throws(() => buildEvidenceBundle(evidence.buffer, checksum.toUpperCase()), /valid SHA-256 checksum/);
});

function entryAt(archive: Uint8Array, offset: number): { name: string; body: Uint8Array } {
  const header = archive.subarray(offset, offset + 512);
  const nameEnd = header.indexOf(0);
  const name = decoder.decode(header.subarray(0, nameEnd));
  const size = Number.parseInt(decoder.decode(header.subarray(124, 135)).replaceAll('\0', '').trim(), 8);
  return { name, body: archive.slice(offset + 512, offset + 512 + size) };
}

function assertHeaderChecksum(header: Uint8Array): void {
  const recorded = Number.parseInt(decoder.decode(header.subarray(148, 154)), 8);
  const normalized = header.slice();
  normalized.fill(0x20, 148, 156);
  assert.equal(recorded, normalized.reduce((sum, value) => sum + value, 0));
}

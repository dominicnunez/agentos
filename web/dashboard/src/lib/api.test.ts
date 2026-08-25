import assert from 'node:assert/strict';
import test from 'node:test';

import { verifySHA256 } from './api.ts';

const evidence = new TextEncoder().encode('{"schema_version":"agentos.aims.evidence.v1"}\n');
const checksum = '7d0afe91bda5a5fecb0cb23901f0f8af0b891eb24ca1807035cda122e68bd539';

test('verifies the checksum over exact evidence bytes', async () => {
  await verifySHA256(evidence.buffer, checksum);
});

test('rejects missing, malformed, and mismatched checksums', async () => {
  await assert.rejects(verifySHA256(evidence.buffer, ''), /valid SHA-256 checksum/);
  await assert.rejects(verifySHA256(evidence.buffer, checksum.toUpperCase()), /valid SHA-256 checksum/);
  await assert.rejects(verifySHA256(evidence.buffer, '0'.repeat(64)), /did not match/);
});

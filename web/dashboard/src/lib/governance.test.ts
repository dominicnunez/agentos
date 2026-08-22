import assert from 'node:assert/strict';
import test from 'node:test';
import { APIError, isDashboardSessionRejection } from './api.ts';
import { confirmationMessageID, hasRetryableIntentConfirmation, loadAllCompletionReviews, safeDisplay, sameCompletionContract, snapshotCompletionEvidence, validateArtifactSelections, validateCompletionFields } from './governance.ts';
import type { CompletionReview } from './types';

function review(id: string): CompletionReview {
  return { review_id: id, task_id: `task-${id}`, fingerprint: id, state: 'PENDING', objective: id, criteria: [], evidence_refs: [], updated_at: '2026-01-01T00:00:00Z' };
}

test('loads every bounded completion-review page', async () => {
  const seen: string[] = [];
  const reviews = await loadAllCompletionReviews(async (after) => {
    seen.push(after);
    return after ? { reviews: [review('2')] } : { reviews: [review('1')], next_after: 'cursor-1' };
  });
  assert.deepEqual(seen, ['', 'cursor-1']);
  assert.deepEqual(reviews.map((item) => item.review_id), ['1', '2']);
});

test('rejects repeated completion-review cursors', async () => {
  await assert.rejects(() => loadAllCompletionReviews(async () => ({ reviews: [], next_after: 'same' })), /repeated cursor/);
});

test('rejects artifact counts before evidence upload', () => {
  const requirements = [{ role: 'report', media_types: ['text/plain'], min_count: 2, max_count: 2 }];
  assert.match(validateArtifactSelections(requirements, { report: [{ name: 'one.txt', size: 1, type: 'text/plain' }] }) ?? '', /requires 2 to 2 files/);
  assert.equal(validateArtifactSelections(requirements, { report: [{ name: 'one.txt', size: 1, type: 'text/plain' }, { name: 'two.txt', size: 1, type: 'text/plain' }] }), null);
});

test('validates completion fields by UTF-8 byte length', () => {
  const requirement = [{ name: 'answer', min_bytes: 1, max_bytes: 4 }];
  assert.equal(validateCompletionFields(requirement, { answer: '😀' }), null);
  assert.match(validateCompletionFields(requirement, { answer: '😀a' }) ?? '', /UTF-8 bytes/);
});

test('removes control and direction-format characters from governed display text', () => {
  assert.equal(safeDisplay('safe\u202Etxt\nnext\titem'), 'safetxt\nnext\titem');
});

test('derives a stable confirmation message identity from the reviewed fingerprint', () => {
  const fingerprint = 'a'.repeat(64);
  assert.equal(confirmationMessageID(fingerprint), `confirmation-${fingerprint}`);
  assert.throws(() => confirmationMessageID('not-a-fingerprint'), /invalid/);
});

test('leaves artifact media validation to content-derived server checks', () => {
  const requirement = [{ role: 'report', media_types: ['application/pdf'], min_count: 1, max_count: 1 }];
  assert.equal(validateArtifactSelections(requirement, { report: [{ name: 'report.txt', size: 10, type: 'text/plain' }] }), null);
});

test('clears stored dashboard sessions only after an explicit credential rejection', () => {
  assert.equal(isDashboardSessionRejection(new APIError(401, 'expired')), true);
  assert.equal(isDashboardSessionRejection(new APIError(503, 'unavailable')), false);
  assert.equal(isDashboardSessionRejection(new TypeError('network failure')), false);
});

test('preserves evidence only for the same durable completion contract', () => {
  const contract = { task_id: 'task-1', task_version: 2, criteria: [] };
  assert.equal(sameCompletionContract(contract, { ...contract }), true);
  assert.equal(sameCompletionContract(contract, { ...contract, task_version: 3 }), false);
  assert.equal(sameCompletionContract(contract, undefined), false);
});

test('snapshots completion evidence before asynchronous encoding', () => {
  const fields = { answer: 'reviewed' };
  const first = { name: 'first.txt', size: 1, type: 'text/plain' };
  const files = { report: [first] };
  const snapshot = snapshotCompletionEvidence(fields, files);
  fields.answer = 'changed';
  files.report.push({ name: 'second.txt', size: 1, type: 'text/plain' });
  assert.deepEqual(snapshot.fields, { answer: 'reviewed' });
  assert.deepEqual(snapshot.files, { report: [first] });
});

test('retains only a complete confirmation retry state after active-intake lookup misses', () => {
  const retry = { task_id: '', conversation_id: 'conversation-1', state: 'AWAITING_CONFIRMATION', intent: { objective: 'work', mode: 'STANDARD', version: 1, fingerprint: 'a'.repeat(64) } };
  assert.equal(hasRetryableIntentConfirmation(retry), true);
  assert.equal(hasRetryableIntentConfirmation({ ...retry, state: 'WORKING' }), false);
  assert.equal(hasRetryableIntentConfirmation({ ...retry, intent: undefined }), false);
});

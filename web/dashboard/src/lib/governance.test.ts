import assert from 'node:assert/strict';
import test from 'node:test';
import { loadAllCompletionReviews, safeDisplay, validateArtifactSelections, validateCompletionFields } from './governance.ts';
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
  assert.equal(safeDisplay('safe\u202Etxt\n'), 'safetxt');
});

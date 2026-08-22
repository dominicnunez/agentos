import assert from 'node:assert/strict';
import test from 'node:test';
import { APIError, emptyJSONPost, isDashboardSessionRejection } from './api.ts';
import { approvalRetryBinding, completionReviewFeedback, confirmationMessageID, confirmationRetryBinding, discardConfirmationRetry, hasRetryableIntentConfirmation, loadAllCompletionReviews, matchesConfirmationRetry, parseApprovalRetryBinding, parseConfirmationRetryBinding, parseReviewRetryBinding, replayApprovalDecision, replayCompletionReviewDecision, reviewRetryBinding, safeDisplay, sameCompletionContract, snapshotCompletionEvidence, terminalApproval, terminalCompletionReview, validateArtifactSelections, validateCompletionFields } from './governance.ts';
import type { Approval, CompletionReview } from './types';

function review(id: string): CompletionReview {
  return { review_id: id, task_id: `task-${id}`, fingerprint: id, state: 'PENDING', objective: id, criteria: [], evidence_refs: [], updated_at: '2026-01-01T00:00:00Z' };
}

function approval(status: string): Approval {
  return { approval_id: 'approval-1', task_id: 'task-1', action: 'publish', resource: 'release', scope: 'public', canonical_effect_descriptor: 'publish release', effect_arguments: {}, boundary: 'PUBLIC_EXTERNAL', risk: 'HIGH', urgency: 'NORMAL', effect_fingerprint: 'a'.repeat(64), status, single_use: true, created_at: '2026-01-01T00:00:00Z' };
}

test('reconciles an uncertain approval before another decision can replace it', async () => {
  const calls: { path: string; body: string }[] = [];
  const responses = [approval('PENDING_DECISION'), approval('APPROVED')];
  const request = async <T>(path: string, options?: RequestInit): Promise<T> => {
    calls.push({ path, body: String(options?.body ?? '') });
    return responses.shift() as T;
  };
  const recorded = await replayApprovalDecision(request, approval('ACKNOWLEDGED'), 'APPROVE');
  assert.equal(recorded.status, 'APPROVED');
  assert.deepEqual(calls.map((call) => call.path), [
    '/api/v1/control/approvals/approval-1/begin',
    '/api/v1/control/approvals/approval-1/decision'
  ]);
  assert.match(calls[1].body, /"decision":"APPROVE"/);
  assert.equal(terminalApproval(recorded), true);
});

test('replays a matching durable completion decision before clearing recovery', async () => {
  const current = { ...review('review-1'), fingerprint: 'b'.repeat(64), state: 'REVISE', feedback: 'keep exact bytes', reviewer_id: 'operator-1' };
  const binding = reviewRetryBinding(current.task_id, current.review_id, current.fingerprint, 'REVISE', current.feedback);
  const calls: { path: string; body: string }[] = [];
  const request = async <T>(path: string, options?: RequestInit): Promise<T> => {
    calls.push({ path, body: String(options?.body ?? '') });
    return current as T;
  };
  const recorded = await replayCompletionReviewDecision(request, current, binding);
  assert.equal(terminalCompletionReview(recorded), true);
  assert.equal(calls[0].path, `/api/v1/user/reviews/${current.task_id}`);
  assert.deepEqual(JSON.parse(calls[0].body), { review_id: current.review_id, fingerprint: current.fingerprint, decision: 'REVISE', feedback: 'keep exact bytes' });
});

test('retains confirmation recovery for downstream durable-work conflicts', () => {
  assert.equal(discardConfirmationRetry(409), false);
  assert.equal(discardConfirmationRetry(412), true);
  assert.equal(discardConfirmationRetry(404), true);
});

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

test('preserves feedback for rejection and revision decisions only', () => {
	assert.equal(completionReviewFeedback('REJECT', 'candidate omitted evidence'), 'candidate omitted evidence');
	assert.equal(completionReviewFeedback('REVISE', 'supply the report'), 'supply the report');
	assert.equal(completionReviewFeedback('APPROVE', 'not applicable'), '');
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

test('marks bodyless recovery mutations as exact JSON', () => {
  const options = emptyJSONPost();
  assert.equal(options.method, 'POST');
  assert.equal(new Headers(options.headers).get('Content-Type'), 'application/json');
  assert.equal(options.body, undefined);
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

test('round-trips a minimal pending confirmation binding', () => {
  const binding = confirmationRetryBinding('conversation-case@partner', 'b'.repeat(64));
  assert.deepEqual(parseConfirmationRetryBinding(JSON.stringify(binding)), binding);
  const view = { task_id: '', conversation_id: binding.conversation_id, state: 'AWAITING_CONFIRMATION', intent: { objective: 'work', mode: 'STANDARD', version: 1, fingerprint: binding.fingerprint } };
  assert.equal(matchesConfirmationRetry(view, binding), true);
  assert.equal(matchesConfirmationRetry(view, { ...binding, fingerprint: 'c'.repeat(64) }), false);
  assert.equal(matchesConfirmationRetry(view, null), false);
});

test('rejects tampered pending confirmation bindings', () => {
  assert.throws(() => parseConfirmationRetryBinding('{'), /invalid/);
  assert.throws(() => parseConfirmationRetryBinding(JSON.stringify({ conversation_id: 'conversation-1', fingerprint: 'b'.repeat(64), authority: 'admin' })), /invalid/);
  assert.throws(() => parseConfirmationRetryBinding(JSON.stringify({ conversation_id: 'conversation\nforged', fingerprint: 'b'.repeat(64) })), /invalid/);
  assert.throws(() => parseConfirmationRetryBinding(JSON.stringify({ conversation_id: 'conversation-1', fingerprint: 'not-a-fingerprint' })), /invalid/);
});

test('round-trips strict approval and review retry bindings', () => {
  const approval = approvalRetryBinding('approval-1', 'a'.repeat(64), 'APPROVE');
  assert.deepEqual(parseApprovalRetryBinding(JSON.stringify(approval)), approval);
  const review = reviewRetryBinding('task-1', 'review-1', 'b'.repeat(64), 'REVISE', '  exact feedback\n');
  assert.deepEqual(parseReviewRetryBinding(JSON.stringify(review)), review);
});

test('rejects authority-shaped and internally inconsistent decision retries', () => {
  assert.throws(() => parseApprovalRetryBinding(JSON.stringify({ approval_id: 'approval-1', fingerprint: 'a'.repeat(64), decision: 'APPROVE', role: 'owner' })), /invalid/);
  assert.throws(() => parseReviewRetryBinding(JSON.stringify({ task_id: 'task-1', review_id: 'review-1', fingerprint: 'b'.repeat(64), decision: 'APPROVE', feedback: 'hidden' })), /invalid/);
  assert.throws(() => parseReviewRetryBinding(JSON.stringify({ task_id: 'task-1', review_id: 'review-1', fingerprint: 'b'.repeat(64), decision: 'REVISE', feedback: '   ' })), /invalid/);
});

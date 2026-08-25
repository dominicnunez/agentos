import type { Approval, CompletionContract, CompletionReview, CompletionReviewPage, TaskView } from './types';

const maximumReviewPages = 100;
const maximumArtifactBytes = 16 * 1024 * 1024;
const maximumCompletionBytes = 32 * 1024 * 1024;

export type ArtifactSelection = { name: string; size: number; type: string };
export type ArtifactRequirement = { role: string; media_types: string[]; min_count: number; max_count: number };
export type FieldRequirement = { name: string; min_bytes: number; max_bytes: number };
export type ConfirmationRetryBinding = { conversation_id: string; fingerprint: string };
export type ApprovalRetryBinding = { approval_id: string; fingerprint: string; decision: 'APPROVE' | 'DENY' };
export type ReviewRetryBinding = { task_id: string; review_id: string; fingerprint: string; decision: 'APPROVE' | 'REJECT' | 'REVISE'; feedback: string };
export type StrategyRetryBinding = {
  request_id: string;
  mission_id: string;
  mission_statement: string;
  goal_id: string;
  goal_objective: string;
  goal_mode: 'TARGET' | 'CONTINUOUS';
  success_criteria: string[];
};
export type DashboardRequest = <T>(path: string, options?: RequestInit) => Promise<T>;

export function discardConfirmationRetry(status: number): boolean {
  return status === 400 || status === 404 || status === 412;
}

export function terminalApproval(approval: Approval): boolean {
  return approval.status === 'APPROVED' || approval.status === 'DENIED';
}

export function terminalCompletionReview(review: CompletionReview): boolean {
  return review.state !== 'PENDING';
}

export async function replayApprovalDecision(request: DashboardRequest, current: Approval, decision: 'APPROVE' | 'DENY'): Promise<Approval> {
  const terminal = (approval: Approval): Approval | null => {
    if (!terminalApproval(approval)) return null;
    const recorded = approval.status === 'APPROVED' ? 'APPROVE' : 'DENY';
    if (recorded !== decision) throw new Error('The approval has a different durable decision.');
    return approval;
  };
  const recorded = terminal(current);
  if (recorded) return recorded;
  const body = JSON.stringify({ effect_fingerprint: current.effect_fingerprint });
  if (current.status === 'PENDING' || current.status === 'NOTIFIED') {
    current = await request<Approval>(`/api/v1/control/approvals/${encodeURIComponent(current.approval_id)}/acknowledge`, { method: 'POST', body });
  }
  const afterAcknowledge = terminal(current);
  if (afterAcknowledge) return afterAcknowledge;
  if (current.status === 'ACKNOWLEDGED') {
    current = await request<Approval>(`/api/v1/control/approvals/${encodeURIComponent(current.approval_id)}/begin`, { method: 'POST', body });
  }
  const afterBegin = terminal(current);
  if (afterBegin) return afterBegin;
  if (current.status !== 'PENDING_DECISION') throw new Error('The approval is not in a decision-ready state.');
  return request<Approval>(`/api/v1/control/approvals/${encodeURIComponent(current.approval_id)}/decision`, {
    method: 'POST',
    body: JSON.stringify({ effect_fingerprint: current.effect_fingerprint, decision })
  });
}

export function replayCompletionReviewDecision(request: DashboardRequest, current: CompletionReview, binding: ReviewRetryBinding): Promise<CompletionReview> {
  return request<CompletionReview>(`/api/v1/user/reviews/${encodeURIComponent(current.task_id)}`, {
    method: 'POST',
    body: JSON.stringify({
      review_id: current.review_id,
      fingerprint: current.fingerprint,
      decision: binding.decision,
      ...(binding.decision === 'APPROVE' ? {} : { feedback: binding.feedback })
    })
  });
}

export function confirmationRetryBinding(conversationID: string, fingerprint: string): ConfirmationRetryBinding {
  if (!validBoundaryIdentifier(conversationID)) throw new Error('Intent conversation identity is invalid.');
  confirmationMessageID(fingerprint);
  return { conversation_id: conversationID, fingerprint };
}

export function parseConfirmationRetryBinding(value: string | null): ConfirmationRetryBinding | null {
  if (!value) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    throw new Error('Stored Intent confirmation retry is invalid.');
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) throw new Error('Stored Intent confirmation retry is invalid.');
  const record = parsed as Record<string, unknown>;
  if (Object.keys(record).sort().join(',') !== 'conversation_id,fingerprint' || typeof record.conversation_id !== 'string' || typeof record.fingerprint !== 'string') {
    throw new Error('Stored Intent confirmation retry is invalid.');
  }
  return confirmationRetryBinding(record.conversation_id, record.fingerprint);
}

export function approvalRetryBinding(approvalID: string, fingerprint: string, decision: 'APPROVE' | 'DENY'): ApprovalRetryBinding {
  if (!validBoundaryIdentifier(approvalID) || !validFingerprint(fingerprint) || (decision !== 'APPROVE' && decision !== 'DENY')) throw new Error('Approval retry binding is invalid.');
  return { approval_id: approvalID, fingerprint, decision };
}

export function parseApprovalRetryBinding(value: string | null): ApprovalRetryBinding | null {
  const record = parseStrictRecord(value, ['approval_id', 'decision', 'fingerprint'], 'approval');
  if (!record) return null;
  if (typeof record.approval_id !== 'string' || typeof record.fingerprint !== 'string' || (record.decision !== 'APPROVE' && record.decision !== 'DENY')) throw new Error('Stored approval retry is invalid.');
  return approvalRetryBinding(record.approval_id, record.fingerprint, record.decision);
}

export function reviewRetryBinding(taskID: string, reviewID: string, fingerprint: string, decision: 'APPROVE' | 'REJECT' | 'REVISE', feedback: string): ReviewRetryBinding {
  if (!validBoundaryIdentifier(taskID) || !validBoundaryIdentifier(reviewID) || !validFingerprint(fingerprint) || !['APPROVE', 'REJECT', 'REVISE'].includes(decision)) throw new Error('Completion-review retry binding is invalid.');
  const bytes = new TextEncoder().encode(feedback).byteLength;
  if (bytes > 64 * 1024 || (decision === 'REVISE' && !feedback.trim()) || (decision === 'APPROVE' && feedback !== '')) throw new Error('Completion-review retry binding is invalid.');
  return { task_id: taskID, review_id: reviewID, fingerprint, decision, feedback };
}

export function parseReviewRetryBinding(value: string | null): ReviewRetryBinding | null {
  const record = parseStrictRecord(value, ['decision', 'feedback', 'fingerprint', 'review_id', 'task_id'], 'completion-review');
  if (!record) return null;
  if (typeof record.task_id !== 'string' || typeof record.review_id !== 'string' || typeof record.fingerprint !== 'string' || typeof record.feedback !== 'string' || (record.decision !== 'APPROVE' && record.decision !== 'REJECT' && record.decision !== 'REVISE')) throw new Error('Stored completion-review retry is invalid.');
  return reviewRetryBinding(record.task_id, record.review_id, record.fingerprint, record.decision, record.feedback);
}

export function strategyRetryBinding(
  requestID: string,
  missionID: string,
  missionStatement: string,
  goalID: string,
  goalObjective: string,
  goalMode: 'TARGET' | 'CONTINUOUS',
  successCriteria: string[]
): StrategyRetryBinding {
  const criteria = [...successCriteria];
  const total = criteria.reduce((bytes, criterion) => bytes + new TextEncoder().encode(criterion).byteLength, 0);
  if (!validStrategyIdentifier(requestID) || !missionID.startsWith('mission-') || !validStrategyIdentifier(missionID) ||
    !goalID.startsWith('goal-') || !validStrategyIdentifier(goalID) || missionID === goalID ||
    !validStrategyText(missionStatement, 16 * 1024) || !validStrategyText(goalObjective, 16 * 1024) ||
    (goalMode !== 'TARGET' && goalMode !== 'CONTINUOUS') || criteria.length < 1 || criteria.length > 32 || total > 64 * 1024 ||
    new Set(criteria).size !== criteria.length || criteria.some((criterion) => !validStrategyText(criterion, 4 * 1024))) {
    throw new Error('Strategy retry binding is invalid.');
  }
  return {
    request_id: requestID,
    mission_id: missionID,
    mission_statement: missionStatement,
    goal_id: goalID,
    goal_objective: goalObjective,
    goal_mode: goalMode,
    success_criteria: criteria
  };
}

export function parseStrategyRetryBinding(value: string | null): StrategyRetryBinding | null {
  const record = parseStrictRecord(value, ['goal_id', 'goal_mode', 'goal_objective', 'mission_id', 'mission_statement', 'request_id', 'success_criteria'], 'strategy');
  if (!record) return null;
  if (typeof record.request_id !== 'string' || typeof record.mission_id !== 'string' || typeof record.mission_statement !== 'string' ||
    typeof record.goal_id !== 'string' || typeof record.goal_objective !== 'string' ||
    (record.goal_mode !== 'TARGET' && record.goal_mode !== 'CONTINUOUS') || !Array.isArray(record.success_criteria) ||
    !record.success_criteria.every((criterion) => typeof criterion === 'string')) {
    throw new Error('Stored strategy retry is invalid.');
  }
  return strategyRetryBinding(record.request_id, record.mission_id, record.mission_statement, record.goal_id, record.goal_objective, record.goal_mode, record.success_criteria as string[]);
}

export function matchesStrategyRetry(
  binding: StrategyRetryBinding,
  missionStatement: string,
  goalObjective: string,
  goalMode: 'TARGET' | 'CONTINUOUS',
  successCriteria: string[]
): boolean {
  return binding.mission_statement === missionStatement && binding.goal_objective === goalObjective && binding.goal_mode === goalMode &&
    binding.success_criteria.length === successCriteria.length && binding.success_criteria.every((criterion, index) => criterion === successCriteria[index]);
}

function parseStrictRecord(value: string | null, keys: string[], name: string): Record<string, unknown> | null {
  if (!value) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    throw new Error(`Stored ${name} retry is invalid.`);
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed) || Object.keys(parsed).sort().join(',') !== keys.join(',')) throw new Error(`Stored ${name} retry is invalid.`);
  return parsed as Record<string, unknown>;
}

export function hasRetryableIntentConfirmation(view: TaskView | null): boolean {
  return Boolean(view?.state === 'AWAITING_CONFIRMATION' && view.intent && view.conversation_id);
}

export function matchesConfirmationRetry(view: TaskView | null, binding: ConfirmationRetryBinding | null): boolean {
  return Boolean(
    hasRetryableIntentConfirmation(view) && binding &&
    view!.conversation_id === binding.conversation_id && view!.intent!.fingerprint === binding.fingerprint
  );
}

export function sameCompletionContract(previous?: CompletionContract, next?: CompletionContract): boolean {
  return Boolean(previous && next && previous.task_id === next.task_id && previous.task_version === next.task_version);
}

export function snapshotCompletionEvidence<T>(
  fields: Record<string, string>,
  files: Record<string, T[]>
): { fields: Record<string, string>; files: Record<string, T[]> } {
  return {
    fields: { ...fields },
    files: Object.fromEntries(Object.entries(files).map(([role, selected]) => [role, [...selected]]))
  };
}

export function safeDisplay(value: string): string {
  return value.replace(/[\p{Cc}\p{Cf}]/gu, (character) =>
    character === '\n' || character === '\r' || character === '\t' ? character : ''
  );
}

export function confirmationMessageID(fingerprint: string): string {
	if (!validFingerprint(fingerprint)) throw new Error('Intent fingerprint is invalid.');
  return `confirmation-${fingerprint}`;
}

export function completionReviewFeedback(decision: 'APPROVE' | 'REJECT' | 'REVISE', feedback: string): string {
  return decision === 'APPROVE' ? '' : feedback;
}

function validFingerprint(value: string): boolean {
  return /^[0-9a-f]{64}$/.test(value);
}

function validBoundaryIdentifier(value: string): boolean {
  if (!value || /[\p{Cc}\p{White_Space}]/u.test(value)) return false;
  try {
    return new TextEncoder().encode(value).byteLength <= 256 && decodeURIComponent(encodeURIComponent(value)) === value;
  } catch {
    return false;
  }
}

function validStrategyText(value: string, maximumBytes: number): boolean {
  if (!value || value !== value.trim() || value.includes('\0')) return false;
  try {
    return new TextEncoder().encode(value).byteLength <= maximumBytes && decodeURIComponent(encodeURIComponent(value)) === value;
  } catch {
    return false;
  }
}

function validStrategyIdentifier(value: string): boolean {
  return value.length <= 256 && /^[A-Za-z0-9_.:-]+$/.test(value);
}

export function validateCompletionFields(
  requirements: FieldRequirement[],
  fields: Record<string, string>
): string | null {
  const required = new Set(requirements.map((item) => item.name));
  for (const requirement of requirements) {
    const length = new TextEncoder().encode(fields[requirement.name] ?? '').byteLength;
    if (length < requirement.min_bytes || length > requirement.max_bytes) {
      return `${requirement.name} must contain ${requirement.min_bytes} to ${requirement.max_bytes} UTF-8 bytes.`;
    }
  }
  for (const name of Object.keys(fields)) {
    if (!required.has(name)) return `Unexpected completion field: ${name}.`;
  }
  return null;
}

export async function loadAllCompletionReviews(
  page: (after: string) => Promise<CompletionReviewPage>
): Promise<CompletionReview[]> {
  const reviews: CompletionReview[] = [];
  const cursors = new Set<string>();
  let after = '';
  for (let number = 0; number < maximumReviewPages; number += 1) {
    const result = await page(after);
    reviews.push(...result.reviews);
    if (!result.next_after) return reviews;
    if (result.next_after === after || cursors.has(result.next_after)) {
      throw new Error('Completion review pagination returned a repeated cursor.');
    }
    cursors.add(result.next_after);
    after = result.next_after;
  }
  throw new Error('Completion review queue exceeds the dashboard safety limit.');
}

export function validateArtifactSelections(
  requirements: ArtifactRequirement[],
  selected: Record<string, ArtifactSelection[]>
): string | null {
  let total = 0;
  for (const requirement of requirements) {
    const files = selected[requirement.role] ?? [];
    if (files.length < requirement.min_count || files.length > requirement.max_count) {
      return `${requirement.role} requires ${requirement.min_count} to ${requirement.max_count} files.`;
    }
    for (const file of files) {
      if (file.size < 1 || file.size > maximumArtifactBytes) {
        return `${file.name} must contain 1 byte to 16 MiB.`;
      }
      total += file.size;
      if (total > maximumCompletionBytes) {
        return 'Completion evidence must not exceed 32 MiB in total.';
      }
    }
  }
  return null;
}

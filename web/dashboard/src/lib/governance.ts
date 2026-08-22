import type { CompletionContract, CompletionReview, CompletionReviewPage, TaskView } from './types';

const maximumReviewPages = 100;
const maximumArtifactBytes = 16 * 1024 * 1024;
const maximumCompletionBytes = 32 * 1024 * 1024;

export type ArtifactSelection = { name: string; size: number; type: string };
export type ArtifactRequirement = { role: string; media_types: string[]; min_count: number; max_count: number };
export type FieldRequirement = { name: string; min_bytes: number; max_bytes: number };
export type ConfirmationRetryBinding = { conversation_id: string; fingerprint: string };

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

export function hasRetryableIntentConfirmation(view: TaskView | null): boolean {
  return Boolean(view?.state === 'AWAITING_CONFIRMATION' && view.intent && view.conversation_id);
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
  if (!/^[0-9a-f]{64}$/.test(fingerprint)) throw new Error('Intent fingerprint is invalid.');
  return `confirmation-${fingerprint}`;
}

function validBoundaryIdentifier(value: string): boolean {
  if (!value || /[\p{Cc}\p{White_Space}]/u.test(value)) return false;
  try {
    return new TextEncoder().encode(value).byteLength <= 256 && decodeURIComponent(encodeURIComponent(value)) === value;
  } catch {
    return false;
  }
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

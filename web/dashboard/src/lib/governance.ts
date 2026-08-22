import type { CompletionReview, CompletionReviewPage } from './types';

const maximumReviewPages = 100;
const maximumArtifactBytes = 16 * 1024 * 1024;
const maximumCompletionBytes = 32 * 1024 * 1024;

export type ArtifactSelection = { name: string; size: number; type: string };
export type ArtifactRequirement = { role: string; media_types: string[]; min_count: number; max_count: number };

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
      if (!file.type || !requirement.media_types.includes(file.type)) {
        return `${file.name} does not declare an allowed media type for ${requirement.role}.`;
      }
      total += file.size;
      if (total > maximumCompletionBytes) {
        return 'Completion evidence must not exceed 32 MiB in total.';
      }
    }
  }
  return null;
}

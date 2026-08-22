export type IntentValue = { value: string; provenance?: string };

export type IntentDraft = {
  objective: string;
  mode: string;
  goal?: IntentValue;
  replaces_work?: IntentValue;
  context?: IntentValue[];
  deliverables?: IntentValue[];
  completion_criteria?: IntentValue[];
  constraints?: IntentValue[];
  resolved_decisions?: { subject: string; value: string }[];
  consequence_candidates?: string[];
  requested_execution_kind?: string;
  version: number;
  fingerprint: string;
};

export type TaskView = {
  task_id: string;
  work_id?: string;
  conversation_id: string;
  state: string;
  prompt?: string;
  result?: string;
  mode?: string;
  trust_label?: string;
  updated_at?: string;
  completion_contract?: CompletionContract;
  intent?: IntentDraft;
};

export type CompletionContract = {
  task_id: string;
  task_version: number;
  criteria: { id: string; description: string; required: boolean }[];
  required_fields?: { name: string; description: string; min_bytes: number; max_bytes: number }[];
  artifact_requirements?: { role: string; media_types: string[]; min_count: number; max_count: number }[];
};

export type Approval = {
  approval_id: string;
  task_id: string;
  action: string;
  resource: string;
  scope: string;
  canonical_effect_descriptor: string;
  effect_arguments: Record<string, string>;
  boundary: string;
  risk: string;
  urgency: string;
  effect_fingerprint: string;
  status: string;
  single_use: boolean;
  created_at: string;
  expires_at?: string;
};

export type CompletionReview = {
  review_id: string;
  task_id: string;
  fingerprint: string;
  state: string;
  objective: string;
  candidate_result?: string;
  result?: string;
  criteria: { id: string; description: string; required: boolean }[];
  evidence_refs: string[];
  updated_at: string;
};

export type CompletionReviewPage = {
  reviews: CompletionReview[];
  next_after?: string;
};

export type DashboardIdentity = {
  organization: string;
  mode: string;
  version: string;
  session_expires_at: string;
};

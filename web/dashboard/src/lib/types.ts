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
  selected_goal_id?: string;
  state: string;
  prompt?: string;
  result?: string;
  mode?: string;
  trust_label?: string;
  updated_at?: string;
  completion_contract?: CompletionContract;
  review_required?: boolean;
  user_input_allowed?: boolean;
  input_recovery_required?: boolean;
  completion_recovery_required?: boolean;
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
  reviewer_id?: string;
  feedback?: string;
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

export type OrganizationSnapshot = {
  organization: { id: string; name: string; policy_version: string; version: number; created_at: string };
  missions: { id: string; statement: string; status: string; version: number; created_at: string }[];
  goals: { id: string; mission_id: string; objective: string; mode: string; success_criteria: string[]; status: string; version: number; created_at: string }[];
  works: { id: string; goal_id?: string; replaces_work_id?: string; objective: string; mode: string; experiment_status?: string; trust_label?: string; status: string; version: number; created_at: string }[];
  tasks: { id: string; work_id: string; parent_id?: string; description: string; execution_kind: string; model_inference_policy: string; depends_on: string[]; assignee_type?: string; assignee_id?: string; status: string; version: number }[];
  teams: { id: string; name: string; mission?: string; member_agent_ids: string[]; status: string; version: number; created_at: string }[];
  agents: { id: string; role: string; status: string; blueprint_status: string; execution_profile_status: string; available: boolean; runtime_adapter: string; model_provider: string; model: string; version: number }[];
};

export type GovernanceFinding = {
  id: string;
  rule_id: string;
  severity: 'CRITICAL' | 'HIGH' | 'MEDIUM' | 'LOW';
  category: string;
  scope_kind: string;
  scope_id: string;
  message: string;
  evidence_refs: string[];
  status: string;
  observed_at: string;
};

export type GovernanceInspection = {
  schema_version: string;
  observed_at: string;
  organization_id: string;
  boundary: { assessment: string; certified: false; authority: string };
  integrity: { algorithm: string; verification: string; ledger_events: number; ledger_sequence: number; ledger_event_id: string; ledger_sha256: string };
  rules: { id: string; category: string; severity: string; title: string; description: string }[];
  findings: GovernanceFinding[];
  summary: { rules_executed: number; findings: number; critical: number; high: number; medium: number; low: number };
  sha256: string;
};

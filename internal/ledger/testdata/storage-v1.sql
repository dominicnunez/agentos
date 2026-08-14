PRAGMA application_id=1095192403;
PRAGMA user_version=1;

CREATE TABLE events (
sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE, organization_id TEXT NOT NULL,
event_type TEXT NOT NULL, source_actor_id TEXT NOT NULL DEFAULT '', source_execution_id TEXT NOT NULL DEFAULT '', recipient_scope TEXT NOT NULL DEFAULT '', recipient_id TEXT NOT NULL DEFAULT '', task_id TEXT NOT NULL DEFAULT '', authorization_refs BLOB NOT NULL, artifact_refs BLOB NOT NULL, payload BLOB NOT NULL,
correlation_id TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, schema_version INTEGER NOT NULL);
CREATE INDEX events_correlation_idx ON events(correlation_id, sequence);
CREATE INDEX events_intake_actor_idx ON events(organization_id,event_type,source_actor_id,sequence);

CREATE TABLE records (
kind TEXT NOT NULL, record_id TEXT NOT NULL, version INTEGER NOT NULL, body BLOB NOT NULL,
admission_event_id TEXT NOT NULL DEFAULT '', admission_fingerprint TEXT NOT NULL DEFAULT '',
created_at TEXT NOT NULL, PRIMARY KEY(kind, record_id, version));
CREATE INDEX records_kind_idx ON records(kind, created_at);
CREATE UNIQUE INDEX records_admission_event_idx ON records(admission_event_id) WHERE admission_event_id<>'';

CREATE TABLE external_work (
organization_id TEXT NOT NULL, request_id TEXT NOT NULL, correlation_id TEXT NOT NULL, intent_id TEXT NOT NULL,
PRIMARY KEY(organization_id, request_id), UNIQUE(organization_id, correlation_id), UNIQUE(intent_id));
CREATE TABLE external_tasks (
organization_id TEXT NOT NULL, task_id TEXT NOT NULL, correlation_id TEXT NOT NULL,
PRIMARY KEY(organization_id, task_id));
CREATE INDEX external_tasks_correlation_idx ON external_tasks(organization_id, correlation_id);

CREATE TABLE inbox (
recipient_scope TEXT NOT NULL, recipient_id TEXT NOT NULL, event_id TEXT NOT NULL UNIQUE,
organization_id TEXT NOT NULL, task_id TEXT NOT NULL DEFAULT '', available_at TEXT NOT NULL,
observed_at TEXT NOT NULL DEFAULT '', observation_event_id TEXT NOT NULL DEFAULT '',
PRIMARY KEY(recipient_scope, recipient_id, event_id));
CREATE INDEX inbox_available_idx ON inbox(recipient_scope, recipient_id, observed_at, available_at);

CREATE TABLE consumed_approvals (
approval_id TEXT PRIMARY KEY, effect_fingerprint TEXT NOT NULL, consumed_at TEXT NOT NULL);

CREATE TABLE inference_policies (
organization_id TEXT NOT NULL, policy_fingerprint TEXT NOT NULL, body BLOB NOT NULL,
activation_event_id TEXT NOT NULL UNIQUE, activated_at TEXT NOT NULL, active INTEGER NOT NULL,
PRIMARY KEY(organization_id,policy_fingerprint));
CREATE UNIQUE INDEX inference_policies_active_idx ON inference_policies(organization_id) WHERE active=1;

CREATE TABLE inference_reservations (
reservation_id TEXT PRIMARY KEY, request_id TEXT NOT NULL, organization_id TEXT NOT NULL,
purpose TEXT NOT NULL, intent_id TEXT NOT NULL DEFAULT '', task_id TEXT NOT NULL DEFAULT '',
execution_id TEXT NOT NULL, correlation_id TEXT NOT NULL, prompt_sha256 TEXT NOT NULL,
provider TEXT NOT NULL, model TEXT NOT NULL, execution_profile_version TEXT NOT NULL,
policy_fingerprint TEXT NOT NULL, state TEXT NOT NULL,
reserved_input_tokens INTEGER NOT NULL, reserved_output_tokens INTEGER NOT NULL,
reserved_cost_nano_usd INTEGER NOT NULL, charged_input_tokens INTEGER NOT NULL,
charged_output_tokens INTEGER NOT NULL, charged_cost_nano_usd INTEGER NOT NULL,
window_started_at TEXT NOT NULL, window_expires_at TEXT NOT NULL,
created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
UNIQUE(organization_id,request_id));
CREATE INDEX inference_reservations_window_idx
ON inference_reservations(organization_id,provider,model,window_started_at,state);

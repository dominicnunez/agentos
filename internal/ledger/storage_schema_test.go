package ledger

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestOpenBootstrapsCurrentStorageContract(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	contract, err := ValidateStorageContract(t.Context(), store.db)
	if err != nil {
		t.Fatal(err)
	}
	if contract.StorageVersion != CurrentStorageVersion || contract.EventSchemaVersion != events.SchemaVersion {
		t.Fatalf("storage contract=%+v", contract)
	}
}

func TestStorageV1FixtureMatchesFrozenFingerprint(t *testing.T) {
	db := createStorageV1Fixture(t, filepath.Join(t.TempDir(), "storage-v1.db"))
	defer func() { _ = db.Close() }()
	fingerprint, err := storageSchemaFingerprint(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	if fingerprint != storageSchemaV1Fingerprint {
		t.Fatalf("storage v1 fingerprint=%s", fingerprint)
	}
}

func TestOpenMigratesStorageV1FixtureAndResealsProjectionAdmissions(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "storage-v1.db")
	legacy := createStorageV1Fixture(t, path)
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "policy-v1", CreatedAt: time.Now().UTC()}
	if err := insertLegacyProjection(ctx, &SQLite{db: legacy}, "ORGANIZATION_CREATED", "organization", string(organization.ID), "", organization); err != nil {
		_ = legacy.Close()
		t.Fatal(err)
	}
	row := legacy.QueryRowContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE sequence=1`)
	event, err := scanEvent(row)
	if err != nil {
		_ = legacy.Close()
		t.Fatal(err)
	}
	legacyAdmission, present, err := events.ResealProjectionEventForMigration(event, events.SchemaVersion, LegacyEventSchemaVersion)
	if err != nil || !present {
		_ = legacy.Close()
		t.Fatalf("create legacy projection admission: present=%t err=%v", present, err)
	}
	legacyPayload, err := json.Marshal(legacyAdmission)
	if err != nil {
		_ = legacy.Close()
		t.Fatal(err)
	}
	if _, err := legacy.ExecContext(ctx, `UPDATE events SET payload=?,schema_version=? WHERE event_id=?`, legacyPayload, LegacyEventSchemaVersion, event.EventID); err != nil {
		_ = legacy.Close()
		t.Fatal(err)
	}
	if _, err := legacy.ExecContext(ctx, `UPDATE records SET admission_fingerprint=? WHERE admission_event_id=?`, legacyAdmission.Admission.Fingerprint, event.EventID); err != nil {
		_ = legacy.Close()
		t.Fatal(err)
	}
	var beforePayload []byte
	if err := legacy.QueryRowContext(ctx, `SELECT payload FROM events WHERE sequence=1`).Scan(&beforePayload); err != nil {
		_ = legacy.Close()
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	contract, err := ValidateStorageContract(ctx, store.db)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if contract.StorageVersion != CurrentStorageVersion || contract.EventSchemaVersion != events.SchemaVersion {
		_ = store.Close()
		t.Fatalf("migrated contract=%+v", contract)
	}
	var afterPayload []byte
	if err := store.db.QueryRowContext(ctx, `SELECT payload FROM events WHERE sequence=1`).Scan(&afterPayload); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if string(afterPayload) == string(beforePayload) {
		_ = store.Close()
		t.Fatal("storage migration did not reseal the projection admission")
	}
	records, err := store.Records(ctx, "organization", string(organization.ID))
	if err != nil || len(records) != 1 {
		_ = store.Close()
		t.Fatalf("migrated projection records=%d err=%v", len(records), err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restarted.Close() }()
	stream, err := restarted.Events(ctx, "legacy-request")
	if err != nil || len(stream) != 1 || stream[0].OrganizationID != "org-1" {
		t.Fatalf("restarted migrated stream=%+v err=%v", stream, err)
	}
	if _, present, err := events.AdmittedProjection(stream[0]); err != nil || !present {
		t.Fatalf("migrated projection admission: present=%t err=%v", present, err)
	}
	integrity, err := restarted.Integrity(ctx)
	if err != nil || integrity.EventCount != 1 || integrity.Sequence != 1 || integrity.EventID != stream[0].EventID || integrity.SHA256 == "" {
		t.Fatalf("migrated event integrity=%+v err=%v", integrity, err)
	}
}

func TestOpenMigratesStorageV2WithoutIntentReviewEvidence(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "storage-v2.db")
	legacy := createStorageV2Fixture(t, path)
	if _, err := legacy.ExecContext(ctx, `INSERT INTO events(event_id,organization_id,event_type,source_actor_id,authorization_refs,artifact_refs,payload,created_at,schema_version) VALUES('audit-1','org-1','AUDIT_NOTE','runtime','[]','[]','{}','2026-08-13T12:00:00Z',?)`, LegacyEventSchemaVersion); err != nil {
		_ = legacy.Close()
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	contract, err := ValidateStorageContract(ctx, store.db)
	if err != nil || contract.StorageVersion != CurrentStorageVersion || contract.EventSchemaVersion != events.SchemaVersion {
		t.Fatalf("migrated v2 contract=%+v err=%v", contract, err)
	}
	var eventVersion int
	if err := store.db.QueryRowContext(ctx, `SELECT schema_version FROM events WHERE event_id='audit-1'`).Scan(&eventVersion); err != nil || eventVersion != events.SchemaVersion {
		t.Fatalf("migrated event version=%d err=%v", eventVersion, err)
	}
}

func TestStorageV2MigrationPreservesReviewedIntentEvidence(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "storage-v2-intent.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	value := core.IntentValue{Value: "hello", Origin: "EXPLICIT", SourceMessageID: "message-1"}
	draft := core.IntentDraft{
		ID: "intent-run-1", OrganizationID: "org-1", Version: 1, Status: core.IntentStatusReadyForReview, Mode: core.IntentModeStandard,
		RequestedExecutionKind: core.ExecutionDeterministic, Objective: "echo hello", Context: []core.IntentValue{}, Deliverables: []core.IntentValue{value},
		CompletionCriteria: []core.IntentValue{value}, Constraints: []core.IntentValue{}, ResolvedDecisions: []core.IntentDecision{}, ConsequenceCandidates: []string{}, MissingUserInputs: []core.IntentValue{}, CreatedAt: now,
	}
	draft.Fingerprint, err = core.FingerprintIntentDraft(draft)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "INTAKE_MESSAGE_RECORDED", SourceActorID: "user-1", TaskID: "task-run-1", CorrelationID: "run-1", Payload: events.IntakeMessageRecordedPayload{MessageID: "message-1", Text: "echo hello", SourcePrincipalID: "user-1", SourcePrincipalKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", RequestedExecutionKind: core.ExecutionDeterministic}}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_DRAFTED", SourceActorID: "runtime", TaskID: "task-run-1", CorrelationID: "run-1", Payload: events.IntentDraftedPayload{SourceMessageID: "message-1", Draft: draft, Reply: "Review."}}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	confirmation := events.IntentConfirmedPayload{IntentID: string(draft.ID), Version: 1, Fingerprint: draft.Fingerprint, ConfirmingActorID: "user-1", ConfirmingActorKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", MessageID: "confirmation-1"}
	if _, err := store.AppendIntentConfirmation(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CONFIRMED", SourceActorID: "user-1", TaskID: "task-run-1", CorrelationID: "run-1", Payload: confirmation}, "", ""); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE events SET schema_version=?`, LegacyEventSchemaVersion); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE event_integrity; DROP TABLE pending_completion_reviews; DROP INDEX events_recent_commit_idx; DROP INDEX pending_approvals_expiry_idx`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE pending_approvals`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	fingerprint, err := storageSchemaFingerprint(ctx, db)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE agentos_storage SET storage_version=2,event_schema_version=?,schema_fingerprint=?`, LegacyEventSchemaVersion, fingerprint); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `PRAGMA user_version=2`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	migrated, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = migrated.Close() }()
	stream, err := migrated.Events(ctx, "run-1")
	if err != nil || len(stream) != 3 {
		t.Fatalf("migrated review stream=%+v err=%v", stream, err)
	}
	if err := events.ValidateReviewedIntentAdmission(stream, stream[2]); err != nil {
		t.Fatalf("migrated reviewed Intent evidence: %v", err)
	}
}

func TestStorageV4MigrationRebuildsBoundedGovernanceQueues(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "storage-v4-governance.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	appendReview := func(eventType, taskID, correlationID, reviewID string) events.Event {
		t.Helper()
		event, err := store.Append(ctx, events.TrustedDraft{
			OrganizationID: "org-1", EventType: eventType, SourceActorID: "runtime",
			TaskID: taskID, CorrelationID: correlationID, Payload: map[string]string{"review_id": reviewID},
		})
		if err != nil {
			t.Fatal(err)
		}
		return event
	}
	appendReview("COMPLETION_REVIEW_REQUESTED", "task-1", "work-1", "review-1")
	appendReview("COMPLETION_REVIEW_DECIDED", "task-1", "work-1", "review-1")
	pending := appendReview("COMPLETION_REVIEW_REQUESTED", "task-2", "work-2", "review-2")
	appendTaskProjectionParents(t, ctx, store, "org-1", "work-3", "work-3")
	terminal := core.Task{ID: "task-3", WorkID: "work-3", Description: "terminalized before review", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending}
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(terminal.ID), CorrelationID: "work-3"},
		ProjectionKind: "task", RecordID: string(terminal.ID), Version: 1, Value: terminal,
	}); err != nil {
		t.Fatal(err)
	}
	appendReview("COMPLETION_REVIEW_REQUESTED", "task-3", "work-3", "review-3")
	terminal.Status = core.TaskFailed
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_WORK_FAILED", SourceActorID: "runtime", TaskID: string(terminal.ID), CorrelationID: "work-3"},
		ProjectionKind: "task", RecordID: string(terminal.ID), Version: 2, Value: terminal,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE event_integrity; DROP TABLE pending_completion_reviews; DROP INDEX events_recent_commit_idx; DROP INDEX pending_approvals_expiry_idx`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	fingerprint, err := storageSchemaFingerprint(ctx, db)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE agentos_storage SET storage_version=4,schema_fingerprint=?; PRAGMA user_version=4`, fingerprint); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	migrated, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = migrated.Close() })
	reviews, err := migrated.PendingCompletionReviewEvents(ctx, "org-1", "", 10)
	if err != nil || len(reviews) != 1 || reviews[0].EventID != pending.EventID {
		t.Fatalf("migrated pending completion reviews=%+v err=%v", reviews, err)
	}
}

func TestStorageMigrationFailsAtomicallyOnAmbiguousV1Layout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt-v1.db")
	legacy := createStorageV1Fixture(t, path)
	if _, err := legacy.ExecContext(t.Context(), `DROP INDEX records_admission_event_idx`); err != nil {
		_ = legacy.Close()
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); err == nil || !strings.Contains(err.Error(), "lacks exact index") {
		t.Fatalf("ambiguous v1 layout was not rejected: %v", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var version, metadataTables int
	if err := db.QueryRowContext(t.Context(), `PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name='agentos_storage'`).Scan(&metadataTables); err != nil {
		t.Fatal(err)
	}
	if version != 1 || metadataTables != 0 {
		t.Fatalf("failed migration partially mutated storage: version=%d metadata=%d", version, metadataTables)
	}
}

func TestOpenRejectsUnsupportedOrMismatchedStorageContract(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *sql.DB)
		want   string
	}{
		{
			name: "wrong application id",
			mutate: func(t *testing.T, db *sql.DB) {
				if _, err := db.ExecContext(t.Context(), `PRAGMA application_id=7`); err != nil {
					t.Fatal(err)
				}
			},
			want: "is not Agent OS",
		},
		{
			name: "future storage version",
			mutate: func(t *testing.T, db *sql.DB) {
				if _, err := db.ExecContext(t.Context(), `PRAGMA user_version=99`); err != nil {
					t.Fatal(err)
				}
			},
			want: "newer than supported",
		},
		{
			name: "event contract mismatch",
			mutate: func(t *testing.T, db *sql.DB) {
				if _, err := db.ExecContext(t.Context(), `UPDATE agentos_storage SET event_schema_version=event_schema_version+1`); err != nil {
					t.Fatal(err)
				}
			},
			want: "metadata does not match",
		},
		{
			name: "schema fingerprint drift",
			mutate: func(t *testing.T, db *sql.DB) {
				if _, err := db.ExecContext(t.Context(), `CREATE INDEX unreviewed_events_type_idx ON events(event_type)`); err != nil {
					t.Fatal(err)
				}
			},
			want: "schema fingerprint does not match",
		},
		{
			name: "durable event schema mismatch",
			mutate: func(t *testing.T, db *sql.DB) {
				if _, err := db.ExecContext(t.Context(), `INSERT INTO events(event_id,organization_id,event_type,source_actor_id,authorization_refs,artifact_refs,payload,created_at,schema_version) VALUES('future-event','org-1','AUDIT_NOTE','runtime',?,?,?,'2026-08-13T12:00:00Z',?)`, []byte("[]"), []byte("[]"), []byte("{}"), events.SchemaVersion+1); err != nil {
					t.Fatal(err)
				}
			},
			want: "outside supported Event Contract",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "agentos.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(t, db)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(path); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("mismatched storage contract error=%v", err)
			}
		})
	}
}

func createStorageV1Fixture(t *testing.T, path string) *sql.DB {
	t.Helper()
	script, err := os.ReadFile(filepath.Join("testdata", "storage-v1.sql"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), string(script)); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return db
}

func createStorageV2Fixture(t *testing.T, path string) *sql.DB {
	t.Helper()
	db := createStorageV1Fixture(t, path)
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := applyStorageMigration(t.Context(), tx, 1, 2); err != nil {
		_ = tx.Rollback()
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `PRAGMA user_version=2`); err != nil {
		_ = tx.Rollback()
		_ = db.Close()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return db
}

package recovery

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/ledger"
	_ "modernc.org/sqlite"
)

func TestBackupAndRestorePreserveSnapshotWithoutOverwriting(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	source := filepath.Join(directory, "live.db")
	live := createTestLedger(t, source)
	t.Cleanup(func() { _ = live.Close() })
	if _, err := live.ExecContext(ctx, `PRAGMA journal_mode=WAL`); err != nil {
		t.Fatal(err)
	}
	if _, err := live.ExecContext(ctx, `INSERT INTO events(event_id,organization_id,event_type,source_actor_id,authorization_refs,artifact_refs,payload,created_at,schema_version) VALUES('event-1','org-1','MESSAGE','agent-1','[]','[]','{}','2026-08-13T12:00:00Z',?)`, events.SchemaVersion); err != nil {
		t.Fatal(err)
	}

	backupPath := filepath.Join(directory, "backup.db")
	backup, err := Backup(ctx, source, backupPath)
	if err != nil {
		t.Fatal(err)
	}
	if backup.Path != backupPath || backup.EventCount != 1 || backup.MaxSequence != 1 || backup.StorageVersion != ledger.CurrentStorageVersion || backup.EventSchemaVersion != events.SchemaVersion || backup.SHA256 == "" || backup.SizeBytes == 0 {
		t.Fatalf("backup=%+v", backup)
	}
	if _, err := live.ExecContext(ctx, `INSERT INTO events(event_id,organization_id,event_type,source_actor_id,authorization_refs,artifact_refs,payload,created_at,schema_version) VALUES('event-2','org-1','MESSAGE','agent-1','[]','[]','{}','2026-08-13T12:01:00Z',?)`, events.SchemaVersion); err != nil {
		t.Fatal(err)
	}
	verified, err := Verify(ctx, backupPath)
	if err != nil || verified.EventCount != 1 || verified.MaxSequence != 1 || verified.StorageVersion != ledger.CurrentStorageVersion || verified.EventSchemaVersion != events.SchemaVersion {
		t.Fatalf("verified=%+v err=%v", verified, err)
	}

	protected := filepath.Join(directory, "protected.db")
	if err := os.WriteFile(protected, []byte("do-not-replace"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Restore(ctx, backupPath, protected); err == nil {
		t.Fatal("restore overwrote an existing destination")
	}
	content, err := os.ReadFile(protected)
	if err != nil || string(content) != "do-not-replace" {
		t.Fatalf("protected destination changed: content=%q err=%v", content, err)
	}

	restoredPath := filepath.Join(directory, "restored.db")
	restored, err := Restore(ctx, backupPath, restoredPath)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Path != restoredPath || restored.EventCount != 1 || restored.MaxSequence != 1 {
		t.Fatalf("restored=%+v", restored)
	}
	restoredDB, err := sql.Open("sqlite", restoredPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restoredDB.Close() }()
	var payload string
	if err := restoredDB.QueryRowContext(ctx, `SELECT payload FROM events WHERE event_id='event-1'`).Scan(&payload); err != nil || payload != "{}" {
		t.Fatalf("restored payload=%q err=%v", payload, err)
	}
	var eventCount int
	if err := restoredDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&eventCount); err != nil || eventCount != 1 {
		t.Fatalf("restored event count=%d err=%v", eventCount, err)
	}
}

func TestOldestSupportedStorageFixtureVerifiesBacksUpRestoresAndMigrates(t *testing.T) {
	ctx := t.Context()
	directory := t.TempDir()
	source := filepath.Join(directory, "storage-v1.db")
	script, err := os.ReadFile(filepath.Join("..", "testdata", "storage-v1.sql"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, string(script)); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO events(event_id,organization_id,event_type,source_actor_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version) VALUES('event-v1','org-1','AUDIT_NOTE','runtime','task-1',?,?,?,'work-v1','2026-08-13T12:00:00Z',?)`, []byte("[]"), []byte("[]"), []byte("{}"), ledger.LegacyEventSchemaVersion); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	verified, err := Verify(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if verified.StorageVersion != ledger.OldestSupportedStorageVersion || verified.EventSchemaVersion != ledger.LegacyEventSchemaVersion {
		t.Fatalf("v1 verification=%+v", verified)
	}
	backupPath := filepath.Join(directory, "storage-v1-backup.db")
	backup, err := Backup(ctx, source, backupPath)
	if err != nil || backup.StorageVersion != ledger.OldestSupportedStorageVersion {
		t.Fatalf("v1 backup=%+v err=%v", backup, err)
	}
	restoredPath := filepath.Join(directory, "storage-v1-restored.db")
	restored, err := Restore(ctx, backupPath, restoredPath)
	if err != nil || restored.StorageVersion != ledger.OldestSupportedStorageVersion {
		t.Fatalf("v1 restore=%+v err=%v", restored, err)
	}
	store, err := ledger.Open(restoredPath)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := store.Events(ctx, "work-v1")
	if err != nil || len(stream) != 1 || stream[0].EventID != "event-v1" {
		_ = store.Close()
		t.Fatalf("migrated restored stream=%+v err=%v", stream, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	migrated, err := Verify(ctx, restoredPath)
	if err != nil || migrated.StorageVersion != ledger.CurrentStorageVersion || migrated.EventSchemaVersion != events.SchemaVersion {
		t.Fatalf("migrated restore=%+v err=%v", migrated, err)
	}
}

func TestLegacyVerificationRejectsTamperedAdmissionsAfterMigration(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "legacy-tampered.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	mission := core.Mission{ID: "mission-1", OrganizationID: organization.ID, Statement: "Mission", Status: core.MissionActive, CreatedAt: now}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization},
		{Event: events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "MISSION_CREATED", SourceActorID: "runtime", CorrelationID: "mission-1"}, ProjectionKind: "mission", RecordID: string(mission.ID), Version: 1, Value: mission},
	} {
		if _, err := store.AppendProjection(ctx, draft); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
	}
	missionStream, err := store.Events(ctx, "mission-1")
	if err != nil || len(missionStream) != 1 {
		_ = store.Close()
		t.Fatalf("mission stream=%+v err=%v", missionStream, err)
	}
	legacyAdmission, present, err := events.ResealProjectionEventForMigration(missionStream[0], events.SchemaVersion, ledger.LegacyEventSchemaVersion)
	if err != nil || !present {
		_ = store.Close()
		t.Fatalf("legacy mission admission: present=%t err=%v", present, err)
	}
	legacyPayload, err := json.Marshal(legacyAdmission)
	if err != nil {
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
	if _, err := db.ExecContext(ctx, `DELETE FROM events WHERE event_type='ORGANIZATION_CREATED'; DELETE FROM records WHERE kind='organization'`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE events SET payload=?,schema_version=? WHERE event_id=?`, legacyPayload, ledger.LegacyEventSchemaVersion, missionStream[0].EventID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE records SET admission_fingerprint=? WHERE admission_event_id=?`, legacyAdmission.Admission.Fingerprint, missionStream[0].EventID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE pending_completion_reviews; DROP INDEX events_recent_commit_idx; DROP INDEX pending_approvals_expiry_idx`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE pending_approvals`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	fingerprint, err := testStorageSchemaFingerprint(ctx, db)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE agentos_storage SET storage_version=2,event_schema_version=?,schema_fingerprint=?`, ledger.LegacyEventSchemaVersion, fingerprint); err != nil {
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
	if _, err := Verify(ctx, path); err == nil || !strings.Contains(err.Error(), "durable parent Organization") {
		t.Fatalf("legacy tampered-admission verification error=%v", err)
	}
}

func testStorageSchemaFingerprint(ctx context.Context, db *sql.DB) (string, error) {
	rows, err := db.QueryContext(ctx, `SELECT type,name,tbl_name,COALESCE(sql,'') FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name,tbl_name`)
	if err != nil {
		return "", err
	}
	defer func() { _ = rows.Close() }()
	hash := sha256.New()
	for rows.Next() {
		var kind, name, table, statement string
		if err := rows.Scan(&kind, &name, &table, &statement); err != nil {
			return "", err
		}
		_, _ = fmt.Fprintf(hash, "%s\x00%s\x00%s\x00%s\n", kind, name, table, strings.TrimSpace(statement))
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func TestBackupAndRestorePreserveInferenceAdmissionAuthority(t *testing.T) {
	ctx := t.Context()
	directory := t.TempDir()
	source := filepath.Join(directory, "live-inference.db")
	live, err := ledger.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	policy := inference.Policy{
		Version: inference.PolicyVersion, OrganizationID: "organization-1", Provider: "provider-1", Model: "model-1",
		ExecutionProfileVersion: "profile-v1", Mode: inference.Subscription,
		MaxInputTokensPerRequest: 100, MaxOutputTokensPerRequest: 20, MaxTokensPerWindow: 500,
		ContinuityReserveTokens: 100, WindowDurationSeconds: 3600, MaxConcurrentRequests: 1, MaxAttemptsPerRequest: 1,
		AuthorizedBy: "local-uid-1000", AuthorizedAt: now.Add(-time.Hour), AuthorizationExpiresAt: now.Add(time.Hour),
	}
	if err := live.ActivateInferencePolicy(ctx, policy); err != nil {
		_ = live.Close()
		t.Fatal(err)
	}
	request := restoredInferenceRequest("request-1")
	reservation, err := live.ReserveInference(ctx, request)
	if err != nil {
		_ = live.Close()
		t.Fatal(err)
	}
	usage := events.InferenceUsageRecordedPayload{
		Source: "provider", Provider: "provider-1", Model: "model-1", InputTokens: 10, OutputTokens: 5, TotalTokens: 15,
	}
	if _, err := live.ReconcileInference(ctx, reservation, &usage, inference.ReconciliationCompleted); err != nil {
		_ = live.Close()
		t.Fatal(err)
	}

	backupPath := filepath.Join(directory, "inference-backup.db")
	if _, err := Backup(ctx, source, backupPath); err != nil {
		_ = live.Close()
		t.Fatal(err)
	}
	if err := live.Close(); err != nil {
		t.Fatal(err)
	}
	restoredPath := filepath.Join(directory, "inference-restored.db")
	if _, err := Restore(ctx, backupPath, restoredPath); err != nil {
		t.Fatal(err)
	}
	restored, err := ledger.Open(restoredPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restored.Close() })
	if _, err := restored.ReserveInference(ctx, restoredInferenceRequest("request-2")); err != nil {
		t.Fatalf("restored inference authority could not admit new work: %v", err)
	}
}

func restoredInferenceRequest(requestID string) inference.InferenceRequest {
	digest := sha256.Sum256([]byte("prompt-" + requestID))
	return inference.InferenceRequest{
		Scope: inference.Scope{
			OrganizationID: "organization-1", Purpose: inference.PurposeTaskExecution, RequestID: requestID,
			TaskID: "task-1", ExecutionID: requestID, CorrelationID: "work-1",
		},
		Descriptor:   execution.ModelDescriptor{Provider: "provider-1", Model: "model-1", ExecutionProfileVersion: "profile-v1"},
		PromptSHA256: hex.EncodeToString(digest[:]),
	}
}

func TestRecoveryRejectsCorruptionWrongSchemaAndCancelledPublication(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	corrupt := filepath.Join(directory, "corrupt.db")
	if err := os.WriteFile(corrupt, []byte("not sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, corrupt); err == nil {
		t.Fatal("corrupt database passed verification")
	}

	wrongSchema := filepath.Join(directory, "wrong-schema.db")
	db, err := sql.Open("sqlite", wrongSchema)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE unrelated(value TEXT)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, wrongSchema); err == nil {
		t.Fatal("non-Agent OS SQLite database passed verification")
	}

	incompleteSchema := filepath.Join(directory, "incomplete-schema.db")
	incompleteDB := createTestLedger(t, incompleteSchema)
	if _, err := incompleteDB.ExecContext(ctx, `ALTER TABLE inbox DROP COLUMN organization_id`); err != nil {
		t.Fatal(err)
	}
	if err := incompleteDB.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, incompleteSchema); err == nil {
		t.Fatal("Agent OS ledger with a missing required column passed verification")
	}

	source := filepath.Join(directory, "source.db")
	sourceDB := createTestLedger(t, source)
	if err := sourceDB.Close(); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	destination := filepath.Join(directory, "cancelled.db")
	if _, err := Backup(cancelled, source, destination); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled backup error=%v", err)
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cancelled backup published destination: %v", err)
	}
}

func TestRecoveryRejectsMissingOrZeroEventTimestamp(t *testing.T) {
	for _, test := range []struct {
		name      string
		createdAt string
	}{
		{name: "missing"},
		{name: "zero", createdAt: "0001-01-01T00:00:00Z"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "ledger.db")
			db := createTestLedger(t, path)
			if _, err := db.ExecContext(ctx, `INSERT INTO events(event_id,organization_id,event_type,source_actor_id,authorization_refs,artifact_refs,payload,created_at,schema_version) VALUES('event-1','org-1','MESSAGE','agent-1','[]','[]','{}',?,?)`, test.createdAt, events.SchemaVersion); err != nil {
				_ = db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			if _, err := Verify(ctx, path); err == nil || !strings.Contains(err.Error(), "invalid timestamp") {
				t.Fatalf("recovery verification error=%v", err)
			}
		})
	}
}

func TestRecoveryRejectsIncompleteOrdinaryEventIdentity(t *testing.T) {
	for _, test := range []struct {
		name     string
		eventID  string
		sequence int64
	}{
		{name: "missing event id", sequence: 1},
		{name: "nonpositive sequence", eventID: "event-1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "ledger.db")
			db := createTestLedger(t, path)
			if _, err := db.ExecContext(ctx, `INSERT INTO events(sequence,event_id,organization_id,event_type,source_actor_id,authorization_refs,artifact_refs,payload,created_at,schema_version) VALUES(?,?,'org-1','MESSAGE','agent-1','[]','[]','{}','2026-08-13T12:00:00Z',?)`, test.sequence, test.eventID, events.SchemaVersion); err != nil {
				_ = db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			if _, err := Verify(ctx, path); err == nil || !strings.Contains(err.Error(), "incomplete envelope") {
				t.Fatalf("recovery verification error=%v", err)
			}
		})
	}
}

func TestRecoveryRejectsUnsupportedOrdinaryEventSchema(t *testing.T) {
	for _, schemaVersion := range []int{0, events.SchemaVersion + 1} {
		t.Run(fmt.Sprintf("schema-%d", schemaVersion), func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "ledger.db")
			db := createTestLedger(t, path)
			if _, err := db.ExecContext(ctx, `INSERT INTO events(event_id,organization_id,event_type,source_actor_id,authorization_refs,artifact_refs,payload,created_at,schema_version) VALUES('event-1','org-1','MESSAGE','agent-1','[]','[]','{}','2026-08-13T12:00:00Z',?)`, schemaVersion); err != nil {
				_ = db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			if _, err := Verify(ctx, path); err == nil || !strings.Contains(err.Error(), "outside supported Event Contract") {
				t.Fatalf("recovery verification error=%v", err)
			}
		})
	}
}

func TestRecoveryRejectsProjectionAdmissionCorruption(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _ = appendRecoveryProjectionChain(t, ctx, store); t.Failed() {
		_ = store.Close()
		return
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, path); err != nil {
		t.Fatalf("valid projection admission failed recovery verification: %v", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE records SET admission_fingerprint=? WHERE kind='task' AND record_id='task-1'`, strings.Repeat("0", 64)); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, path); err == nil {
		t.Fatal("recovery verification accepted corrupt projection admission")
	}
}

func TestRecoveryRejectsProjectionOrganizationMismatch(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	taskEvent, taskRecord := appendRecoveryProjectionChain(t, ctx, store)
	if t.Failed() {
		_ = store.Close()
		return
	}
	payload, present, err := events.AdmittedProjection(taskEvent)
	if err != nil || !present {
		_ = store.Close()
		t.Fatalf("task projection admission is invalid: present=%t err=%v", present, err)
	}
	taskEvent.OrganizationID = "org-2"
	sealed, err := events.SealProjectionEvent(taskEvent, taskRecord, payload.Detail)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	body, err := json.Marshal(sealed)
	if err != nil {
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
	if _, err := db.ExecContext(ctx, `UPDATE events SET organization_id=?,payload=? WHERE event_id=?`, taskEvent.OrganizationID, body, taskEvent.EventID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE records SET admission_fingerprint=? WHERE admission_event_id=?`, sealed.Admission.Fingerprint, taskEvent.EventID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, path); err == nil {
		t.Fatal("recovery verification accepted a cross-organization projection")
	}
}

func TestRecoveryRejectsMissingProjectionOrganization(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _ = appendRecoveryProjectionState(t, ctx, store, true); t.Failed() {
		_ = store.Close()
		return
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	deleteRecoveryProjection(t, ctx, path, "organization", "org-1", 1)
	if _, err := Verify(ctx, path); err == nil {
		t.Fatal("recovery verification accepted projections for a missing Organization")
	}
}

func TestRecoveryRejectsChildBeforeOrganizationAdmission(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	mission := core.Mission{ID: "mission-1", OrganizationID: organization.ID, Statement: "Mission", Status: core.MissionActive, CreatedAt: now}
	organizationEvent, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"},
		ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	missionEvent, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "MISSION_CREATED", SourceActorID: "runtime", CorrelationID: "mission-1"},
		ProjectionKind: "mission", RecordID: string(mission.ID), Version: 1, Value: mission,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	swapRecoveryProjectionSequences(t, ctx, path, organizationEvent, missionEvent)
	if _, err := Verify(ctx, path); err == nil || !strings.Contains(err.Error(), "durable parent Organization") {
		t.Fatalf("child-before-Organization recovery error=%v", err)
	}
}

func TestRecoveryRejectsMissingGoalMission(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	mission := core.Mission{ID: "mission-1", OrganizationID: organization.ID, Statement: "Mission", Status: core.MissionActive, CreatedAt: now}
	goal := core.Goal{ID: "goal-1", OrganizationID: organization.ID, MissionID: mission.ID, Objective: "Outcome", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "verified", Origin: "TEST"}}, Status: core.GoalActive, CreatedAt: now}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup-1"}, ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization},
		{Event: events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "MISSION_CREATED", SourceActorID: "runtime", CorrelationID: "mission-1"}, ProjectionKind: "mission", RecordID: string(mission.ID), Version: 1, Value: mission},
		{Event: events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "GOAL_CREATED", SourceActorID: "runtime", CorrelationID: "goal-1"}, ProjectionKind: "goal", RecordID: string(goal.ID), Version: 1, Value: goal},
	} {
		if _, err := store.AppendProjection(ctx, draft); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM events WHERE event_type='MISSION_CREATED'; DELETE FROM records WHERE kind='mission'`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, path); err == nil {
		t.Fatal("recovery verification accepted a Goal with a missing Mission")
	}
}

func TestRecoveryRejectsGoalBeforeMissionAdmission(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	mission := core.Mission{ID: "mission-1", OrganizationID: organization.ID, Statement: "Mission", Status: core.MissionActive, CreatedAt: now}
	goal := core.Goal{ID: "goal-1", OrganizationID: organization.ID, MissionID: mission.ID, Objective: "Outcome", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "verified", Origin: "TEST"}}, Status: core.GoalActive, CreatedAt: now}
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"},
		ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization,
	}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	missionEvent, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "MISSION_CREATED", SourceActorID: "runtime", CorrelationID: "mission-1"},
		ProjectionKind: "mission", RecordID: string(mission.ID), Version: 1, Value: mission,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	goalEvent, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "GOAL_CREATED", SourceActorID: "runtime", CorrelationID: "goal-1"},
		ProjectionKind: "goal", RecordID: string(goal.ID), Version: 1, Value: goal,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	swapRecoveryProjectionSequences(t, ctx, path, missionEvent, goalEvent)
	if _, err := Verify(ctx, path); err == nil || !strings.Contains(err.Error(), "durable same-organization Mission") {
		t.Fatalf("Goal-before-Mission recovery error=%v", err)
	}
}

func TestRecoveryRejectsNoncontiguousProjectionRevisions(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	mission := core.Mission{ID: "mission-1", OrganizationID: organization.ID, Statement: "Mission", Status: core.MissionActive, CreatedAt: now}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup-1"}, ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization},
		{Event: events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "MISSION_CREATED", SourceActorID: "runtime", CorrelationID: "mission-1"}, ProjectionKind: "mission", RecordID: string(mission.ID), Version: 1, Value: mission},
	} {
		if _, err := store.AppendProjection(ctx, draft); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
	}
	mission.Statement = "Revised mission"
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "MISSION_REVISED", SourceActorID: "runtime", CorrelationID: "mission-1"},
		ProjectionKind: "mission", RecordID: string(mission.ID), Version: 2, Value: mission,
	}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	deleteRecoveryProjection(t, ctx, path, "mission", mission.ID, 1)
	if _, err := Verify(ctx, path); err == nil {
		t.Fatal("recovery verification accepted a projection revision gap")
	}
}

func TestRecoveryRejectsReorderedProjectionRevisionEvents(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	mission := core.Mission{ID: "mission-1", OrganizationID: organization.ID, Statement: "Mission", Status: core.MissionActive, CreatedAt: now}
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup-1"},
		ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization,
	}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	first, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "MISSION_CREATED", SourceActorID: "runtime", CorrelationID: "mission-1"},
		ProjectionKind: "mission", RecordID: string(mission.ID), Version: 1, Value: mission,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	mission.Statement = "Revised mission"
	second, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "MISSION_REVISED", SourceActorID: "runtime", CorrelationID: "mission-1"},
		ProjectionKind: "mission", RecordID: string(mission.ID), Version: 2, Value: mission,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	swapRecoveryProjectionSequences(t, ctx, path, first, second)
	if _, err := Verify(ctx, path); err == nil {
		t.Fatal("recovery verification accepted projection events in reverse revision order")
	}
}

func TestRecoveryRejectsWorkBeforeIntentAdmission(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	intent := core.Intent{ID: "intent-1", OrganizationID: organization.ID, NormalizedObjective: "objective", CreatedAt: now}
	work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"},
		ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization,
	}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	intentEvent, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: "work-1"},
		ProjectionKind: "intent", RecordID: string(intent.ID), Version: 1, Value: intent,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	workEvent, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: "work-1"},
		ProjectionKind: "work", RecordID: string(work.ID), Version: 1, Value: work,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	swapRecoveryProjectionSequences(t, ctx, path, intentEvent, workEvent)
	if _, err := Verify(ctx, path); err == nil || !strings.Contains(err.Error(), "work requires its durable Intent") {
		t.Fatalf("Work-before-Intent recovery error=%v", err)
	}
}

func TestRecoveryRejectsGoalBoundWorkWithoutConfirmation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	mission := core.Mission{ID: "mission-1", OrganizationID: organization.ID, Statement: "Mission", Status: core.MissionActive, CreatedAt: now}
	criterion := core.IntentValue{Value: "verified", Origin: "USER"}
	goal := core.Goal{ID: "goal-1", OrganizationID: organization.ID, MissionID: mission.ID, Objective: "Outcome", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{criterion}, Status: core.GoalActive, CreatedAt: now}
	var goalEvent events.Event
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "MISSION_CREATED", SourceActorID: "runtime", CorrelationID: "mission-1"}, ProjectionKind: "mission", RecordID: string(mission.ID), Version: 1, Value: mission},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "GOAL_CREATED", SourceActorID: "runtime", CorrelationID: "goal-1"}, ProjectionKind: "goal", RecordID: string(goal.ID), Version: 1, Value: goal},
	} {
		admitted, err := store.AppendProjection(ctx, draft)
		if err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		if draft.ProjectionKind == "goal" {
			goalEvent = admitted
		}
	}
	const correlationID = "run-1"
	const taskID = "task-run-1"
	const messageID = "message-1"
	intentDraft := core.IntentDraft{
		ID: "intent-run-1", OrganizationID: organization.ID, Version: 1, Status: core.IntentStatusReadyForReview, Mode: core.IntentModeStandard,
		RequestedExecutionKind: core.ExecutionDeterministic,
		Goal:                   &core.IntentValue{Value: string(goal.ID), Origin: "EXPLICIT", SourceMessageID: messageID},
		Objective:              "bounded work",
		Context:                []core.IntentValue{}, Deliverables: []core.IntentValue{{Value: "result", Origin: "USER"}}, CompletionCriteria: []core.IntentValue{criterion},
		Constraints: []core.IntentValue{}, ResolvedDecisions: []core.IntentDecision{}, ConsequenceCandidates: []string{}, MissingUserInputs: []core.IntentValue{}, CreatedAt: now,
	}
	intentDraft.Fingerprint, err = core.FingerprintIntentDraft(intentDraft)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "INTAKE_MESSAGE_RECORDED", SourceActorID: "user-1", TaskID: taskID, CorrelationID: correlationID,
		Payload: events.IntakeMessageRecordedPayload{MessageID: messageID, Text: "perform bounded work for goal-1", SourcePrincipalID: "user-1", SourcePrincipalKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", RequestedExecutionKind: core.ExecutionDeterministic},
	}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "INTENT_DRAFTED", SourceActorID: "runtime", TaskID: taskID, CorrelationID: correlationID,
		Payload: events.IntentDraftedPayload{SourceMessageID: messageID, Draft: intentDraft, Reply: "Review the proposed intent."},
	}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	confirmation := events.IntentConfirmedPayload{IntentID: string(intentDraft.ID), GoalID: string(goal.ID), Version: 1, Fingerprint: intentDraft.Fingerprint, ConfirmingActorID: "user-1", ConfirmingActorKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", MessageID: "confirmation-1"}
	confirmationEvent, err := store.AppendIntentConfirmation(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CONFIRMED", SourceActorID: "user-1", TaskID: taskID, CorrelationID: correlationID, Payload: confirmation}, goal.ID, "")
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	intent := core.Intent{ID: intentDraft.ID, OrganizationID: organization.ID, GoalID: goal.ID, OriginalInstruction: "perform bounded work for goal-1", NormalizedObjective: intentDraft.Objective, AcceptedFingerprint: intentDraft.Fingerprint, SourcePrincipalID: "user-1", SourcePrincipalKind: core.PrincipalHuman, SourceChannel: "HUMAN_DIRECT", SourceMessageID: messageID, CompletionCriteria: intentDraft.CompletionCriteria, CreatedAt: now}
	work := core.Work{ID: "work-1", IntentID: intent.ID, GoalID: goal.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: correlationID}, ProjectionKind: "intent", RecordID: string(intent.ID), Version: 1, Value: intent},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: correlationID}, ProjectionKind: "work", RecordID: string(work.ID), Version: 1, Value: work},
	} {
		if _, err := store.AppendProjection(ctx, draft); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, path); err != nil {
		t.Fatalf("valid reviewed Goal-bound Work failed recovery: %v", err)
	}
	pristine, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		want   string
		tamper func(*testing.T, string)
	}{
		{name: "missing confirmation", want: "one prior intent confirmation", tamper: func(t *testing.T, path string) {
			deleteRecoveryEvent(t, ctx, path, `event_id=?`, confirmationEvent.EventID)
		}},
		{name: "missing intake evidence", want: "current durable reviewed draft", tamper: func(t *testing.T, path string) {
			deleteRecoveryEvent(t, ctx, path, `correlation_id=? AND event_type='INTAKE_MESSAGE_RECORDED'`, correlationID)
		}},
		{name: "missing reviewed draft", want: "current durable reviewed draft", tamper: func(t *testing.T, path string) {
			deleteRecoveryEvent(t, ctx, path, `correlation_id=? AND event_type='INTENT_DRAFTED'`, correlationID)
		}},
		{name: "goal admitted after confirmation", want: "active Goal at admission", tamper: func(t *testing.T, path string) {
			swapRecoveryProjectionAndOrdinarySequences(t, ctx, path, goalEvent, confirmationEvent)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			tampered := filepath.Join(t.TempDir(), "ledger.db")
			if err := os.WriteFile(tampered, pristine, 0o600); err != nil {
				t.Fatal(err)
			}
			test.tamper(t, tampered)
			if _, err := Verify(ctx, tampered); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("invalid Goal-bound confirmation recovery error=%v want=%q", err, test.want)
			}
		})
	}
}

func TestRecoveryRechecksReplacementFailureAtConfirmationSequence(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "replacement.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	gateway := events.NewGateway(store)
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	predecessorIntent := core.Intent{
		ID: "intent-old", OrganizationID: organization.ID, OriginalInstruction: "old work", NormalizedObjective: "old work",
		SourcePrincipalID: "runtime", SourcePrincipalKind: core.PrincipalRuntime, SourceChannel: "INTERNAL", AcceptedFingerprint: "internal-old", CreatedAt: now,
	}
	predecessor := core.Work{ID: "work-old", IntentID: predecessorIntent.ID, Objective: predecessorIntent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
	for _, projection := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: "old"}, ProjectionKind: "intent", RecordID: string(predecessorIntent.ID), Version: 1, Value: predecessorIntent},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: "old"}, ProjectionKind: "work", RecordID: string(predecessor.ID), Version: 1, Value: predecessor},
	} {
		if _, err := store.AppendProjection(ctx, projection); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
	}
	application := app.New(gateway)
	const requestID = "replacement"
	const messageID = "message-replacement"
	if _, err := application.RecordIntakeMessage(ctx, app.IntakeMessage{
		RequestID: requestID, OrganizationID: "org-1", MessageID: messageID, Text: "Replace work-old with a verified result",
		SourcePrincipalID: "user-1", SourcePrincipalKind: core.PrincipalHuman, SourceChannel: "HUMAN_DIRECT", RequestedKind: core.ExecutionDeterministic,
	}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	correlationID, found, err := gateway.ResolveExternalWork(ctx, "org-1", requestID)
	if err != nil || !found {
		_ = store.Close()
		t.Fatalf("resolve replacement correlation: found=%t err=%v", found, err)
	}
	draft := core.IntentDraft{
		ID: core.ID("intent-" + correlationID), OrganizationID: organization.ID, Version: 1, Status: core.IntentStatusReadyForReview, Mode: core.IntentModeStandard,
		RequestedExecutionKind: core.ExecutionDeterministic,
		ReplacesWork:           &core.IntentValue{Value: string(predecessor.ID), Origin: "EXPLICIT", SourceMessageID: messageID},
		Objective:              "echo replacement result",
		Context:                []core.IntentValue{}, Deliverables: []core.IntentValue{{Value: "replacement result", Origin: "EXPLICIT", SourceMessageID: messageID}}, CompletionCriteria: []core.IntentValue{{Value: "replacement result verified", Origin: "EXPLICIT", SourceMessageID: messageID}},
		Constraints: []core.IntentValue{}, ResolvedDecisions: []core.IntentDecision{}, ConsequenceCandidates: []string{}, MissingUserInputs: []core.IntentValue{}, CreatedAt: now,
	}
	draft.Fingerprint, err = core.FingerprintIntentDraft(draft)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if _, err := application.RecordIntentDraft(ctx, "org-1", requestID, messageID, draft, "Review replacement Work."); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	predecessor.Status = core.WorkFailed
	failureEvent, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_FAILED", SourceActorID: "runtime", CorrelationID: "old", Payload: map[string]string{"reason": "bounded failure"}},
		ProjectionKind: "work", RecordID: string(predecessor.ID), Version: 2, Value: predecessor,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	result, err := application.ConfirmIntent(ctx, app.IntentConfirmation{
		RequestID: requestID, OrganizationID: "org-1", MessageID: "confirmation-replacement", Fingerprint: draft.Fingerprint,
		SourcePrincipalID: "user-1", SourcePrincipalKind: core.PrincipalHuman, SourceChannel: "HUMAN_DIRECT", Kind: core.ExecutionDeterministic,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	var confirmationEvent events.Event
	for _, event := range result.Events {
		if event.EventType == "INTENT_CONFIRMED" {
			confirmationEvent = event
		}
	}
	if confirmationEvent.EventID == "" {
		_ = store.Close()
		t.Fatal("replacement confirmation was not persisted")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, path); err != nil {
		t.Fatalf("valid replacement failed recovery verification: %v", err)
	}
	swapRecoveryProjectionAndOrdinarySequences(t, ctx, path, failureEvent, confirmationEvent)
	if _, err := Verify(ctx, path); err == nil || !strings.Contains(err.Error(), "prior failed Work with the same Goal binding at admission") {
		t.Fatalf("recovery accepted replacement before predecessor failure: %v", err)
	}
}

func TestRecoveryRequiresConfirmationForRuntimeReplacement(t *testing.T) {
	if !intentRequiresConfirmation(core.Intent{ReplacesWorkID: "work-old", SourcePrincipalID: "runtime", SourcePrincipalKind: core.PrincipalRuntime, SourceChannel: "INTERNAL"}) {
		t.Fatal("recovery treated runtime replacement lineage as unreviewed internal Work")
	}
}

func deleteRecoveryEvent(t *testing.T, ctx context.Context, path, predicate string, args ...any) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	result, err := db.ExecContext(ctx, `DELETE FROM events WHERE `+predicate, args...)
	if err != nil {
		t.Fatal(err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		t.Fatalf("deleted recovery events=%d error=%v", affected, err)
	}
}

func TestRecoveryRejectsOrphanGoalBoundConfirmation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	mission := core.Mission{ID: "mission-1", OrganizationID: organization.ID, Statement: "Mission", Status: core.MissionActive, CreatedAt: now}
	goal := core.Goal{ID: "goal-1", OrganizationID: organization.ID, MissionID: mission.ID, Objective: "Outcome", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "verified", Origin: "TEST"}}, Status: core.GoalActive, CreatedAt: now}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "MISSION_CREATED", SourceActorID: "runtime", CorrelationID: "mission-1"}, ProjectionKind: "mission", RecordID: string(mission.ID), Version: 1, Value: mission},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "GOAL_CREATED", SourceActorID: "runtime", CorrelationID: "goal-1"}, ProjectionKind: "goal", RecordID: string(goal.ID), Version: 1, Value: goal},
	} {
		if _, err := store.AppendProjection(ctx, draft); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(events.IntentConfirmedPayload{
		IntentID: "intent-orphan", GoalID: string(goal.ID), Version: 1, Fingerprint: "orphan-fingerprint",
		ConfirmingActorID: "user-1", ConfirmingActorKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", MessageID: "confirmation-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO events(event_id,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"orphan-confirmation", "org-1", "INTENT_CONFIRMED", "user-1", "", "", "", "task-orphan", `[]`, `[]`, payload, "orphan", now.Format(time.RFC3339Nano), events.SchemaVersion); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, path); err == nil || !strings.Contains(err.Error(), "current durable reviewed draft") {
		t.Fatalf("recovery accepted an orphan Goal-bound intent confirmation: %v", err)
	}
}

func TestRecoveryRevalidatesIntakeAbandonmentBoundaries(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "abandoned-intake.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	application := app.New(events.NewGateway(store))
	if _, err := application.RecordIntakeMessage(ctx, app.IntakeMessage{
		RequestID: "request-1", OrganizationID: "org-1", MessageID: "message-1", Text: "Prepare bounded work.",
		SourcePrincipalID: "user-1", SourcePrincipalKind: core.PrincipalHuman, SourceChannel: "HUMAN_DIRECT",
	}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if _, err := application.AbandonIntake(ctx, app.IntakeAbandonment{
		RequestID: "request-1", OrganizationID: "org-1", MessageID: "abandon-1",
		SourcePrincipalID: "user-1", SourcePrincipalKind: core.PrincipalHuman, SourceChannel: "HUMAN_DIRECT",
	}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, path); err != nil {
		t.Fatalf("valid abandoned intake failed recovery: %v", err)
	}
	pristine, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		want   string
		tamper func(*testing.T, *sql.DB)
	}{
		{name: "malformed payload", want: "abandonment event contract is invalid", tamper: func(t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, `UPDATE events SET payload='{}' WHERE event_type='INTAKE_ABANDONED'`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "wrong actor", want: "abandonment event contract is invalid", tamper: func(t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, `UPDATE events SET source_actor_id='user-2' WHERE event_type='INTAKE_ABANDONED'`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "duplicate", want: "multiple durable abandonments", tamper: func(t *testing.T, db *sql.DB) {
			if _, err := db.ExecContext(ctx, `INSERT INTO events(event_id,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version)
SELECT 'duplicate-abandonment',organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE event_type='INTAKE_ABANDONED'`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "after confirmation", want: "confirmed intent cannot be abandoned", tamper: func(t *testing.T, db *sql.DB) {
			var sequence int64
			if err := db.QueryRowContext(ctx, `SELECT sequence FROM events WHERE event_type='INTAKE_ABANDONED'`).Scan(&sequence); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `UPDATE events SET sequence=? WHERE event_type='INTAKE_ABANDONED'`, sequence+2); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO events(sequence,event_id,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version)
SELECT ?,'prior-confirmation',organization_id,'INTENT_CONFIRMED',source_actor_id,'','','',task_id,'[]','[]','{}',correlation_id,created_at,schema_version FROM events WHERE event_type='INTAKE_ABANDONED'`, sequence+1); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			tampered := filepath.Join(t.TempDir(), "ledger.db")
			if err := os.WriteFile(tampered, pristine, 0o600); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", tampered)
			if err != nil {
				t.Fatal(err)
			}
			test.tamper(t, db)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := Verify(ctx, tampered); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("recovery error=%v want=%q", err, test.want)
			}
		})
	}
}

func TestRecoveryRejectsTaskBeforeWorkAdmission(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	intent := core.Intent{ID: "intent-1", OrganizationID: organization.ID, NormalizedObjective: "objective", CreatedAt: now}
	work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
	task := core.Task{ID: "task-1", WorkID: work.ID, Description: "task", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: "work-1"}, ProjectionKind: "intent", RecordID: string(intent.ID), Version: 1, Value: intent},
	} {
		if _, err := store.AppendProjection(ctx, draft); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
	}
	workEvent, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: "work-1"},
		ProjectionKind: "work", RecordID: string(work.ID), Version: 1, Value: work,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	taskEvent, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "work-1"},
		ProjectionKind: "task", RecordID: string(task.ID), Version: 1, Value: task,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	swapRecoveryProjectionSequences(t, ctx, path, workEvent, taskEvent)
	if _, err := Verify(ctx, path); err == nil || !strings.Contains(err.Error(), "task requires its exact active Work") {
		t.Fatalf("Task-before-Work recovery error=%v", err)
	}
}

func TestRecoveryRejectsTaskBeforeAssigneeAdmission(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	blueprint := core.AgentBlueprint{ID: "blueprint-1", OrganizationID: organization.ID, Version: "v1", Role: "worker", OperatingInstructions: "bounded work", RequiredCapabilityClasses: []string{}, Status: "ACTIVE", CreatedAt: now}
	profile := core.ExecutionProfile{ID: "profile-1", OrganizationID: organization.ID, Version: "v1", ModelProvider: "provider", Model: "model", PromptVersion: "v1", ToolRefs: []string{}, Status: "ACTIVE", CreatedAt: now}
	agent := core.Agent{ID: "agent-1", OrganizationID: organization.ID, BlueprintID: blueprint.ID, BlueprintVersion: blueprint.Version, ExecutionProfileID: profile.ID, ExecutionProfileVersion: profile.Version, RuntimeAdapter: "local", Status: "ACTIVE"}
	intent := core.Intent{ID: "intent-1", OrganizationID: organization.ID, NormalizedObjective: "objective", CreatedAt: now}
	work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
	config := core.AgentConfig{BlueprintID: blueprint.ID, BlueprintVersion: blueprint.Version, ProfileID: profile.ID, ProfileVersion: profile.Version, RuntimeAdapter: agent.RuntimeAdapter}
	task := core.Task{ID: "task-1", WorkID: work.ID, Description: "task", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: &config, TaskContractVersion: "1", Status: core.TaskPending}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: "work-1"}, ProjectionKind: "intent", RecordID: string(intent.ID), Version: 1, Value: intent},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: "work-1"}, ProjectionKind: "work", RecordID: string(work.ID), Version: 1, Value: work},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "AGENT_BLUEPRINT_CREATED", SourceActorID: "runtime", CorrelationID: "roster"}, ProjectionKind: "agent_blueprint", RecordID: string(blueprint.ID), Version: 1, Value: blueprint},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_PROFILE_CREATED", SourceActorID: "runtime", CorrelationID: "roster"}, ProjectionKind: "execution_profile", RecordID: string(profile.ID), Version: 1, Value: profile},
	} {
		if _, err := store.AppendProjection(ctx, draft); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
	}
	agentEvent, err := store.AppendProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "AGENT_CREATED", SourceActorID: "runtime", CorrelationID: "roster"}, ProjectionKind: "agent", RecordID: string(agent.ID), Version: 1, Value: agent})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	taskEvent, err := store.AppendProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "work-1"}, ProjectionKind: "task", RecordID: string(task.ID), Version: 1, Value: task})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	swapRecoveryProjectionSequences(t, ctx, path, agentEvent, taskEvent)
	if _, err := Verify(ctx, path); err == nil || !strings.Contains(err.Error(), "invalid Task assignment") {
		t.Fatalf("Task-before-assignee recovery error=%v", err)
	}
}

func TestRecoveryRejectsMislabeledTaskLifecycle(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = appendRecoveryProjectionChain(t, ctx, store)
	task := core.Task{ID: "task-1", WorkID: "work-1", Description: "recovery task", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskRunning}
	started, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_STARTED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "work-1"},
		ProjectionKind: "task", RecordID: string(task.ID), Version: 2, Value: task,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	payload, present, err := events.AdmittedProjection(started)
	if err != nil || !present {
		_ = store.Close()
		t.Fatalf("execution start admission is invalid: present=%t err=%v", present, err)
	}
	started.EventType = "TASK_RESUMED"
	sealed, err := events.SealProjectionEvent(started, payload.Projection, payload.Detail)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	body, err := json.Marshal(sealed)
	if err != nil {
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
	if _, err := db.ExecContext(ctx, `UPDATE events SET event_type=?,payload=? WHERE event_id=?`, started.EventType, body, started.EventID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE records SET admission_fingerprint=? WHERE admission_event_id=?`, sealed.Admission.Fingerprint, started.EventID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, path); err == nil {
		t.Fatal("recovery verification accepted a Task status under the wrong lifecycle event")
	}
}

func TestRecoveryRejectsCompletedTaskWithoutEvidenceChain(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = appendRecoveryProjectionChain(t, ctx, store)
	task := core.Task{ID: "task-1", WorkID: "work-1", Description: "recovery task", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskRunning}
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_STARTED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "work-1"},
		ProjectionKind: "task", RecordID: string(task.ID), Version: 2, Value: task,
	}); err != nil {
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
	var sequence int64
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0)+1 FROM events`).Scan(&sequence); err != nil {
		t.Fatal(err)
	}
	task.Status = core.TaskCompleted
	value, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	record := events.ProjectionRecord{ProjectionKind: "task", RecordID: string(task.ID), Version: 3, CorrelationID: "work-1", Value: value}
	boundary := events.Event{
		EventID: "forged-task-completion", Sequence: sequence, OrganizationID: "org-1", EventType: "TASK_VERIFIED_COMPLETE", SourceActorID: "runtime",
		TaskID: string(task.ID), AuthorizationRefs: []string{}, ArtifactRefs: []string{}, CorrelationID: "work-1", CreatedAt: time.Now().UTC(), SchemaVersion: events.SchemaVersion,
	}
	decision := events.CompletionDecisionPayload{Contract: core.CompletionContract{TaskID: task.ID, TaskVersion: 2}, Result: events.CompletionDecisionResultPayload{Complete: true}, OutcomeEventRef: "missing-outcome"}
	detail, err := json.Marshal(decision)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := events.SealProjectionEvent(boundary, record, detail)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(sealed)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO events(sequence,event_id,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		boundary.Sequence, boundary.EventID, boundary.OrganizationID, boundary.EventType, boundary.SourceActorID, "", "", "", boundary.TaskID, `[]`, `[]`, payload, boundary.CorrelationID, boundary.CreatedAt.Format(time.RFC3339Nano), boundary.SchemaVersion); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO records(kind,record_id,version,body,admission_event_id,admission_fingerprint,created_at) VALUES(?,?,?,?,?,?,?)`, "task", string(task.ID), 3, body, boundary.EventID, sealed.Admission.Fingerprint, boundary.CreatedAt.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, path); err == nil || !strings.Contains(err.Error(), "completed Task task-1 lacks exact durable evidence") {
		t.Fatalf("recovery accepted a status-only Task completion: %v", err)
	}
}

func TestRecoveryRejectsMislabeledAgentLifecycle(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	blueprint := core.AgentBlueprint{ID: "blueprint-1", OrganizationID: organization.ID, Version: "v1", Role: "worker", OperatingInstructions: "bounded work", RequiredCapabilityClasses: []string{}, Status: "ACTIVE", CreatedAt: now}
	profile := core.ExecutionProfile{ID: "profile-1", OrganizationID: organization.ID, Version: "v1", ModelProvider: "provider", Model: "model", PromptVersion: "v1", ToolRefs: []string{}, Status: "ACTIVE", CreatedAt: now}
	agent := core.Agent{ID: "agent-1", OrganizationID: organization.ID, BlueprintID: blueprint.ID, BlueprintVersion: blueprint.Version, ExecutionProfileID: profile.ID, ExecutionProfileVersion: profile.Version, RuntimeAdapter: "local", Status: "ACTIVE"}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "organization", RecordID: "org-1", Version: 1, Value: organization},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "AGENT_BLUEPRINT_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "agent_blueprint", RecordID: string(blueprint.ID), Version: 1, Value: blueprint},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_PROFILE_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "execution_profile", RecordID: string(profile.ID), Version: 1, Value: profile},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "AGENT_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "agent", RecordID: string(agent.ID), Version: 1, Value: agent},
	} {
		if _, err := store.AppendProjection(ctx, draft); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
	}
	agent.Status = "INACTIVE"
	deactivated, err := store.AppendProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "AGENT_DEACTIVATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "agent", RecordID: string(agent.ID), Version: 2, Value: agent})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	payload, present, err := events.AdmittedProjection(deactivated)
	if err != nil || !present {
		_ = store.Close()
		t.Fatalf("Agent deactivation admission is invalid: present=%t err=%v", present, err)
	}
	deactivated.EventType = "AGENT_REACTIVATED"
	sealed, err := events.SealProjectionEvent(deactivated, payload.Projection, payload.Detail)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	body, err := json.Marshal(sealed)
	if err != nil {
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
	if _, err := db.ExecContext(ctx, `UPDATE events SET event_type=?,payload=? WHERE event_id=?`, deactivated.EventType, body, deactivated.EventID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE records SET admission_fingerprint=? WHERE admission_event_id=?`, sealed.Admission.Fingerprint, deactivated.EventID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, path); err == nil {
		t.Fatal("recovery verification accepted INACTIVE Agent state under AGENT_REACTIVATED")
	}
}

func TestRecoveryRejectsMislabeledGoalAchievement(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	refined, goal := appendRecoveryRefinedGoal(t, ctx, store)
	payload, present, err := events.AdmittedProjection(refined)
	if err != nil || !present {
		_ = store.Close()
		t.Fatalf("Goal refinement admission is invalid: present=%t err=%v", present, err)
	}
	goal.Status = core.GoalAchieved
	payload.Projection.Value, err = json.Marshal(goal)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	sealed, err := events.SealProjectionEvent(refined, payload.Projection, payload.Detail)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	eventBody, err := json.Marshal(sealed)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	recordBody, err := json.Marshal(payload.Projection)
	if err != nil {
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
	if _, err := db.ExecContext(ctx, `UPDATE events SET payload=? WHERE event_id=?`, eventBody, refined.EventID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE records SET body=?,admission_fingerprint=? WHERE admission_event_id=?`, recordBody, sealed.Admission.Fingerprint, refined.EventID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, path); err == nil {
		t.Fatal("recovery verification accepted achieved Goal state under GOAL_REFINED")
	}
}

func TestRecoveryRejectsMislabeledGoalLifecycle(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	refined, _ := appendRecoveryRefinedGoal(t, ctx, store)
	payload, present, err := events.AdmittedProjection(refined)
	if err != nil || !present {
		_ = store.Close()
		t.Fatalf("Goal refinement admission is invalid: present=%t err=%v", present, err)
	}
	refined.EventType = "GOAL_PAUSED"
	sealed, err := events.SealProjectionEvent(refined, payload.Projection, payload.Detail)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	eventBody, err := json.Marshal(sealed)
	if err != nil {
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
	if _, err := db.ExecContext(ctx, `UPDATE events SET event_type=?,payload=? WHERE event_id=?`, refined.EventType, eventBody, refined.EventID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE records SET admission_fingerprint=? WHERE admission_event_id=?`, sealed.Admission.Fingerprint, refined.EventID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, path); err == nil {
		t.Fatal("recovery verification accepted ACTIVE Goal state under GOAL_PAUSED")
	}
}

func TestRecoveryRejectsMislabeledWorkLifecycle(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _ = appendRecoveryProjectionChain(t, ctx, store); t.Failed() {
		_ = store.Close()
		return
	}
	failed := recoveryCurrentWork(t, ctx, store)
	failed.Status = core.WorkFailed
	failedEvent, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_FAILED", SourceActorID: "runtime", CorrelationID: "work-1"},
		ProjectionKind: "work", RecordID: "work-1", Version: 2, Value: failed,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	payload, present, err := events.AdmittedProjection(failedEvent)
	if err != nil || !present {
		_ = store.Close()
		t.Fatalf("Work failure admission is invalid: present=%t err=%v", present, err)
	}
	failed.Status = core.WorkActive
	payload.Projection.Value, err = json.Marshal(failed)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	resealRecoveryProjection(t, ctx, store, path, failedEvent, payload)
	if _, err := Verify(ctx, path); err == nil {
		t.Fatal("recovery verification accepted ACTIVE Work state under WORK_FAILED")
	}
}

func TestRecoveryRejectsMislabeledMissionLifecycle(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	mission := core.Mission{ID: "mission-1", OrganizationID: organization.ID, Statement: "durable direction", Status: core.MissionActive, CreatedAt: now}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "MISSION_CREATED", SourceActorID: "runtime", CorrelationID: "mission-1"}, ProjectionKind: "mission", RecordID: string(mission.ID), Version: 1, Value: mission},
	} {
		if _, err := store.AppendProjection(ctx, draft); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
	}
	revised := mission
	revised.Statement = "refined durable direction"
	revisedEvent, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "MISSION_REVISED", SourceActorID: "runtime", CorrelationID: "mission-1"},
		ProjectionKind: "mission", RecordID: string(mission.ID), Version: 2, Value: revised,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	payload, present, err := events.AdmittedProjection(revisedEvent)
	if err != nil || !present {
		_ = store.Close()
		t.Fatalf("Mission revision admission is invalid: present=%t err=%v", present, err)
	}
	revisedEvent.EventType = "MISSION_RETIRED"
	resealRecoveryProjection(t, ctx, store, path, revisedEvent, payload)
	if _, err := Verify(ctx, path); err == nil {
		t.Fatal("recovery verification accepted an ACTIVE Mission under MISSION_RETIRED")
	}
}

func TestRecoveryRejectsCompletedWorkWithoutEvidence(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _ = appendRecoveryProjectionChain(t, ctx, store); t.Failed() {
		_ = store.Close()
		return
	}
	work := recoveryCurrentWork(t, ctx, store)
	work.Status = core.WorkFailed
	failedEvent, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_FAILED", SourceActorID: "runtime", CorrelationID: "work-1"},
		ProjectionKind: "work", RecordID: "work-1", Version: 2, Value: work,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	payload, present, err := events.AdmittedProjection(failedEvent)
	if err != nil || !present {
		_ = store.Close()
		t.Fatalf("Work failure admission is invalid: present=%t err=%v", present, err)
	}
	work.Status = core.WorkCompleted
	payload.Projection.Value, err = json.Marshal(work)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	payload.Detail, err = json.Marshal(events.WorkCompletionTransitionPayload{EvidenceEventRef: "missing-evidence", Fingerprint: strings.Repeat("0", 64)})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	failedEvent.EventType = "WORK_COMPLETED"
	resealRecoveryProjection(t, ctx, store, path, failedEvent, payload)
	if _, err := Verify(ctx, path); err == nil {
		t.Fatal("recovery verification accepted completed Work without durable evidence")
	}
}

func resealRecoveryProjection(t *testing.T, ctx context.Context, store *ledger.SQLite, path string, event events.Event, payload events.ProjectionEventPayload) {
	t.Helper()
	sealed, err := events.SealProjectionEvent(event, payload.Projection, payload.Detail)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	eventBody, err := json.Marshal(sealed)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	recordBody, err := json.Marshal(payload.Projection)
	if err != nil {
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
	if _, err := db.ExecContext(ctx, `UPDATE events SET organization_id=?,event_type=?,payload=? WHERE event_id=?`, event.OrganizationID, event.EventType, eventBody, event.EventID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE records SET body=?,admission_fingerprint=? WHERE admission_event_id=?`, recordBody, sealed.Admission.Fingerprint, event.EventID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func recoveryCurrentWork(t *testing.T, ctx context.Context, store *ledger.SQLite) core.Work {
	t.Helper()
	bodies, err := store.Records(ctx, "work", "work-1")
	if err != nil || len(bodies) != 1 {
		t.Fatalf("current recovery Work records=%d err=%v", len(bodies), err)
	}
	var record events.ProjectionRecord
	var work core.Work
	if json.Unmarshal(bodies[0], &record) != nil || json.Unmarshal(record.Value, &work) != nil {
		t.Fatal("current recovery Work is invalid")
	}
	return work
}

func appendRecoveryRefinedGoal(t *testing.T, ctx context.Context, store *ledger.SQLite) (events.Event, core.Goal) {
	t.Helper()
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	mission := core.Mission{ID: "mission-1", OrganizationID: organization.ID, Statement: "produce bounded outcomes", Status: core.MissionActive, CreatedAt: now}
	goal := core.Goal{
		ID: "goal-1", OrganizationID: organization.ID, MissionID: mission.ID, Objective: "produce one bounded outcome",
		Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "bounded outcome", Origin: "USER"}}, Status: core.GoalActive, CreatedAt: now,
	}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "MISSION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "mission", RecordID: string(mission.ID), Version: 1, Value: mission},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "GOAL_CREATED", SourceActorID: "runtime", CorrelationID: "goal-1"}, ProjectionKind: "goal", RecordID: string(goal.ID), Version: 1, Value: goal},
	} {
		if _, err := store.AppendProjection(ctx, draft); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
	}
	goal.Objective = "produce one verified bounded outcome"
	refined, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "GOAL_REFINED", SourceActorID: "runtime", CorrelationID: "goal-1"},
		ProjectionKind: "goal", RecordID: string(goal.ID), Version: 2, Value: goal,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	return refined, goal
}

func swapRecoveryProjectionSequences(t *testing.T, ctx context.Context, path string, first, second events.Event) {
	t.Helper()
	firstBody, firstFingerprint := resealRecoveryProjectionAtSequence(t, first, second.Sequence)
	secondBody, secondFingerprint := resealRecoveryProjectionAtSequence(t, second, first.Sequence)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE events SET sequence=-sequence WHERE event_id IN (?,?)`, first.EventID, second.EventID); err != nil {
		_ = tx.Rollback()
		_ = db.Close()
		t.Fatal(err)
	}
	for _, update := range []struct {
		eventID     string
		sequence    int64
		body        []byte
		fingerprint string
	}{
		{first.EventID, second.Sequence, firstBody, firstFingerprint},
		{second.EventID, first.Sequence, secondBody, secondFingerprint},
	} {
		if _, err := tx.ExecContext(ctx, `UPDATE events SET sequence=?,payload=? WHERE event_id=?`, update.sequence, update.body, update.eventID); err != nil {
			_ = tx.Rollback()
			_ = db.Close()
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE records SET admission_fingerprint=? WHERE admission_event_id=?`, update.fingerprint, update.eventID); err != nil {
			_ = tx.Rollback()
			_ = db.Close()
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func swapRecoveryProjectionAndOrdinarySequences(t *testing.T, ctx context.Context, path string, projection, ordinary events.Event) {
	t.Helper()
	projectionBody, projectionFingerprint := resealRecoveryProjectionAtSequence(t, projection, ordinary.Sequence)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE events SET sequence=-sequence WHERE event_id IN (?,?)`, projection.EventID, ordinary.EventID); err != nil {
		_ = tx.Rollback()
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE events SET sequence=? WHERE event_id=?`, projection.Sequence, ordinary.EventID); err != nil {
		_ = tx.Rollback()
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE events SET sequence=?,payload=? WHERE event_id=?`, ordinary.Sequence, projectionBody, projection.EventID); err != nil {
		_ = tx.Rollback()
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE records SET admission_fingerprint=? WHERE admission_event_id=?`, projectionFingerprint, projection.EventID); err != nil {
		_ = tx.Rollback()
		_ = db.Close()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func resealRecoveryProjectionAtSequence(t *testing.T, event events.Event, sequence int64) ([]byte, string) {
	t.Helper()
	payload, present, err := events.AdmittedProjection(event)
	if err != nil || !present {
		t.Fatalf("projection admission is invalid: present=%t err=%v", present, err)
	}
	event.Sequence = sequence
	sealed, err := events.SealProjectionEvent(event, payload.Projection, payload.Detail)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(sealed)
	if err != nil {
		t.Fatal(err)
	}
	return body, sealed.Admission.Fingerprint
}

func TestRecoveryRejectsInvalidProjectionRevisionHistory(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	blueprint := core.AgentBlueprint{ID: "blueprint-1", OrganizationID: organization.ID, Version: "blueprint-v1", Role: "worker", OperatingInstructions: "Complete assigned work.", RequiredCapabilityClasses: []string{}, Status: "ACTIVE", CreatedAt: now}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup-1"}, ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization},
		{Event: events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "AGENT_BLUEPRINT_CREATED", SourceActorID: "runtime", CorrelationID: "roster-1"}, ProjectionKind: "agent_blueprint", RecordID: string(blueprint.ID), Version: 1, Value: blueprint},
	} {
		if _, err := store.AppendProjection(ctx, draft); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
	}
	blueprint.Status = "INACTIVE"
	blueprintEvent, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "AGENT_BLUEPRINT_UPDATED", SourceActorID: "runtime", CorrelationID: "roster-1"},
		ProjectionKind: "agent_blueprint", RecordID: string(blueprint.ID), Version: 2, Value: blueprint,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	payload, present, err := events.AdmittedProjection(blueprintEvent)
	if err != nil || !present {
		_ = store.Close()
		t.Fatalf("blueprint projection admission is invalid: present=%t err=%v", present, err)
	}
	blueprint.Role = "administrator"
	payload.Projection.Value, err = json.Marshal(blueprint)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	sealed, err := events.SealProjectionEvent(blueprintEvent, payload.Projection, payload.Detail)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	eventBody, err := json.Marshal(sealed)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	recordBody, err := json.Marshal(payload.Projection)
	if err != nil {
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
	if _, err := db.ExecContext(ctx, `UPDATE events SET payload=? WHERE event_id=?`, eventBody, blueprintEvent.EventID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE records SET body=?,admission_fingerprint=? WHERE admission_event_id=?`, recordBody, sealed.Admission.Fingerprint, blueprintEvent.EventID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, path); err == nil {
		t.Fatal("recovery verification accepted an immutable blueprint change")
	}
}

func TestRecoveryRejectsInvalidHistoricalBlueprintValue(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	blueprint := core.AgentBlueprint{ID: "blueprint-1", OrganizationID: organization.ID, Version: "v1", Role: "worker", OperatingInstructions: "bounded work", RequiredCapabilityClasses: []string{}, Status: "ACTIVE", CreatedAt: now}
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"},
		ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization,
	}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	created, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "AGENT_BLUEPRINT_CREATED", SourceActorID: "runtime", CorrelationID: "roster"},
		ProjectionKind: "agent_blueprint", RecordID: string(blueprint.ID), Version: 1, Value: blueprint,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	blueprint.Status = "INACTIVE"
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "AGENT_BLUEPRINT_UPDATED", SourceActorID: "runtime", CorrelationID: "roster"},
		ProjectionKind: "agent_blueprint", RecordID: string(blueprint.ID), Version: 2, Value: blueprint,
	}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	payload, present, err := events.AdmittedProjection(created)
	if err != nil || !present {
		_ = store.Close()
		t.Fatalf("blueprint creation admission is invalid: present=%t err=%v", present, err)
	}
	invalid := blueprint
	invalid.Status = "UNSUPPORTED"
	payload.Projection.Value, err = json.Marshal(invalid)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	resealRecoveryProjection(t, ctx, store, path, created, payload)
	if _, err := Verify(ctx, path); err == nil || !strings.Contains(err.Error(), "invalid Agent blueprint projection") {
		t.Fatalf("recovery accepted an invalid blueprint v1 hidden by valid v2: %v", err)
	}
}

func TestRecoveryRejectsTeamTenantReassignment(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	organizations := []core.Organization{
		{ID: "org-1", Name: "Organization 1", PolicyVersion: "v1", CreatedAt: now},
		{ID: "org-2", Name: "Organization 2", PolicyVersion: "v1", CreatedAt: now},
	}
	for _, organization := range organizations {
		if _, err := store.AppendProjection(ctx, events.ProjectionDraft{
			Event:          events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"},
			ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization,
		}); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
	}
	team := core.Team{ID: "team-1", OrganizationID: "org-1", Name: "Team", MemberAgentIDs: []core.ID{}, Status: "ACTIVE", CreatedAt: now}
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "TEAM_CREATED", SourceActorID: "runtime", CorrelationID: "roster"},
		ProjectionKind: "team", RecordID: string(team.ID), Version: 1, Value: team,
	}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	team.Name = "Revised Team"
	revised, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "TEAM_REVISED", SourceActorID: "runtime", CorrelationID: "roster"},
		ProjectionKind: "team", RecordID: string(team.ID), Version: 2, Value: team,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	payload, present, err := events.AdmittedProjection(revised)
	if err != nil || !present {
		_ = store.Close()
		t.Fatalf("Team revision admission is invalid: present=%t err=%v", present, err)
	}
	team.OrganizationID = "org-2"
	payload.Projection.Value, err = json.Marshal(team)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	revised.OrganizationID = "org-2"
	resealRecoveryProjection(t, ctx, store, path, revised, payload)
	if _, err := Verify(ctx, path); err == nil {
		t.Fatal("recovery verification accepted Team tenant reassignment")
	}
}

func TestRecoveryRejectsInvalidHistoricalTeamRoster(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"},
		ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization,
	}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	team := core.Team{ID: "team-1", OrganizationID: organization.ID, Name: "Team", MemberAgentIDs: []core.ID{}, Status: "ACTIVE", CreatedAt: now}
	created, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "TEAM_CREATED", SourceActorID: "runtime", CorrelationID: "roster"},
		ProjectionKind: "team", RecordID: string(team.ID), Version: 1, Value: team,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	team.Name = "Revised Team"
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "TEAM_REVISED", SourceActorID: "runtime", CorrelationID: "roster"},
		ProjectionKind: "team", RecordID: string(team.ID), Version: 2, Value: team,
	}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	payload, present, err := events.AdmittedProjection(created)
	if err != nil || !present {
		_ = store.Close()
		t.Fatalf("Team creation admission is invalid: present=%t err=%v", present, err)
	}
	team.Name = "Team"
	team.MemberAgentIDs = []core.ID{"missing-agent"}
	payload.Projection.Value, err = json.Marshal(team)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	resealRecoveryProjection(t, ctx, store, path, created, payload)
	if _, err := Verify(ctx, path); err == nil || !strings.Contains(err.Error(), "invalid Team roster") {
		t.Fatalf("historically invalid Team roster recovery error=%v", err)
	}
}

func TestRecoveryRejectsInvalidAgentRosterBinding(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	blueprint := core.AgentBlueprint{ID: "blueprint-1", OrganizationID: organization.ID, Version: "blueprint-v1", Role: "worker", OperatingInstructions: "Complete assigned work.", RequiredCapabilityClasses: []string{}, Status: "ACTIVE", CreatedAt: now}
	profile := core.ExecutionProfile{ID: "profile-1", OrganizationID: organization.ID, Version: "profile-v1", ModelProvider: "fake", Model: "fake-model/v1", PromptVersion: "prompt-v1", ToolRefs: []string{}, Status: "ACTIVE", CreatedAt: now}
	agent := core.Agent{ID: "agent-1", OrganizationID: organization.ID, BlueprintID: blueprint.ID, BlueprintVersion: blueprint.Version, ExecutionProfileID: profile.ID, ExecutionProfileVersion: profile.Version, RuntimeAdapter: "local", Status: "ACTIVE"}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup-1"}, ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization},
		{Event: events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "AGENT_BLUEPRINT_CREATED", SourceActorID: "runtime", CorrelationID: "roster-1"}, ProjectionKind: "agent_blueprint", RecordID: string(blueprint.ID), Version: 1, Value: blueprint},
		{Event: events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "EXECUTION_PROFILE_CREATED", SourceActorID: "runtime", CorrelationID: "roster-1"}, ProjectionKind: "execution_profile", RecordID: string(profile.ID), Version: 1, Value: profile},
		{Event: events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "AGENT_CREATED", SourceActorID: "runtime", CorrelationID: "roster-1"}, ProjectionKind: "agent", RecordID: string(agent.ID), Version: 1, Value: agent},
	} {
		if _, err := store.AppendProjection(ctx, draft); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	deleteRecoveryProjection(t, ctx, path, "agent_blueprint", blueprint.ID, 1)
	if _, err := Verify(ctx, path); err == nil {
		t.Fatal("recovery verification accepted an Agent with a missing blueprint")
	}
}

func TestRecoveryRejectsAgentBeforeConfigurationAdmission(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	blueprint := core.AgentBlueprint{ID: "blueprint-1", OrganizationID: organization.ID, Version: "v1", Role: "worker", OperatingInstructions: "bounded work", RequiredCapabilityClasses: []string{}, Status: "ACTIVE", CreatedAt: now}
	profile := core.ExecutionProfile{ID: "profile-1", OrganizationID: organization.ID, Version: "v1", ModelProvider: "provider", Model: "model", PromptVersion: "v1", ToolRefs: []string{}, Status: "ACTIVE", CreatedAt: now}
	agent := core.Agent{ID: "agent-1", OrganizationID: organization.ID, BlueprintID: blueprint.ID, BlueprintVersion: blueprint.Version, ExecutionProfileID: profile.ID, ExecutionProfileVersion: profile.Version, RuntimeAdapter: "local", Status: "ACTIVE"}
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	blueprintEvent, err := store.AppendProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "AGENT_BLUEPRINT_CREATED", SourceActorID: "runtime", CorrelationID: "roster"}, ProjectionKind: "agent_blueprint", RecordID: string(blueprint.ID), Version: 1, Value: blueprint})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_PROFILE_CREATED", SourceActorID: "runtime", CorrelationID: "roster"}, ProjectionKind: "execution_profile", RecordID: string(profile.ID), Version: 1, Value: profile}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	agentEvent, err := store.AppendProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "AGENT_CREATED", SourceActorID: "runtime", CorrelationID: "roster"}, ProjectionKind: "agent", RecordID: string(agent.ID), Version: 1, Value: agent})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	swapRecoveryProjectionSequences(t, ctx, path, blueprintEvent, agentEvent)
	if _, err := Verify(ctx, path); err == nil || !strings.Contains(err.Error(), "invalid pinned configuration at admission") {
		t.Fatalf("Agent-before-configuration recovery error=%v", err)
	}
}

func TestRecoveryRejectsHistoricalAgentConfigurationBinding(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	blueprintOne := core.AgentBlueprint{ID: "blueprint-1", OrganizationID: organization.ID, Version: "v1", Role: "worker", OperatingInstructions: "bounded work", RequiredCapabilityClasses: []string{}, Status: "ACTIVE", CreatedAt: now}
	blueprintTwo := blueprintOne
	blueprintTwo.ID = "blueprint-2"
	profile := core.ExecutionProfile{ID: "profile-1", OrganizationID: organization.ID, Version: "v1", ModelProvider: "provider", Model: "model", PromptVersion: "v1", ToolRefs: []string{}, Status: "ACTIVE", CreatedAt: now}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "organization", RecordID: "org-1", Version: 1, Value: organization},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "AGENT_BLUEPRINT_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "agent_blueprint", RecordID: string(blueprintOne.ID), Version: 1, Value: blueprintOne},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "AGENT_BLUEPRINT_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "agent_blueprint", RecordID: string(blueprintTwo.ID), Version: 1, Value: blueprintTwo},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_PROFILE_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "execution_profile", RecordID: string(profile.ID), Version: 1, Value: profile},
	} {
		if _, err := store.AppendProjection(ctx, draft); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
	}
	agent := core.Agent{ID: "agent-1", OrganizationID: organization.ID, BlueprintID: blueprintOne.ID, BlueprintVersion: blueprintOne.Version, ExecutionProfileID: profile.ID, ExecutionProfileVersion: profile.Version, RuntimeAdapter: "local", Status: "ACTIVE"}
	created, err := store.AppendProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "AGENT_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "agent", RecordID: string(agent.ID), Version: 1, Value: agent})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	agent.BlueprintID = blueprintTwo.ID
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "AGENT_CONFIGURATION_UPDATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "agent", RecordID: string(agent.ID), Version: 2, Value: agent}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	payload, present, err := events.AdmittedProjection(created)
	if err != nil || !present {
		_ = store.Close()
		t.Fatalf("Agent creation admission is invalid: present=%t err=%v", present, err)
	}
	agent.BlueprintID = "missing-blueprint"
	payload.Projection.Value, err = json.Marshal(agent)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	sealed, err := events.SealProjectionEvent(created, payload.Projection, payload.Detail)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	eventBody, err := json.Marshal(sealed)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	recordBody, err := json.Marshal(payload.Projection)
	if err != nil {
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
	if _, err := db.ExecContext(ctx, `UPDATE events SET payload=? WHERE event_id=?`, eventBody, created.EventID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE records SET body=?,admission_fingerprint=? WHERE admission_event_id=?`, recordBody, sealed.Admission.Fingerprint, created.EventID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, path); err == nil {
		t.Fatal("recovery verification accepted an invalid historical Agent configuration binding")
	}
}

func TestRecoveryRejectsSupersededAgentDispatchBinding(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	intent := core.Intent{ID: "intent-1", OrganizationID: organization.ID, NormalizedObjective: "objective", CreatedAt: now}
	work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
	blueprint := core.AgentBlueprint{ID: "blueprint-1", OrganizationID: organization.ID, Version: "v1", Role: "worker", OperatingInstructions: "bounded work", RequiredCapabilityClasses: []string{}, Status: "ACTIVE", CreatedAt: now}
	profile := core.ExecutionProfile{ID: "profile-1", OrganizationID: organization.ID, Version: "v1", ModelProvider: "provider", Model: "model", PromptVersion: "v1", ToolRefs: []string{}, Status: "ACTIVE", CreatedAt: now}
	agent := core.Agent{ID: "agent-1", OrganizationID: organization.ID, BlueprintID: blueprint.ID, BlueprintVersion: blueprint.Version, ExecutionProfileID: profile.ID, ExecutionProfileVersion: profile.Version, RuntimeAdapter: "local", Status: "ACTIVE"}
	config := core.AgentConfig{BlueprintID: blueprint.ID, BlueprintVersion: blueprint.Version, ProfileID: profile.ID, ProfileVersion: profile.Version, RuntimeAdapter: agent.RuntimeAdapter}
	task := core.Task{ID: "task-1", WorkID: work.ID, Description: "task", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: &config, TaskContractVersion: "1", Status: core.TaskPending}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: "work-1"}, ProjectionKind: "intent", RecordID: string(intent.ID), Version: 1, Value: intent},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: "work-1"}, ProjectionKind: "work", RecordID: string(work.ID), Version: 1, Value: work},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "AGENT_BLUEPRINT_CREATED", SourceActorID: "runtime", CorrelationID: "roster"}, ProjectionKind: "agent_blueprint", RecordID: string(blueprint.ID), Version: 1, Value: blueprint},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_PROFILE_CREATED", SourceActorID: "runtime", CorrelationID: "roster"}, ProjectionKind: "execution_profile", RecordID: string(profile.ID), Version: 1, Value: profile},
	} {
		if _, err := store.AppendProjection(ctx, draft); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
	}
	created, err := store.AppendProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "AGENT_CREATED", SourceActorID: "runtime", CorrelationID: "roster"}, ProjectionKind: "agent", RecordID: string(agent.ID), Version: 1, Value: agent})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "work-1"}, ProjectionKind: "task", RecordID: string(task.ID), Version: 1, Value: task}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	agent.Status = "INACTIVE"
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "AGENT_DEACTIVATED", SourceActorID: "runtime", CorrelationID: "roster"}, ProjectionKind: "agent", RecordID: string(agent.ID), Version: 2, Value: agent}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	agent.Status = "ACTIVE"
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "AGENT_REACTIVATED", SourceActorID: "runtime", CorrelationID: "roster"}, ProjectionKind: "agent", RecordID: string(agent.ID), Version: 3, Value: agent}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	task.Status = core.TaskRunning
	started, _, err := store.AppendExecutionStart(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_STARTED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "work-1"},
		ProjectionKind: "task", RecordID: string(task.ID), Version: 2, Value: task,
	}, []events.InboxRoute{{Scope: events.RecipientTask, ID: string(task.ID)}, {Scope: events.RecipientAgent, ID: string(agent.ID)}}, func([]events.InboxSelection) error { return nil })
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	payload, present, err := events.AdmittedProjection(started)
	if err != nil || !present {
		_ = store.Close()
		t.Fatalf("execution start admission: present=%t err=%v", present, err)
	}
	var detail events.ExecutionStartDetail
	if err := json.Unmarshal(payload.Detail, &detail); err != nil || detail.DispatchBinding == nil {
		_ = store.Close()
		t.Fatal("execution start dispatch binding is missing")
	}
	forged := *detail.DispatchBinding
	forged.AgentRecordVersion = 1
	forged.AgentEventRef = created.EventID
	detail.DispatchBinding = &forged
	payload.Detail, err = json.Marshal(detail)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	sealed, err := events.SealProjectionEvent(started, payload.Projection, payload.Detail)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	eventBody, err := json.Marshal(sealed)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, path); err != nil {
		t.Fatalf("valid Agent dispatch failed recovery before tampering: %v", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE events SET payload=? WHERE event_id=?`, eventBody, started.EventID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE records SET admission_fingerprint=? WHERE admission_event_id=?`, sealed.Admission.Fingerprint, started.EventID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, path); err == nil || !strings.Contains(err.Error(), "superseded roster revision") {
		t.Fatalf("superseded Agent dispatch recovery error=%v", err)
	}
}

func TestRecoveryRejectsInvalidTaskDAGBinding(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _ = appendRecoveryProjectionChain(t, ctx, store); t.Failed() {
		_ = store.Close()
		return
	}
	child := core.Task{ID: "task-2", WorkID: "work-1", ParentID: "task-1", Description: "child task", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending}
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(child.ID), CorrelationID: "work-1"},
		ProjectionKind: "task", RecordID: string(child.ID), Version: 1, Value: child,
	}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	deleteRecoveryProjection(t, ctx, path, "task", "task-1", 1)
	if _, err := Verify(ctx, path); err == nil {
		t.Fatal("recovery verification accepted a Task with a missing parent")
	}
}

func TestRecoveryRejectsCyclicTaskDAG(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	firstEvent, _ := appendRecoveryProjectionChain(t, ctx, store)
	if t.Failed() {
		_ = store.Close()
		return
	}
	second := core.Task{ID: "task-2", WorkID: "work-1", Description: "second task", DependsOn: []core.ID{"task-1"}, ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending}
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(second.ID), CorrelationID: "work-1"},
		ProjectionKind: "task", RecordID: string(second.ID), Version: 1, Value: second,
	}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	payload, present, err := events.AdmittedProjection(firstEvent)
	if err != nil || !present {
		_ = store.Close()
		t.Fatalf("first Task projection admission is invalid: present=%t err=%v", present, err)
	}
	var first core.Task
	if err := json.Unmarshal(payload.Projection.Value, &first); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	first.DependsOn = []core.ID{second.ID}
	payload.Projection.Value, err = json.Marshal(first)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	sealed, err := events.SealProjectionEvent(firstEvent, payload.Projection, payload.Detail)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	eventBody, err := json.Marshal(sealed)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	recordBody, err := json.Marshal(payload.Projection)
	if err != nil {
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
	if _, err := db.ExecContext(ctx, `UPDATE events SET payload=? WHERE event_id=?`, eventBody, firstEvent.EventID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE records SET body=?,admission_fingerprint=? WHERE admission_event_id=?`, recordBody, sealed.Admission.Fingerprint, firstEvent.EventID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, path); err == nil || !strings.Contains(err.Error(), "dependency cycle") {
		t.Fatalf("cyclic Task DAG recovery error=%v", err)
	}
}

func deleteRecoveryProjection(t *testing.T, ctx context.Context, path, kind string, recordID core.ID, version int) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	var eventID string
	if err := tx.QueryRowContext(ctx, `SELECT admission_event_id FROM records WHERE kind=? AND record_id=? AND version=?`, kind, recordID, version).Scan(&eventID); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM records WHERE kind=? AND record_id=? AND version=?`, kind, recordID, version); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM events WHERE event_id=?`, eventID); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func appendRecoveryProjectionChain(t *testing.T, ctx context.Context, store *ledger.SQLite) (events.Event, events.ProjectionRecord) {
	return appendRecoveryProjectionState(t, ctx, store, true)
}

func appendRecoveryProjectionState(t *testing.T, ctx context.Context, store *ledger.SQLite, includeOrganization bool) (events.Event, events.ProjectionRecord) {
	t.Helper()
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	intent := core.Intent{ID: "intent-1", OrganizationID: organization.ID, NormalizedObjective: "objective", CreatedAt: now}
	work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
	task := core.Task{ID: "task-1", WorkID: work.ID, Description: "recovery task", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending}
	drafts := []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: "work-1"}, ProjectionKind: "intent", RecordID: string(intent.ID), Version: 1, Value: intent},
		{Event: events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: "work-1"}, ProjectionKind: "work", RecordID: string(work.ID), Version: 1, Value: work},
	}
	if includeOrganization {
		drafts = append([]events.ProjectionDraft{{Event: events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup-1"}, ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization}}, drafts...)
	}
	for _, draft := range drafts {
		if _, err := store.AppendProjection(ctx, draft); err != nil {
			t.Errorf("append recovery projection %s: %v", draft.ProjectionKind, err)
			return events.Event{}, events.ProjectionRecord{}
		}
	}
	taskEvent, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "work-1"},
		ProjectionKind: "task", RecordID: string(task.ID), Version: 1, Value: task,
	})
	if err != nil {
		t.Errorf("append recovery Task projection: %v", err)
		return events.Event{}, events.ProjectionRecord{}
	}
	taskValue, err := json.Marshal(task)
	if err != nil {
		t.Errorf("encode recovery Task projection: %v", err)
		return events.Event{}, events.ProjectionRecord{}
	}
	return taskEvent, events.ProjectionRecord{ProjectionKind: "task", RecordID: string(task.ID), Version: 1, CorrelationID: "work-1", Value: taskValue}
}

func TestBackupRefusesExistingDestination(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source.db")
	db := createTestLedger(t, source)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(directory, "existing.db")
	if err := os.WriteFile(destination, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Backup(context.Background(), source, destination); err == nil {
		t.Fatal("backup overwrote an existing destination")
	}
	content, err := os.ReadFile(destination)
	if err != nil || string(content) != "keep" {
		t.Fatalf("existing destination changed: content=%q err=%v", content, err)
	}
}

func TestBackupRefusesStaleDestinationSidecars(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source.db")
	db := createTestLedger(t, source)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"-journal", "-shm", "-wal"} {
		t.Run(suffix, func(t *testing.T) {
			destination := filepath.Join(directory, "recovered"+suffix+".db")
			sidecar := destination + suffix
			if err := os.WriteFile(sidecar, []byte("stale"), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Backup(context.Background(), source, destination); err == nil {
				t.Fatal("backup published beside an existing SQLite sidecar")
			}
			if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("backup published destination: %v", err)
			}
			content, err := os.ReadFile(sidecar)
			if err != nil || string(content) != "stale" {
				t.Fatalf("existing sidecar changed: content=%q err=%v", content, err)
			}
		})
	}
}

func TestRecoveryEscapesSQLiteDSNCharacters(t *testing.T) {
	directory := t.TempDir()
	seed := filepath.Join(directory, "seed.db")
	db := createTestLedger(t, seed)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	special := "#"
	if runtime.GOOS != "windows" {
		special = "?"
	}
	source := filepath.Join(directory, "source"+special+"snapshot.db")
	if err := os.Rename(seed, source); err != nil {
		t.Fatal(err)
	}
	verified, err := Verify(context.Background(), source)
	if err != nil || verified.Path != source {
		t.Fatalf("verify special path: result=%+v err=%v", verified, err)
	}
	destination := filepath.Join(directory, "backup"+special+"dated.db")
	backup, err := Backup(context.Background(), source, destination)
	if err != nil || backup.Path != destination {
		t.Fatalf("backup special path: result=%+v err=%v", backup, err)
	}
	if _, err := os.Stat(destination); err != nil {
		t.Fatalf("exact backup destination missing: %v", err)
	}
	if special == "?" {
		prefix := filepath.Join(directory, "backup")
		if _, err := os.Stat(prefix); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("SQLite opened DSN prefix instead of exact path: %v", err)
		}
	}
}

func TestOpenReadOnlySQLiteRejectsWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	writable := createTestLedger(t, path)
	if err := writable.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := openReadOnlySQLite(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(t.Context(), `INSERT INTO events(event_id,payload) VALUES('forbidden','write')`); err == nil {
		t.Fatal("read-only recovery connection accepted a write")
	}
}

func createTestLedger(t *testing.T, path string) *sql.DB {
	t.Helper()
	store, err := ledger.Open(path)
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
	return db
}

package recovery

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
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
	if _, err := live.ExecContext(ctx, `INSERT INTO events(event_id,payload) VALUES('event-1','{}')`); err != nil {
		t.Fatal(err)
	}

	backupPath := filepath.Join(directory, "backup.db")
	backup, err := Backup(ctx, source, backupPath)
	if err != nil {
		t.Fatal(err)
	}
	if backup.Path != backupPath || backup.EventCount != 1 || backup.MaxSequence != 1 || backup.SHA256 == "" || backup.SizeBytes == 0 {
		t.Fatalf("backup=%+v", backup)
	}
	if _, err := live.ExecContext(ctx, `INSERT INTO events(event_id,payload) VALUES('event-2','{}')`); err != nil {
		t.Fatal(err)
	}
	verified, err := Verify(ctx, backupPath)
	if err != nil || verified.EventCount != 1 || verified.MaxSequence != 1 {
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
	if _, _ = appendRecoveryProjectionState(t, ctx, store, false); t.Failed() {
		_ = store.Close()
		return
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, path); err == nil {
		t.Fatal("recovery verification accepted projections for a missing Organization")
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
	if _, err := Verify(ctx, path); err == nil {
		t.Fatal("recovery verification accepted projection events in reverse revision order")
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
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(context.Background(), `CREATE TABLE events(
sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE, organization_id TEXT NOT NULL DEFAULT '',
event_type TEXT NOT NULL DEFAULT '', source_actor_id TEXT NOT NULL DEFAULT '', source_execution_id TEXT NOT NULL DEFAULT '',
recipient_scope TEXT NOT NULL DEFAULT '', recipient_id TEXT NOT NULL DEFAULT '', task_id TEXT NOT NULL DEFAULT '',
authorization_refs BLOB NOT NULL DEFAULT '[]', artifact_refs BLOB NOT NULL DEFAULT '[]', payload BLOB NOT NULL,
correlation_id TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL DEFAULT '', schema_version INTEGER NOT NULL DEFAULT 1);
CREATE TABLE records(kind TEXT NOT NULL, record_id TEXT NOT NULL, version INTEGER NOT NULL, body BLOB NOT NULL, admission_event_id TEXT NOT NULL DEFAULT '', admission_fingerprint TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL DEFAULT '', PRIMARY KEY(kind, record_id, version));
CREATE TABLE inbox(recipient_scope TEXT NOT NULL, recipient_id TEXT NOT NULL, event_id TEXT NOT NULL UNIQUE, organization_id TEXT NOT NULL DEFAULT '', task_id TEXT NOT NULL DEFAULT '', available_at TEXT NOT NULL DEFAULT '', observed_at TEXT NOT NULL DEFAULT '', observation_event_id TEXT NOT NULL DEFAULT '');
CREATE TABLE consumed_approvals(approval_id TEXT PRIMARY KEY, effect_fingerprint TEXT NOT NULL, consumed_at TEXT NOT NULL);
CREATE TABLE external_work(organization_id TEXT NOT NULL, request_id TEXT NOT NULL, correlation_id TEXT NOT NULL, intent_id TEXT NOT NULL, PRIMARY KEY(organization_id, request_id));
CREATE TABLE external_tasks(organization_id TEXT NOT NULL, task_id TEXT NOT NULL, correlation_id TEXT NOT NULL, PRIMARY KEY(organization_id, task_id));`)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return db
}

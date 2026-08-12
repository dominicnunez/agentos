package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestExternalWorkIndexMigratesLegacyCorrelation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	legacy, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "org-1", PolicyVersion: "v1", CreatedAt: now}
	agent := core.Agent{ID: "agent-local-org-1", OrganizationID: organization.ID, BlueprintVersion: "v1-local-worker", ExecutionProfileVersion: "v1-fake", RuntimeAdapter: "local", Status: "ACTIVE"}
	intent := core.Intent{ID: "intent-legacy-request", OrganizationID: organization.ID, OriginalInstruction: "echo legacy", NormalizedObjective: "echo legacy", SourcePrincipalID: "agent-1", SourcePrincipalKind: core.PrincipalExternalAgent, SourceChannel: "A2A", SourceMessageID: "message-1", CreatedAt: now}
	goal := core.Goal{ID: "goal-legacy-request", IntentID: intent.ID, Objective: intent.OriginalInstruction, Status: "COMPLETED", CreatedAt: now}
	task := core.Task{ID: "task-legacy-request", GoalID: goal.ID, Description: intent.OriginalInstruction, ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, AssigneeType: "AGENT", AssigneeID: agent.ID, TaskContractVersion: "1", Status: core.TaskCompleted}
	for _, projection := range []struct {
		eventType, kind, recordID, taskID string
		value                             any
	}{
		{"ORGANIZATION_CREATED", "organization", string(organization.ID), "", organization},
		{"AGENT_CREATED", "agent", string(agent.ID), "", agent},
		{"INTENT_CREATED", "intent", string(intent.ID), "", intent},
		{"GOAL_CREATED", "goal", string(goal.ID), "", goal},
		{"TASK_VERIFIED_COMPLETE", "task", string(task.ID), string(task.ID), task},
	} {
		if err := insertLegacyProjection(context.Background(), legacy, projection.eventType, projection.kind, projection.recordID, projection.taskID, projection.value); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := legacy.db.ExecContext(context.Background(), `DELETE FROM external_work`); err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	migrated, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = migrated.Close() })
	correlationID, found, err := migrated.ResolveExternalWork(context.Background(), "org-1", "legacy-request")
	if err != nil || !found || correlationID != "legacy-request" {
		t.Fatalf("correlation=%q found=%t err=%v", correlationID, found, err)
	}
	requestID, found, err := migrated.ResolveExternalRequest(context.Background(), "org-1", correlationID)
	if err != nil || !found || requestID != "legacy-request" {
		t.Fatalf("request=%q found=%t err=%v", requestID, found, err)
	}
	requestID, taskCorrelationID, found, err := migrated.ResolveExternalTask(context.Background(), "org-1", "task-legacy-request")
	if err != nil || !found || requestID != "legacy-request" || taskCorrelationID != correlationID {
		t.Fatalf("task request=%q correlation=%q found=%t err=%v", requestID, taskCorrelationID, found, err)
	}
	stream, err := migrated.Events(context.Background(), correlationID)
	if err != nil || len(stream) != 5 {
		t.Fatalf("legacy stream=%+v err=%v", stream, err)
	}
	if err := migrated.Close(); err != nil {
		t.Fatal(err)
	}
	migrated, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	after, err := migrated.Events(context.Background(), correlationID)
	if err != nil || len(after) != len(stream) {
		t.Fatalf("repeated migration changed legacy work: before=%d after=%d err=%v", len(stream), len(after), err)
	}
}

func TestReserveExternalWorkAvoidsLegacyCorrelationNamespace(t *testing.T) {
	ctx := context.Background()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.Append(ctx, events.TrustedDraft{OrganizationID: "attacker-org", EventType: "LEGACY_EVENT", CorrelationID: "w-collision", Payload: map[string]string{"source": "legacy"}}); err != nil {
		t.Fatal(err)
	}
	candidates := []string{"w-collision", "w-reserved"}
	next := 0
	store.newWorkCorrelation = func() (string, error) {
		if next >= len(candidates) {
			return "", errors.New("unexpected allocation attempt")
		}
		candidate := candidates[next]
		next++
		return candidate, nil
	}

	correlationID, err := store.ReserveExternalWork(ctx, "victim-org", "request-1")
	if err != nil || correlationID != "w-reserved" {
		t.Fatalf("reserved correlation=%q err=%v", correlationID, err)
	}
	replayed, err := store.ReserveExternalWork(ctx, "victim-org", "request-1")
	if err != nil || replayed != correlationID || next != len(candidates) {
		t.Fatalf("replayed reservation=%q attempts=%d err=%v", replayed, next, err)
	}
	legacy, err := store.Events(ctx, "w-collision")
	if err != nil || len(legacy) != 1 || legacy[0].OrganizationID != "attacker-org" {
		t.Fatalf("legacy stream changed=%+v err=%v", legacy, err)
	}
}

func insertLegacyProjection(ctx context.Context, l *SQLite, eventType, kind, recordID, taskID string, value any) error {
	encodedValue, err := json.Marshal(value)
	if err != nil {
		return err
	}
	record := events.ProjectionRecord{ProjectionKind: kind, RecordID: recordID, Version: 1, CorrelationID: "legacy-request", Value: encodedValue}
	body, err := json.Marshal(record)
	if err != nil {
		return err
	}
	payload := events.ProjectionEventPayload{Projection: record}
	if _, err := appendEvent(ctx, l.db, events.TrustedDraft{OrganizationID: "org-1", EventType: eventType, SourceActorID: "runtime", TaskID: taskID, CorrelationID: "legacy-request", Payload: payload}); err != nil {
		return err
	}
	_, err = l.db.ExecContext(ctx, `INSERT INTO records(kind,record_id,version,body,created_at) VALUES(?,?,?,?,?)`, kind, recordID, 1, body, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func TestAppendAndRead(t *testing.T) {
	l, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := l.Close(); err != nil {
			t.Errorf("close ledger: %v", err)
		}
	})
	e, err := l.Append(context.Background(), events.TrustedDraft{OrganizationID: "o", EventType: "TASK_CREATED", TaskID: "1", Payload: map[string]string{"ok": "yes"}, CorrelationID: "c"})
	if err != nil {
		t.Fatal(err)
	}
	if e.Sequence != 1 {
		t.Fatalf("sequence=%d", e.Sequence)
	}
	got, err := l.Events(context.Background(), "c")
	if err != nil || len(got) != 1 {
		t.Fatalf("events=%d err=%v", len(got), err)
	}
}

func TestEventsSurviveReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agentos.db")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append(ctx, events.TrustedDraft{OrganizationID: "o", EventType: "TASK_BLOCKED", TaskID: "task-1", Payload: map[string]string{"reason": "input required"}, CorrelationID: "request-1"}); err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	eventsAfterRestart, err := l.Events(ctx, "request-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(eventsAfterRestart) != 1 || eventsAfterRestart[0].EventType != "TASK_BLOCKED" || eventsAfterRestart[0].TaskID != "task-1" {
		t.Fatalf("persisted events after reopen=%+v", eventsAfterRestart)
	}
}

func TestProjectionVersionConflictRollsBackItsEvent(t *testing.T) {
	ctx := context.Background()
	l, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	draft := events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_BLOCKED", RecipientScope: events.RecipientTask, RecipientID: "task-parent", TaskID: "task-1", CorrelationID: "request-1"},
		ProjectionKind: "task",
		RecordID:       "task-1",
		Version:        1,
		Value:          map[string]string{"status": "PENDING"},
	}
	if _, err := l.AppendProjection(ctx, draft); err != nil {
		t.Fatal(err)
	}
	if _, err := l.AppendProjection(ctx, draft); err == nil {
		t.Fatal("duplicate projection version was accepted")
	}
	stream, err := l.Events(ctx, "request-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(stream) != 1 {
		t.Fatalf("projection failure left an orphan event: %+v", stream)
	}
	available, err := l.Inbox(ctx, events.RecipientTask, "task-parent")
	if err != nil || len(available) != 1 || available[0].EventID != stream[0].EventID {
		t.Fatalf("projection failure changed addressed availability: inbox=%+v err=%v", available, err)
	}
}

func TestProjectionBatchRollsBackCompleteTaskGraph(t *testing.T) {
	ctx := context.Background()
	l, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	drafts := []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", TaskID: "task-1", CorrelationID: "request-1"}, ProjectionKind: "task", RecordID: "task-1", Version: 1, Value: core.Task{ID: "task-1"}},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", TaskID: "task-1", CorrelationID: "request-1"}, ProjectionKind: "task", RecordID: "task-1", Version: 1, Value: core.Task{ID: "task-1"}},
	}
	if _, err := l.AppendProjections(ctx, drafts); err == nil {
		t.Fatal("conflicting Task-DAG batch was accepted")
	}
	stream, err := l.Events(ctx, "request-1")
	if err != nil || len(stream) != 0 {
		t.Fatalf("failed Task-DAG batch left events=%+v err=%v", stream, err)
	}
	records, err := l.Records(ctx, "task", "task-1")
	if err != nil || len(records) != 0 {
		t.Fatalf("failed Task-DAG batch left records=%d err=%v", len(records), err)
	}
}

func TestChildTaskIsNotExternallyAddressable(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agentos.db")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	correlationID, err := l.ReserveExternalWork(ctx, "org-1", "request-1")
	if err != nil {
		t.Fatal(err)
	}
	rootID := "task-" + correlationID
	childID := rootID + "-child"
	drafts := []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", TaskID: childID, CorrelationID: correlationID}, ProjectionKind: "task", RecordID: childID, Version: 1, Value: core.Task{ID: core.ID(childID), ParentID: core.ID(rootID)}},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", TaskID: rootID, CorrelationID: correlationID}, ProjectionKind: "task", RecordID: rootID, Version: 1, Value: core.Task{ID: core.ID(rootID)}},
	}
	if _, err := l.AppendProjections(ctx, drafts); err != nil {
		t.Fatal(err)
	}
	requestID, resolved, found, err := l.ResolveExternalTask(ctx, "org-1", rootID)
	if err != nil || !found || requestID != "request-1" || resolved != correlationID {
		t.Fatalf("root resolution request=%q correlation=%q found=%v err=%v", requestID, resolved, found, err)
	}
	if _, _, found, err := l.ResolveExternalTask(ctx, "org-1", childID); err != nil || found {
		t.Fatalf("internal child became externally addressable: found=%v err=%v", found, err)
	}
	// Simulate a row created by the former startup migration and prove the next
	// startup repairs the durable boundary rather than preserving stale access.
	if _, err := l.db.ExecContext(ctx, `INSERT INTO external_tasks(organization_id,task_id,correlation_id) VALUES(?,?,?)`, "org-1", childID, correlationID); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	if _, _, found, err := l.ResolveExternalTask(ctx, "org-1", childID); err != nil || found {
		t.Fatalf("startup migration preserved an externally addressable child: found=%v err=%v", found, err)
	}
	if requestID, resolved, found, err := l.ResolveExternalTask(ctx, "org-1", rootID); err != nil || !found || requestID != "request-1" || resolved != correlationID {
		t.Fatalf("startup migration lost root addressability: request=%q correlation=%q found=%v err=%v", requestID, resolved, found, err)
	}
}

func TestMessageInboxSurvivesReopenAndObservation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agentos.db")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	message, err := l.Append(ctx, events.TrustedDraft{
		OrganizationID: "org-1",
		EventType:      "MESSAGE",
		SourceActorID:  "agent-1",
		RecipientScope: events.RecipientAgent,
		RecipientID:    "agent-2",
		TaskID:         "task-1",
		Payload:        map[string]any{"body": "restart-safe handoff"},
	})
	if err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	available, err := l.Inbox(ctx, events.RecipientAgent, "agent-2")
	if err != nil || len(available) != 1 || available[0].EventID != message.EventID {
		_ = l.Close()
		t.Fatalf("reopened inbox=%+v err=%v", available, err)
	}
	observation, err := l.ObserveInbox(ctx, events.TrustedDraft{
		OrganizationID: "org-1",
		EventType:      "INBOX_EVENTS_OBSERVED",
		SourceActorID:  "agent-2",
		RecipientScope: events.RecipientAgent,
		RecipientID:    "agent-2",
		TaskID:         "task-2",
		Payload:        map[string]any{"event_ids": []string{message.EventID}},
	}, events.RecipientAgent, "agent-2", []string{message.EventID})
	if err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	if observation.EventType != "INBOX_EVENTS_OBSERVED" {
		_ = l.Close()
		t.Fatalf("observation=%+v", observation)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	available, err = l.Inbox(ctx, events.RecipientAgent, "agent-2")
	if err != nil || len(available) != 0 {
		t.Fatalf("observed inbox after reopen=%+v err=%v", available, err)
	}
	stream, err := l.Events(ctx, "")
	if err != nil || len(stream) != 2 || stream[1].EventID != observation.EventID {
		t.Fatalf("durable message/observation stream=%+v err=%v", stream, err)
	}
}

func TestMessageRollbackOnInboxFailure(t *testing.T) {
	ctx := context.Background()
	l, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	if _, err := l.db.ExecContext(ctx, `CREATE TRIGGER fail_inbox_insert BEFORE INSERT ON inbox BEGIN SELECT RAISE(FAIL, 'injected inbox failure'); END;`); err != nil {
		t.Fatal(err)
	}
	_, err = l.Append(ctx, events.TrustedDraft{
		OrganizationID: "org-1",
		EventType:      "MESSAGE",
		SourceActorID:  "agent-1",
		RecipientScope: events.RecipientAgent,
		RecipientID:    "agent-2",
		Payload:        map[string]any{"body": "must not become available"},
	})
	if err == nil {
		t.Fatal("injected inbox projection failure was ignored")
	}
	stream, readErr := l.Events(ctx, "")
	if readErr != nil || len(stream) != 0 {
		t.Fatalf("failed message left ledger evidence: events=%+v err=%v", stream, readErr)
	}
	available, readErr := l.Inbox(ctx, events.RecipientAgent, "agent-2")
	if readErr != nil || len(available) != 0 {
		t.Fatalf("failed message became available: inbox=%+v err=%v", available, readErr)
	}
}

func TestOpenMigratesEventRoutingColumns(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `CREATE TABLE events (
sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE, organization_id TEXT NOT NULL,
event_type TEXT NOT NULL, source_actor_id TEXT NOT NULL DEFAULT '', source_execution_id TEXT NOT NULL DEFAULT '', task_id TEXT NOT NULL DEFAULT '', authorization_refs BLOB NOT NULL, artifact_refs BLOB NOT NULL, payload BLOB NOT NULL,
correlation_id TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, schema_version INTEGER NOT NULL);`)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	message, err := l.Append(ctx, events.TrustedDraft{
		OrganizationID: "org-1",
		EventType:      "MESSAGE",
		SourceActorID:  "agent-1",
		RecipientScope: events.RecipientTask,
		RecipientID:    "task-1",
		Payload:        map[string]any{"body": "after migration"},
	})
	if err != nil {
		t.Fatal(err)
	}
	available, err := l.Inbox(ctx, events.RecipientTask, "task-1")
	if err != nil || len(available) != 1 || available[0].EventID != message.EventID {
		t.Fatalf("migrated inbox=%+v err=%v", available, err)
	}
}

func TestApprovalConsumptionAndAttemptTransitionAreAtomic(t *testing.T) {
	ctx := context.Background()
	l, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	lease := core.CapabilityLease{ID: "lease-1", ActorID: "actor-1", OriginTaskID: "task-1", Action: "send", Resource: "customer-1", Scope: "org-1"}
	if err := l.AppendRecord(ctx, "org-1", "CAPABILITY_GRANTED", "human-1", "task-1", nil, nil, "capability_lease", "lease-1", 1, lease); err != nil {
		t.Fatal(err)
	}
	pending := map[string]any{"effect_obligation_id": "effect-1", "status": "PENDING"}
	if err := l.AppendRecord(ctx, "org-1", "EFFECT_OBLIGATION_TRANSITIONED", "", "task-1", nil, nil, "effect", "effect-1", 1, pending); err != nil {
		t.Fatal(err)
	}
	obligation := core.EffectObligation{ID: "effect-1", OrganizationID: "org-1", TaskID: "task-1", ActorID: "actor-1", Action: "send", Resource: "customer-1", Scope: "org-1", ConsequenceBoundary: core.BoundaryPublicExternal, AuthorizationRefs: []string{"lease-1"}, ApprovalRef: "approval-1"}
	fingerprint, err := core.FingerprintEffect(obligation)
	if err != nil {
		t.Fatal(err)
	}
	obligation.EffectFingerprint = fingerprint
	approval := core.HumanApproval{ID: "approval-1", OrganizationID: "org-1", TaskID: "task-1", EffectObligationID: "effect-1", Action: "send", Resource: "customer-1", Boundary: core.BoundaryPublicExternal, Status: core.ApprovalApproved, EffectFingerprint: fingerprint, SingleUse: true}
	if err := l.AppendRecord(ctx, "org-1", "APPROVAL_DECIDED", "human-1", "task-1", nil, nil, "approval", "approval-1", 1, approval); err != nil {
		t.Fatal(err)
	}
	if _, err := l.db.ExecContext(ctx, `CREATE TRIGGER fail_effect_attempt BEFORE INSERT ON records WHEN NEW.kind='effect' AND NEW.version=2 BEGIN SELECT RAISE(FAIL, 'injected attempt failure'); END;`); err != nil {
		t.Fatal(err)
	}
	attempted := map[string]any{"effect_obligation_id": "effect-1", "status": "ATTEMPTED"}
	_, err = l.AuthorizeAndAppendEffectAttempt(ctx, obligation, 2, attempted)
	if err == nil {
		t.Fatal("injected attempt failure was ignored")
	}
	stream, err := l.Events(ctx, "")
	if err != nil || len(stream) != 3 || stream[1].EventType != "EFFECT_OBLIGATION_TRANSITIONED" || stream[2].EventType != "APPROVAL_DECIDED" {
		t.Fatalf("failed atomic transition left approval consumption: events=%+v err=%v", stream, err)
	}
	rows, err := l.Records(ctx, "effect", "effect-1")
	if err != nil || len(rows) != 1 {
		t.Fatalf("failed atomic transition changed effect: rows=%d err=%v", len(rows), err)
	}
	if _, err := l.db.ExecContext(ctx, `DROP TRIGGER fail_effect_attempt`); err != nil {
		t.Fatal(err)
	}
	if _, err := l.AuthorizeAndAppendEffectAttempt(ctx, obligation, 2, attempted); err != nil {
		t.Fatalf("rolled-back approval could not be retried: %v", err)
	}
	if _, err := l.AuthorizeAndAppendEffectAttempt(ctx, obligation, 3, attempted); err == nil {
		t.Fatal("consumed single-use approval was reused")
	}
}

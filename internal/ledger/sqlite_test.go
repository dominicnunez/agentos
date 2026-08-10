package ledger

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestAppendAndRead(t *testing.T) {
	l, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
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
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", TaskID: "task-1", CorrelationID: "request-1"},
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

func TestInboxProjectionFailureRollsBackMessage(t *testing.T) {
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
	approval := core.HumanApproval{ID: "approval-1", OrganizationID: "org-1", TaskID: "task-1", EffectObligationID: "effect-1", Action: "send", Resource: "customer-1", Boundary: core.BoundaryPublicExternal, Status: core.ApprovalApproved, EffectFingerprint: "fingerprint-1", SingleUse: true}
	if err := l.AppendRecord(ctx, "org-1", "APPROVAL_DECIDED", "human-1", "task-1", nil, nil, "approval", "approval-1", 1, approval); err != nil {
		t.Fatal(err)
	}
	obligation := core.EffectObligation{ID: "effect-1", OrganizationID: "org-1", TaskID: "task-1", ActorID: "actor-1", Action: "send", Resource: "customer-1", Scope: "org-1", ConsequenceBoundary: core.BoundaryPublicExternal, EffectFingerprint: "fingerprint-1", AuthorizationRefs: []string{"lease-1"}, ApprovalRef: "approval-1"}
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


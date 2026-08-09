package ledger

import (
	"context"
	"path/filepath"
	"testing"

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

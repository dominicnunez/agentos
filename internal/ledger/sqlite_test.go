package ledger

import (
	"context"
	"github.com/dominicnunez/agentos/internal/events"
	"testing"
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

package ledger

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestSecurityFreezeReleaseRejectsStaleAddressedWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	reader, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	writer, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	ctx, release, err := reader.BeginExecutionContext(t.Context(), "org-1")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	appendInferenceFreeze(t, writer, "org-1", 1, true)
	appendInferenceFreeze(t, writer, "org-1", 2, false)
	draft := events.TrustedDraft{OrganizationID: "org-1", EventType: "MESSAGE", SourceActorID: "agent-1", RecipientScope: events.RecipientAgent, RecipientID: "agent-2", TaskID: "task-1", CorrelationID: "stale-message", Payload: map[string]any{"body": "stale coordination"}}
	if _, err := reader.Append(context.WithoutCancel(ctx), draft); !errors.Is(err, core.ErrOrganizationFrozen) {
		t.Fatalf("stale addressed write: %v", err)
	}
	stream, err := reader.Events(t.Context(), "stale-message")
	if err != nil || len(stream) != 0 {
		t.Fatalf("stale write persisted event: %v", err)
	}
	inbox, err := reader.Inbox(t.Context(), events.RecipientAgent, "agent-2")
	if err != nil || len(inbox) != 0 {
		t.Fatalf("stale write persisted delivery: %v", err)
	}
	fresh, finish, err := reader.BeginExecutionContext(t.Context(), "org-1")
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	if _, err := reader.Append(fresh, draft); err != nil {
		t.Fatalf("fresh addressed write denied: %v", err)
	}
}

func TestSecurityFreezeSuspendsInboxWithoutDiscardingMessages(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	draft := events.TrustedDraft{OrganizationID: "org-1", EventType: "MESSAGE", SourceActorID: "agent-1", RecipientScope: events.RecipientAgent, RecipientID: "agent-2", TaskID: "task-1", CorrelationID: "held-inbox", Payload: map[string]any{"body": "durable coordination"}}
	message, err := store.Append(t.Context(), draft)
	if err != nil {
		t.Fatal(err)
	}
	appendInferenceFreeze(t, store, "org-1", 1, true)
	if _, err = store.Append(t.Context(), draft); !errors.Is(err, core.ErrOrganizationFrozen) {
		t.Fatalf("held message publication: %v", err)
	}
	inbox, err := store.Inbox(t.Context(), events.RecipientAgent, "agent-2")
	if err != nil {
		t.Fatal(err)
	}
	if len(inbox) != 0 {
		t.Fatal("held inbox delivered messages")
	}
	stream, err := store.Events(t.Context(), "held-inbox")
	if err != nil {
		t.Fatal(err)
	}
	if len(stream) != 1 || stream[0].EventID != message.EventID {
		t.Fatal("hold lost durable message or persisted rejected send")
	}
	appendInferenceFreeze(t, store, "org-1", 2, false)
	inbox, err = store.Inbox(t.Context(), events.RecipientAgent, "agent-2")
	if err != nil {
		t.Fatal(err)
	}
	if len(inbox) != 1 || inbox[0].EventID != message.EventID {
		t.Fatal("release did not restore original unobserved message")
	}
}

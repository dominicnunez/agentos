package ledger

import (
	"errors"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

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

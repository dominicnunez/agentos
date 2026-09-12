package ledger

import (
	"path/filepath"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestFreezeInboxUsesWarmHistory(t *testing.T) {
	var allocations []float64
	for _, count := range []int{16, 256} {
		store, err := Open(":memory:")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		seedFreezeHistory(t, store, count)
		message := appendFreezeInboxMessage(t, store, "org-1", "agent-2")
		if _, err := store.ReadFreeze(t.Context(), "org-1"); err != nil {
			t.Fatal(err)
		}
		allocations = append(allocations, testing.AllocsPerRun(1, func() {
			pending, err := store.Inbox(t.Context(), events.RecipientAgent, "agent-2")
			if err != nil || len(pending) != 1 || pending[0].EventID != message.EventID {
				t.Fatalf("pending inbox lost its message: count=%d err=%v", len(pending), err)
			}
		}))
	}
	// An unchanged, warmed history that is sixteen times longer must not
	// multiply the allocations of a poll for the same single message. Compare
	// growth, rather than imposing a machine-specific latency threshold.
	if allocations[1] > 2*allocations[0] {
		t.Fatalf("inbox allocations grow with warmed freeze history: 16=%g, 256=%g", allocations[0], allocations[1])
	}
}

func TestFreezeInboxRechecksOtherWriter(t *testing.T) {
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
	seedFreezeHistory(t, reader, 16)
	message := appendFreezeInboxMessage(t, reader, "org-1", "agent-2")
	other := appendFreezeInboxMessage(t, reader, "org-2", "agent-3")
	if _, err := reader.ReadFreeze(t.Context(), "org-1"); err != nil {
		t.Fatal(err)
	}
	appendInferenceFreeze(t, writer, "org-1", 17, true)
	pending, err := reader.Inbox(t.Context(), events.RecipientAgent, "agent-2")
	if err != nil || len(pending) != 0 {
		t.Fatalf("warmed inbox ignored another writer's hold: count=%d err=%v", len(pending), err)
	}
	pending, err = reader.Inbox(t.Context(), events.RecipientAgent, "agent-3")
	if err != nil || len(pending) != 1 || pending[0].EventID != other.EventID {
		t.Fatalf("hold affected another organization: count=%d err=%v", len(pending), err)
	}
	appendInferenceFreeze(t, writer, "org-1", 18, false)
	pending, err = reader.Inbox(t.Context(), events.RecipientAgent, "agent-2")
	if err != nil || len(pending) != 1 || pending[0].EventID != message.EventID {
		t.Fatalf("release lost the original pending message: count=%d err=%v", len(pending), err)
	}
	if _, err := writer.db.ExecContext(t.Context(), `UPDATE records SET body='{}' WHERE kind='organization_freeze' AND record_id='org-1' AND version=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Inbox(t.Context(), events.RecipientAgent, "agent-2"); err == nil {
		t.Fatal("warmed inbox accepted rewritten invalid freeze history")
	}
}

func BenchmarkFreezeInboxWarmHistory(b *testing.B) {
	store, err := Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })
	seedFreezeHistory(b, store, 4096)
	message := appendFreezeInboxMessage(b, store, "org-1", "agent-2")
	if _, err := store.ReadFreeze(b.Context(), "org-1"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pending, err := store.Inbox(b.Context(), events.RecipientAgent, "agent-2")
		if err != nil || len(pending) != 1 || pending[0].EventID != message.EventID {
			b.Fatalf("pending inbox lost its message: count=%d err=%v", len(pending), err)
		}
	}
}

func appendFreezeInboxMessage(t testing.TB, store *SQLite, organization, recipient string) events.Event {
	t.Helper()
	message, err := store.Append(t.Context(), events.TrustedDraft{
		OrganizationID: organization, EventType: "MESSAGE", SourceActorID: "agent-1",
		RecipientScope: events.RecipientAgent, RecipientID: recipient,
		TaskID: "task-1", CorrelationID: "warm-inbox-" + organization,
		Payload: map[string]any{"body": "durable coordination"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return message
}

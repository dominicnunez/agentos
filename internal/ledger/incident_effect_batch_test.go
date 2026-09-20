package ledger

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func seedIncidentEffects(t *testing.T, store *SQLite, count int) {
	t.Helper()
	first := incidentEffectFixture(t, store)
	task := core.Task{ID: first.TaskID, AssigneeID: first.ActorID}
	lease := core.CapabilityLease{ID: "incident-lease", ActorID: first.ActorID, ActorKind: first.ActorKind, OriginTaskID: first.TaskID, Action: first.Action, Resource: first.Resource, Scope: first.Scope}
	for i := 1; i < count; i++ {
		appendApprovedEffectAttempt(t, store, task, lease, fmt.Sprintf("effect-%03d", i), fmt.Sprintf("approval-%03d", i))
	}
}

func TestIncidentEffectHistoryBudget(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	seedIncidentEffects(t, store, 2)
	// Whitespace preserves each valid decoded record while making their
	// combined supporting evidence exceed the reader's allocation budget.
	if _, err := store.db.ExecContext(t.Context(), `UPDATE records SET body=CAST(body || ? AS BLOB) WHERE kind='effect'`, strings.Repeat(" ", 1100000)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256); err == nil {
		t.Fatal("accepted oversized combined effect record histories")
	}
}

func TestIncidentEffectGroupsStayExact(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	seedIncidentEffects(t, store, 2)
	if _, err := store.db.ExecContext(t.Context(), `DELETE FROM records WHERE kind='effect' AND record_id='incident-effect'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `INSERT INTO records SELECT kind,record_id,version+1,body,admission_event_id,admission_fingerprint,created_at FROM records WHERE kind='effect' AND record_id='effect-001'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256); err == nil {
		t.Fatal("equal aggregate count concealed an orphan and missing effect history")
	}
}

func TestIncidentDistinctEffectGrowth(t *testing.T) {
	for _, count := range []int{1, 180} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			seedIncidentEffects(t, store, count)
			start := time.Now()
			for range 5 {
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256)
				if err != nil {
					t.Fatal(err)
				}
				if len(snapshot.RelatedEvents) != count {
					t.Fatalf("effect histories=%d want=%d", len(snapshot.RelatedEvents), count)
				}
			}
			t.Logf("five complete reads with %d distinct effects: %s", count, time.Since(start))
		})
	}
}

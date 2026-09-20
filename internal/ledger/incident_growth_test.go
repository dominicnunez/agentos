package ledger

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/replay"
)

// Measure the complete reader and renderer, including the intentionally global
// integrity check. Unrelated history may increase verification work, but must
// not expand the private dependency closure or the public timeline.
func TestIncidentUnrelatedHistoryGrowth(t *testing.T) {
	for _, unrelated := range []int{0, 1000} {
		t.Run(fmt.Sprint(unrelated), func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			appendTaskProjectionParents(t, t.Context(), store, "org-1", "incident-work", "work-1")
			agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "selected", false)
			appendPendingAgentExecutionTask(t, t.Context(), store, "incident-work", "task-1", agent, config)
			for i := 0; i < unrelated; i++ {
				_, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", CorrelationID: "unrelated", EventType: "AUDIT_NOTE", Payload: map[string]string{"agent_id": "missing-agent", "text": "unrelated retained history"}})
				if err != nil {
					t.Fatal(err)
				}
			}
			read := func() error {
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "incident-work", 256)
				if err != nil {
					return err
				}
				if len(snapshot.Work.Events) != 3 || len(snapshot.DependencyEvents) != 4 || len(snapshot.RelatedEvents) != 0 {
					return fmt.Errorf("unrelated history expanded incident: public=%d private=%d related=%d", len(snapshot.Work.Events), len(snapshot.DependencyEvents), len(snapshot.RelatedEvents))
				}
				_, err = replay.ProjectIncident(snapshot, "incident-work")
				return err
			}
			started := time.Now()
			if err := read(); err != nil {
				t.Fatal(err)
			}
			first := time.Since(started)
			started = time.Now()
			allocations := testing.AllocsPerRun(3, func() {
				if err := read(); err != nil {
					t.Fatal(err)
				}
			})
			t.Logf("unrelated=%d first=%s repeated=%s allocations/read=%.0f", unrelated, first, time.Since(started)/4, allocations)
			var callers sync.WaitGroup
			errors := make(chan error, 4)
			for range 4 {
				callers.Go(func() { errors <- read() })
			}
			callers.Wait()
			close(errors)
			for err := range errors {
				if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

package app_test

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/replay"
)

// Measure the complete reader and renderer, including the intentionally global
// integrity check. Unrelated history may increase verification work, but must
// not expand the private dependency closure or the public timeline.
func TestIncidentUnrelatedHistoryGrowth(t *testing.T) {
	for _, unrelated := range []int{0, 1000} {
		t.Run(fmt.Sprint(unrelated), func(t *testing.T) {
			store, err := ledger.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			result, err := app.New(events.NewGateway(store)).Submit(t.Context(), app.Submit{RequestID: "incident-work", OrganizationID: "org-1", Statement: "echo incident result", Kind: core.ExecutionDeterministic})
			if err != nil {
				t.Fatal(err)
			}
			if result.Work.Status != core.WorkCompleted || len(result.Events) == 0 {
				t.Fatal("baseline Work did not complete")
			}
			correlation := result.Events[0].CorrelationID
			baseline, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < unrelated; i++ {
				_, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", CorrelationID: "unrelated", EventType: "AUDIT_NOTE", Payload: map[string]string{"agent_id": "missing-agent", "text": "unrelated retained history"}})
				if err != nil {
					t.Fatal(err)
				}
			}
			read := func() error {
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
				if err != nil {
					return err
				}
				sameEvent := func(a, b events.Event) bool { return a.EventID == b.EventID }
				if !slices.EqualFunc(snapshot.Work.Events, baseline.Work.Events, sameEvent) || !slices.EqualFunc(snapshot.DependencyEvents, baseline.DependencyEvents, sameEvent) || !slices.EqualFunc(snapshot.RelatedEvents, baseline.RelatedEvents, sameEvent) {
					return fmt.Errorf("unrelated history changed incident selection: public=%d private=%d related=%d", len(snapshot.Work.Events), len(snapshot.DependencyEvents), len(snapshot.RelatedEvents))
				}
				_, err = replay.ProjectIncident(snapshot, correlation)
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
			t.Logf("unrelated=%d public=%d private=%d first=%s repeated=%s allocations/read=%.0f", unrelated, len(baseline.Work.Events), len(baseline.DependencyEvents), first, time.Since(started)/4, allocations)
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

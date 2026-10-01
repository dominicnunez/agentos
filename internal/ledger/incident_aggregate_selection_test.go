package ledger

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

// Selection-level evidence separates exact references from inverse applicability
// and checks metadata ordering before recursive document expansion.
func TestIncidentAggregateReferenceBudget(t *testing.T) {
	for _, mode := range []string{"unused-inverse", "direct-foreign", "direct-oversized"} {
		t.Run(mode, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			organization := "foreign-org"
			if mode == "direct-oversized" {
				organization = "org-1"
			}
			source, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: organization, EventType: "AUDIT_NOTE", SourceActorID: "runtime", Payload: map[string]string{"note": "selection fixture"}})
			if err != nil {
				t.Fatal(err)
			}
			source.EventType = "WORK_COMPLETION_EVALUATED"
			unused := InsertUnusedAggregateForTest(t, store, source, map[string]string{"work_id": "selected-work", "padding": strings.Repeat("x", events.MaximumIncidentEvidenceBytes+1)})
			payload := map[string]string{"work_id": "selected-work"}
			if mode != "unused-inverse" {
				payload = map[string]string{"event_ref": unused}
			}
			seed, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "CAPABILITY_CHECKED", SourceActorID: "runtime", Payload: payload})
			if err != nil {
				t.Fatal(err)
			}
			d := incidentDependencies{organization: "org-1", budget: incidentBudget{events: events.MaximumIncidentEvidence, bytes: events.MaximumIncidentEvidenceBytes}, stream: map[string]events.Event{}, keys: map[incidentKey]bool{}, refs: map[string]bool{}, reverse: map[incidentKey]bool{}, correlations: map[string]bool{}, executions: map[string]bool{}}
			if err := d.add(seed); err != nil {
				t.Fatal(err)
			}
			err = store.withTx(t.Context(), func(tx *sql.Tx) error { return d.expandReferences(t.Context(), tx, []events.Event{seed}) })
			if mode == "unused-inverse" {
				if err != nil {
					t.Fatalf("unused aggregate entered recursive metadata budget: %v", err)
				}
				if _, selected := d.stream[unused]; selected {
					t.Fatal("unused foreign aggregate selected through inverse identity")
				}
				return
			}
			want := "crosses organization"
			if mode == "direct-oversized" {
				want = "exceeds support limit"
			}
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("exact reference lost metadata preflight: %v", err)
			}
		})
	}
}

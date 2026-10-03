package ledger_test

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
	ledgerrecovery "github.com/dominicnunez/agentos/internal/ledger/recovery"
)

func TestIncidentIncomingDetails(t *testing.T) {
	testIncidentIncomingDetails(t, false, false)
}

func TestIncidentMissingAdmissionDetails(t *testing.T) {
	testIncidentIncomingDetails(t, true, false)
}

func TestIncidentMissingAdmissionSameOrg(t *testing.T) {
	testIncidentIncomingDetails(t, true, true)
}

func testIncidentIncomingDetails(t *testing.T, missingAdmission, sameOrg bool) {
	t.Helper()
	ledger.ParallelIncidentTestForTest(t)
	for _, tc := range []struct {
		event, targetEvent, field string
		array                     bool
	}{
		{"WORK_COMPLETED", "WORK_COMPLETION_EVALUATED", "evidence_event_ref", false},
		{"TASK_VERIFIED_COMPLETE", "TOOL_OUTCOME_RECORDED", "outcome_event_ref", false},
		{"EXECUTION_STARTED", "PLAN_CREATED", "strategic_event_refs", true},
	} {
		t.Run(tc.event, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "details.db")
			store, err := ledger.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			runtime := app.New(events.NewGateway(store))
			var selected, incoming events.Event
			for _, id := range []string{"selected", "independent"} {
				organization := "org-1"
				if id == "independent" && !sameOrg {
					organization = "org-2"
				}
				result, err := runtime.Submit(t.Context(), app.Submit{RequestID: id, OrganizationID: organization, Statement: "echo " + id, Kind: core.ExecutionDeterministic})
				if err != nil {
					t.Fatal(err)
				}
				for _, event := range result.Events {
					if id == "selected" && event.EventType == tc.targetEvent {
						selected = event
					}
					if id == "independent" && event.EventType == tc.event {
						incoming = event
					}
				}
			}
			if selected.EventID == "" || incoming.EventID == "" {
				t.Fatal("writer did not admit required fixture events")
			}
			baseline, err := store.VerifiedIncidentEvents(t.Context(), "org-1", selected.CorrelationID, 256)
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range baseline.DependencyEvents {
				// Same-organization histories may already be required by the
				// Plan owner's candidate validation. That variant checks exact
				// rejection; foreign fixtures discriminate incoming discovery.
				if event.EventID == incoming.EventID && !sameOrg {
					t.Fatal("unrelated admission selected before reference changed")
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := ledgerrecovery.Verify(t.Context(), path); err != nil {
				t.Fatalf("valid full recovery: %v", err)
			}
			store, err = ledger.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			var ref any = selected.EventID
			if tc.array {
				ref = []string{selected.EventID}
			}
			ledger.ChangeIncidentDetailForTest(t, store, incoming.EventID, tc.field, ref)
			if missingAdmission {
				ledger.RemoveIncidentAdmissionForTest(t, store, incoming.EventID)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := ledgerrecovery.Verify(t.Context(), path); err == nil {
				t.Fatal("full recovery accepted mismatched transition evidence")
			}
			store, err = ledger.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", selected.CorrelationID, 256)
			if err == nil {
				t.Fatal("incident omitted incoming transition detail")
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("failed incident returned partial evidence")
			}
		})
	}
}

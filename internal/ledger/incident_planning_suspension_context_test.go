package ledger

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentIncomingPlanningSuspensionContext(t *testing.T) {
	for _, name := range []string{"legacy_without_request", "future_request", "open_request"} {
		t.Run(name, func(t *testing.T) {
			stopped := name == "open_request"
			store, err := Open(filepath.Join(t.TempDir(), "planning-suspension.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			selected := modelStopManifest(t, store, false, "selected-call")
			foreignDraft := modelStopDraft(selected)
			foreignDraft.OrganizationID = "other-org"
			foreignDraft.CorrelationID = "other-run"
			foreignDraft.TaskID = "task-other-run"
			foreignDraft.SourceExecutionID = "other-call"
			foreignDraft.EventType = selected.EventType
			foreignDraft.Payload = json.RawMessage(selected.Payload)
			foreign, err := store.Append(t.Context(), foreignDraft)
			if err != nil {
				t.Fatal(err)
			}
			if stopped {
				if _, _, err := store.RequestModelStop(t.Context(), foreign.EventID, "containment_unavailable"); err != nil {
					t.Fatal(err)
				}
			}
			suspensionDraft := modelStopDraft(foreign)
			suspensionDraft.EventType = "PLANNING_CONTAINMENT_SUSPENDED"
			reference := selected.EventID
			if stopped {
				reference = foreign.EventID
			}
			suspensionDraft.Payload = map[string]string{"context_event_ref": reference}
			suspension, err := store.Append(t.Context(), suspensionDraft)
			if err != nil {
				t.Fatal(err)
			}
			if name == "future_request" {
				if _, _, err := store.RequestModelStop(t.Context(), foreign.EventID, "containment_unavailable"); err != nil {
					t.Fatal(err)
				}
			}
			full, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if err := events.ValidateModelStops(full, nil); err != nil {
				t.Fatalf("writer-created suspension failed full model owner: %v", err)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), selected.OrganizationID, selected.CorrelationID, 256); err != nil {
				t.Fatalf("valid foreign suspension affected selected incident: %v", err)
			}
			if !stopped {
				return
			}
			if err := changeModelStopPayload(t, store, suspension.EventID, map[string]string{"context_event_ref": selected.EventID}); err != nil {
				t.Fatal(err)
			}
			full, err = store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if err := events.ValidateModelStops(full, nil); err == nil {
				t.Fatal("full model owner accepted a suspension contradicting its open request")
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), selected.OrganizationID, selected.CorrelationID, 256); err == nil {
				t.Fatal("incident omitted foreign suspension with an open request and selected context reference")
			}
		})
	}
}

package ledger

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentIncomingPlanningFailure(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	selected := modelStopManifest(t, store, false, "selected-call")
	if _, _, err := store.RequestModelStop(t.Context(), selected.EventID, "caller_cancelled"); err != nil {
		t.Fatal(err)
	}
	draft := modelStopDraft(selected)
	draft.OrganizationID, draft.CorrelationID, draft.TaskID, draft.SourceExecutionID = "other-org", "other-run", "task-other-run", "other-call"
	draft.EventType, draft.Payload = selected.EventType, json.RawMessage(selected.Payload)
	foreign, err := store.Append(t.Context(), draft)
	if err != nil {
		t.Fatal(err)
	}
	draft.EventType = "PLANNING_FAILED"
	draft.Payload = map[string]string{"code": "model_failed", "reason": "Model did not produce a plan", "evidence_event_ref": foreign.EventID}
	failure, err := store.Append(t.Context(), draft)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := events.ValidateModelStops(stream, nil); err != nil {
		t.Fatalf("valid full history: %v", err)
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), selected.OrganizationID, selected.CorrelationID, 256); err != nil {
		t.Fatalf("valid selected history: %v", err)
	}
	changeIncidentStopRef(t, store, failure, "evidence_event_ref", selected.EventID)
	stream, err = store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := events.ValidateModelStops(stream, nil); err == nil {
		t.Fatal("full owner accepted cross-organization planning failure")
	}
	snapshot, err := store.VerifiedIncidentEvents(t.Context(), selected.OrganizationID, selected.CorrelationID, 256)
	if err == nil {
		t.Fatal("incident omitted foreign planning failure naming selected model context")
	}
	if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
		t.Fatal("failed incident returned partial evidence")
	}
}

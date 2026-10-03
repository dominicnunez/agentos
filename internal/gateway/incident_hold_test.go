package gateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/intake"
	"github.com/dominicnunez/agentos/internal/ledger"
)

func TestIncidentReplayShowsLastStart(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	runtime := app.New(events.NewGateway(store))
	result, err := runtime.Submit(t.Context(), app.Submit{RequestID: "finished-before-hold", OrganizationID: "org-1", Statement: "echo finished", Kind: core.ExecutionDeterministic})
	if err != nil || result.Task.Status != core.TaskCompleted {
		t.Fatalf("seed task: %v", err)
	}
	var lastStart events.Event
	for _, event := range result.Events {
		if event.EventType == "EXECUTION_STARTED" {
			lastStart = event
		}
	}
	if lastStart.EventID == "" {
		t.Fatal("task lacks admitted execution")
	}
	executionID, err := events.ContainmentExecutionID(lastStart)
	if err != nil {
		t.Fatal(err)
	}
	control := newGatewayFreezeControl(t, store, LocalHuman{UID: 1000, ID: "local-uid-1000", OrganizationID: "org-1"})
	held := freezeRequest(t, control, http.MethodPost, freezeControlPath, 1000, `{"frozen":true,"reason":"inspect completed work","expected_event_ref":"","expected_version":0}`)
	if held.Code != http.StatusOK {
		t.Fatalf("hold: %d %s", held.Code, held.Body.String())
	}
	hold := decodeLedgerFreezeSnapshot(t, held)
	handler := testHumanHandler(t, intake.New(runtime))
	response := serveHuman(handler, http.MethodGet, "/v1/user/incidents/replay?conversation_id=finished-before-hold", testOwnerMarker, "")
	if response.Code != http.StatusOK {
		t.Fatalf("incident: %d %s", response.Code, response.Body.String())
	}
	var report struct {
		Containment struct {
			Holds []struct {
				EventRef       string `json:"event_ref"`
				LastAdmissions []struct {
					EventRef    string `json:"event_ref"`
					Kind        string `json:"kind"`
					TaskID      string `json:"task_id"`
					ExecutionID string `json:"execution_id"`
				} `json:"last_admissions"`
			} `json:"holds"`
		} `json:"containment"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Containment.Holds) != 1 || report.Containment.Holds[0].EventRef != hold.EventRef || len(report.Containment.Holds[0].LastAdmissions) != 1 {
		t.Fatalf("missing admission boundary: %s", response.Body.String())
	}
	admission := report.Containment.Holds[0].LastAdmissions[0]
	if admission.EventRef != lastStart.EventID || admission.Kind != "EXECUTION_START" || admission.TaskID != lastStart.TaskID || admission.ExecutionID != executionID {
		t.Fatalf("wrong last admitted execution: %+v", admission)
	}
	otherControl := newGatewayFreezeControl(t, store, LocalHuman{UID: 1001, ID: "other-owner", OrganizationID: "org-2"})
	other := freezeRequest(t, otherControl, http.MethodPost, freezeControlPath, 1001, `{"frozen":true,"reason":"other tenant private reason","expected_event_ref":"","expected_version":0}`)
	if other.Code != http.StatusOK {
		t.Fatalf("other hold: %d", other.Code)
	}
	after := serveHuman(handler, http.MethodGet, "/v1/user/incidents/replay?conversation_id=finished-before-hold", testOwnerMarker, "")
	if after.Code != http.StatusOK || response.Body.String() != after.Body.String() {
		t.Fatal("unrelated tenant activity changed public incident report")
	}
}

func TestIncidentReplayGrowth(t *testing.T) {
	for _, size := range []struct{ selected, unrelated int }{{0, 0}, {180, 1000}} {
		t.Run(fmt.Sprintf("selected-%d-other-%d", size.selected, size.unrelated), func(t *testing.T) {
			store, err := ledger.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			runtime := app.New(events.NewGateway(store))
			result, err := runtime.Submit(t.Context(), app.Submit{RequestID: "growth", OrganizationID: "org-1", Statement: "echo growth", Kind: core.ExecutionDeterministic})
			if err != nil || len(result.Events) == 0 {
				t.Fatalf("seed work: %v", err)
			}
			for i := range size.selected + size.unrelated {
				org, correlation := "org-1", result.Events[0].CorrelationID
				if i >= size.selected {
					org, correlation = "org-other", "unrelated"
				}
				if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: org, CorrelationID: correlation, EventType: "AUDIT_NOTE", Payload: map[string]int{"index": i}}); err != nil {
					t.Fatal(err)
				}
			}
			handler := testHumanHandler(t, intake.New(runtime))
			start := time.Now()
			var first string
			for range 5 {
				response := serveHuman(handler, http.MethodGet, "/v1/user/incidents/replay?conversation_id=growth", testOwnerMarker, "")
				if response.Code != http.StatusOK {
					t.Fatalf("incident: %d %s", response.Code, response.Body.String())
				}
				if first == "" {
					first = response.Body.String()
				} else if first != response.Body.String() {
					t.Fatal("repeated read changed the report")
				}
			}
			t.Logf("five complete HTTP reports: %s; response bytes=%d; selected extra=%d; unrelated=%d", time.Since(start), len(first), size.selected, size.unrelated)
		})
	}
}

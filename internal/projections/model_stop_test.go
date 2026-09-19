package projections

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
)

type modelReplayLedger struct {
	*ledger.SQLite
	stream []events.Event
}

func (l modelReplayLedger) Events(context.Context, string) ([]events.Event, error) {
	return l.stream, nil
}

func TestRebuildRejectsInvalidModelHistory(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for _, execution := range []string{"first", "second"} {
		manifest, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org", TaskID: "task-run", CorrelationID: "run", SourceExecutionID: execution, SourceActorID: "runtime", EventType: "PLANNING_CONTEXT_MANIFESTED", Payload: events.PlanningContextPayload{PlanID: "plan-run", IntentID: "intent-run", IntentFingerprint: "fingerprint", PromptVersion: "v1", Provider: "provider", Model: "model", ExecutionProfileVersion: "v1", InputEventRefs: []string{"input"}}})
		if err != nil {
			t.Fatal(err)
		}
		request, _, err := store.RequestModelStop(t.Context(), manifest.EventID, "runtime_shutdown")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.RecordModelStop(t.Context(), request.EventID, &events.ModelStopReturn{LocalState: "NOT_STARTED"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := New(events.NewGateway(store)).Rebuild(t.Context()); err != nil {
		t.Fatalf("valid model history: %v", err)
	}
	stream, err := store.Events(t.Context(), "run")
	if err != nil {
		t.Fatal(err)
	}
	for _, defect := range []string{"early-retry", "early-invalid-request"} {
		t.Run(defect, func(t *testing.T) {
			corrupted := append([]events.Event(nil), stream...)
			if defect == "early-retry" {
				corrupted[1], corrupted[2], corrupted[3] = stream[3], stream[1], stream[2]
				for i := range corrupted {
					corrupted[i].Sequence = int64(i + 1)
				}
			} else {
				payload := events.ModelStopRequest{ContextEventRef: "missing", ReasonClass: "runtime_shutdown"}
				corrupted[1].Payload, err = json.Marshal(payload)
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := New(events.NewGateway(modelReplayLedger{SQLite: store, stream: corrupted})).Rebuild(t.Context()); err == nil || !strings.Contains(err.Error(), "model") {
				t.Fatalf("invalid model history replay error=%v", err)
			}
		})
	}
}

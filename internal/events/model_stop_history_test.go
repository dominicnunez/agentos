package events

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func modelStopHistoryFixture(t *testing.T) []Event {
	t.Helper()
	at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	makeEvent := func(id, kind string, sequence int64, payload any) Event {
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		return Event{EventID: id, Sequence: sequence, OrganizationID: "org", TaskID: "task-run", CorrelationID: "run", SourceExecutionID: "model-1", SourceActorID: "runtime", SchemaVersion: SchemaVersion, CreatedAt: at.Add(time.Duration(sequence) * time.Second), EventType: kind, Payload: body}
	}
	returned := at.Add(3 * time.Second)
	return []Event{
		makeEvent("context", "PLANNING_CONTEXT_MANIFESTED", 1, PlanningContextPayload{PlanID: "plan-run", IntentID: "intent-run", IntentFingerprint: "fingerprint", PromptVersion: "v1", Provider: "provider", Model: "model", ExecutionProfileVersion: "v1", InputEventRefs: []string{"input"}}),
		makeEvent("request", "MODEL_STOP_REQUESTED", 2, ModelStopRequest{ContextEventRef: "context", ReasonClass: "runtime_shutdown"}),
		makeEvent("uncertain", "MODEL_STOP_UNCERTAIN", 3, ModelStopResult{StopRequestRef: "request"}),
		makeEvent("usage", "INFERENCE_USAGE_RECORDED", 4, InferenceUsageRecordedPayload{Source: "provider", Provider: "provider", Model: "model", InputTokens: 1, TotalTokens: 1}),
		makeEvent("confirmed", "MODEL_STOP_CONFIRMED", 5, ModelStopResult{StopRequestRef: "request", LocalState: "RETURNED", ReturnedAt: &returned, UsageEventRef: "usage", ProviderStop: &core.ProviderStopEvidence{LocalTurnStopped: true, RemoteStatus: "UNCERTAIN"}}),
	}
}

func TestModelStopHistoryRejectsContradictions(t *testing.T) {
	if err := ValidateModelStops(modelStopHistoryFixture(t), nil); err != nil {
		t.Fatal(err)
	}
	mutatePayload := func(event *Event, fn func(map[string]any)) {
		var payload map[string]any
		if json.Unmarshal(event.Payload, &payload) != nil {
			t.Fatal("invalid fixture")
		}
		fn(payload)
		event.Payload, _ = json.Marshal(payload)
	}
	for _, test := range []struct {
		name   string
		mutate func([]Event) []Event
	}{
		{"wrong context", func(s []Event) []Event {
			mutatePayload(&s[1], func(p map[string]any) { p["context_event_ref"] = "usage" })
			return s
		}},
		{"wrong tenant", func(s []Event) []Event { s[4].OrganizationID = "other"; return s }},
		{"wrong actor", func(s []Event) []Event { s[1].SourceActorID = "agent"; return s }},
		{"wrong usage", func(s []Event) []Event {
			mutatePayload(&s[4], func(p map[string]any) { p["usage_event_ref"] = "context" })
			return s
		}},
		{"no local return", func(s []Event) []Event {
			mutatePayload(&s[4], func(p map[string]any) { delete(p, "returned_at") })
			return s
		}},
		{"uncertain return", func(s []Event) []Event {
			mutatePayload(&s[2], func(p map[string]any) { p["local_state"] = "RETURNED" })
			return s
		}},
		{"false not started", func(s []Event) []Event {
			mutatePayload(&s[4], func(p map[string]any) {
				p["local_state"] = "NOT_STARTED"
				delete(p, "returned_at")
				delete(p, "usage_event_ref")
				delete(p, "provider_stop")
			})
			return s
		}},
		{"false remote confirmation", func(s []Event) []Event {
			mutatePayload(&s[4], func(p map[string]any) { p["provider_stop"] = map[string]any{"remote_status": "CONFIRMED"} })
			return s
		}},
		{"missing usage confirmation", func(s []Event) []Event { return s[:4] }},
		{"duplicate confirmation", func(s []Event) []Event {
			extra := s[4]
			extra.EventID = "duplicate"
			extra.Sequence = 6
			return append(s, extra)
		}},
		{"ordinary post-stop activity", func(s []Event) []Event {
			extra := s[4]
			extra.EventID = "output"
			extra.Sequence = 6
			extra.EventType = "CUSTOM_OUTPUT"
			extra.Payload = []byte(`{}`)
			return append(s, extra)
		}},
		{"ordinary planning failure omits execution", func(s []Event) []Event {
			extra := s[4]
			extra.EventID = "failure"
			extra.Sequence = 6
			extra.EventType = "PLANNING_FAILED"
			extra.TaskID = ""
			extra.SourceExecutionID = ""
			extra.Payload = []byte(`{"code":"FAILED","reason":"ordinary failure","evidence_event_ref":"context"}`)
			return append(s, extra)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateModelStops(test.mutate(modelStopHistoryFixture(t)), nil); err == nil {
				t.Fatal("accepted contradictory stop history")
			}
		})
	}
}

func TestModelStopHistoryRejectsEarlyBadLaterGood(t *testing.T) {
	bad := modelStopHistoryFixture(t)
	bad[1].Payload = []byte(`{"context_event_ref":"context","reason_class":"security_hold"}`)
	good := modelStopHistoryFixture(t)
	for i := range good {
		good[i].EventID += "-later"
		good[i].Sequence += 10
		good[i].OrganizationID = "other"
	}
	good[1].Payload = []byte(`{"context_event_ref":"context-later","reason_class":"runtime_shutdown"}`)
	good[2].Payload = []byte(`{"stop_request_ref":"request-later"}`)
	var payload ModelStopResult
	_ = json.Unmarshal(good[4].Payload, &payload)
	payload.StopRequestRef = "request-later"
	payload.UsageEventRef = "usage-later"
	good[4].Payload, _ = json.Marshal(payload)
	if err := ValidateModelStops(good, nil); err != nil {
		t.Fatal(err)
	}
	if err := ValidateModelStops(append(bad, good...), nil); err == nil {
		t.Fatal("later valid evidence erased earlier invalid stop")
	}
}

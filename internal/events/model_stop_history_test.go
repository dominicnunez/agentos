package events

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestModelStopRetryRequiresPriorEvidence(t *testing.T) {
	for _, normalization := range []bool{false, true} {
		for _, evidence := range []string{"not_started", "not_sent", "ordinary_failure", "ordinary_success"} {
			for _, earlyRetry := range []bool{false, true} {
				t.Run(fmt.Sprintf("normalization=%t/%s/early=%t", normalization, evidence, earlyRetry), func(t *testing.T) {
					fixture := modelStopHistoryFixture(t)
					first := fixture[0]
					if normalization {
						first.EventType = "INTENT_NORMALIZATION_CONTEXT_MANIFESTED"
						first.Payload, _ = json.Marshal(IntentNormalizationContextPayload{SourceMessageID: "message", PromptVersion: "v1", Provider: "provider", Model: "model", ExecutionProfileVersion: "v1", InputEventRefs: []string{"input"}})
					}
					next := first
					next.EventID, next.SourceExecutionID = "next", "model-2"
					var closure []Event
					switch evidence {
					case "not_started":
						confirmed := fixture[4]
						confirmed.Payload, _ = json.Marshal(ModelStopResult{StopRequestRef: "request", LocalState: "NOT_STARTED"})
						closure = []Event{fixture[1], confirmed}
					case "not_sent":
						proof := first
						proof.EventID, proof.EventType = "proof", "INFERENCE_NOT_SENT"
						proof.Payload, _ = json.Marshal(map[string]string{"request_id": first.SourceExecutionID, "prompt_sha256": strings.Repeat("a", 64)})
						closure = []Event{proof}
					case "ordinary_failure":
						failure := first
						failure.EventID, failure.EventType = "failure", "PLANNING_FAILED"
						failure.TaskID, failure.SourceExecutionID = "", ""
						failure.Payload = []byte(`{"code":"FAILED","reason":"ordinary failure","evidence_event_ref":"context"}`)
						if normalization {
							failure.EventType, failure.TaskID, failure.SourceExecutionID = "INTENT_NORMALIZATION_FAILED", first.TaskID, first.SourceExecutionID
							failure.Payload = []byte(`{}`)
						}
						closure = []Event{failure}
					case "ordinary_success":
						success := first
						success.EventID, success.EventType = "success", "PLAN_CREATED"
						plan := core.Plan{ID: "plan-run", IntentID: "intent-run", IntentFingerprint: "fingerprint", Version: 1, CreatedAt: first.CreatedAt}
						plan.Fingerprint, _ = core.FingerprintPlan(plan)
						success.Payload, _ = json.Marshal(plan)
						if normalization {
							draft := core.IntentDraft{ID: "intent-run", OrganizationID: "org", Version: 1, CreatedAt: first.CreatedAt}
							draft.Fingerprint, _ = core.FingerprintIntentDraft(draft)
							success.EventType = "INTENT_DRAFTED"
							success.Payload, _ = json.Marshal(IntentDraftedPayload{SourceMessageID: "message", Draft: draft})
						}
						closure = []Event{success}
					}
					stream := []Event{first}
					if earlyRetry {
						stream = append(stream, next)
					}
					stream = append(stream, closure...)
					if !earlyRetry {
						stream = append(stream, next)
					}
					for i := range stream {
						stream[i].Sequence = int64(i + 1)
					}
					wantDenied := earlyRetry || evidence == "ordinary_success" || evidence == "ordinary_failure" && !normalization
					if err := ValidateModelStops(stream, nil); (err != nil) != wantDenied {
						t.Fatalf("retry denied=%t want=%t; validation error=%v", err != nil, wantDenied, err)
					}
				})
			}
		}
	}
}

func TestModelUndispatchedRejectsMalformedGuardProof(t *testing.T) {
	for _, defect := range []string{"nonhex", "schema", "recipient", "authority", "artifact"} {
		t.Run(defect, func(t *testing.T) {
			manifest := modelStopHistoryFixture(t)[0]
			proof := manifest
			proof.EventID, proof.EventType, proof.Sequence = "proof", "INFERENCE_NOT_SENT", 2
			hash := strings.Repeat("a", 64)
			switch defect {
			case "nonhex":
				hash = strings.Repeat("z", 64)
			case "schema":
				proof.SchemaVersion++
			case "recipient":
				proof.RecipientID = "other"
			case "authority":
				proof.AuthorizationRefs = []string{"unexpected"}
			case "artifact":
				proof.ArtifactRefs = []string{"unexpected"}
			}
			proof.Payload, _ = json.Marshal(map[string]string{"request_id": proof.SourceExecutionID, "prompt_sha256": hash})
			stream := []Event{manifest, proof}
			if got := ModelUndispatchedExecutions(stream, manifest.OrganizationID, manifest.CorrelationID); len(got) != 0 {
				t.Fatalf("malformed proof granted retry: %v", got)
			}
			valid := manifest
			valid.EventID, valid.EventType, valid.Sequence = "valid-proof", "INFERENCE_NOT_SENT", 3
			valid.Payload, _ = json.Marshal(map[string]string{"request_id": valid.SourceExecutionID, "prompt_sha256": strings.Repeat("a", 64)})
			next := manifest
			next.EventID, next.SourceExecutionID, next.Sequence = "next", "model-2", 4
			if err := ValidateModelStops(append(stream, valid, next), nil); err == nil {
				t.Fatal("later valid proof erased malformed earlier retry evidence")
			}
		})
	}
}

func TestModelResultCannotOmitManifestedExecution(t *testing.T) {
	for _, normalization := range []bool{false, true} {
		for _, state := range []string{"legacy", "manifested", "stopped", "different_input"} {
			t.Run(fmt.Sprintf("normalization=%t/%s", normalization, state), func(t *testing.T) {
				stream := modelStopHistoryFixture(t)
				result := stream[0]
				result.EventID, result.Sequence, result.SourceExecutionID = "result", 6, ""
				result.EventType, result.Payload = "PLAN_CREATED", []byte(`{}`)
				payload := IntentDraftedPayload{SourceMessageID: "message"}
				if normalization {
					stream[0].EventType = "INTENT_NORMALIZATION_CONTEXT_MANIFESTED"
					stream[0].Payload, _ = json.Marshal(IntentNormalizationContextPayload{SourceMessageID: "message", PromptVersion: "v1", Provider: "provider", Model: "model", ExecutionProfileVersion: "v1", InputEventRefs: []string{"input"}})
					if state == "different_input" {
						payload.SourceMessageID = "new-message"
					}
					result.EventType = "INTENT_DRAFTED"
					result.Payload, _ = json.Marshal(payload)
				}
				switch state {
				case "legacy":
					stream = nil
				case "manifested", "different_input":
					stream = stream[:1]
				}
				want := state == "legacy" || normalization && state == "different_input"
				if err := ValidateModelStops(append(stream, result), nil); (err == nil) != want {
					t.Fatalf("full replay allowed=%t want=%t err=%v", err == nil, want, err)
				}
				if normalization {
					if got := validIntentDraftExecution(stream, result, payload); got != want {
						t.Fatalf("filtered draft reader allowed=%t want=%t", got, want)
					}
				} else if err := ValidatePlanExecution(result, stream); (err == nil) != want {
					t.Fatalf("filtered plan reader allowed=%t want=%t err=%v", err == nil, want, err)
				}
			})
		}
	}
}

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

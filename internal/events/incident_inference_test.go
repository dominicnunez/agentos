package events

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestIncidentInferenceAdmissionContract(t *testing.T) {
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	valid := InferenceReservedPayload{ReservationID: "reservation", RequestID: "call", Purpose: "PLANNING", IntentID: "intent", PolicyFingerprint: strings.Repeat("a", 64), PromptSHA256: strings.Repeat("b", 64), Provider: "provider", Model: "model", ExecutionProfileVersion: "profile", ReservedInputTokens: 100, ReservedOutputTokens: 20, WindowStartedAt: now, WindowExpiresAt: now.Add(time.Hour), AdmittedAt: now.Format(time.RFC3339Nano)}
	cases := map[string]func(*Event, *InferenceReservedPayload){
		"empty reservation":       func(_ *Event, p *InferenceReservedPayload) { p.ReservationID = "" },
		"empty request":           func(_ *Event, p *InferenceReservedPayload) { p.RequestID = "" },
		"unknown purpose":         func(_ *Event, p *InferenceReservedPayload) { p.Purpose = "OTHER" },
		"unbound work":            func(e *Event, p *InferenceReservedPayload) { p.IntentID = ""; e.TaskID = "" },
		"missing execution":       func(e *Event, _ *InferenceReservedPayload) { e.SourceExecutionID = "" },
		"missing tenant":          func(e *Event, _ *InferenceReservedPayload) { e.OrganizationID = "" },
		"missing correlation":     func(e *Event, _ *InferenceReservedPayload) { e.CorrelationID = "" },
		"wrong actor":             func(e *Event, _ *InferenceReservedPayload) { e.SourceActorID = "worker" },
		"recipient":               func(e *Event, _ *InferenceReservedPayload) { e.RecipientID = "worker" },
		"recipient scope":         func(e *Event, _ *InferenceReservedPayload) { e.RecipientScope = "task" },
		"authorization":           func(e *Event, _ *InferenceReservedPayload) { e.AuthorizationRefs = []string{"ref"} },
		"artifact":                func(e *Event, _ *InferenceReservedPayload) { e.ArtifactRefs = []string{"ref"} },
		"schema":                  func(e *Event, _ *InferenceReservedPayload) { e.SchemaVersion = -1 },
		"policy":                  func(_ *Event, p *InferenceReservedPayload) { p.PolicyFingerprint = "bad" },
		"prompt":                  func(_ *Event, p *InferenceReservedPayload) { p.PromptSHA256 = "bad" },
		"provider":                func(_ *Event, p *InferenceReservedPayload) { p.Provider = "" },
		"model":                   func(_ *Event, p *InferenceReservedPayload) { p.Model = "" },
		"profile":                 func(_ *Event, p *InferenceReservedPayload) { p.ExecutionProfileVersion = "" },
		"input":                   func(_ *Event, p *InferenceReservedPayload) { p.ReservedInputTokens = 0 },
		"output":                  func(_ *Event, p *InferenceReservedPayload) { p.ReservedOutputTokens = -1 },
		"cost":                    func(_ *Event, p *InferenceReservedPayload) { p.ReservedCostNanoUSD = -1 },
		"window":                  func(_ *Event, p *InferenceReservedPayload) { p.WindowExpiresAt = p.WindowStartedAt },
		"admitted":                func(_ *Event, p *InferenceReservedPayload) { p.AdmittedAt = "bad" },
		"expired":                 func(_ *Event, p *InferenceReservedPayload) { p.AdmittedAt = p.WindowExpiresAt.Format(time.RFC3339Nano) },
		"connection":              func(_ *Event, p *InferenceReservedPayload) { p.ConnectionID = "invalid connection" },
		"connection without time": func(_ *Event, p *InferenceReservedPayload) { p.ConnectionID = "account"; p.AdmittedAt = "" },
		"manifest request": func(_ *Event, p *InferenceReservedPayload) {
			p.ExecutionManifestRef = "manifest"
			p.RequestID = "other"
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			p := valid
			e := Event{EventID: "event", OrganizationID: "org", EventType: "INFERENCE_RESERVED", SourceActorID: "runtime", SourceExecutionID: "call", CorrelationID: "work", SchemaVersion: SchemaVersion}
			e.Payload, _ = json.Marshal(p)
			if _, ok, err := AdmissionForIncident(e); err != nil || !ok {
				t.Fatalf("valid admission rejected: %v", err)
			}
			mutate(&e, &p)
			e.Payload, _ = json.Marshal(p)
			if _, _, err := AdmissionForIncident(e); err == nil {
				t.Fatal("accepted invalid inference admission")
			}
		})
	}
	for _, body := range []string{`{}`, `{"private":"never disclose"}`, `{"request_id":"call","request_id":"other"}`, `null`} {
		t.Run(body, func(t *testing.T) {
			e := Event{EventID: "event", OrganizationID: "org", EventType: "INFERENCE_RESERVED", SourceActorID: "runtime", SourceExecutionID: "call", CorrelationID: "work", SchemaVersion: SchemaVersion, Payload: []byte(body)}
			if _, _, err := AdmissionForIncident(e); err == nil {
				t.Fatal("accepted malformed inference payload")
			}
		})
	}
	// Older library callers have no application manifest or admission timestamp.
	p := valid
	p.AdmittedAt = ""
	p.RequestID = "independent-request"
	body, _ := json.Marshal(p)
	if _, ok, err := AdmissionForIncident(Event{EventID: "legacy", OrganizationID: "org", EventType: "INFERENCE_RESERVED", SourceActorID: "runtime", SourceExecutionID: "call", CorrelationID: "work", SchemaVersion: SchemaVersion, Payload: body}); err != nil || !ok {
		t.Fatalf("legacy library reservation rejected: %v", err)
	}
}

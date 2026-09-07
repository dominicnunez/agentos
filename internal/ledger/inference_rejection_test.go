package ledger

import (
	"encoding/json"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"strings"
	"testing"
)

func TestRouteRejectionAttributionAndReplay(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	origin, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org", CorrelationID: "work", SourceActorID: "runtime", EventType: "PLAN_CREATED", Payload: core.Plan{Tasks: []core.PlanTask{{Key: "agent-task", ExecutionKind: core.ExecutionAgent}, {Key: "software", ExecutionKind: core.ExecutionDeterministic}}}})
	if err != nil {
		t.Fatal(err)
	}
	payload := events.InferenceRouteRejectedPayload{Version: 1, Purpose: "TASK_ASSIGNMENT", OriginEventRef: origin.EventID, PlannedTaskKey: "agent-task", Category: "NO_ELIGIBLE_ACCOUNT", RequirementsFingerprint: strings.Repeat("a", 64)}
	draft := events.TrustedDraft{OrganizationID: "org", CorrelationID: "work", SourceActorID: "runtime", EventType: "INFERENCE_ROUTE_REJECTED", Payload: payload}
	for name, mutate := range map[string]func(*events.TrustedDraft, *events.InferenceRouteRejectedPayload){
		"organization":  func(d *events.TrustedDraft, p *events.InferenceRouteRejectedPayload) { d.OrganizationID = "other" },
		"work":          func(d *events.TrustedDraft, p *events.InferenceRouteRejectedPayload) { d.CorrelationID = "other" },
		"agent-forgery": func(d *events.TrustedDraft, p *events.InferenceRouteRejectedPayload) { d.SourceActorID = "agent" },
		"authority": func(d *events.TrustedDraft, p *events.InferenceRouteRejectedPayload) {
			d.AuthorizationRefs = []string{"lease"}
		},
		"attempt": func(d *events.TrustedDraft, p *events.InferenceRouteRejectedPayload) { d.SourceExecutionID = "attempt" },
		"purpose": func(d *events.TrustedDraft, p *events.InferenceRouteRejectedPayload) {
			p.Purpose = "PLANNING"
			p.PlannedTaskKey = ""
		},
		"unknown-origin": func(d *events.TrustedDraft, p *events.InferenceRouteRejectedPayload) { p.OriginEventRef = "missing" },
		"unknown-task":   func(d *events.TrustedDraft, p *events.InferenceRouteRejectedPayload) { p.PlannedTaskKey = "missing" },
		"software-task":  func(d *events.TrustedDraft, p *events.InferenceRouteRejectedPayload) { p.PlannedTaskKey = "software" },
		"raw-error":      func(d *events.TrustedDraft, p *events.InferenceRouteRejectedPayload) { p.Category = "private-canary" },
		"digest": func(d *events.TrustedDraft, p *events.InferenceRouteRejectedPayload) {
			p.RequirementsFingerprint = "private-canary"
		},
		"version": func(d *events.TrustedDraft, p *events.InferenceRouteRejectedPayload) { p.Version = 2 },
	} {
		t.Run(name, func(t *testing.T) {
			d, p := draft, payload
			mutate(&d, &p)
			d.Payload = p
			if _, err := store.Append(t.Context(), d); err == nil {
				t.Fatal("invalid diagnostic admitted")
			}
			stream, err := store.Events(t.Context(), "work")
			if err != nil || len(stream) != 1 {
				t.Fatal("rejected diagnostic changed history", err)
			}
		})
	}
	for _, body := range []string{
		`{"version":1,"Version":1}`, `{"version":1,"version":1}`, `{"version":1,"raw_error":"private-canary"}`,
	} {
		d := draft
		d.Payload = json.RawMessage(body)
		if _, err := store.Append(t.Context(), d); err == nil {
			t.Fatal("ambiguous or open diagnostic schema accepted")
		}
	}
	recorded, err := store.Append(t.Context(), draft)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatal("valid diagnostic failed replay", err)
	}
	var reservations int
	if err := store.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM inference_reservations").Scan(&reservations); err != nil || reservations != 0 {
		t.Fatal("diagnostic reserved inference", err)
	}
	original := append([]byte(nil), recorded.Payload...)
	for _, mutate := range []func(*events.InferenceRouteRejectedPayload){
		func(p *events.InferenceRouteRejectedPayload) { p.OriginEventRef = recorded.EventID },
		func(p *events.InferenceRouteRejectedPayload) { p.PlannedTaskKey = "missing" },
		func(p *events.InferenceRouteRejectedPayload) { p.Category = "private-canary" },
	} {
		p := payload
		mutate(&p)
		body, _ := json.Marshal(p)
		if _, err := store.db.ExecContext(t.Context(), "UPDATE events SET payload=? WHERE event_id=?", body, recorded.EventID); err != nil {
			t.Fatal(err)
		}
		if err := store.ValidateInferenceAdmissions(t.Context()); err == nil {
			t.Fatal("tampered diagnostic passed replay")
		}
	}
	if _, err := store.db.ExecContext(t.Context(), "UPDATE events SET payload=? WHERE event_id=?", original, recorded.EventID); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatal(err)
	}
}

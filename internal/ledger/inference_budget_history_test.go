package ledger

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

func TestInferenceBudgetHistoryUsesChargesAtAdmission(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	policy := testInferencePolicy(now)
	policy.Version, policy.ConnectionID = inference.ConnectionPolicyVersion, "first"
	policy.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 200, MaxCostNanoUSDPerWindow: 1000000, MaxConcurrentRequests: 2}
	makeEvent := func(kind string, payload any) events.Event {
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		return events.Event{OrganizationID: policy.OrganizationID, EventType: kind, Payload: body}
	}
	activation := events.Event{EventID: "activation", OrganizationID: policy.OrganizationID, EventType: "INFERENCE_POLICY_ACTIVATED"}
	firstPayload := events.InferenceReservedPayload{ReservationID: "first", AdmittedAt: now.Format(time.RFC3339Nano), ReservedInputTokens: 100, ReservedOutputTokens: 20, ReservedCostNanoUSD: 400000, WindowStartedAt: now, WindowExpiresAt: now.Add(time.Hour)}
	first := makeEvent("INFERENCE_RESERVED", firstPayload)
	secondPayload := firstPayload
	secondPayload.ReservationID = "second"
	second := makeEvent("INFERENCE_RESERVED", secondPayload)
	released := makeEvent("INFERENCE_RECONCILED", events.InferenceReconciledPayload{ReservationID: "first", State: inferenceStateNotSent})
	uncertain := makeEvent("INFERENCE_RECONCILED", events.InferenceReconciledPayload{ReservationID: "first", State: inferenceStateUncertain, ChargedInputTokens: 100, ChargedOutputTokens: 20, ChargedCostNanoUSD: 400000})
	for _, tt := range []struct {
		name    string
		stream  []events.Event
		wantErr bool
	}{
		{"late release cannot justify earlier admission", []events.Event{activation, first, second, released}, true},
		{"release before admission", []events.Event{activation, first, released, second}, false},
		{"uncertainty retains charge", []events.Event{activation, first, uncertain, second}, true},
		{"single reservation", []events.Event{activation, first}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := validateOrganizationBudgetHistory(tt.stream, map[string]inference.Policy{"activation": policy})
			if (err != nil) != tt.wantErr {
				t.Fatalf("history error=%v wantErr=%v", err, tt.wantErr)
			}
		})
	}
}

func TestInferenceBudgetHistoryRejectsInterruptedPolicySet(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	first := testInferencePolicy(now)
	first.Version, first.ConnectionID = inference.ConnectionPolicyVersion, "first"
	first.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, MaxCostNanoUSDPerWindow: 1000000, MaxConcurrentRequests: 2}
	second := first
	second.ConnectionID = "second"
	newFirst, newSecond := first, second
	budget := *first.OrganizationBudget
	budget.MaxTokensPerWindow = 200
	newFirst.OrganizationBudget, newSecond.OrganizationBudget = &budget, &budget
	newFirst.AuthorizedAt, newSecond.AuthorizedAt = now, now
	policies := map[string]inference.Policy{"first": first, "second": second, "new-first": newFirst, "new-second": newSecond}
	activation := func(id string) events.Event {
		return events.Event{EventID: id, OrganizationID: first.OrganizationID, EventType: "INFERENCE_POLICY_ACTIVATED"}
	}
	base := []events.Event{activation("first"), activation("second"), activation("new-first")}
	interrupted := append(append([]events.Event(nil), base...), events.Event{EventType: "AUDIT_NOTE", OrganizationID: first.OrganizationID}, activation("new-second"))
	complete := append(append([]events.Event(nil), base...), activation("new-second"))
	if err := validateOrganizationBudgetHistory(base, policies); err == nil {
		t.Fatal("incomplete policy set accepted")
	}
	if err := validateOrganizationBudgetHistory(interrupted, policies); err == nil {
		t.Fatal("interleaved event accepted during shared limit change")
	}
	if err := validateOrganizationBudgetHistory(complete, policies); err != nil {
		t.Fatal(err)
	}
	newSecond.AuthorizedBy = "other-controller"
	policies["new-second"] = newSecond
	if err := validateOrganizationBudgetHistory(complete, policies); err == nil {
		t.Fatal("mixed authorizations accepted for shared limit change")
	}
}

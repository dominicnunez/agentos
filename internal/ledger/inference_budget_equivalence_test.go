package ledger

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

func TestInferenceIncrementalBudgetMatchesReference(t *testing.T) {
	for seed := int64(0); seed < 80; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
			var stream []events.Event
			activations := make(map[string]inference.Policy)
			pending := make(map[string][]string)
			emit := func(org, kind string, payload any) {
				body, err := json.Marshal(payload)
				if err != nil {
					t.Fatal(err)
				}
				stream = append(stream, events.Event{OrganizationID: org, EventType: kind, Payload: body})
			}
			activate := func(org string, step int) {
				policy := testInferencePolicy(now)
				policy.OrganizationID = org
				policy.Version, policy.ConnectionID = inference.ConnectionPolicyVersion, "connection"
				limit := int64(100000)
				if seed%5 == 0 {
					limit = 200
				}
				policy.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: []int64{60, 120, 3600}[rng.Intn(3)], MaxTokensPerWindow: limit, MaxCostNanoUSDPerWindow: 1000000000, MaxConcurrentRequests: 100}
				id := fmt.Sprintf("%s-policy-%d", org, step)
				activations[id] = policy
				stream = append(stream, events.Event{EventID: id, OrganizationID: org, EventType: "INFERENCE_POLICY_ACTIVATED"})
			}
			for step := 0; step < 100; step++ {
				org := []string{"organization-1", "organization-2"}[rng.Intn(2)]
				if step == 20 || step == 60 {
					activate("organization-1", step)
					activate("organization-2", step)
				}
				now = now.Add(time.Duration(rng.Intn(90)) * time.Second)
				if seed%7 == 0 && step == 45 {
					now = now.Add(-5 * time.Minute)
				}
				if len(pending[org]) > 0 && rng.Intn(2) == 0 {
					id := pending[org][0]
					pending[org] = pending[org][1:]
					payload := events.InferenceReconciledPayload{ReservationID: id, State: inferenceStateCompleted, ChargedInputTokens: 10, ChargedOutputTokens: 5, ChargedCostNanoUSD: 70000}
					switch rng.Intn(4) {
					case 0:
						payload.State, payload.ChargedInputTokens, payload.ChargedOutputTokens, payload.ChargedCostNanoUSD = inferenceStateNotSent, 0, 0, 0
					case 1:
						payload.State, payload.ChargedInputTokens, payload.ChargedOutputTokens, payload.ChargedCostNanoUSD = inferenceStateUncertain, 100, 20, 400000
					case 2:
						payload.State, payload.ChargedInputTokens, payload.ChargedOutputTokens, payload.ChargedCostNanoUSD = inferenceStateViolation, 1000, 500, 1000000
					}
					emit(org, "INFERENCE_RECONCILED", payload)
				} else {
					id := fmt.Sprintf("request-%d", step)
					start, end := inferenceWindow(now, time.Hour)
					at := now.Format(time.RFC3339Nano)
					if step < 20 && seed%2 == 0 {
						at = ""
					}
					emit(org, "INFERENCE_RESERVED", events.InferenceReservedPayload{ReservationID: id, AdmittedAt: at, WindowStartedAt: start, WindowExpiresAt: end, ReservedInputTokens: 100, ReservedOutputTokens: 20, ReservedCostNanoUSD: 400000})
					pending[org] = append(pending[org], id)
				}
				want := referenceOrganizationBudgetHistory(stream, activations)
				got := validateOrganizationBudgetHistory(stream, activations)
				if (want != nil) != (got != nil) {
					t.Fatalf("step%d incremental=%v reference=%v", step, got, want)
				}
			}
		})
	}
}

func TestInferenceIncrementalBudgetRetainsOverflowingViolationEvidence(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	policy := testInferencePolicy(now)
	policy.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 10000, MaxCostNanoUSDPerWindow: math.MaxInt64, MaxConcurrentRequests: 3}
	activation := events.Event{EventID: "activation", OrganizationID: policy.OrganizationID, EventType: "INFERENCE_POLICY_ACTIVATED"}
	stream := []events.Event{activation}
	emit := func(kind string, payload any) {
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		stream = append(stream, events.Event{OrganizationID: policy.OrganizationID, EventType: kind, Payload: body})
	}
	for _, id := range []string{"first", "second"} {
		emit("INFERENCE_RESERVED", events.InferenceReservedPayload{ReservationID: id, AdmittedAt: now.Format(time.RFC3339Nano), WindowStartedAt: now, WindowExpiresAt: now.Add(time.Hour), ReservedInputTokens: 100, ReservedOutputTokens: 20, ReservedCostNanoUSD: 400000})
	}
	emit("INFERENCE_RECONCILED", events.InferenceReconciledPayload{ReservationID: "first", State: inferenceStateViolation, ChargedInputTokens: 100, ChargedOutputTokens: 20, ChargedCostNanoUSD: math.MaxInt64})
	emit("INFERENCE_RECONCILED", events.InferenceReconciledPayload{ReservationID: "second", State: inferenceStateViolation, ChargedInputTokens: 100, ChargedOutputTokens: 20, ChargedCostNanoUSD: 400000})
	activations := map[string]inference.Policy{"activation": policy}
	for _, validate := range []func([]events.Event, map[string]inference.Policy) error{referenceOrganizationBudgetHistory, validateOrganizationBudgetHistory} {
		if err := validate(stream, activations); err != nil {
			t.Fatalf("violation evidence was rejected: %v", err)
		}
	}
	emit("INFERENCE_RESERVED", events.InferenceReservedPayload{ReservationID: "third", AdmittedAt: now.Format(time.RFC3339Nano), WindowStartedAt: now, WindowExpiresAt: now.Add(time.Hour), ReservedInputTokens: 100, ReservedOutputTokens: 20, ReservedCostNanoUSD: 400000})
	for _, validate := range []func([]events.Event, map[string]inference.Policy) error{referenceOrganizationBudgetHistory, validateOrganizationBudgetHistory} {
		if err := validate(stream, activations); err == nil {
			t.Fatal("overflowing cumulative usage allowed another admission")
		}
	}
}

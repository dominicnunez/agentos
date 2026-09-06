package ledger

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

func BenchmarkInferenceOrganizationBudgetHistory(b *testing.B) {
	for _, count := range []int{100, 1000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
			policy := testInferencePolicy(now)
			policy.Version, policy.ConnectionID = inference.ConnectionPolicyVersion, "connection"
			policy.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000000, MaxCostNanoUSDPerWindow: 1000000000, MaxConcurrentRequests: 2}
			stream := []events.Event{{EventID: "activation", OrganizationID: policy.OrganizationID, EventType: "INFERENCE_POLICY_ACTIVATED"}}
			for i := range count {
				id := fmt.Sprint(i)
				reserved, err := json.Marshal(events.InferenceReservedPayload{ReservationID: id, AdmittedAt: now.Add(time.Duration(i) * time.Millisecond).Format(time.RFC3339Nano), ReservedInputTokens: 100, ReservedOutputTokens: 20, ReservedCostNanoUSD: 400000, WindowStartedAt: now, WindowExpiresAt: now.Add(time.Hour)})
				if err != nil {
					b.Fatal(err)
				}
				reconciled, err := json.Marshal(events.InferenceReconciledPayload{ReservationID: id, State: inferenceStateCompleted, ChargedInputTokens: 10, ChargedOutputTokens: 5, ChargedCostNanoUSD: 70000})
				if err != nil {
					b.Fatal(err)
				}
				stream = append(stream, events.Event{OrganizationID: policy.OrganizationID, EventType: "INFERENCE_RESERVED", Payload: reserved}, events.Event{OrganizationID: policy.OrganizationID, EventType: "INFERENCE_RECONCILED", Payload: reconciled})
			}
			activations := map[string]inference.Policy{"activation": policy}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := validateOrganizationBudgetHistory(stream, activations); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

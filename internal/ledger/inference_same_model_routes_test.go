package ledger

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/inference"
)

func TestInferenceSameModelAccountsHaveIndependentRouteBudgets(t *testing.T) {
	for _, dimension := range []string{"tokens", "cost"} {
		for _, firstConnection := range []string{"first", ""} {
			t.Run(dimension+"/"+firstConnection, func(t *testing.T) {
				now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
				store, err := Open(filepath.Join(t.TempDir(), "same-model.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				store.now = func() time.Time { return now }
				budget := inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 250, MaxCostNanoUSDPerWindow: 1000000, MaxConcurrentRequests: 3}
				if dimension == "cost" {
					budget.MaxTokensPerWindow = 10000
				}
				for _, connection := range []string{firstConnection, "second", "third"} {
					policy := testInferencePolicy(now)
					if connection != "" {
						policy.Version, policy.ConnectionID = inference.ConnectionPolicyVersion, connection
						policy.OrganizationBudget = &budget
					}
					if dimension == "cost" {
						policy.MaxTokensPerWindow = 10000
						policy.Pricing.MaxCostNanoUSDPerWindow = 400000
					}
					if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
						t.Fatal(err)
					}
				}
				for i, connection := range []string{firstConnection, "second"} {
					request := testInferenceRequest([]string{"first-call", "second-call"}[i])
					request.ConnectionID = connection
					reservation, err := store.ReserveInference(t.Context(), request)
					if err != nil {
						t.Fatalf("untouched account %q lost its route %s allowance: %v", connection, dimension, err)
					}
					if _, err := store.ReconcileInference(t.Context(), reservation, nil, inference.ReconciliationUncertain); err != nil {
						t.Fatal(err)
					}
				}
				request := testInferenceRequest("third-call")
				request.ConnectionID = "third"
				if _, err := store.ReserveInference(t.Context(), request); err == nil || !strings.Contains(err.Error(), "organization inference budget exhausted") {
					t.Fatalf("separate account bypassed organization %s cap: %v", dimension, err)
				}
				if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

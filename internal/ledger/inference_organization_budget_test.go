package ledger

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/inference"
)

func TestInferenceOrganizationBudgetSurvivesProviderReplacementAndRestart(t *testing.T) {
	for _, dimension := range []string{"tokens", "cost"} {
		t.Run(dimension, func(t *testing.T) {
			now := time.Date(2026, 9, 6, 12, 0, 0, 123, time.UTC)
			path := filepath.Join(t.TempDir(), "shared.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			store.now = func() time.Time { return now }
			legacy := testInferencePolicy(now)
			if err := store.ActivateInferencePolicy(t.Context(), legacy); err != nil {
				t.Fatal(err)
			}
			// Usage accrued before the shared policy exists must still count.
			reservation, err := store.ReserveInference(t.Context(), testInferenceRequest("prior-provider"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.ReconcileInference(t.Context(), reservation, nil, inference.ReconciliationUncertain); err != nil {
				t.Fatal(err)
			}
			shared := legacy
			shared.Version, shared.ConnectionID = inference.ConnectionPolicyVersion, "shared-policy"
			shared.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, MaxCostNanoUSDPerWindow: 1000000, MaxConcurrentRequests: 2}
			if dimension == "tokens" {
				shared.OrganizationBudget.MaxTokensPerWindow = 200
			} else {
				shared.OrganizationBudget.MaxCostNanoUSDPerWindow = 700000
			}
			if err := store.ActivateInferencePolicy(t.Context(), shared); err != nil {
				t.Fatal(err)
			}
			legacy.Provider, legacy.Model = "provider-2", "model-2"
			legacy.AuthorizedAt = legacy.AuthorizedAt.Add(time.Minute)
			// A different route window must not partition the shared accounting.
			legacy.WindowDurationSeconds = 60
			if err := store.ActivateInferencePolicy(t.Context(), legacy); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			store.now = func() time.Time { return now.Add(time.Second) }
			request := testInferenceRequest("replacement-provider")
			request.Descriptor.Provider, request.Descriptor.Model = legacy.Provider, legacy.Model
			if _, err := store.ReserveInference(t.Context(), request); err == nil || !strings.Contains(err.Error(), "organization inference budget exhausted") {
				t.Fatalf("provider replacement bypassed shared %s cap: %v", dimension, err)
			}
			var count int
			if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM inference_reservations`).Scan(&count); err != nil || count != 1 {
				t.Fatalf("denial persisted a reservation: count=%d err=%v", count, err)
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.ExecContext(t.Context(), `UPDATE inference_reservations SET created_at=? WHERE reservation_id=?`, now.Add(-24*time.Hour).Format(time.RFC3339Nano), reservation.ID); err != nil {
				t.Fatal(err)
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err == nil {
				t.Fatal("altered accounting timestamp passed event validation")
			}
			if _, err := store.ReserveInference(t.Context(), request); err == nil || !strings.Contains(err.Error(), "validate shared inference accounting") {
				t.Fatalf("altered timestamp bypassed shared admission validation: %v", err)
			}
			// Reconstruct the old event format, which did not bind created_at.
			// Its original route window must still prevent a budget reset.
			if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET payload=CAST(json_remove(payload,'$.admitted_at') AS BLOB) WHERE event_type='INFERENCE_RESERVED' AND json_extract(payload,'$.reservation_id')=?`, reservation.ID); err != nil {
				t.Fatal(err)
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
				t.Fatalf("historical event format rejected: %v", err)
			}
			if _, err := store.ReserveInference(t.Context(), request); err == nil || !strings.Contains(err.Error(), "organization inference budget exhausted") {
				t.Fatalf("unbound historical timestamp bypassed shared cap: %v", err)
			}
		})
	}
}

func TestInferenceOrganizationBudgetRetainsOutstandingCallsAcrossWindow(t *testing.T) {
	for _, dimension := range []string{"tokens", "concurrency"} {
		t.Run(dimension, func(t *testing.T) {
			now := time.Date(2026, 9, 6, 12, 0, 30, 0, time.UTC)
			store, err := Open(filepath.Join(t.TempDir(), "outstanding.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			store.now = func() time.Time { return now }
			legacy := testInferencePolicy(now)
			legacy.MaxTokensPerWindow, legacy.MaxConcurrentRequests = 10000, 10
			shared := legacy
			shared.Version, shared.ConnectionID = inference.ConnectionPolicyVersion, "shared-policy"
			shared.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 60, MaxTokensPerWindow: 1000, MaxCostNanoUSDPerWindow: 1000000, MaxConcurrentRequests: 2}
			if dimension == "tokens" {
				shared.OrganizationBudget.MaxTokensPerWindow = 200
			} else {
				shared.OrganizationBudget.MaxConcurrentRequests = 1
			}
			for _, policy := range []inference.Policy{legacy, shared} {
				if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
					t.Fatal(err)
				}
			}
			first, err := store.ReserveInference(t.Context(), testInferenceRequest("outstanding"))
			if err != nil {
				t.Fatal(err)
			}
			now = now.Add(time.Minute)
			if _, err := store.ReserveInference(t.Context(), testInferenceRequest("next-window")); err == nil || !strings.Contains(err.Error(), "organization inference budget exhausted") {
				t.Fatalf("outstanding call bypassed shared %s cap at rollover: %v", dimension, err)
			}
			if _, err := store.ReconcileInference(t.Context(), first, nil, inference.ReconciliationNotSent); err != nil {
				t.Fatal(err)
			}
			if _, err := store.ReserveInference(t.Context(), testInferenceRequest("after-release")); err != nil {
				t.Fatalf("released reservation retained shared budget: %v", err)
			}
		})
	}
}

package ledger

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/inference"
)

func TestInferenceConnectionActivationsCoexistAndRejectStaleActiveRevision(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "connections.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	legacy := testInferencePolicy(now)
	first, second := legacy, legacy
	first.Version, first.ConnectionID = inference.ConnectionPolicyVersion, "first"
	first.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, MaxCostNanoUSDPerWindow: 1000000, MaxConcurrentRequests: 2}
	second.Version, second.ConnectionID = inference.ConnectionPolicyVersion, "second"
	second.OrganizationBudget = first.OrganizationBudget
	second.Provider, second.Model = "provider-2", "model-2"
	for _, policy := range []inference.Policy{legacy, first, second} {
		if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatal(err)
	}
	// A connection cannot obtain a larger organization allowance by publishing
	// inconsistent limits. Rejection must leave both authority and history intact.
	conflicting := second
	conflicting.ConnectionID = "conflicting"
	budget := *second.OrganizationBudget
	budget.MaxTokensPerWindow++
	conflicting.OrganizationBudget = &budget
	var beforeEvents, afterEvents int
	if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM events`).Scan(&beforeEvents); err != nil {
		t.Fatal(err)
	}
	if err := store.ActivateInferencePolicy(t.Context(), conflicting); err == nil {
		t.Fatal("conflicting organization budget activated")
	}
	if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM events`).Scan(&afterEvents); err != nil || afterEvents != beforeEvents {
		t.Fatalf("rejected activation changed events: before=%d after=%d err=%v", beforeEvents, afterEvents, err)
	}
	reservation, err := store.ReserveInference(t.Context(), testInferenceRequest("legacy-with-connections"))
	if err != nil {
		t.Fatal(err)
	}
	// Replacing an unrelated connection cannot strand the legacy reservation.
	oldFingerprint, err := first.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	first.AuthorizedAt = first.AuthorizedAt.Add(time.Minute)
	if err := store.ActivateInferencePolicy(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReconcileInference(t.Context(), reservation, nil, inference.ReconciliationNotSent); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM inference_policies WHERE active=1`).Scan(&count); err != nil || count != 3 {
		t.Fatalf("active policies=%d err=%v", count, err)
	}
	if _, err := store.db.ExecContext(t.Context(), `UPDATE inference_policies SET active=0 WHERE connection_id='first'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `UPDATE inference_policies SET active=1 WHERE policy_fingerprint=?`, oldFingerprint); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err == nil {
		t.Fatal("retired connection revision regained authority")
	}
}

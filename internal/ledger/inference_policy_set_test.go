package ledger

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/inference"
)

func TestInferencePolicySetChangesSharedLimitsAtomically(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "policy-set.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.now = func() time.Time { return now }
	var policies []inference.Policy
	for _, connection := range []string{"first", "second"} {
		policy := testInferencePolicy(now)
		policy.Version, policy.ConnectionID = inference.ConnectionPolicyVersion, connection
		policy.Provider, policy.Model = connection, connection
		policy.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, MaxCostNanoUSDPerWindow: 1000000, MaxConcurrentRequests: 2}
		policies = append(policies, policy)
	}
	if err := store.ActivateInferencePolicies(t.Context(), policies); err != nil {
		t.Fatal(err)
	}
	request := testInferenceRequest("prior-use")
	request.ConnectionID, request.Descriptor.Provider, request.Descriptor.Model = "first", "first", "first"
	reservation, err := store.ReserveInference(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	for i := range policies {
		policies[i].AuthorizedAt = policies[i].AuthorizedAt.Add(time.Minute)
		policies[i].OrganizationBudget.MaxTokensPerWindow = 200
	}
	if err := store.ActivateInferencePolicies(t.Context(), policies); err == nil {
		t.Fatal("policy set changed while an affected connection had an outstanding call")
	}
	if _, err := store.ReconcileInference(t.Context(), reservation, nil, inference.ReconciliationUncertain); err != nil {
		t.Fatal(err)
	}
	var before int
	if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM events`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	assertUnchanged := func() {
		t.Helper()
		var after int
		if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM events`).Scan(&after); err != nil || after != before {
			t.Fatalf("failed set changed history: before=%d after=%d err=%v", before, after, err)
		}
		if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.ActivateInferencePolicies(t.Context(), policies[:1]); err == nil {
		t.Fatal("partial shared limit update accepted")
	}
	assertUnchanged()
	expired := append([]inference.Policy(nil), policies...)
	pricing := *expired[1].Pricing
	pricing.ExpiresAt = now.Add(-time.Second)
	expired[1].Pricing = &pricing
	if err := store.ActivateInferencePolicies(t.Context(), expired); err == nil {
		t.Fatal("expired second member accepted")
	}
	assertUnchanged()
	if err := store.ActivateInferencePolicies(t.Context(), policies); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatal(err)
	}
	request = testInferenceRequest("after-update")
	request.ConnectionID, request.Descriptor.Provider, request.Descriptor.Model = "second", "second", "second"
	if _, err := store.ReserveInference(t.Context(), request); err == nil || !strings.Contains(err.Error(), "organization inference budget exhausted") {
		t.Fatalf("budget update reset prior charges: %v", err)
	}
}

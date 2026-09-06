package ledger

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/inference"
)

func TestInferenceCatalogPolicyPersistsAndExpiresAtAdmission(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "catalog.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }
	policy := testInferencePolicy(now)
	policy.Version, policy.ConnectionID = inference.ConnectionPolicyVersion, "account"
	policy.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, MaxCostNanoUSDPerWindow: 1000000, MaxConcurrentRequests: 2}
	policy.Routing = &inference.RoutePolicy{OrganizationID: policy.OrganizationID, Locality: inference.CloudAllowed, DataClasses: []string{"internal"}}
	policy.Catalog = &inference.CatalogDefinition{Capabilities: []inference.Capability{inference.Text}, ContextTokens: 120, OutputTokens: 20, DataClasses: []string{"internal"}, ValidUntil: now.Add(time.Minute)}
	if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.now = func() time.Time { return now }
	tx, err := store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	loaded, fingerprint, err := activeInferencePolicy(t.Context(), tx, policy.OrganizationID, policy.ConnectionID)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	wantFingerprint, err := policy.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	if fingerprint != wantFingerprint || loaded.Catalog == nil || loaded.Catalog.DataClasses[0] != "internal" {
		t.Fatal("catalog was not persisted with exact policy")
	}
	policy.Catalog.DataClasses[0] = "secret"
	changedFingerprint, err := policy.Fingerprint()
	if err != nil || changedFingerprint == fingerprint {
		t.Fatal("catalog changes did not change reviewed policy fingerprint")
	}
	request := testInferenceRequest("catalog-expired")
	request.ConnectionID = "account"
	request.Scope.Routing = &inference.RouteRequirements{OrganizationID: policy.OrganizationID, Capabilities: []inference.Capability{inference.Text}, InputTokens: 100, OutputTokens: 20, Locality: inference.CloudAllowed, DataClass: "internal"}
	store.now = func() time.Time { return now.Add(time.Minute) }
	if _, err := store.ReserveInference(t.Context(), request); err == nil || !strings.Contains(err.Error(), "catalog") {
		t.Fatalf("expired catalog admitted: %v", err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatalf("rejected admission corrupted policy history: %v", err)
	}
}

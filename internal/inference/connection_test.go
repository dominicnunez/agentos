package inference

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/execution"
)

func TestConnectionPolicyPreservesLegacyFingerprint(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	policy := selectablePolicy(now, Subscription, "subscription")
	// Frozen v1 wire representation, independent of the current struct layout.
	legacy := `{"version":1,"organization_id":"organization-1","provider":"subscription","model":"model-1","execution_profile_version":"profile-v1","mode":"SUBSCRIPTION","max_input_tokens_per_request":100,"max_output_tokens_per_request":20,"max_tokens_per_window":500,"continuity_reserve_tokens":100,"window_duration_seconds":3600,"max_concurrent_requests":1,"max_attempts_per_request":1,"authorized_by":"local-uid-1000","authorized_at":"2026-08-14T11:00:00Z","authorization_expires_at":"2026-08-14T13:00:00Z"}`
	body, err := json.Marshal(policy)
	if err != nil || string(body) != legacy {
		t.Fatalf("legacy policy bytes changed: %s, %v", body, err)
	}
	digest := sha256.Sum256([]byte(legacy))
	fingerprint, err := policy.Fingerprint()
	if err != nil || fingerprint != hex.EncodeToString(digest[:]) {
		t.Fatalf("legacy policy fingerprint changed: %v", err)
	}
	policy.ConnectionID = "connection-a"
	if policy.Validate() == nil {
		t.Fatal("legacy policy accepted new connection semantics")
	}
	policy.Version = ConnectionPolicyVersion
	policy.OrganizationBudget = &OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, MaxConcurrentRequests: 2}
	first, err := policy.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	policy.ConnectionID = "connection-b"
	second, err := policy.Fingerprint()
	if err != nil || first == second {
		t.Fatal("connection substitution did not change policy fingerprint")
	}
	policy.OrganizationBudget.MaxTokensPerWindow++
	third, err := policy.Fingerprint()
	if err != nil || second == third {
		t.Fatal("organization budget substitution did not change policy fingerprint")
	}
	policy.OrganizationBudget = nil
	if policy.Validate() == nil {
		t.Fatal("connection policy accepted missing organization budget")
	}
}

func TestManagerRequiresExactConnection(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	policy := selectablePolicy(now, Subscription, "subscription")
	policy.Version, policy.ConnectionID = ConnectionPolicyVersion, "configured"
	policy.OrganizationBudget = &OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, MaxConcurrentRequests: 2}
	manager := Manager{Pools: []Pool{{ID: "pool", Policy: policy, Available: true}}}
	request := PoolRequest{Descriptor: execution.ModelDescriptor{Provider: policy.Provider, Model: policy.Model, ExecutionProfileVersion: policy.ExecutionProfileVersion}}
	for _, connection := range []string{"", "absent", "https://endpoint", "with space"} {
		request.ConnectionID = connection
		if _, err := manager.Select(now, request); err == nil {
			t.Fatalf("unconfigured connection %q selected a pool", connection)
		}
	}
	request.ConnectionID = policy.ConnectionID
	if _, err := manager.Select(now, request); err != nil {
		t.Fatal(err)
	}
}

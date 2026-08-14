package inference

import (
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/execution"
)

func selectablePolicy(now time.Time, mode AccessMode, provider string) Policy {
	policy := Policy{
		Version: PolicyVersion, OrganizationID: "organization-1", Provider: provider, Model: "model-1",
		ExecutionProfileVersion: "profile-v1", Mode: mode,
		MaxInputTokensPerRequest: 100, MaxOutputTokensPerRequest: 20, MaxTokensPerWindow: 500,
		ContinuityReserveTokens: 100, WindowDurationSeconds: 3600, MaxConcurrentRequests: 1, MaxAttemptsPerRequest: 1,
		AuthorizedBy: "local-uid-1000", AuthorizedAt: now.Add(-time.Hour), AuthorizationExpiresAt: now.Add(time.Hour),
	}
	if mode == MeteredAPI {
		policy.Pricing = &Pricing{
			InputNanoUSDPerMillionTokens: 1, OutputNanoUSDPerMillionTokens: 1,
			MaxCostNanoUSDPerRequest: 2, MaxCostNanoUSDPerWindow: 100, ExpiresAt: now.Add(time.Hour),
		}
	}
	return policy
}

func TestManagerSelectsAuthorizedLocalPoolDeterministically(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	local := selectablePolicy(now, Local, "local-runtime")
	manager := Manager{Pools: []Pool{
		{ID: "z-metered", Policy: selectablePolicy(now, MeteredAPI, "metered"), Available: true},
		{ID: "b-local", Policy: local, Available: true},
		{ID: "a-local-unavailable", Policy: local, Available: false},
	}}
	selection, err := manager.Select(now, PoolRequest{Descriptor: execution.ModelDescriptor{
		Provider: local.Provider, Model: local.Model, ExecutionProfileVersion: local.ExecutionProfileVersion,
	}})
	if err != nil || selection.PoolID != "b-local" || selection.Mode != Local || selection.ReservedCostNanoUSD != 0 {
		t.Fatalf("local selection=%+v err=%v", selection, err)
	}
}

func TestManagerPreservesConcurrencyAndContinuityReserve(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	policy := selectablePolicy(now, Subscription, "subscription")
	request := PoolRequest{Descriptor: execution.ModelDescriptor{Provider: policy.Provider, Model: policy.Model, ExecutionProfileVersion: policy.ExecutionProfileVersion}}
	for name, pool := range map[string]Pool{
		"concurrency": {ID: "pool-1", Policy: policy, Available: true, ActiveRequests: 1},
		"reserve":     {ID: "pool-1", Policy: policy, Available: true, ChargedTokens: 281},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := (Manager{Pools: []Pool{pool}}).Select(now, request); err == nil {
				t.Fatal("infeasible pool was selected")
			}
		})
	}
	if _, err := (Manager{Pools: []Pool{{ID: "pool-1", Policy: policy, Available: true, ChargedTokens: 280}}}).Select(now, request); err != nil {
		t.Fatalf("exact reserve boundary was rejected: %v", err)
	}
}

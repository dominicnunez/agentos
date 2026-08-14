// Package inference provides deterministic, reserve-aware model admission.
package inference

import (
	"fmt"
	"sort"
	"time"

	"github.com/dominicnunez/agentos/internal/execution"
)

type Pool struct {
	ID                 string
	Policy             Policy
	Available          bool
	ActiveRequests     int
	ChargedTokens      int64
	ChargedCostNanoUSD int64
}

type PoolRequest struct {
	Descriptor execution.ModelDescriptor
}

type PoolSelection struct {
	PoolID               string
	PolicyFingerprint    string
	Mode                 AccessMode
	ReservedInputTokens  int64
	ReservedOutputTokens int64
	ReservedCostNanoUSD  int64
}

type Manager struct{ Pools []Pool }

// Select applies hard identity, availability, authorization, concurrency,
// reserve, and cost constraints before deterministic pool ordering. It uses
// exact integer accounting for subscription, metered, and local pools alike.
func (m Manager) Select(now time.Time, request PoolRequest) (PoolSelection, error) {
	if !validValue(request.Descriptor.Provider) || !validValue(request.Descriptor.Model) || !validValue(request.Descriptor.ExecutionProfileVersion) {
		return PoolSelection{}, fmt.Errorf("inference pool request identity is incomplete")
	}
	pools := append([]Pool(nil), m.Pools...)
	sort.Slice(pools, func(i, j int) bool { return pools[i].ID < pools[j].ID })
	seen := make(map[string]struct{}, len(pools))
	failure := "no configured pool matches the requested model identity"
	for _, pool := range pools {
		if !validValue(pool.ID) || pool.Policy.Validate() != nil || pool.ActiveRequests < 0 || pool.ChargedTokens < 0 || pool.ChargedCostNanoUSD < 0 {
			return PoolSelection{}, fmt.Errorf("inference pool configuration is invalid")
		}
		if _, duplicate := seen[pool.ID]; duplicate {
			return PoolSelection{}, fmt.Errorf("inference pool identity is duplicated")
		}
		seen[pool.ID] = struct{}{}
		policy := pool.Policy
		if policy.Provider != request.Descriptor.Provider || policy.Model != request.Descriptor.Model || policy.ExecutionProfileVersion != request.Descriptor.ExecutionProfileVersion {
			continue
		}
		if !pool.Available {
			failure = "matching pool is unavailable"
			continue
		}
		if now.Before(policy.AuthorizedAt) || !now.Before(policy.AuthorizationExpiresAt) {
			failure = "inference authorization is missing, not yet valid, or expired"
			continue
		}
		if pool.ActiveRequests >= policy.MaxConcurrentRequests {
			failure = "inference concurrency limit is exhausted"
			continue
		}
		reservedTokens := policy.MaxInputTokensPerRequest + policy.MaxOutputTokensPerRequest
		if pool.ChargedTokens > policy.MaxTokensPerWindow-policy.ContinuityReserveTokens-reservedTokens {
			failure = "inference token budget would consume its continuity reserve"
			continue
		}
		reservedCost, err := policy.ReservedCostNanoUSD()
		if err != nil {
			return PoolSelection{}, err
		}
		if policy.Mode == MeteredAPI {
			if policy.Pricing == nil || !now.Before(policy.Pricing.ExpiresAt) {
				failure = "metered inference pricing is missing or stale"
				continue
			}
			if reservedCost > policy.Pricing.MaxCostNanoUSDPerRequest || pool.ChargedCostNanoUSD > policy.Pricing.MaxCostNanoUSDPerWindow-reservedCost {
				failure = "inference cost budget is exhausted"
				continue
			}
		}
		fingerprint, err := policy.Fingerprint()
		if err != nil {
			return PoolSelection{}, err
		}
		return PoolSelection{
			PoolID: pool.ID, PolicyFingerprint: fingerprint, Mode: policy.Mode,
			ReservedInputTokens: policy.MaxInputTokensPerRequest, ReservedOutputTokens: policy.MaxOutputTokensPerRequest,
			ReservedCostNanoUSD: reservedCost,
		}, nil
	}
	return PoolSelection{}, fmt.Errorf("no feasible inference pool: %s", failure)
}

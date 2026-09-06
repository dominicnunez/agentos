package inference

import (
	"errors"
	"fmt"
)

var ErrOrganizationBudgetExhausted = errors.New("organization inference budget exhausted")

// OrganizationBudget applies across every configured connection and model.
// It supplements individual policy limits; it does not allocate a fresh budget
// when a connection is added or a request falls back to another provider.
type OrganizationBudget struct {
	WindowDurationSeconds   int64 `json:"window_duration_seconds"`
	MaxTokensPerWindow      int64 `json:"max_tokens_per_window"`
	ContinuityReserveTokens int64 `json:"continuity_reserve_tokens"`
	MaxCostNanoUSDPerWindow int64 `json:"max_cost_nano_usd_per_window"`
	MaxConcurrentRequests   int   `json:"max_concurrent_requests"`
}

func (b OrganizationBudget) Validate() error {
	if b.WindowDurationSeconds < 60 || b.WindowDurationSeconds > 366*24*60*60 ||
		b.MaxTokensPerWindow < 1 || b.ContinuityReserveTokens < 0 || b.ContinuityReserveTokens >= b.MaxTokensPerWindow ||
		b.MaxCostNanoUSDPerWindow < 0 || b.MaxConcurrentRequests < 1 || b.MaxConcurrentRequests > 1024 {
		return fmt.Errorf("organization inference budget is invalid")
	}
	return nil
}

// Allows uses subtraction after validation to avoid overflowing accumulated
// integer accounting. Zero monetary budget permits only zero-cost reservations.
func (b OrganizationBudget) Allows(active int, chargedTokens, chargedCost, input, output, cost int64) bool {
	if b.Validate() != nil || active < 0 || chargedTokens < 0 || chargedCost < 0 || input < 1 || output < 1 || cost < 0 || active >= b.MaxConcurrentRequests {
		return false
	}
	available := b.MaxTokensPerWindow - b.ContinuityReserveTokens
	if chargedTokens > available || input > available-chargedTokens || output > available-chargedTokens-input {
		return false
	}
	return chargedCost <= b.MaxCostNanoUSDPerWindow && cost <= b.MaxCostNanoUSDPerWindow-chargedCost
}

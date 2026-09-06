package inference

import (
	"math"
	"testing"
)

func TestConnectionOrganizationBudgetBoundaries(t *testing.T) {
	budget := OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, ContinuityReserveTokens: 100, MaxCostNanoUSDPerWindow: 100, MaxConcurrentRequests: 2}
	for _, tt := range []struct {
		name                                  string
		active                                int
		tokens, cost, input, output, estimate int64
		want                                  bool
	}{
		{"exact limits", 1, 700, 80, 100, 100, 20, true},
		{"continuity reserve", 1, 701, 80, 100, 100, 20, false},
		{"cost exhausted", 1, 700, 81, 100, 100, 20, false},
		{"concurrency exhausted", 2, 0, 0, 100, 100, 20, false},
		{"negative accounting", 0, -1, 0, 100, 100, 20, false},
		{"input overflow", 0, 0, 0, math.MaxInt64, 1, 0, false},
		{"output overflow", 0, 0, 0, 1, math.MaxInt64, 0, false},
		{"cost overflow", 0, 0, math.MaxInt64, 1, 1, 1, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := budget.Allows(tt.active, tt.tokens, tt.cost, tt.input, tt.output, tt.estimate); got != tt.want {
				t.Fatalf("Allows=%v want %v", got, tt.want)
			}
		})
	}
	budget.MaxCostNanoUSDPerWindow = 0
	if !budget.Allows(0, 0, 0, 1, 1, 0) || budget.Allows(0, 0, 0, 1, 1, 1) {
		t.Fatal("zero monetary cap must permit only zero-cost reservations")
	}
	budget.MaxTokensPerWindow, budget.ContinuityReserveTokens = math.MaxInt64, 0
	if !budget.Allows(0, math.MaxInt64-2, 0, 1, 1, 0) || budget.Allows(0, math.MaxInt64-1, 0, 1, 1, 0) {
		t.Fatal("token accounting overflowed at the integer boundary")
	}
}

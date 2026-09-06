package ledger

import (
	"fmt"
	"math"
	"time"
)

// Totals are local to one replay snapshot and one organization window. Changes
// of window or policy rebuild them from retained charges; ordinary reservations
// and reconciliations update them without rescanning the organization's history.
type historicalBudgetTotals struct {
	start, end   time.Time
	active       int
	tokens, cost int64
}

func (t *historicalBudgetTotals) includes(c historicalInferenceCharge) bool {
	if c.outstanding {
		return true
	}
	if !c.admittedAt.IsZero() {
		return !c.admittedAt.Before(t.start) && c.admittedAt.Before(t.end)
	}
	return c.admission.WindowStartedAt.Before(t.end) && c.admission.WindowExpiresAt.After(t.start)
}

func (t *historicalBudgetTotals) add(c historicalInferenceCharge) error {
	if t.includes(c) {
		if c.input < 0 || c.output < 0 || c.cost < 0 || c.input > math.MaxInt64-t.tokens || c.output > math.MaxInt64-t.tokens-c.input || c.cost > math.MaxInt64-t.cost {
			return fmt.Errorf("historical shared budget accounting overflow")
		}
		t.tokens += c.input + c.output
		t.cost += c.cost
	}
	if c.outstanding {
		t.active++
	}
	return nil
}

func (t *historicalBudgetTotals) remove(c historicalInferenceCharge) {
	// Only charges previously added to these exact totals can be removed.
	if t.includes(c) {
		t.tokens -= c.input + c.output
		t.cost -= c.cost
	}
	if c.outstanding {
		t.active--
	}
}

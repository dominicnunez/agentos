package ledger

import (
	"fmt"
	"math/big"
	"sort"
	"time"
)

// Replay queries may change window duration or arrive out of timestamp order.
// Index completed admissions by time and legacy window endpoints, and retain
// outstanding charges separately. Updates and arbitrary window queries are
// logarithmic in the number of admission timestamps, independent of decisions.
type routeChargeIndex struct {
	times                   []time.Time
	completed, starts, ends []routeChargeAmount
	outstanding             routeChargeAmount
	active                  int
	accounts                map[routeAccountWindow]*routeChargeAmount
	accountActive           map[string]int
}

type routeAccountWindow struct {
	connection, provider, model string
	start                       time.Time
}

// Prefix aggregates can exceed int64 across unrelated windows even when each
// queried window is valid. Keep exact intermediate sums and check the result.
type routeChargeAmount struct{ tokens, cost big.Int }

func (a *routeChargeAmount) add(b *routeChargeAmount, sign int) {
	if sign > 0 {
		a.tokens.Add(&a.tokens, &b.tokens)
		a.cost.Add(&a.cost, &b.cost)
	} else {
		a.tokens.Sub(&a.tokens, &b.tokens)
		a.cost.Sub(&a.cost, &b.cost)
	}
}

func (a *routeChargeAmount) values() (int64, int64, error) {
	if a.tokens.Sign() < 0 || a.cost.Sign() < 0 || !a.tokens.IsInt64() || !a.cost.IsInt64() {
		return 0, 0, fmt.Errorf("routing snapshot accounting overflow")
	}
	return a.tokens.Int64(), a.cost.Int64(), nil
}

func newRouteChargeIndex(times []time.Time) *routeChargeIndex {
	sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
	unique := times[:0]
	for _, at := range times {
		if len(unique) == 0 || !at.Equal(unique[len(unique)-1]) {
			unique = append(unique, at)
		}
	}
	return &routeChargeIndex{times: unique, completed: make([]routeChargeAmount, len(unique)+1), starts: make([]routeChargeAmount, len(unique)+1), ends: make([]routeChargeAmount, len(unique)+1), accounts: make(map[routeAccountWindow]*routeChargeAmount), accountActive: make(map[string]int)}
}

func (x *routeChargeIndex) update(tree []routeChargeAmount, at time.Time, amount *routeChargeAmount, sign int) error {
	i := sort.Search(len(x.times), func(i int) bool { return !x.times[i].Before(at) })
	if i == len(x.times) || !x.times[i].Equal(at) {
		return fmt.Errorf("routing charge timestamp absent from replay index")
	}
	for i++; i < len(tree); i += i & -i {
		tree[i].add(amount, sign)
	}
	return nil
}

func (x *routeChargeIndex) prefix(tree []routeChargeAmount, at time.Time, inclusive bool) routeChargeAmount {
	i := sort.Search(len(x.times), func(i int) bool {
		if inclusive {
			return x.times[i].After(at)
		}
		return !x.times[i].Before(at)
	})
	var sum routeChargeAmount
	for ; i > 0; i -= i & -i {
		sum.add(&tree[i], 1)
	}
	return sum
}

// sign=-1 removes the exact outstanding reservation before reconciliation;
// sign=1 adds its replacement. The caller proves event/row correspondence first.
func (x *routeChargeIndex) change(c historicalInferenceCharge, sign int) error {
	if c.input < 0 || c.output < 0 || c.cost < 0 || (sign != 1 && sign != -1) {
		return fmt.Errorf("routing snapshot contains invalid charges")
	}
	var amount routeChargeAmount
	amount.tokens.SetInt64(c.input)
	amount.tokens.Add(&amount.tokens, big.NewInt(c.output))
	amount.cost.SetInt64(c.cost)
	key := routeAccountWindow{c.admission.ConnectionID, c.admission.Provider, c.admission.Model, c.admission.WindowStartedAt.UTC()}
	if x.accounts[key] == nil {
		x.accounts[key] = &routeChargeAmount{}
	}
	x.accounts[key].add(&amount, sign)
	if c.outstanding {
		x.outstanding.add(&amount, sign)
		x.active += sign
		x.accountActive[c.admission.ConnectionID] += sign
		return nil
	}
	if !c.admittedAt.IsZero() {
		return x.update(x.completed, c.admittedAt, &amount, sign)
	}
	if err := x.update(x.starts, c.admission.WindowStartedAt, &amount, sign); err != nil {
		return err
	}
	return x.update(x.ends, c.admission.WindowExpiresAt, &amount, sign)
}

func (x *routeChargeIndex) organization(start, end time.Time) (historicalBudgetTotals, error) {
	sum := x.prefix(x.completed, end, false)
	before := x.prefix(x.completed, start, false)
	sum.add(&before, -1)
	// Legacy intervals overlap [start,end) iff their start < end and end > start.
	legacyStarts := x.prefix(x.starts, end, false)
	legacyEnds := x.prefix(x.ends, start, true)
	sum.add(&legacyStarts, 1)
	sum.add(&legacyEnds, -1)
	sum.add(&x.outstanding, 1)
	tokens, cost, err := sum.values()
	return historicalBudgetTotals{start: start, end: end, active: x.active, tokens: tokens, cost: cost}, err
}

func (x *routeChargeIndex) account(key routeAccountWindow) (int, int64, int64, error) {
	amount := x.accounts[key]
	if amount == nil {
		return x.accountActive[key.connection], 0, 0, nil
	}
	tokens, cost, err := amount.values()
	return x.accountActive[key.connection], tokens, cost, err
}

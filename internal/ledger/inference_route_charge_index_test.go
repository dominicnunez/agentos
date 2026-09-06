package ledger

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
)

func routeIndexCharges(n int) ([]historicalInferenceCharge, []time.Time) {
	base := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	charges := make([]historicalInferenceCharge, n)
	var times []time.Time
	for i := range charges {
		at := base.Add(time.Duration(i) * time.Minute)
		start, end := inferenceWindow(at, time.Duration(1+i%5)*time.Hour)
		if i%3 == 0 {
			at = time.Time{}
		} // Legacy sealed-window evidence.
		charges[i] = historicalInferenceCharge{admission: events.InferenceReservedPayload{ConnectionID: fmt.Sprint(i % 3), Provider: fmt.Sprint(i % 2), Model: fmt.Sprint(i % 4), WindowStartedAt: start, WindowExpiresAt: end}, input: int64(i + 1), output: 10, cost: int64(i + 2), admittedAt: at, outstanding: true}
		times = append(times, at, start, end)
	}
	return charges, times
}

func TestRouteChargeIndexMatchesReferenceAcrossSnapshots(t *testing.T) {
	charges, times := routeIndexCharges(120)
	index := newRouteChargeIndex(times)
	var current []historicalInferenceCharge
	rng := rand.New(rand.NewSource(73))
	check := func() {
		t.Helper()
		// Arbitrary windows exercise nonmonotonic selection times and changing
		// policy durations, including boundaries of legacy overlapping windows.
		for range 8 {
			start := time.Date(2026, 9, 6, 10+rng.Intn(6), rng.Intn(2)*30, 0, 0, time.UTC)
			end := start.Add(time.Duration(1+rng.Intn(4)) * time.Hour)
			want := historicalBudgetTotals{start: start, end: end}
			for _, charge := range current {
				if err := want.add(charge); err != nil {
					t.Fatal(err)
				}
			}
			got, err := index.organization(start, end)
			if err != nil || got != want {
				t.Fatalf("window %v..%v got %+v, %v; want %+v", start, end, got, err, want)
			}
		}
		for _, charge := range charges {
			key := routeAccountWindow{charge.admission.ConnectionID, charge.admission.Provider, charge.admission.Model, charge.admission.WindowStartedAt.UTC()}
			var active int
			var tokens, cost int64
			for _, c := range current {
				if c.admission.ConnectionID != key.connection {
					continue
				}
				if c.outstanding {
					active++
				}
				if c.admission.Provider == key.provider && c.admission.Model == key.model && c.admission.WindowStartedAt.Equal(key.start) {
					tokens += c.input + c.output
					cost += c.cost
				}
			}
			a, tok, usd, err := index.account(key)
			if err != nil || a != active || tok != tokens || usd != cost {
				t.Fatalf("account %+v: got %d/%d/%d, %v; want %d/%d/%d", key, a, tok, usd, err, active, tokens, cost)
			}
		}
	}
	for _, c := range charges {
		if err := index.change(c, 1); err != nil {
			t.Fatal(err)
		}
		current = append(current, c)
		check()
	}
	for _, i := range rng.Perm(len(current)) {
		if err := index.change(current[i], -1); err != nil {
			t.Fatal(err)
		}
		current[i].outstanding = false
		current[i].input, current[i].output, current[i].cost = int64(rng.Intn(200)), int64(rng.Intn(20)), int64(rng.Intn(200))
		if err := index.change(current[i], 1); err != nil {
			t.Fatal(err)
		}
		check()
	}
}

func TestRouteChargeIndexExactBoundariesAndOverflow(t *testing.T) {
	base := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	index := newRouteChargeIndex([]time.Time{base, base.Add(time.Hour), base.Add(2 * time.Hour)})
	charge := historicalInferenceCharge{admission: events.InferenceReservedPayload{WindowStartedAt: base, WindowExpiresAt: base.Add(time.Hour)}, admittedAt: base, input: math.MaxInt64, cost: math.MaxInt64}
	if err := index.change(charge, 1); err != nil {
		t.Fatal(err)
	}
	charge.admittedAt = base.Add(time.Hour)
	charge.admission.WindowStartedAt = charge.admittedAt
	charge.admission.WindowExpiresAt = base.Add(2 * time.Hour)
	if err := index.change(charge, 1); err != nil {
		t.Fatal(err)
	}
	for _, start := range []time.Time{base, base.Add(time.Hour)} {
		got, err := index.organization(start, start.Add(time.Hour))
		if err != nil || got.tokens != math.MaxInt64 || got.cost != math.MaxInt64 {
			t.Fatalf("unrelated window overflow: %+v %v", got, err)
		}
	}
	if _, err := index.organization(base, base.Add(2*time.Hour)); err == nil {
		t.Fatal("combined window overflow accepted")
	}
	charge.input, charge.cost = 1, 1
	charge.admittedAt = time.Time{}
	legacy := newRouteChargeIndex([]time.Time{base, base.Add(time.Hour), base.Add(2 * time.Hour)})
	if err := legacy.change(charge, 1); err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int{0, 1, 2} {
		got, err := legacy.organization(base.Add(time.Duration(offset)*time.Hour), base.Add(time.Duration(offset+1)*time.Hour))
		want := int64(0)
		if offset == 1 {
			want = 1
		}
		if err != nil || got.tokens != want {
			t.Fatalf("legacy boundary %d: %+v %v", offset, got, err)
		}
	}
}

func BenchmarkRouteChargeIndexReplay(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			charges, times := routeIndexCharges(n)
			b.ResetTimer()
			for range b.N {
				index := newRouteChargeIndex(append([]time.Time(nil), times...))
				for _, c := range charges {
					if err := index.change(c, 1); err != nil {
						b.Fatal(err)
					}
					if err := index.change(c, -1); err != nil {
						b.Fatal(err)
					}
					c.outstanding = false
					if err := index.change(c, 1); err != nil {
						b.Fatal(err)
					}
					if _, err := index.organization(c.admission.WindowStartedAt, c.admission.WindowExpiresAt); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

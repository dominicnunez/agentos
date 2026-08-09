// Package inference provides deterministic, reserve-aware model selection.
package inference

import (
	"fmt"
	"sort"
	"time"
)

type AccessMode string

const (
	Subscription AccessMode = "SUBSCRIPTION"
	MeteredAPI   AccessMode = "METERED_API"
	Local        AccessMode = "LOCAL"
)

type UsageSnapshot struct {
	Source     string     `json:"source"`
	ObservedAt time.Time  `json:"observed_at"`
	Confidence float64    `json:"confidence"`
	Remaining  float64    `json:"remaining"`
	Unit       string     `json:"unit"`
	ResetAt    *time.Time `json:"reset_at,omitempty"`
	Basis      string     `json:"basis,omitempty"`
}
type Pool struct {
	ID, Provider             string
	Mode                     AccessMode
	AllowedModels            []string
	Available                bool
	ConcurrencyLimit, Active int
	Snapshot                 UsageSnapshot
	ContinuityReserve        float64
}
type Request struct {
	RequiredModel  string
	EstimatedUsage float64
}
type Selection struct {
	PoolID, Provider, Model string
	Snapshot                UsageSnapshot
}
type Manager struct{ Pools []Pool }

func (m Manager) Select(r Request) (Selection, error) {
	pools := append([]Pool(nil), m.Pools...)
	sort.Slice(pools, func(i, j int) bool { return pools[i].ID < pools[j].ID })
	for _, p := range pools {
		if !p.Available || (p.ConcurrencyLimit > 0 && p.Active >= p.ConcurrencyLimit) || p.Snapshot.Remaining-r.EstimatedUsage < p.ContinuityReserve {
			continue
		}
		for _, model := range p.AllowedModels {
			if r.RequiredModel == "" || r.RequiredModel == model {
				return Selection{p.ID, p.Provider, model, p.Snapshot}, nil
			}
		}
	}
	return Selection{}, fmt.Errorf("no feasible inference pool preserves configured reserve")
}

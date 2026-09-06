package inference

import (
	"slices"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/modelinput"
)

func TestBrokerDecisionRecordsDeterministicSelection(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		explicit    string
		preferences []string
		reason      modelinput.RouteReason
	}{
		{"local", []string{"cloud"}, modelinput.RouteExplicit},
		{"", []string{"local"}, modelinput.RoutePreferred},
		{"", nil, modelinput.RouteOrdered},
	} {
		broker, request, policy := brokerFixture(now)
		request.ConnectionID, request.PreferredConnections = tc.explicit, tc.preferences
		selected, err := broker.Select(now, request, policy)
		if err != nil {
			t.Fatal(err)
		}
		if selected.Decision.Reason != tc.reason || !selected.Decision.SelectedAt.Equal(now) || selected.ValidateFor(request) != nil {
			t.Fatalf("decision does not describe selected pool: %+v", selected)
		}
		slices.Reverse(broker.Routes)
		slices.Reverse(broker.Manager.Pools)
		permuted, err := broker.Select(now, request, policy)
		if err != nil || permuted.Decision != selected.Decision {
			t.Fatalf("permutation changed decision: %v", err)
		}
		changed := selected
		changed.Pool.ReservedCostNanoUSD++
		if changed.ValidateFor(request) == nil {
			t.Fatal("decision accepted altered reservation estimate")
		}
		changed = selected
		changed.Descriptor.Model = "other"
		if changed.ValidateFor(request) == nil {
			t.Fatal("decision accepted another model")
		}
	}
}

func TestBrokerBindsCatalogToActivePolicy(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	for _, mutate := range []func(*Policy){
		func(p *Policy) { p.Catalog = nil },
		func(p *Policy) { p.Catalog.DataClasses = []string{"public"} },
		func(p *Policy) { p.Catalog.ValidUntil = now.Add(time.Minute) },
		func(p *Policy) { p.Catalog.ContextTokens = 200 },
	} {
		b, request, policy := brokerFixture(now)
		request.ConnectionID = "cloud"
		mutate(&b.Manager.Pools[0].Policy)
		if _, err := b.Select(now, request, policy); err == nil {
			t.Fatal("stale catalog authorized selection")
		}
	}
}

func TestPersistedRoutingRulesOverridePermissiveCaller(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	for _, restrict := range []func(*RoutePolicy){
		func(p *RoutePolicy) { p.Locality = LocalOnly },
		func(p *RoutePolicy) { p.DeniedProviders = []string{"cloud"} },
		func(p *RoutePolicy) { p.AllowedProviders = []string{"local"} },
	} {
		b, request, callerPolicy := brokerFixture(now)
		for i := range b.Manager.Pools {
			restrict(b.Manager.Pools[i].Policy.Routing)
		}
		request.PreferredConnections = []string{"cloud"}
		got, err := b.Select(now, request, callerPolicy)
		if err != nil || got.ConnectionID != "local" {
			t.Fatalf("persisted policy selection=%+v err=%v", got, err)
		}
		cloud := b.Routes[0]
		if _, err := b.Manager.Select(now, PoolRequest{ConnectionID: cloud.ConnectionID, Descriptor: cloud.Descriptor}); err == nil {
			t.Fatal("direct admission bypassed persisted routing rule")
		}
	}
	b, _, _ := brokerFixture(now)
	policies := []Policy{b.Manager.Pools[0].Policy, b.Manager.Pools[1].Policy}
	if err := ValidatePolicySet(policies); err != nil {
		t.Fatal(err)
	}
	policies[1].Routing.Locality = LocalOnly
	if err := ValidatePolicySet(policies); err == nil {
		t.Fatal("conflicting organization routing rules admitted")
	}
}

func TestBrokerCostAndPreferencesAreDeterministic(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	b, request, policy := brokerFixture(now)
	request.PreferredConnections = []string{"absent", "local", "cloud"}
	for range 2 {
		got, err := b.Select(now, request, policy)
		if err != nil || got.ConnectionID != "local" {
			t.Fatalf("preference selection=%+v err=%v", got, err)
		}
		slices.Reverse(b.Routes)
		slices.Reverse(b.Manager.Pools)
	}
	request.PreferredConnections = []string{"cloud"}
	zero := int64(0)
	request.MaxCostNanoUSD = &zero
	got, err := b.Select(now, request, policy)
	if err != nil || got.ConnectionID != "local" {
		t.Fatalf("cost constraint selection=%+v err=%v", got, err)
	}
	request.ConnectionID = "cloud"
	if _, err := b.Select(now, request, policy); err == nil {
		t.Fatal("explicit account bypassed cost bound")
	}
	limit := int64(2)
	request.MaxCostNanoUSD = &limit
	if got, err := b.Select(now, request, policy); err != nil || got.ConnectionID != "cloud" {
		t.Fatalf("exact cost boundary=%+v err=%v", got, err)
	}
}

func brokerFixture(now time.Time) (Broker, RouteRequirements, RoutePolicy) {
	b := Broker{}
	for _, id := range []string{"cloud", "local"} {
		mode := MeteredAPI
		if id == "local" {
			mode = Local
		}
		p := selectablePolicy(now, mode, id)
		p.Version, p.ConnectionID = ConnectionPolicyVersion, id
		p.OrganizationBudget = &OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, MaxCostNanoUSDPerWindow: 1000, MaxConcurrentRequests: 2}
		p.Routing = &RoutePolicy{OrganizationID: p.OrganizationID, Locality: CloudAllowed, DataClasses: []string{"internal"}}
		p.Catalog = &CatalogDefinition{Capabilities: []Capability{Text}, Local: id == "local", ContextTokens: 120, OutputTokens: 20, DataClasses: []string{"internal"}, ValidUntil: now.Add(time.Hour)}
		descriptor := execution.ModelDescriptor{Provider: p.Provider, Model: p.Model, ExecutionProfileVersion: p.ExecutionProfileVersion}
		b.Routes = append(b.Routes, RouteMetadata{ConnectionID: id, Descriptor: descriptor, Capabilities: []Capability{Text}, Local: id == "local", ContextTokens: 120, OutputTokens: 20, DataClasses: []string{"internal"}, ValidUntil: now.Add(time.Hour)})
		b.Manager.Pools = append(b.Manager.Pools, Pool{ID: id, Policy: p, Available: true})
	}
	return b, RouteRequirements{OrganizationID: "organization-1", Capabilities: []Capability{Text}, InputTokens: 100, OutputTokens: 20, Locality: CloudAllowed, DataClass: "internal"}, RoutePolicy{OrganizationID: "organization-1", Locality: CloudAllowed, DataClasses: []string{"internal"}}
}

func TestBrokerConstraintsBeforePreferences(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	b, request, policy := brokerFixture(now)
	request.PreferredConnections = []string{"cloud"}
	policy.Locality = LocalOnly
	selected, err := b.Select(now, request, policy)
	if err != nil || selected.ConnectionID != "local" {
		t.Fatalf("locality selection=%+v err=%v", selected, err)
	}
	request.ConnectionID = "cloud"
	if _, err := b.Select(now, request, policy); err == nil {
		t.Fatal("explicit account bypassed local policy")
	}
	request.ConnectionID = ""
	b.Manager.Pools[1].Available = false
	if _, err := b.Select(now, request, policy); err == nil {
		t.Fatal("unavailable local route fell back to cloud")
	}
}

func TestBrokerFailsClosed(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		mutate func(*Broker, *RouteRequirements, *RoutePolicy)
	}{
		{"unsupported capability", func(_ *Broker, r *RouteRequirements, _ *RoutePolicy) { r.Capabilities = []Capability{Text, Vision} }},
		{"unknown capability", func(_ *Broker, r *RouteRequirements, _ *RoutePolicy) { r.Capabilities = []Capability{"imaginary"} }},
		{"context overflow", func(_ *Broker, r *RouteRequirements, _ *RoutePolicy) { r.InputTokens++ }},
		{"output overflow", func(_ *Broker, r *RouteRequirements, _ *RoutePolicy) { r.OutputTokens++ }},
		{"data classification", func(_ *Broker, r *RouteRequirements, _ *RoutePolicy) { r.DataClass = "secret" }},
		{"tenant substitution", func(_ *Broker, r *RouteRequirements, _ *RoutePolicy) { r.OrganizationID = "other" }},
		{"provider denial", func(_ *Broker, _ *RouteRequirements, p *RoutePolicy) { p.DeniedProviders = []string{"cloud", "local"} }},
		{"no allowed provider", func(_ *Broker, r *RouteRequirements, _ *RoutePolicy) { r.AllowedProviders = []string{"absent"} }},
		{"expired metadata", func(b *Broker, _ *RouteRequirements, _ *RoutePolicy) {
			for i := range b.Routes {
				b.Routes[i].ValidUntil = now
			}
		}},
		{"unknown context", func(b *Broker, _ *RouteRequirements, _ *RoutePolicy) {
			for i := range b.Routes {
				b.Routes[i].ContextTokens = 0
			}
		}},
		{"foreign budget", func(b *Broker, _ *RouteRequirements, _ *RoutePolicy) {
			for i := range b.Manager.Pools {
				b.Manager.Pools[i].Policy.OrganizationID = "other"
			}
		}},
		{"exhausted budget", func(b *Broker, _ *RouteRequirements, _ *RoutePolicy) {
			for i := range b.Manager.Pools {
				b.Manager.Pools[i].ChargedTokens = 500
			}
		}},
		{"false local metadata", func(b *Broker, r *RouteRequirements, _ *RoutePolicy) {
			b.Routes[0].Local = true
			r.ConnectionID = "cloud"
			r.Locality = LocalOnly
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, r, p := brokerFixture(now)
			tc.mutate(&b, &r, &p)
			if _, err := b.Select(now, r, p); err == nil {
				t.Fatal("ineligible route accepted")
			}
		})
	}
}

package inference

import (
	"context"
	"errors"
	"testing"
	"time"
)

type catalogSelectionStore struct {
	guardStore
	called bool
	err    error
}

func (s *catalogSelectionStore) SelectInferenceRoute(_ context.Context, _ *ConnectionRegistry, request RouteRequirements) (RouteSelection, error) {
	s.called = true
	request.Capabilities[0] = Vision
	request.PreferredConnections[0] = "changed"
	*request.MaxCostNanoUSD = 99
	return RouteSelection{}, s.err
}

func TestRegistrySelectionRequiresAuthorityAndIsolatesRequirements(t *testing.T) {
	model := &guardModel{}
	connections := []Connection{{ID: "account", Adapter: model}}
	legacy, err := NewConnectionRegistry(&guardStore{}, connections)
	if err != nil {
		t.Fatal(err)
	}
	cost := int64(10)
	request := RouteRequirements{OrganizationID: "organization-1", Capabilities: []Capability{Text}, InputTokens: 10, OutputTokens: 5, Locality: CloudAllowed, DataClass: "internal", PreferredConnections: []string{"account"}, MaxCostNanoUSD: &cost}
	if _, err := legacy.Select(t.Context(), request); err == nil {
		t.Fatal("selection used a store without authoritative routing")
	}
	if _, err := legacy.Adapter("account"); err != nil {
		t.Fatal("legacy explicit adapter became unavailable", err)
	}
	denied := errors.New("authoritative selection denied")
	store := &catalogSelectionStore{err: denied}
	registry, err := NewConnectionRegistry(store, connections)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := registry.Select(ctx, request); !errors.Is(err, context.Canceled) || store.called {
		t.Fatal("canceled selection reached store", err)
	}
	invalid := request.Clone()
	invalid.Locality = "unknown"
	if _, err := registry.Select(t.Context(), invalid); err == nil || store.called {
		t.Fatal("invalid constraints reached store")
	}
	if _, err := registry.Select(t.Context(), request); !errors.Is(err, denied) || !store.called {
		t.Fatal("selection did not preserve authoritative failure", err)
	}
	if request.Capabilities[0] != Text || request.PreferredConnections[0] != "account" || cost != 10 {
		t.Fatal("selector mutated caller constraints")
	}
	if model.called {
		t.Fatal("selection invoked provider")
	}
}

func TestConnectionCatalogIsBoundAndImmutable(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	model := &guardModel{}
	metadata := RouteMetadata{ConnectionID: "account", Descriptor: model.Descriptor(), Capabilities: []Capability{Text}, ContextTokens: 120, OutputTokens: 20, DataClasses: []string{"internal"}, ValidUntil: now.Add(time.Hour)}
	registry, err := NewConnectionRegistry(&guardStore{}, []Connection{{ID: "account", Adapter: model, Metadata: &metadata}, {ID: "legacy", Adapter: model}})
	if err != nil {
		t.Fatal(err)
	}
	metadata.Capabilities[0] = Vision
	metadata.DataClasses[0] = "secret"
	catalog := registry.Catalog()
	if len(catalog) != 1 || catalog[0].ConnectionID != "account" || catalog[0].Capabilities[0] != Text || catalog[0].DataClasses[0] != "internal" {
		t.Fatal("caller changed catalog through constructor input")
	}
	catalog[0].Capabilities[0], catalog[0].DataClasses[0] = ToolCalling, "secret"
	if got := registry.Catalog()[0]; got.Capabilities[0] != Text || got.DataClasses[0] != "internal" {
		t.Fatal("caller changed catalog through returned snapshot")
	}
	policy := selectablePolicy(now, MeteredAPI, model.Descriptor().Provider)
	policy.Model, policy.ExecutionProfileVersion = model.Descriptor().Model, model.Descriptor().ExecutionProfileVersion
	policy.Version, policy.ConnectionID = ConnectionPolicyVersion, "account"
	policy.OrganizationBudget = &OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, MaxCostNanoUSDPerWindow: 1000, MaxConcurrentRequests: 2}
	policy.Routing = &RoutePolicy{OrganizationID: policy.OrganizationID, Locality: CloudAllowed, DataClasses: []string{"internal"}}
	policy.Catalog = &CatalogDefinition{Capabilities: []Capability{Text}, ContextTokens: 120, OutputTokens: 20, DataClasses: []string{"internal"}, ValidUntil: now.Add(time.Hour)}
	pools := []Pool{{ID: "account", Policy: policy, Available: true}}
	request := RouteRequirements{OrganizationID: policy.OrganizationID, Capabilities: []Capability{Text}, InputTokens: 100, OutputTokens: 20, Locality: CloudAllowed, DataClass: "internal"}
	routePolicy := RoutePolicy{OrganizationID: policy.OrganizationID, Locality: CloudAllowed, DataClasses: []string{"internal"}}
	if got, err := registry.SelectRoute(now, pools, request, routePolicy); err != nil || got.ConnectionID != "account" {
		t.Fatalf("catalog selection=%+v err=%v", got, err)
	}
	request.Capabilities = []Capability{Vision}
	if _, err := registry.SelectRoute(now, pools, request, routePolicy); err == nil {
		t.Fatal("mutated catalog granted vision")
	}
	if model.called {
		t.Fatal("catalog selection contacted provider")
	}
}

func TestConnectionCatalogRejectsIdentityAndTransportClaims(t *testing.T) {
	model := &guardModel{}
	for _, change := range []func(*RouteMetadata){
		func(m *RouteMetadata) { m.ConnectionID = "other" },
		func(m *RouteMetadata) { m.Descriptor.Model = "other" },
		func(m *RouteMetadata) { m.Capabilities = []Capability{Vision} },
		func(m *RouteMetadata) { m.ValidUntil = time.Time{} },
	} {
		metadata := RouteMetadata{ConnectionID: "account", Descriptor: model.Descriptor(), Capabilities: []Capability{Text}, ValidUntil: time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)}
		change(&metadata)
		if _, err := NewConnectionRegistry(&guardStore{}, []Connection{{ID: "account", Adapter: model, Metadata: &metadata}}); err == nil {
			t.Fatal("invalid catalog admitted")
		}
	}
}

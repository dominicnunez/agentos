package inference

import (
	"testing"
	"time"
)

func TestRoutingScopeOwnsItsDecision(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	broker, request, policy := brokerFixture(now)
	selection, err := broker.Select(now, request, policy)
	if err != nil {
		t.Fatal(err)
	}
	selection.Decision.SnapshotSequence = 1
	scope := Scope{OrganizationID: request.OrganizationID, Purpose: PurposePlanning, RequestID: "execution", IntentID: "intent", ExecutionID: "execution", CorrelationID: "work", Routing: &request, RoutingDecision: &selection.Decision}
	ctx, err := WithScope(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	selection.Decision.PolicyFingerprint = "changed"
	got, err := scopeFromContext(ctx)
	if err != nil || got.RoutingDecision.PolicyFingerprint == "changed" {
		t.Fatal("caller mutated decision through input", err)
	}
	got.RoutingDecision.SnapshotSequence = 999
	again, err := scopeFromContext(ctx)
	if err != nil || again.RoutingDecision.SnapshotSequence != 1 {
		t.Fatal("returned scope mutated stored decision", err)
	}
	scope.Routing = nil
	if _, err := WithScope(t.Context(), scope); err == nil {
		t.Fatal("decision without requirements accepted")
	}
}

func TestRoutingScopeOwnsItsConstraints(t *testing.T) {
	cost := int64(100)
	routing := RouteRequirements{OrganizationID: "org", Capabilities: []Capability{Text}, InputTokens: 100, OutputTokens: 20, Locality: LocalOnly, DataClass: "internal", MaxCostNanoUSD: &cost}
	scope := Scope{OrganizationID: "org", Purpose: PurposePlanning, RequestID: "execution", IntentID: "intent", ExecutionID: "execution", CorrelationID: "work", Routing: &routing}
	ctx, err := WithScope(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	routing.Locality, routing.Capabilities[0], cost = CloudAllowed, Vision, 1000
	got, err := scopeFromContext(ctx)
	if err != nil || got.Routing.Locality != LocalOnly || got.Routing.Capabilities[0] != Text || *got.Routing.MaxCostNanoUSD != 100 {
		t.Fatalf("context constraints mutated: %+v %v", got.Routing, err)
	}
	got.Routing.DataClass, got.Routing.Capabilities[0] = "secret", Vision
	again, err := scopeFromContext(ctx)
	if err != nil || again.Routing.DataClass != "internal" || again.Routing.Capabilities[0] != Text {
		t.Fatal("returned scope changed future context")
	}
	scope.OrganizationID = "other"
	if _, err := WithScope(t.Context(), scope); err == nil {
		t.Fatal("cross-tenant constraints accepted")
	}
}

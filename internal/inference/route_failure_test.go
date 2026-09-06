package inference

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRoutingFailureCategoriesDoNotExposeCauses(t *testing.T) {
	private := errors.New("database path or credential synthetic-private-canary")
	for _, cause := range []error{private, context.Canceled, context.DeadlineExceeded, ErrOrganizationBudgetExhausted} {
		store := &catalogSelectionStore{err: cause}
		registry, err := NewConnectionRegistry(store, []Connection{{ID: "account", Adapter: &guardModel{}}})
		if err != nil {
			t.Fatal(err)
		}
		cost := int64(1)
		request := RouteRequirements{OrganizationID: "org", Capabilities: []Capability{Text}, InputTokens: 1, OutputTokens: 1, Locality: CloudAllowed, DataClass: "internal", PreferredConnections: []string{"account"}, MaxCostNanoUSD: &cost}
		_, err = registry.Select(t.Context(), request)
		if !errors.Is(err, cause) || strings.Contains(err.Error(), "synthetic-private-canary") {
			t.Fatal("routing failure lost identity or leaked private cause", err)
		}
		if got, want := RouteFailureCategory(err), RouteFailureCategory(cause); got != want {
			t.Fatalf("code=%s want=%s", got, want)
		}
	}
}

func TestBrokerReportsBoundedFailureCategories(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		code   RouteFailureCode
		mutate func(*Broker, *RouteRequirements)
	}{
		{RouteInvalidRequirements, func(_ *Broker, r *RouteRequirements) { r.InputTokens = 0 }},
		{RouteInvalidCatalog, func(b *Broker, _ *RouteRequirements) { b.Routes[0].ConnectionID = "invalid connection" }},
		{RoutePolicyDenied, func(_ *Broker, r *RouteRequirements) { r.DataClass = "unapproved" }},
		{RouteNoEligibleAccount, func(_ *Broker, r *RouteRequirements) { r.Capabilities = []Capability{Vision} }},
	} {
		broker, request, policy := brokerFixture(now)
		tc.mutate(&broker, &request)
		_, err := broker.Select(now, request, policy)
		if RouteFailureCategory(err) != tc.code {
			t.Fatalf("code=%s want=%s err=%v", RouteFailureCategory(err), tc.code, err)
		}
	}
	if RouteFailureCategory(nil) != "" {
		t.Fatal("success classified as failure")
	}
}

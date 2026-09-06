package modelinput

import (
	"math"
	"strconv"
	"strings"
	"testing"
)

func TestTaskRoutingIntersectionPreservesSecurityFloor(t *testing.T) {
	cost := int64(10)
	base := RouteRequirements{OrganizationID: "org", ConnectionID: "local", Capabilities: []Capability{Text, ToolCalling}, InputTokens: 100, OutputTokens: 20, Locality: LocalOnly, DataClass: "internal", AllowedProviders: []string{"safe", "denied"}, DeniedProviders: []string{"denied"}, MaxCostNanoUSD: &cost}
	rule := RouteRequirements{OrganizationID: "org", Capabilities: []Capability{Text}, InputTokens: 1, OutputTokens: 1, Locality: CloudAllowed, DataClass: "internal", PreferredConnections: []string{"cloud"}}
	got, err := IntersectRouteRequirements(base, rule)
	if err != nil {
		t.Fatal(err)
	}
	if got.Locality != LocalOnly || got.ConnectionID != "local" || got.InputTokens != 100 || got.OutputTokens != 20 || got.MaxCostNanoUSD == nil || *got.MaxCostNanoUSD != 10 || len(got.Capabilities) != 2 || len(got.AllowedProviders) != 1 || got.AllowedProviders[0] != "safe" || len(got.DeniedProviders) != 1 {
		t.Fatalf("task key weakened default: %+v", got)
	}
	for name, mutate := range map[string]func(*RouteRequirements){
		"classification":     func(r *RouteRequirements) { r.DataClass = "public" },
		"organization":       func(r *RouteRequirements) { r.OrganizationID = "other" },
		"account":            func(r *RouteRequirements) { r.ConnectionID = "cloud" },
		"disjoint providers": func(r *RouteRequirements) { r.AllowedProviders = []string{"cloud"} },
		"denied only":        func(r *RouteRequirements) { r.AllowedProviders = []string{"denied"} },
	} {
		changed := rule.Clone()
		mutate(&changed)
		if _, err := IntersectRouteRequirements(base, changed); err == nil {
			t.Fatal("conflicting rule accepted", name)
		}
	}
	got.AllowedProviders[0] = "changed"
	*got.MaxCostNanoUSD = 1000
	if base.AllowedProviders[0] != "safe" || cost != 10 {
		t.Fatal("intersection aliases default")
	}
}

func TestRoutingRequirementsFingerprintAndClone(t *testing.T) {
	cost := int64(100)
	r := RouteRequirements{OrganizationID: "org", Capabilities: []Capability{Text}, InputTokens: 100, OutputTokens: 20, Locality: LocalOnly, DataClass: "internal", MaxCostNanoUSD: &cost}
	fingerprint, err := r.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	clone := r.Clone()
	r.Capabilities, r.Locality, cost = []Capability{Text, Vision}, CloudAllowed, 1000
	if got, err := clone.Fingerprint(); err != nil || got != fingerprint {
		t.Fatal("clone changed through original constraints")
	}
	if got, err := r.Fingerprint(); err != nil || got == fingerprint {
		t.Fatal("changed constraints retained fingerprint")
	}
}

func TestRoutingRequirementsRejectInvalidAndOversizedConstraints(t *testing.T) {
	for _, mutate := range []func(*RouteRequirements){
		func(r *RouteRequirements) { r.Capabilities = nil },
		func(r *RouteRequirements) { r.Capabilities = []Capability{} },
		func(r *RouteRequirements) { r.Capabilities = []Capability{Vision} },
		func(r *RouteRequirements) { r.Capabilities = []Capability{"unknown"} },
		func(r *RouteRequirements) { r.Capabilities = []Capability{Text, Text} },
		func(r *RouteRequirements) { r.InputTokens = math.MaxInt64 },
		func(r *RouteRequirements) { r.Locality = "unknown" },
		func(r *RouteRequirements) { r.ConnectionID = "https://account" },
		func(r *RouteRequirements) { r.PreferredConnections = []string{"bad id"} },
		func(r *RouteRequirements) { r.DataClass = "internal\nsecret" },
		func(r *RouteRequirements) {
			for i := range 1024 {
				r.AllowedProviders = append(r.AllowedProviders, strings.Repeat("p", 480)+strconv.Itoa(i))
			}
		},
	} {
		r := RouteRequirements{OrganizationID: "org", Capabilities: []Capability{Text}, InputTokens: 100, OutputTokens: 20, Locality: LocalOnly, DataClass: "internal"}
		mutate(&r)
		if _, err := r.Canonical(); err == nil {
			t.Fatal("invalid route requirements accepted")
		}
	}
}

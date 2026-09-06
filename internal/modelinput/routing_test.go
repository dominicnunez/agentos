package modelinput

import (
	"math"
	"strconv"
	"strings"
	"testing"
)

func TestRoutingRequirementsFingerprintAndClone(t *testing.T) {
	cost := int64(100)
	r := RouteRequirements{OrganizationID: "org", Capabilities: []Capability{Text}, InputTokens: 100, OutputTokens: 20, Locality: LocalOnly, DataClass: "internal", MaxCostNanoUSD: &cost}
	fingerprint, err := r.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	clone := r.Clone()
	r.Capabilities[0], r.Locality, cost = Vision, CloudAllowed, 1000
	if got, err := clone.Fingerprint(); err != nil || got != fingerprint {
		t.Fatal("clone changed through original constraints")
	}
	if got, err := r.Fingerprint(); err != nil || got == fingerprint {
		t.Fatal("changed constraints retained fingerprint")
	}
}

func TestRoutingRequirementsRejectInvalidAndOversizedConstraints(t *testing.T) {
	for _, mutate := range []func(*RouteRequirements){
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

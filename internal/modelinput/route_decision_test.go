package modelinput

import (
	"encoding/json"
	"math"
	"testing"
	"time"
)

func TestRouteDecisionBindsRequirementsAndOrdering(t *testing.T) {
	requirements := RouteRequirements{OrganizationID: "org", Capabilities: []Capability{Text}, InputTokens: 10, OutputTokens: 5, Locality: LocalOnly, DataClass: "internal", PreferredConnections: []string{"account"}}
	fingerprint, err := requirements.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	decision := RouteDecision{Version: 1, RequirementsFingerprint: fingerprint, PolicyFingerprint: TextDigest("policy"), SelectedAt: time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC), ConnectionID: "account", Provider: "provider", Model: "model", ExecutionProfileVersion: "v1", Local: true, Reason: RoutePreferred, ReservedInputTokens: 10, ReservedOutputTokens: 5}
	if err := decision.ValidateFor(requirements); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(decision)
	if err != nil {
		t.Fatal(err)
	}
	var restored RouteDecision
	if err := json.Unmarshal(body, &restored); err != nil || restored != decision {
		t.Fatalf("decision roundtrip changed: %v", err)
	}
	for _, mutate := range []func(*RouteDecision){
		func(d *RouteDecision) { d.Version = 0 },
		func(d *RouteDecision) { d.RequirementsFingerprint = TextDigest("other") },
		func(d *RouteDecision) { d.PolicyFingerprint = "unbound" },
		func(d *RouteDecision) { d.SelectedAt = time.Time{} },
		func(d *RouteDecision) { d.ConnectionID = "bad account" },
		func(d *RouteDecision) { d.Provider = "provider\nsecret" },
		func(d *RouteDecision) { d.Local = false },
		func(d *RouteDecision) { d.Reason = RouteExplicit },
		func(d *RouteDecision) { d.ReservedInputTokens = 9 },
		func(d *RouteDecision) { d.ReservedOutputTokens = 4 },
		func(d *RouteDecision) { d.ReservedInputTokens = math.MaxInt64 },
		func(d *RouteDecision) { d.ReservedCostNanoUSD = -1 },
		func(d *RouteDecision) { d.SharedBudgetRejections = 1025 },
	} {
		changed := decision
		mutate(&changed)
		if err := changed.ValidateFor(requirements); err == nil {
			t.Fatalf("invalid decision accepted: %+v", changed)
		}
	}
	requirements.DataClass = "public"
	if err := decision.ValidateFor(requirements); err == nil {
		t.Fatal("decision accepted another classification")
	}
	for _, tc := range []struct {
		connection  string
		preferences []string
		reason      RouteReason
	}{
		{"account", []string{"other"}, RouteExplicit},
		{"", nil, RouteOrdered},
	} {
		requirements.ConnectionID, requirements.PreferredConnections = tc.connection, tc.preferences
		decision.RequirementsFingerprint, err = requirements.Fingerprint()
		if err != nil {
			t.Fatal(err)
		}
		decision.Reason = tc.reason
		if err := decision.ValidateFor(requirements); err != nil {
			t.Fatal(err)
		}
	}
}

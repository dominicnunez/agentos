package main

import (
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/bootstrap"
)

func TestParseNanoUSDIsExactAndRejectsAmbiguity(t *testing.T) {
	for input, want := range map[string]int64{
		"0.000000001": 1,
		"0.25":        250_000_000,
		"2.50":        2_500_000_000,
		"100":         100_000_000_000,
	} {
		got, err := parseNanoUSD(input)
		if err != nil || got != want {
			t.Fatalf("parseNanoUSD(%q)=%d,%v want %d", input, got, err, want)
		}
	}
	for _, input := range []string{"", "0", "-1", ".5", "1.", " 1", "1e2", "1.0000000001"} {
		if _, err := parseNanoUSD(input); err == nil {
			t.Fatalf("ambiguous USD amount %q was accepted", input)
		}
	}
}

func TestDiscoveredOpenAIModelsRequireDatedSnapshots(t *testing.T) {
	if !validDiscoveredOpenAIModel("gpt-test-2026-01-01") {
		t.Fatal("dated snapshot was rejected")
	}
	for _, model := range []string{"gpt-latest", "gpt-test-2026-01-01\nother", " gpt-test-2026-01-01"} {
		if validDiscoveredOpenAIModel(model) {
			t.Fatalf("unsafe model %q was accepted", model)
		}
	}
}

func TestDoctorInferencePolicyReportsExpiredAuthorizationAndPricing(t *testing.T) {
	now := time.Now().UTC()
	config := bootstrap.NewConfig(bootstrap.ModeUser, bootstrap.Owner{Username: "test", UID: 1000, GID: 1000}, bootstrap.Paths{}, now)
	config.Providers = []bootstrap.Provider{testOpenAIProvider(config, "gpt-test-2026-01-01", "key")}
	if err := doctorInferencePolicy(config, now); err != nil {
		t.Fatal(err)
	}
	config.Providers[0].InferencePolicy.AuthorizationExpiresAt = now
	if err := doctorInferencePolicy(config, now); err == nil {
		t.Fatal("expired authorization was reported healthy")
	}
	config.Providers[0] = testOpenAIProvider(config, "gpt-test-2026-01-01", "key")
	config.Providers[0].InferencePolicy.Pricing.ExpiresAt = now
	if err := doctorInferencePolicy(config, now); err == nil {
		t.Fatal("stale pricing was reported healthy")
	}
}

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/bootstrap"
	"github.com/dominicnunez/agentos/internal/inference"
)

func TestApplyReviewedProvidersValidatesWholeSetBeforeServiceMutation(t *testing.T) {
	paths, err := bootstrap.UserPaths(t.TempDir(), t.TempDir(), 1000)
	if err != nil {
		t.Fatal(err)
	}
	config := bootstrap.NewConfig(bootstrap.ModeUser, bootstrap.Owner{Username: "owner", UID: 1000, GID: 1000}, paths, time.Now().UTC())
	first := testOpenAIProvider(config, "gpt-test-2026-01-01", "first")
	first.InferencePolicy.Version = inference.ConnectionPolicyVersion
	first.InferencePolicy.ConnectionID = "first"
	first.InferencePolicy.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000000, MaxCostNanoUSDPerWindow: first.InferencePolicy.Pricing.MaxCostNanoUSDPerWindow, MaxConcurrentRequests: 2}
	second := first
	second.SecretRef, second.InferencePolicy.ConnectionID = "second", "second"
	config.Providers = []bootstrap.Provider{first, second}
	config.Routing = &bootstrap.ProviderRouting{TaskDefault: "first", Planning: "second", Normalization: "second"}
	directory := filepath.Join(paths.ConfigDir, "credentials")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"first", "second"} {
		if err := os.WriteFile(filepath.Join(directory, name+".cred"), []byte("synthetic encrypted credential"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	state := bootstrap.State{Version: bootstrap.ConfigVersion, Mode: bootstrap.ModeUser, Stage: bootstrap.StageReady}
	applied := 0
	apply := func(_ context.Context, got bootstrap.Config) error {
		applied++
		if len(got.Providers) != 2 || got.Routing.Planning != "second" {
			t.Fatal("provider set was truncated")
		}
		return nil
	}
	if err := applyReviewedProviderConfig(t.Context(), config, state, 1000, apply); err != nil || applied != 1 {
		t.Fatalf("valid configuration not applied: %v", err)
	}
	if err := applyReviewedProviderConfig(t.Context(), config, state, 1001, apply); err == nil || applied != 1 {
		t.Fatal("wrong user changed service")
	}
	if err := os.Remove(filepath.Join(directory, "second.cred")); err != nil {
		t.Fatal(err)
	}
	if err := applyReviewedProviderConfig(t.Context(), config, state, 1000, apply); err == nil || applied != 1 {
		t.Fatal("missing second credential changed service")
	}
	state.Stage = bootstrap.StageProvider
	if err := applyReviewedProviderConfig(t.Context(), config, state, 1000, apply); err == nil || applied != 1 {
		t.Fatal("incomplete setup changed service")
	}
}

package bootstrap

import (
	"github.com/dominicnunez/agentos/internal/inference"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRoutedConfigRoundTripAndInvalidRoutes(t *testing.T) {
	paths, err := UserPaths(t.TempDir(), t.TempDir(), 1000)
	if err != nil {
		t.Fatal(err)
	}
	config := NewConfig(ModeUser, Owner{Username: "owner", UID: 1000, GID: 1000}, paths, time.Now().UTC())
	first := testOpenAIProvider(config, "gpt-test-2026-01-01")
	first.InferencePolicy.Version, first.InferencePolicy.ConnectionID = inference.ConnectionPolicyVersion, "first"
	first.InferencePolicy.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 100000, MaxCostNanoUSDPerWindow: 100, MaxConcurrentRequests: 2}
	second := first
	second.SecretRef, second.InferencePolicy.ConnectionID = "second-key", "second"
	config.Providers = []Provider{first, second}
	config.Routing = &ProviderRouting{TaskDefault: "first", Planning: "second", Normalization: "second", TaskConnections: map[string]string{"research": "second"}}
	if err := config.ValidateReady(); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(paths.ConfigDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := SaveConfig(ConfigPath(paths), config); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadConfig(ConfigPath(paths))
	if err != nil || !reflect.DeepEqual(config, loaded) {
		t.Fatalf("routing roundtrip changed: %v", err)
	}
	for _, key := range []string{"", "Research", "with space", "slash/key", strings.Repeat("a", 65)} {
		changed := *config.Routing
		changed.TaskConnections = map[string]string{key: "second"}
		if err := changed.Validate(config.Providers); err == nil {
			t.Fatalf("invalid key %q accepted", key)
		}
	}
	config.Routing = nil
	if err := config.ValidateReady(); err == nil {
		t.Fatal("named accounts accepted implicit routing")
	}
}

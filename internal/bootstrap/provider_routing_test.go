package bootstrap

import (
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/modelinput"
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
	first.InferencePolicy.Routing = &inference.RoutePolicy{OrganizationID: first.InferencePolicy.OrganizationID, Locality: inference.CloudAllowed, DataClasses: []string{"internal"}}
	first.InferencePolicy.Catalog = &inference.CatalogDefinition{Capabilities: []inference.Capability{inference.Text}, ContextTokens: 100000, OutputTokens: 20000, DataClasses: []string{"internal"}, ValidUntil: first.InferencePolicy.AuthorizationExpiresAt}
	second := first
	second.SecretRef, second.InferencePolicy.ConnectionID = "second-key", "second"
	config.Providers = []Provider{first, second}
	config.Routing = &ProviderRouting{TaskDefault: "first", Planning: "second", Normalization: "second", TaskConnections: map[string]string{"research": "second"}}
	config.Routing.Requirements = &modelinput.RouteRequirements{OrganizationID: first.InferencePolicy.OrganizationID, Capabilities: []modelinput.Capability{modelinput.Text}, InputTokens: 1000, OutputTokens: 100, Locality: modelinput.CloudAllowed, DataClass: "internal"}
	research := config.Routing.Requirements.Clone()
	research.PreferredConnections = []string{"second"}
	config.Routing.TaskRequirements = map[string]modelinput.RouteRequirements{"research": research}
	config.Routing.PlanningRequirements = modelinput.CloneRouteRequirements(&research)
	config.Routing.NormalizationRequirements = modelinput.CloneRouteRequirements(config.Routing.Requirements)
	if err := config.ValidateReady(); err != nil {
		t.Fatal(err)
	}
	withoutCatalog := append([]Provider(nil), config.Providers...)
	withoutCatalog[1].InferencePolicy.Catalog = nil
	noCatalogs := append([]Provider(nil), withoutCatalog...)
	noCatalogs[0].InferencePolicy.Catalog = nil
	for _, purpose := range []string{"task", "planning", "normalization", "task-specific"} {
		changed := ProviderRouting{TaskDefault: "first", Planning: "first", Normalization: "first"}
		if err := changed.Validate(noCatalogs); err != nil {
			t.Fatal("legacy explicit routing rejected", err)
		}
		switch purpose {
		case "task":
			changed.Requirements = config.Routing.Requirements
		case "planning":
			changed.PlanningRequirements = config.Routing.PlanningRequirements
		case "normalization":
			changed.NormalizationRequirements = config.Routing.NormalizationRequirements
		case "task-specific":
			changed.TaskRequirements = map[string]modelinput.RouteRequirements{"research": research}
		}
		if err := changed.Validate(noCatalogs); err == nil {
			t.Fatalf("%s broker requirements accepted without any catalog", purpose)
		}
	}
	for _, purpose := range []string{"task", "planning", "normalization", "pin-default", "pin-specific"} {
		changed := *config.Routing
		changed.Requirements = modelinput.CloneRouteRequirements(config.Routing.Requirements)
		changed.PlanningRequirements = modelinput.CloneRouteRequirements(config.Routing.PlanningRequirements)
		changed.NormalizationRequirements = modelinput.CloneRouteRequirements(config.Routing.NormalizationRequirements)
		changed.TaskConnections, changed.TaskRequirements = nil, nil
		if err := changed.Validate(withoutCatalog); err != nil {
			t.Fatal("soft preference must permit an eligible catalog account", err)
		}
		switch purpose {
		case "task":
			changed.Requirements.ConnectionID = "second"
		case "planning":
			changed.PlanningRequirements.ConnectionID = "second"
		case "normalization":
			changed.NormalizationRequirements.ConnectionID = "second"
		case "pin-default":
			changed.TaskConnections = map[string]string{"research": "second"}
		case "pin-specific":
			changed.TaskConnections = map[string]string{"research": "second"}
			changed.TaskRequirements = map[string]modelinput.RouteRequirements{"research": research}
		}
		if err := changed.Validate(withoutCatalog); err == nil {
			t.Fatalf("%s hard route accepted account without catalog", purpose)
		}
		if err := changed.Validate(config.Providers); err != nil {
			t.Fatalf("%s catalog-enabled hard route rejected: %v", purpose, err)
		}
	}
	for _, mutate := range []func(*ProviderRouting){
		func(r *ProviderRouting) { r.Requirements = nil },
		func(r *ProviderRouting) { r.PlanningRequirements = nil },
		func(r *ProviderRouting) { r.NormalizationRequirements = nil },
		func(r *ProviderRouting) { r.PlanningRequirements.OrganizationID = "other" },
		func(r *ProviderRouting) { r.NormalizationRequirements.Locality = "unknown" },
	} {
		changed := *config.Routing
		changed.PlanningRequirements = modelinput.CloneRouteRequirements(config.Routing.PlanningRequirements)
		changed.NormalizationRequirements = modelinput.CloneRouteRequirements(config.Routing.NormalizationRequirements)
		mutate(&changed)
		if err := changed.Validate(config.Providers); err == nil {
			t.Fatal("missing or invalid purpose requirements passed readiness")
		}
	}
	for _, mutate := range []func(*inference.CatalogDefinition){
		func(c *inference.CatalogDefinition) { c.Local = true },
		func(c *inference.CatalogDefinition) { c.ValidUntil = c.ValidUntil.Add(time.Hour) },
		func(c *inference.CatalogDefinition) { c.Capabilities = []inference.Capability{inference.Vision} },
	} {
		changed := first
		catalog := *first.InferencePolicy.Catalog
		mutate(&catalog)
		changed.InferencePolicy.Catalog = &catalog
		if err := changed.Validate(); err == nil {
			t.Fatal("invalid catalog metadata passed readiness")
		}
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
		changed.TaskConnections = nil
		changed.TaskRequirements = map[string]modelinput.RouteRequirements{key: research}
		if err := changed.Validate(config.Providers); err == nil {
			t.Fatalf("invalid requirement key %q accepted", key)
		}
	}
	for _, mutate := range []func(*modelinput.RouteRequirements){
		func(r *modelinput.RouteRequirements) { r.OrganizationID = "other" },
		func(r *modelinput.RouteRequirements) { r.InputTokens = 0 },
		func(r *modelinput.RouteRequirements) { r.Locality = "unknown" },
		func(r *modelinput.RouteRequirements) { r.ConnectionID = "missing" },
		func(r *modelinput.RouteRequirements) { r.PreferredConnections = []string{"missing"} },
		func(r *modelinput.RouteRequirements) { r.ConnectionID = "first" },
	} {
		changed := *config.Routing
		requirements := research.Clone()
		mutate(&requirements)
		changed.TaskRequirements = map[string]modelinput.RouteRequirements{"research": requirements}
		if err := changed.Validate(config.Providers); err == nil {
			t.Fatal("invalid or conflicting task requirements passed readiness")
		}
	}
	config.Routing = nil
	if err := config.ValidateReady(); err == nil {
		t.Fatal("named accounts accepted implicit routing")
	}
}

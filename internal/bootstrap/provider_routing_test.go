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

func TestRoutingReadinessUsesCurrentCatalogValidity(t *testing.T) {
	now := time.Now().UTC()
	paths, err := UserPaths(t.TempDir(), t.TempDir(), 1000)
	if err != nil {
		t.Fatal(err)
	}
	config := NewConfig(ModeUser, Owner{Username: "owner", UID: 1000, GID: 1000}, paths, now)
	first := testOpenAIProvider(config, "gpt-test-2026-01-01")
	p := &first.InferencePolicy
	p.Version, p.ConnectionID = inference.ConnectionPolicyVersion, "first"
	p.AuthorizedAt = now.Add(-time.Hour)
	p.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 100000, MaxCostNanoUSDPerWindow: 100, MaxConcurrentRequests: 2}
	p.Routing = &inference.RoutePolicy{OrganizationID: p.OrganizationID, Locality: inference.CloudAllowed, DataClasses: []string{"internal"}}
	p.Catalog = &inference.CatalogDefinition{Capabilities: []inference.Capability{inference.Text}, ContextTokens: 1000, OutputTokens: 100, DataClasses: []string{"internal"}, ValidUntil: now.Add(-time.Minute)}
	second := first
	second.InferencePolicy.ConnectionID = "second"
	requirements := modelinput.RouteRequirements{OrganizationID: p.OrganizationID, Capabilities: []modelinput.Capability{modelinput.Text}, InputTokens: 100, OutputTokens: 100, Locality: modelinput.CloudAllowed, DataClass: "internal"}
	routing := ProviderRouting{TaskDefault: "first", Planning: "first", Normalization: "first", Requirements: &requirements, PlanningRequirements: &requirements, NormalizationRequirements: &requirements}
	config.Providers, config.Routing = []Provider{first, second}, &routing
	if err := routing.validateAt(config.Providers, now.Add(-2*time.Minute)); err != nil {
		t.Fatal("catalog should have been feasible before expiration", err)
	}
	if err := config.ValidateReady(); err == nil {
		t.Fatal("startup accepted only expired catalogs while authorization and pricing are valid")
	}
	for _, until := range []time.Time{now.Add(-time.Nanosecond), now, now.Add(time.Nanosecond)} {
		catalog := *p.Catalog
		catalog.ValidUntil = until
		config.Providers[1].InferencePolicy.Catalog = &catalog
		err := routing.validateAt(config.Providers, now)
		if (err == nil) != until.After(now) {
			t.Fatalf("catalog boundary %v: %v", until, err)
		}
	}
	valid := *p.Catalog
	valid.ValidUntil = now.Add(30 * time.Minute)
	config.Providers[1].InferencePolicy.Catalog = &valid
	if err := config.ValidateReady(); err != nil {
		t.Fatal("startup rejected a current alternative catalog", err)
	}
	for _, purpose := range []string{"task", "planning", "normalization", "specific", "pin"} {
		changed := routing
		hard := requirements.Clone()
		hard.ConnectionID = "first"
		switch purpose {
		case "task":
			changed.Requirements = &hard
		case "planning":
			changed.PlanningRequirements = &hard
		case "normalization":
			changed.NormalizationRequirements = &hard
		case "specific":
			changed.TaskRequirements = map[string]modelinput.RouteRequirements{"research": hard}
		case "pin":
			changed.TaskConnections = map[string]string{"research": "first"}
		}
		if err := changed.validateAt(config.Providers, now); err == nil {
			t.Fatalf("%s hard route accepted expired account", purpose)
		}
	}
}

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
	config.Routing.Requirements = &modelinput.RouteRequirements{OrganizationID: first.InferencePolicy.OrganizationID, Capabilities: []modelinput.Capability{modelinput.Text}, InputTokens: 100, OutputTokens: 100, Locality: modelinput.CloudAllowed, DataClass: "internal"}
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
	legacyProviders := append([]Provider(nil), noCatalogs...)
	for i := range legacyProviders {
		legacyProviders[i].InferencePolicy.Routing = nil
	}
	for _, purpose := range []string{"task", "planning", "normalization", "task-specific"} {
		changed := ProviderRouting{TaskDefault: "first", Planning: "first", Normalization: "first"}
		if err := changed.Validate(legacyProviders); err != nil {
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
	for _, purpose := range []string{"task", "planning", "normalization", "pin", "soft-task", "soft-planning", "soft-normalization", "soft-specific"} {
		for name, mutate := range map[string]func(*modelinput.RouteRequirements){
			"capability": func(r *modelinput.RouteRequirements) {
				r.Capabilities = []modelinput.Capability{modelinput.Text, modelinput.Vision}
			},
			"class":            func(r *modelinput.RouteRequirements) { r.DataClass = "secret" },
			"input":            func(r *modelinput.RouteRequirements) { r.InputTokens = 100001 },
			"output":           func(r *modelinput.RouteRequirements) { r.OutputTokens = 20001 },
			"denied-provider":  func(r *modelinput.RouteRequirements) { r.DeniedProviders = []string{second.InferencePolicy.Provider} },
			"allowed-provider": func(r *modelinput.RouteRequirements) { r.AllowedProviders = []string{"unconfigured-provider"} },
			"locality":         func(r *modelinput.RouteRequirements) { r.Locality = modelinput.LocalOnly },
		} {
			changed := *config.Routing
			changed.TaskConnections, changed.TaskRequirements = nil, nil
			constraints := config.Routing.Requirements.Clone()
			constraints.ConnectionID = "second"
			if strings.HasPrefix(purpose, "soft-") {
				constraints.ConnectionID = ""
			}
			switch purpose {
			case "task", "soft-task":
				changed.Requirements = &constraints
			case "planning", "soft-planning":
				changed.PlanningRequirements = &constraints
			case "normalization", "soft-normalization":
				changed.NormalizationRequirements = &constraints
			case "soft-specific":
				changed.TaskRequirements = map[string]modelinput.RouteRequirements{"research": constraints}
			case "pin":
				constraints.ConnectionID = ""
				changed.Requirements = &constraints
				changed.TaskConnections = map[string]string{"research": "second"}
			}
			if err := changed.Validate(config.Providers); err != nil {
				t.Fatalf("eligible %s hard route rejected: %v", purpose, err)
			}
			mutate(&constraints)
			if purpose == "soft-specific" {
				changed.TaskRequirements["research"] = constraints
			}
			if err := changed.Validate(config.Providers); err == nil {
				t.Fatalf("%s hard route accepted incompatible %s", purpose, name)
			}
		}
	}
	// A soft route may use the second account when the default cannot fit it.
	oneEligible := append([]Provider(nil), config.Providers...)
	small := *oneEligible[0].InferencePolicy.Catalog
	small.OutputTokens = 99
	oneEligible[0].InferencePolicy.Catalog = &small
	if err := config.Routing.Validate(oneEligible); err != nil {
		t.Fatal("soft routing rejected an eligible alternative account", err)
	}
	for name, mutate := range map[string]func(*inference.OrganizationBudget){
		"tokens":     func(b *inference.OrganizationBudget) { b.MaxTokensPerWindow = 199 },
		"continuity": func(b *inference.OrganizationBudget) { b.ContinuityReserveTokens = b.MaxTokensPerWindow - 199 },
		"cost":       func(b *inference.OrganizationBudget) { b.MaxCostNanoUSDPerWindow = 0 },
	} {
		providers := append([]Provider(nil), config.Providers...)
		budget := *providers[0].InferencePolicy.OrganizationBudget
		mutate(&budget)
		for i := range providers {
			providers[i].InferencePolicy.OrganizationBudget = &budget
		}
		if err := config.Routing.Validate(providers); err == nil {
			t.Fatalf("readiness accepted impossible shared %s budget", name)
		}
	}
	for _, purpose := range []string{"task", "planning", "normalization", "pin"} {
		changed := *config.Routing
		changed.TaskConnections, changed.TaskRequirements = nil, nil
		switch purpose {
		case "task":
			changed.TaskDefault = "second"
			changed.Requirements = nil
		case "planning":
			changed.Planning = "second"
			changed.PlanningRequirements = nil
		case "normalization":
			changed.Normalization = "second"
			changed.NormalizationRequirements = nil
		case "pin":
			changed.Requirements = nil
			changed.TaskDefault = "second"
			changed.TaskConnections = map[string]string{"research": "second"}
		}
		if err := changed.Validate(withoutCatalog); err == nil {
			t.Fatalf("%s explicit route bypassed governance", purpose)
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

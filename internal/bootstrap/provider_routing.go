package bootstrap

import (
	"fmt"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/modelinput"
	"regexp"
)

// ProviderRouting configures accounts and task requirements for inference purposes.
type ProviderRouting struct {
	TaskDefault     string            `json:"task_default"`
	TaskConnections map[string]string `json:"task_connections,omitempty"`
	Planning        string            `json:"planning"`
	Normalization   string            `json:"normalization"`
	// Requirements enable ledger-backed task selection. Task-specific entries
	// supply complete requirements; explicit TaskConnections remain hard limits.
	Requirements              *modelinput.RouteRequirements           `json:"task_requirements,omitempty"`
	TaskRequirements          map[string]modelinput.RouteRequirements `json:"task_requirements_by_key,omitempty"`
	PlanningRequirements      *modelinput.RouteRequirements           `json:"planning_requirements,omitempty"`
	NormalizationRequirements *modelinput.RouteRequirements           `json:"normalization_requirements,omitempty"`
}

func (r *ProviderRouting) Validate(providers []Provider) error {
	if r == nil {
		if len(providers) != 1 || providers[0].InferencePolicy.ConnectionID != "" {
			return fmt.Errorf("named or multiple providers require explicit provider routing")
		}
		return nil
	}
	policies := make([]inference.Policy, len(providers))
	ids := map[string]bool{}
	catalogs := map[string]bool{}
	stores := map[string]bool{}
	for i, provider := range providers {
		policies[i] = provider.InferencePolicy
		ids[provider.InferencePolicy.ConnectionID] = true
		catalogs[provider.InferencePolicy.ConnectionID] = provider.InferencePolicy.Catalog != nil
		if provider.CodexCredential != "" {
			if stores[provider.CodexCredential] {
				return fmt.Errorf("connections cannot share a mutable Codex credential store")
			}
			stores[provider.CodexCredential] = true
		}
	}
	if err := inference.ValidatePolicySet(policies); err != nil {
		return err
	}
	for _, id := range []string{r.TaskDefault, r.Planning, r.Normalization} {
		if !ids[id] {
			return fmt.Errorf("every inference purpose requires a configured connection")
		}
	}
	for _, purpose := range []struct {
		connection   string
		requirements *modelinput.RouteRequirements
	}{{r.TaskDefault, r.Requirements}, {r.Planning, r.PlanningRequirements}, {r.Normalization, r.NormalizationRequirements}} {
		if catalogs[purpose.connection] && purpose.requirements == nil {
			return fmt.Errorf("catalog-enabled inference purposes require explicit routing requirements")
		}
	}
	if len(r.TaskConnections) > 1024 {
		return fmt.Errorf("too many task connection rules")
	}
	for key, id := range r.TaskConnections {
		if !routingTaskKey.MatchString(key) || !ids[id] {
			return fmt.Errorf("task route requires a valid task key and configured connection")
		}
	}
	if len(r.TaskRequirements) > 1024 {
		return fmt.Errorf("too many task requirement rules")
	}
	validateRequirements := func(requirements modelinput.RouteRequirements) error {
		hasCatalog := false
		for _, present := range catalogs {
			hasCatalog = hasCatalog || present
		}
		if !hasCatalog {
			return fmt.Errorf("broker routing requires at least one catalog-enabled connection")
		}
		if _, err := requirements.Canonical(); err != nil {
			return err
		}
		if requirements.OrganizationID != policies[0].OrganizationID {
			return fmt.Errorf("task requirements must belong to the configured organization")
		}
		if requirements.ConnectionID != "" && !ids[requirements.ConnectionID] {
			return fmt.Errorf("task requirements name an unconfigured connection")
		}
		if requirements.ConnectionID != "" && !catalogs[requirements.ConnectionID] {
			return fmt.Errorf("hard routing requirements require a catalog-enabled connection")
		}
		if requirements.ConnectionID != "" {
			for _, policy := range policies {
				if policy.ConnectionID != requirements.ConnectionID {
					continue
				}
				metadata, err := policy.Catalog.Metadata(policy)
				if err != nil || policy.Routing == nil {
					return fmt.Errorf("hard route lacks reviewed catalog and routing policy")
				}
				// Check static feasibility at authorization time, without live
				// accounting, network calls, or a wall-clock-dependent config.
				broker := inference.Broker{Routes: []inference.RouteMetadata{metadata}, Manager: inference.Manager{Pools: []inference.Pool{{ID: policy.ConnectionID, Policy: policy, Available: true}}}}
				if _, err := broker.Select(policy.AuthorizedAt, requirements, *policy.Routing); err != nil {
					return fmt.Errorf("hard route cannot satisfy its configured requirements: %w", err)
				}
			}
		}
		for _, id := range requirements.PreferredConnections {
			if !ids[id] {
				return fmt.Errorf("task preference names an unconfigured connection")
			}
		}
		return nil
	}
	for _, requirements := range []*modelinput.RouteRequirements{r.Requirements, r.PlanningRequirements, r.NormalizationRequirements} {
		if requirements != nil {
			if err := validateRequirements(*requirements); err != nil {
				return err
			}
		}
	}
	for key, requirements := range r.TaskRequirements {
		if !routingTaskKey.MatchString(key) {
			return fmt.Errorf("task requirements require a valid task key")
		}
		if err := validateRequirements(requirements); err != nil {
			return err
		}
	}
	for key, connection := range r.TaskConnections {
		requirements := r.Requirements
		if specific, ok := r.TaskRequirements[key]; ok {
			requirements = &specific
		}
		if catalogs[connection] && requirements == nil {
			return fmt.Errorf("catalog-enabled task accounts require explicit routing requirements")
		}
		if requirements != nil && !catalogs[connection] {
			return fmt.Errorf("broker task pins require a catalog-enabled connection")
		}
		if requirements != nil && requirements.ConnectionID != "" && requirements.ConnectionID != connection {
			return fmt.Errorf("task connection conflicts with routing requirements")
		}
		if requirements != nil {
			pinned := requirements.Clone()
			pinned.ConnectionID = connection
			if err := validateRequirements(pinned); err != nil {
				return err
			}
		}
	}
	return nil
}

// Routing keys use the planner wire format without depending on orchestration.
var routingTaskKey = regexp.MustCompile("^[a-z0-9][a-z0-9-]{0,63}$")

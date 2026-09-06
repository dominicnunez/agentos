package bootstrap

import (
	"fmt"
	"github.com/dominicnunez/agentos/internal/inference"
	"regexp"
)

// ProviderRouting explicitly selects configured accounts for each inference purpose.
type ProviderRouting struct {
	TaskDefault     string            `json:"task_default"`
	TaskConnections map[string]string `json:"task_connections,omitempty"`
	Planning        string            `json:"planning"`
	Normalization   string            `json:"normalization"`
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
	stores := map[string]bool{}
	for i, provider := range providers {
		policies[i] = provider.InferencePolicy
		ids[provider.InferencePolicy.ConnectionID] = true
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
	if len(r.TaskConnections) > 1024 {
		return fmt.Errorf("too many task connection rules")
	}
	for key, id := range r.TaskConnections {
		if !routingTaskKey.MatchString(key) || !ids[id] {
			return fmt.Errorf("task route requires a valid task key and configured connection")
		}
	}
	return nil
}

// Routing keys use the planner wire format without depending on orchestration.
var routingTaskKey = regexp.MustCompile("^[a-z0-9][a-z0-9-]{0,63}$")

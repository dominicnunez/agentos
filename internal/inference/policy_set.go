package inference

import "fmt"

// ValidatePolicySet checks a complete reviewed connection set before startup or activation.
func ValidatePolicySet(policies []Policy) error {
	if len(policies) == 0 || len(policies) > 1024 {
		return fmt.Errorf("inference policy set must contain 1 to 1024 connections")
	}
	first := policies[0]
	seen := make(map[string]bool)
	for _, policy := range policies {
		if policy.Validate() != nil || policy.Version != ConnectionPolicyVersion || policy.OrganizationID != first.OrganizationID || policy.AuthorizedBy != first.AuthorizedBy || !policy.AuthorizedAt.Equal(first.AuthorizedAt) || seen[policy.ConnectionID] {
			return fmt.Errorf("inference policy set requires distinct connections with one organization and authorization")
		}
		if *policy.OrganizationBudget != *first.OrganizationBudget {
			return fmt.Errorf("inference policy set has conflicting organization budgets")
		}
		seen[policy.ConnectionID] = true
	}
	return nil
}

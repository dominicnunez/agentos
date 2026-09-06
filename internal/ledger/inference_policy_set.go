package ledger

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dominicnunez/agentos/internal/inference"
)

// ActivateInferencePolicies changes a reviewed set in one writer transaction.
// When shared limits change, all affected connections must participate. No
// reservation can observe intermediate policy revisions, and a failed member
// rolls back every activation event and row in the set.
func (l *SQLite) ActivateInferencePolicies(ctx context.Context, policies []inference.Policy) error {
	if len(policies) == 0 || len(policies) > 1024 {
		return fmt.Errorf("inference policy set must contain 1 to 1024 connections")
	}
	first := policies[0]
	seen := make(map[string]bool)
	for _, policy := range policies {
		if policy.Validate() != nil || policy.Version != inference.ConnectionPolicyVersion || policy.OrganizationID != first.OrganizationID || policy.AuthorizedBy != first.AuthorizedBy || !policy.AuthorizedAt.Equal(first.AuthorizedAt) || seen[policy.ConnectionID] {
			return fmt.Errorf("inference policy set requires distinct connections with one organization and authorization")
		}
		if *policy.OrganizationBudget != *first.OrganizationBudget {
			return fmt.Errorf("inference policy set has conflicting organization budgets")
		}
		seen[policy.ConnectionID] = true
	}
	return l.withTx(ctx, func(tx *sql.Tx) error {
		if err := validateInferenceAdmissionsSnapshot(ctx, tx); err != nil {
			return err
		}
		now := l.nowUTC()
		for _, policy := range policies {
			if err := activateInferencePolicyInTx(ctx, tx, policy, now, false); err != nil {
				return err
			}
		}
		return validateInferenceAdmissionsSnapshot(ctx, tx)
	})
}

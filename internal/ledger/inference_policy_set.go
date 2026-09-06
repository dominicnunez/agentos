package ledger

import (
	"context"
	"database/sql"

	"github.com/dominicnunez/agentos/internal/inference"
)

// ActivateInferencePolicies changes a reviewed set in one writer transaction.
// When shared limits change, all affected connections must participate. No
// reservation can observe intermediate policy revisions, and a failed member
// rolls back every activation event and row in the set.
func (l *SQLite) ActivateInferencePolicies(ctx context.Context, policies []inference.Policy) error {
	if err := inference.ValidatePolicySet(policies); err != nil {
		return err
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

package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/inference"
)

// SelectInferenceRoute reads account and shared budgets from one verified
// snapshot. It does not reserve, contact a provider, or replace dispatch-time
// admission: a later reservation must still recheck current policy and budgets.
func (l *SQLite) SelectInferenceRoute(ctx context.Context, registry *inference.ConnectionRegistry, request inference.RouteRequirements) (inference.RouteSelection, error) {
	if l == nil || l.db == nil || registry == nil {
		return inference.RouteSelection{}, fmt.Errorf("inference selection dependencies are required")
	}
	tx, err := l.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return inference.RouteSelection{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := validateInferenceAdmissionsSnapshot(ctx, tx); err != nil {
		return inference.RouteSelection{}, err
	}
	now := l.nowUTC()
	frozen, err := organizationFrozenAtSequence(ctx, tx, core.ID(request.OrganizationID), 0)
	if err != nil {
		return inference.RouteSelection{}, err
	}
	if frozen {
		return inference.RouteSelection{}, fmt.Errorf("organization is frozen; inference selection denied")
	}
	pools, active, err := inferenceSelectionPools(ctx, tx, request.OrganizationID, now)
	if err != nil {
		return inference.RouteSelection{}, err
	}
	var policy *inference.RoutePolicy
	for _, pool := range pools {
		if pool.Policy.Routing == nil {
			return inference.RouteSelection{}, fmt.Errorf("active account lacks routing policy")
		}
		if policy != nil && !inference.SameRoutePolicy(policy, pool.Policy.Routing) {
			return inference.RouteSelection{}, fmt.Errorf("active accounts disagree on routing policy")
		}
		policy = pool.Policy.Routing
	}
	if policy == nil {
		return inference.RouteSelection{}, fmt.Errorf("organization lacks an active routing policy")
	}
	budget, err := activeOrganizationBudget(ctx, tx, request.OrganizationID, nil)
	if err != nil {
		return inference.RouteSelection{}, err
	}
	sharedUse, err := readOrganizationBudgetUse(ctx, tx, request.OrganizationID, now, active, budget)
	if err != nil {
		return inference.RouteSelection{}, err
	}
	sharedBudgetRejections := 0
	for len(pools) > 0 {
		selected, err := registry.SelectRoute(now, pools, request, *policy)
		if err != nil {
			return inference.RouteSelection{}, err
		}
		err = sharedUse.allows(selected.Pool.ReservedInputTokens, selected.Pool.ReservedOutputTokens, selected.Pool.ReservedCostNanoUSD)
		if err == nil {
			selected.Decision.SharedBudgetRejections = sharedBudgetRejections
			if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0) FROM events`).Scan(&selected.Decision.SnapshotSequence); err != nil {
				return inference.RouteSelection{}, err
			}
			// Prove the candidate against the same snapshot before returning it.
			// A backwards clock must fail here, rather than emit a decision that
			// admission or replay would later reject.
			pending := inference.InferenceRequest{Scope: inference.Scope{OrganizationID: request.OrganizationID, Routing: &request, RoutingDecision: &selected.Decision}}
			if err := validateInferenceAdmissionsSnapshot(ctx, tx, pending); err != nil {
				return inference.RouteSelection{}, err
			}
			if err := tx.Commit(); err != nil {
				return inference.RouteSelection{}, err
			}
			return selected, nil
		}
		if !errors.Is(err, inference.ErrOrganizationBudgetExhausted) {
			return inference.RouteSelection{}, err
		}
		sharedBudgetRejections++
		// Another eligible account may reserve fewer tokens or less money. Apply
		// every original requirement again; this is selection, not a call retry.
		remaining := pools[:0]
		for _, pool := range pools {
			if pool.Policy.ConnectionID != selected.ConnectionID {
				remaining = append(remaining, pool)
			}
		}
		pools = remaining
	}
	return inference.RouteSelection{}, inference.ErrOrganizationBudgetExhausted
}

func inferenceSelectionPools(ctx context.Context, tx *sql.Tx, organizationID string, now time.Time) ([]inference.Pool, int, error) {
	rows, err := tx.QueryContext(ctx, `SELECT connection_id FROM inference_policies WHERE organization_id=? AND active=1 ORDER BY connection_id LIMIT 1025`, organizationID)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, 0, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	if err := rows.Close(); err != nil {
		return nil, 0, err
	}
	if len(ids) > 1024 {
		return nil, 0, fmt.Errorf("too many active inference connections")
	}
	var active int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM inference_reservations WHERE organization_id=? AND state=?`, organizationID, inferenceStateReserved).Scan(&active); err != nil {
		return nil, 0, err
	}
	var pools []inference.Pool
	for _, id := range ids {
		policy, fingerprint, err := activeInferencePolicy(ctx, tx, organizationID, id)
		if err != nil {
			return nil, 0, err
		}
		if policy.Catalog == nil {
			continue
		}
		pool := inference.Pool{ID: fingerprint, Policy: policy, Available: true}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM inference_reservations WHERE organization_id=? AND connection_id=? AND state=?`, organizationID, id, inferenceStateReserved).Scan(&pool.ActiveRequests); err != nil {
			return nil, 0, err
		}
		start, _ := inferenceWindow(now, time.Duration(policy.WindowDurationSeconds)*time.Second)
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(charged_input_tokens+charged_output_tokens),0),COALESCE(SUM(charged_cost_nano_usd),0) FROM inference_reservations WHERE organization_id=? AND connection_id=? AND provider=? AND model=? AND window_started_at=?`, organizationID, id, policy.Provider, policy.Model, start.Format(time.RFC3339Nano)).Scan(&pool.ChargedTokens, &pool.ChargedCostNanoUSD); err != nil {
			return nil, 0, err
		}
		pools = append(pools, pool)
	}
	return pools, active, nil
}

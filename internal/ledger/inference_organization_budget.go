package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"time"

	"github.com/dominicnunez/agentos/internal/inference"
)

// Every active connection carries the same reviewed organization limits.
// Check inside the activation transaction before writing either events or rows.
// The replaced connection is excluded so a sole connection can revise its limits.
func validateConnectionBudgetAgreement(ctx context.Context, tx *sql.Tx, candidate inference.Policy) error {
	budget, err := activeOrganizationBudget(ctx, tx, candidate.OrganizationID, &candidate.ConnectionID)
	if err != nil {
		return err
	}
	if budget != nil && *budget != *candidate.OrganizationBudget {
		return fmt.Errorf("active connections disagree on organization inference budget")
	}
	rows, err := tx.QueryContext(ctx, `SELECT body FROM inference_policies WHERE organization_id=? AND connection_id<>? AND active=1`, candidate.OrganizationID, candidate.ConnectionID)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			return err
		}
		var active inference.Policy
		if err := decodeExactJSONBytes(body, &active); err != nil {
			return err
		}
		if active.Version == inference.ConnectionPolicyVersion && !inference.SameRoutePolicy(active.Routing, candidate.Routing) {
			return fmt.Errorf("active connections disagree on organization routing policy")
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

func activeOrganizationBudget(ctx context.Context, tx *sql.Tx, organizationID string, excludedConnection *string) (*inference.OrganizationBudget, error) {
	rows, err := tx.QueryContext(ctx, `SELECT connection_id,policy_fingerprint,body FROM inference_policies WHERE organization_id=? AND active=1`, organizationID)
	if err != nil {
		return nil, fmt.Errorf("read organization inference budgets: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var budget *inference.OrganizationBudget
	for rows.Next() {
		var connectionID, fingerprint string
		var body []byte
		if err := rows.Scan(&connectionID, &fingerprint, &body); err != nil {
			return nil, fmt.Errorf("scan organization inference budget: %w", err)
		}
		var policy inference.Policy
		if err := decodeExactJSONBytes(body, &policy); err != nil {
			return nil, fmt.Errorf("organization inference policy is malformed: %w", err)
		}
		calculated, err := policy.Fingerprint()
		if err != nil || calculated != fingerprint || policy.OrganizationID != organizationID || policy.ConnectionID != connectionID {
			return nil, fmt.Errorf("organization inference policy identity is invalid")
		}
		if excludedConnection != nil && connectionID == *excludedConnection {
			continue
		}
		if policy.OrganizationBudget != nil {
			if budget != nil && *policy.OrganizationBudget != *budget {
				return nil, fmt.Errorf("active connections disagree on organization inference budget")
			}
			budget = policy.OrganizationBudget
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate organization inference budgets: %w", err)
	}
	return budget, nil
}

// Count every provider and model against the organization window, independent
// of the per-route window. Outstanding calls also retain their reservation when
// a window rolls over. Parse timestamps rather than comparing variable-width
// RFC3339Nano strings lexically, and fail closed on arithmetic overflow.
func enforceOrganizationBudget(ctx context.Context, tx *sql.Tx, organizationID string, now time.Time, active int, input, output, cost int64) error {
	budget, err := activeOrganizationBudget(ctx, tx, organizationID, nil)
	if err != nil || budget == nil {
		return err
	}
	if err := validateInferenceAdmissionsSnapshot(ctx, tx); err != nil {
		return fmt.Errorf("validate shared inference accounting: %w", err)
	}
	use, err := readOrganizationBudgetUse(ctx, tx, organizationID, now, active, budget)
	if err != nil {
		return err
	}
	return use.allows(input, output, cost)
}

type organizationBudgetUse struct {
	budget       *inference.OrganizationBudget
	active       int
	tokens, cost int64
}

func (u organizationBudgetUse) allows(input, output, cost int64) error {
	if u.budget != nil && !u.budget.Allows(u.active, u.tokens, u.cost, input, output, cost) {
		return inference.ErrOrganizationBudgetExhausted
	}
	return nil
}

// The caller must validate inference history in this same immutable transaction
// first. Candidate selection can reuse these totals without repeating replay.
func readOrganizationBudgetUse(ctx context.Context, tx *sql.Tx, organizationID string, now time.Time, active int, budget *inference.OrganizationBudget) (organizationBudgetUse, error) {
	if budget == nil {
		return organizationBudgetUse{}, nil
	}
	start, end := inferenceWindow(now, time.Duration(budget.WindowDurationSeconds)*time.Second)
	rows, err := tx.QueryContext(ctx, `SELECT COALESCE((SELECT json_extract(e.payload,'$.admitted_at') FROM events e WHERE e.organization_id=r.organization_id AND e.event_type='INFERENCE_RESERVED' AND e.source_execution_id=r.execution_id AND json_extract(e.payload,'$.reservation_id')=r.reservation_id),''),r.window_started_at,r.window_expires_at,r.state,r.charged_input_tokens,r.charged_output_tokens,r.charged_cost_nano_usd FROM inference_reservations r WHERE r.organization_id=?`, organizationID)
	if err != nil {
		return organizationBudgetUse{}, fmt.Errorf("read shared inference budget use: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var tokens, chargedCost int64
	for rows.Next() {
		var admittedAt, windowStart, windowEnd, state string
		var chargedInput, chargedOutput, rowCost int64
		if err := rows.Scan(&admittedAt, &windowStart, &windowEnd, &state, &chargedInput, &chargedOutput, &rowCost); err != nil {
			return organizationBudgetUse{}, fmt.Errorf("scan shared inference budget use: %w", err)
		}
		// Historical events did not bind a precise admission time. Their sealed
		// route window is the available evidence, so count overlapping windows
		// conservatively rather than trusting the unbound row timestamp.
		rowStart, _ := time.Parse(time.RFC3339Nano, windowStart)
		rowEnd, _ := time.Parse(time.RFC3339Nano, windowEnd)
		inWindow := rowStart.Before(end) && rowEnd.After(start)
		if admittedAt != "" {
			admitted, err := time.Parse(time.RFC3339Nano, admittedAt)
			if err != nil || admitted.After(now) {
				return organizationBudgetUse{}, fmt.Errorf("shared inference budget admission time is invalid")
			}
			inWindow = !admitted.Before(start) && admitted.Before(end)
		}
		if state != inferenceStateReserved && !inWindow {
			continue
		}
		if chargedInput > math.MaxInt64-tokens || chargedOutput > math.MaxInt64-tokens-chargedInput || rowCost > math.MaxInt64-chargedCost {
			return organizationBudgetUse{}, fmt.Errorf("shared inference budget accounting overflow")
		}
		tokens += chargedInput + chargedOutput
		chargedCost += rowCost
	}
	if err := rows.Err(); err != nil {
		return organizationBudgetUse{}, fmt.Errorf("iterate shared inference budget use: %w", err)
	}
	return organizationBudgetUse{budget: budget, active: active, tokens: tokens, cost: chargedCost}, nil
}

package ledger

import (
	"context"
	"fmt"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

// validateExecutionNotStopped rejects new activity for the exact execution
// named by a committed stop request. Callers run it inside the transaction that
// would publish or reserve new work, so the denial and prospective write share
// one durable view.
func validateExecutionNotStopped(ctx context.Context, queryer rowsQueryer, draft events.TrustedDraft) error {
	if draft.OrganizationID == "" || draft.TaskID == "" || draft.CorrelationID == "" || draft.SourceExecutionID == "" {
		return nil
	}
	// Suspension has no permitted projection successor. Its latest admitted
	// revision therefore supplies an exact stop reference without scanning every
	// stopped execution in the organization on each subsequent publication.
	rows, err := queryer.QueryContext(ctx, `SELECT event_id FROM events
WHERE event_id=(SELECT json_extract(p.payload,'$.detail.stop_request_ref')
FROM records r JOIN events p ON p.event_id=r.admission_event_id
WHERE r.kind='task' AND r.record_id=? ORDER BY r.version DESC LIMIT 1)
AND organization_id=? AND task_id=? AND correlation_id=? AND source_execution_id=?
AND event_type='EXECUTION_STOP_REQUESTED'`,
		draft.TaskID, draft.OrganizationID, draft.TaskID, draft.CorrelationID, draft.SourceExecutionID)
	if err != nil {
		return fmt.Errorf("inspect execution stop admission: %w", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return fmt.Errorf("inspect execution stop admission: %w", err)
		}
		return nil
	}
	var requestRef string
	if err := rows.Scan(&requestRef); err != nil {
		return fmt.Errorf("inspect execution stop admission: %w", err)
	}
	if requestRef == "" {
		return fmt.Errorf("execution stop admission lacks a durable request identity")
	}
	return fmt.Errorf("execution stop request %s denies new activity: %w", requestRef, core.ErrExecutionStopped)
}

// validateTaskNotSuspended rejects a new effect attempt when the Task's exact
// latest admitted revision is an execution suspension. This deliberately keys
// on the suspension transition rather than its detail so legacy admitted
// suspensions retain their terminal effect-admission boundary.
func validateTaskNotSuspended(ctx context.Context, queryer rowsQueryer, organization, taskID string) error {
	if organization == "" || taskID == "" {
		return nil
	}
	rows, err := queryer.QueryContext(ctx, `SELECT event_id FROM events
WHERE event_id=(SELECT r.admission_event_id FROM records r
WHERE r.kind='task' AND r.record_id=? ORDER BY r.version DESC LIMIT 1)
AND organization_id=? AND task_id=? AND event_type='TASK_EXECUTION_SUSPENDED'`,
		taskID, organization, taskID)
	if err != nil {
		return fmt.Errorf("inspect task suspension admission: %w", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return fmt.Errorf("inspect task suspension admission: %w", err)
		}
		return nil
	}
	var suspensionRef string
	if err := rows.Scan(&suspensionRef); err != nil {
		return fmt.Errorf("inspect task suspension admission: %w", err)
	}
	if suspensionRef == "" {
		return fmt.Errorf("task suspension admission lacks a durable identity")
	}
	return fmt.Errorf("task suspension %s denies a new effect attempt: %w", suspensionRef, core.ErrExecutionStopped)
}

package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/dominicnunez/agentos/internal/events"
)

// These families share execution/context identity. Task/Work projection
// history, task-only effects, authority activations and coordination/output
// policy decisions retain their own readers and are not inferred here.
const incidentExecutionTypes = `'EXECUTION_STARTED','EXECUTION_CONTEXT_MANIFESTED','EXECUTION_FINISHED','TASK_EXECUTION_SUSPENDED',
'PLANNING_CONTEXT_MANIFESTED','INTENT_NORMALIZATION_CONTEXT_MANIFESTED',
'MODEL_STOP_REQUESTED','MODEL_STOP_UNCERTAIN','MODEL_STOP_CONFIRMED',
'EXECUTION_STOP_REQUESTED','EXECUTION_STOP_UNCERTAIN','EXECUTION_STOP_CONFIRMED',
'TOOL_OUTCOME_RECORDED','INFERENCE_USAGE_RECORDED','INFERENCE_RESERVED','INFERENCE_RECONCILED','INFERENCE_NOT_SENT',
'PLAN_CREATED','PLANNING_FAILED','INTENT_DRAFTED','INTENT_NORMALIZATION_FAILED','PLANNING_CONTAINMENT_SUSPENDED','INTENT_NORMALIZATION_SUSPENDED'`

func validateIncidentExecutionEvidence(ctx context.Context, tx *sql.Tx, work []events.Event, freezes []events.OrganizationFreezeAdmission, budget *incidentBudget) error {
	stream, err := incidentExecutionHistory(ctx, tx, work, budget)
	if err != nil {
		return err
	}
	if err := events.ValidateExecutionStops(stream, freezes); err != nil {
		return err
	}
	if err := events.ValidateModelStops(stream, freezes); err != nil {
		return err
	}
	if err := events.ValidateSecurityHoldOutcomes(stream, freezes); err != nil {
		return err
	}
	if err := validateIncidentInference(ctx, tx, stream, freezes); err != nil {
		return err
	}
	if len(work) == 0 {
		return nil
	}
	// Some legacy ordinary outcomes predate manifest-specific validators.
	// Their meaning stays unchanged, but a declared link to this execution
	// cannot silently place its evidence in another Work correlation.
	for _, event := range stream {
		if event.CorrelationID != work[0].CorrelationID {
			return fmt.Errorf("incident execution evidence crosses its selected correlation")
		}
	}
	return validateIncidentExecutionRows(ctx, tx, work)
}

// Count linked accounting rows even when a corrupt history moved both the
// reservation event and its row, or removed the event altogether. This query
// returns one scalar and is not repeated per execution or reservation.
func validateIncidentExecutionRows(ctx context.Context, tx *sql.Tx, work []events.Event) error {
	executions := map[string]bool{}
	tasks := map[string]bool{}
	expected := 0
	for _, event := range work {
		if event.TaskID != "" {
			tasks[event.TaskID] = true
		}
		if event.SourceExecutionID != "" {
			executions[event.SourceExecutionID] = true
		}
		if event.EventType == "INFERENCE_RESERVED" {
			expected++
		}
		if event.EventType == "EXECUTION_STARTED" {
			id, err := events.ContainmentExecutionID(event)
			if err != nil {
				return err
			}
			executions[id] = true
		}
	}
	if len(executions) == 0 && len(tasks) == 0 {
		return nil
	}
	ids := make([]string, 0, len(executions))
	for id := range executions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	args := []any{work[0].OrganizationID}
	for _, id := range ids {
		args = append(args, id)
	}
	where := ""
	if len(ids) != 0 {
		where = `execution_id IN (` + strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",") + `)`
	}
	if len(tasks) != 0 {
		taskIDs := make([]string, 0, len(tasks))
		for id := range tasks {
			taskIDs = append(taskIDs, id)
		}
		sort.Strings(taskIDs)
		if where != "" {
			where += " OR "
		}
		where += `task_id IN (` + strings.TrimSuffix(strings.Repeat("?,", len(taskIDs)), ",") + `)`
		for _, id := range taskIDs {
			args = append(args, id)
		}
	}
	args = append(args, expected+1)
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM (SELECT 1 FROM inference_reservations WHERE organization_id=? AND (`+where+`) LIMIT ?)`, args...).Scan(&count); err != nil {
		return err
	}
	if count != expected {
		return fmt.Errorf("incident execution accounting lacks its selected reservation events")
	}
	return nil
}

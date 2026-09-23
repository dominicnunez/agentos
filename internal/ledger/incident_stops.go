package ledger

import (
	"context"
	"database/sql"
	"sort"
	"strings"

	"github.com/dominicnunez/agentos/internal/events"
)

// Discover the execution families consumed by the shared inference, stop, and
// legacy hold validators. Correlation is a selector, never the only identity.
func incidentExecutionHistory(ctx context.Context, tx *sql.Tx, work []events.Event, budget *incidentBudget) ([]events.Event, error) {
	if len(work) == 0 {
		return nil, nil
	}
	where, args, err := incidentExecutionSelection(work)
	if err != nil {
		return nil, err
	}
	where += ` AND correlation_id<>?`
	args = append(args, work[0].CorrelationID)
	additional, err := incidentEvents(ctx, tx, budget, where, args...)
	if err != nil {
		return nil, err
	}
	stream := append(append([]events.Event(nil), work...), additional...)
	sort.Slice(stream, func(i, j int) bool { return stream[i].Sequence < stream[j].Sequence })
	return stream, nil
}

// The same inverse relationships apply when an execution is private supporting
// evidence rather than part of the displayed Work.
func incidentExecutionSelection(work []events.Event) (string, []any, error) {
	refs := make([]string, 0, len(work))
	executions := map[string]bool{}
	tasks := map[string]bool{}
	for _, event := range work {
		refs = append(refs, event.EventID)
		if event.TaskID != "" {
			tasks[event.TaskID] = true
		}
		if event.SourceExecutionID != "" {
			executions[event.SourceExecutionID] = true
		}
		if event.EventType == "EXECUTION_STARTED" {
			id, err := events.ContainmentExecutionID(event)
			if err != nil {
				return "", nil, err
			}
			executions[id] = true
		}
	}
	sort.Strings(refs)
	ids := make([]string, 0, len(executions))
	for id := range executions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	taskIDs := make([]string, 0, len(tasks))
	for id := range tasks {
		taskIDs = append(taskIDs, id)
	}
	sort.Strings(taskIDs)
	args := []any{work[0].OrganizationID}
	links := ""
	if len(ids) != 0 {
		links = `source_execution_id IN (` + strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",") + `) OR `
		for _, id := range ids {
			args = append(args, id)
		}
	}
	// The stop validators reject any ordinary event from a stopped execution,
	// including labels added later. Exact execution envelopes therefore have
	// no label allowlist. Task-only evidence and payload links use the owned
	// families so independent effect records keep their separate reader.
	links += `(event_type IN (` + incidentExecutionTypes + `) AND (`
	if len(taskIDs) != 0 {
		links += `task_id IN (` + strings.TrimSuffix(strings.Repeat("?,", len(taskIDs)), ",") + `) OR `
		for _, id := range taskIDs {
			args = append(args, id)
		}
	}
	// Enumerate duplicate reference keys too. Nested references are restricted
	// to contract-owned interruption and projection details, not arbitrary
	// tool output fields. Authority hold references are deliberately excluded.
	links += `EXISTS (WITH nodes AS MATERIALIZED (SELECT id,parent,key,value FROM json_tree(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END)) SELECT 1 FROM nodes AS leaf LEFT JOIN nodes AS container ON container.id=leaf.parent AND container.parent=0 WHERE ((leaf.parent=0 AND leaf.key IN ('context_event_ref','execution_start_ref','execution_manifest_ref','stop_request_ref','usage_event_ref','outcome_event_ref','finish_event_ref','evidence_event_ref')) OR (container.key='observed_effect' AND leaf.key='stop_request_ref') OR (container.key='detail' AND leaf.key IN ('stop_request_ref','execution_start_ref'))) AND leaf.value IN (` + strings.TrimSuffix(strings.Repeat("?,", len(refs)), ",") + `))`
	for _, ref := range refs {
		args = append(args, ref)
	}
	if len(ids) != 0 {
		links += ` OR EXISTS (SELECT 1 FROM json_each(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END) WHERE key IN ('execution_id','request_id') AND value IN (` + strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",") + `))`
		for _, id := range ids {
			args = append(args, id)
		}
	}
	_, reservationIDs, _, err := incidentInferenceRequirements(work)
	if err != nil {
		return "", nil, err
	}
	if len(reservationIDs) != 0 {
		links += ` OR EXISTS (SELECT 1 FROM json_each(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END) WHERE key='reservation_id' AND value IN (` + strings.TrimSuffix(strings.Repeat("?,", len(reservationIDs)), ",") + `))`
		for _, id := range reservationIDs {
			args = append(args, id)
		}
	}
	links += `))`
	return `organization_id=? AND (` + links + `)`, args, nil
}

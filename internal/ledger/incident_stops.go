package ledger

import (
	"context"
	"database/sql"
	"sort"
	"strings"

	"github.com/dominicnunez/agentos/internal/events"
)

// Validate linked stop evidence in the same snapshot as the selected work. A
// stop cannot escape its exact lifecycle binding by changing its correlation.
// Selection is evidence discovery only; the shared full validators decide
// whether each request, result and referenced audit event is legitimate.
func validateIncidentStops(ctx context.Context, tx *sql.Tx, work []events.Event, freezes []events.OrganizationFreezeAdmission, budget *incidentBudget) error {
	if len(work) == 0 {
		return nil
	}
	refs := make([]string, 0, len(work))
	executions := map[string]bool{}
	for _, event := range work {
		refs = append(refs, event.EventID)
		if event.SourceExecutionID != "" {
			executions[event.SourceExecutionID] = true
		}
		if event.EventType == "EXECUTION_STARTED" {
			id, err := events.ContainmentExecutionID(event)
			if err != nil {
				return err
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
	args := []any{work[0].OrganizationID, work[0].CorrelationID}
	links := ""
	if len(ids) != 0 {
		links = `source_execution_id IN (` + strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",") + `) OR `
		for _, id := range ids {
			args = append(args, id)
		}
	}
	// json_each considers every declared top-level reference, including a
	// duplicate key, while CASE prevents unrelated malformed JSON from
	// poisoning this tenant-scoped reader before semantic validation.
	links += `EXISTS (SELECT 1 FROM json_each(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END) WHERE key IN ('context_event_ref','execution_start_ref','stop_request_ref','usage_event_ref','outcome_event_ref','finish_event_ref') AND value IN (` + strings.TrimSuffix(strings.Repeat("?,", len(refs)), ",") + `))`
	for _, ref := range refs {
		args = append(args, ref)
	}
	additional, err := incidentEvents(ctx, tx, budget, `organization_id=? AND correlation_id<>? AND event_type IN ('MODEL_STOP_REQUESTED','MODEL_STOP_UNCERTAIN','MODEL_STOP_CONFIRMED','EXECUTION_STOP_REQUESTED','EXECUTION_STOP_UNCERTAIN','EXECUTION_STOP_CONFIRMED') AND (`+links+`)`, args...)
	if err != nil {
		return err
	}
	stream := append(append([]events.Event(nil), work...), additional...)
	sort.Slice(stream, func(i, j int) bool { return stream[i].Sequence < stream[j].Sequence })
	if err := events.ValidateExecutionStops(stream, freezes); err != nil {
		return err
	}
	return events.ValidateModelStops(stream, freezes)
}

package ledger

import (
	"context"
	"database/sql"

	"github.com/dominicnunez/agentos/internal/events"
)

// Drive the join from events once. Each projection identity probes the records
// primary key; letting SQLite reverse the join would repeat an event-table scan
// for every current Task. Scope is deliberately global, just like the exact
// transition lookup: a foreign duplicate must still invalidate the binding.
// Unary plus removes column affinity in the final comparisons, preserving the
// exact lookup's JSON-value versus bound-parameter semantics after index probes.
const taskCompletionEventsSQL = `SELECT e.event_id,e.sequence,e.organization_id,e.event_type,e.source_actor_id,e.source_execution_id,e.recipient_scope,e.recipient_id,e.task_id,e.authorization_refs,e.artifact_refs,e.payload,e.correlation_id,e.created_at,e.schema_version,r.record_id
FROM events e CROSS JOIN records r
ON r.kind='task' AND r.record_id=json_extract(e.payload,'$.projection.record_id')
AND r.version=json_extract(e.payload,'$.projection.version')
WHERE e.event_type='TASK_VERIFIED_COMPLETE'
AND json_extract(e.payload,'$.projection.projection_kind')='task'
AND json_extract(e.payload,'$.projection.record_id')=+r.record_id
AND json_extract(e.payload,'$.projection.version')=+r.version
AND r.version=(SELECT MAX(latest.version) FROM records latest WHERE latest.kind=r.kind AND latest.record_id=r.record_id)
ORDER BY e.sequence`

func taskCompletionEvents(ctx context.Context, tx *sql.Tx) (map[string][]events.Event, error) {
	rows, err := tx.QueryContext(ctx, taskCompletionEventsSQL)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := make(map[string][]events.Event)
	for rows.Next() {
		var id string
		event, err := scanEvent(completionEventRow{row: rows, id: &id})
		if err != nil {
			return nil, err
		}
		// Two matches already prove ambiguity, matching the existing LIMIT 2.
		if len(result[id]) < 2 {
			result[id] = append(result[id], event)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, rows.Close()
}

type completionEventRow struct {
	row rowScanner
	id  *string
}

func (r completionEventRow) Scan(dest ...any) error {
	return r.row.Scan(append(dest, r.id)...)
}

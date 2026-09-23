package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"github.com/dominicnunez/agentos/internal/events"
)

func (d *incidentDependencies) loadExecutionEvidence(ctx context.Context, tx *sql.Tx) error {
	stream := make([]events.Event, 0, len(d.stream))
	ids := make([]string, 0, len(d.stream))
	for id, event := range d.stream {
		ids = append(ids, id)
		stream = append(stream, event)
	}
	if len(stream) == 0 {
		return nil
	}
	sort.Slice(stream, func(i, j int) bool { return stream[i].Sequence < stream[j].Sequence })
	where, args, err := incidentExecutionSelection(stream)
	if err != nil {
		return err
	}
	sort.Strings(ids)
	where += ` AND event_id NOT IN (` + incidentMarks(len(ids)) + `)`
	for _, id := range ids {
		args = append(args, id)
	}
	loaded, err := incidentEvents(ctx, tx, &d.budget, where, args...)
	if err != nil {
		return fmt.Errorf("incident supporting execution evidence: %w", err)
	}
	for _, event := range loaded {
		if err := d.add(event); err != nil {
			return err
		}
	}
	return nil
}

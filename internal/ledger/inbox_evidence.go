package ledger

import (
	"context"
	"database/sql"

	"github.com/dominicnunez/agentos/internal/events"
)

// ReadInboxObservations reads the exact durable inbox bindings for offline
// recovery. Bounded runtime readers select only their required observations.
func ReadInboxObservations(ctx context.Context, db *sql.DB) (map[string]events.InboxObservationBinding, error) {
	return inboxObservationBindings(ctx, db)
}

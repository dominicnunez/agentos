package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/dominicnunez/agentos/internal/events"
)

func (l *SQLite) appendInferenceRouteRejection(ctx context.Context, draft events.TrustedDraft) (events.Event, error) {
	if err := events.ValidateInferenceRouteRejection(draft); err != nil {
		return events.Event{}, err
	}
	var payload events.InferenceRouteRejectedPayload
	body, err := json.Marshal(draft.Payload)
	if err != nil {
		return events.Event{}, err
	}
	if err := decodeExactJSONBytes(body, &payload); err != nil {
		return events.Event{}, err
	}
	var recorded events.Event
	err = l.withTx(ctx, func(tx *sql.Tx) error {
		origin, found, err := eventByID(ctx, tx, payload.OriginEventRef)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("routing rejection origin is missing")
		}
		if err := events.ValidateInferenceRouteRejectionOrigin(draft, origin); err != nil {
			return err
		}
		recorded, err = appendEvent(ctx, tx, draft)
		return err
	})
	return recorded, err
}

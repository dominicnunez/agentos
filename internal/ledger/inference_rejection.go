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

func validateInferenceRouteRejections(stream []events.Event) error {
	origins := make(map[string]events.Event)
	for _, event := range stream {
		switch event.EventType {
		case "WORK_CREATED", "PLAN_CREATED", "INTAKE_MESSAGE_RECORDED":
			origins[event.EventID] = event
		case "INFERENCE_ROUTE_REJECTED":
			var payload events.InferenceRouteRejectedPayload
			if err := decodeExactJSONBytes(event.Payload, &payload); err != nil {
				return fmt.Errorf("invalid routing rejection history")
			}
			draft := events.TrustedDraft{OrganizationID: event.OrganizationID, EventType: event.EventType, SourceActorID: event.SourceActorID,
				SourceExecutionID: event.SourceExecutionID, RecipientScope: event.RecipientScope, RecipientID: event.RecipientID, TaskID: event.TaskID,
				AuthorizationRefs: event.AuthorizationRefs, ArtifactRefs: event.ArtifactRefs, CorrelationID: event.CorrelationID, Payload: event.Payload}
			origin, found := origins[payload.OriginEventRef]
			if !found || origin.Sequence >= event.Sequence || event.SchemaVersion != events.SchemaVersion {
				return fmt.Errorf("invalid routing rejection origin history")
			}
			if err := events.ValidateInferenceRouteRejectionOrigin(draft, origin); err != nil {
				return err
			}
		}
	}
	return nil
}

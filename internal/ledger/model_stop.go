package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func (l *SQLite) RequestModelStop(ctx context.Context, contextEventRef, reason string) (events.Event, bool, error) {
	if contextEventRef == "" || !events.ValidModelStopReason(reason) {
		return events.Event{}, false, fmt.Errorf("model stop requires its exact context and reason")
	}
	var request events.Event
	var completed bool
	err := l.withFreezeTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		manifest, found, err := eventByID(ctx, tx, contextEventRef)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("model stop context is unavailable")
		}
		if err := events.ValidateModelStopContext(manifest); err != nil {
			return err
		}
		stream, freezes, err := modelStopHistory(ctx, tx, manifest)
		if err != nil {
			return err
		}
		if err := events.ValidateModelStops(stream, freezes); err != nil {
			return err
		}
		for _, event := range stream {
			if event.EventType == "MODEL_STOP_REQUESTED" {
				var payload events.ModelStopRequest
				if decodeExactJSONBytes(event.Payload, &payload) != nil || payload.ContextEventRef != contextEventRef {
					return fmt.Errorf("model stop belongs to a different context")
				}
				request = event
				return nil
			}
			closed, err := events.ModelStopCompletion(event, manifest)
			if err != nil {
				return err
			}
			if closed {
				hold, err := executionIntervalHoldThrough(ctx, tx, modelStopDraft(manifest), event.Sequence)
				if err != nil {
					return err
				}
				if hold != nil {
					return fmt.Errorf("model closure crossed a committed hold")
				}
				completed = true
				return nil
			}
		}
		hold, err := executionIntervalHoldThrough(ctx, tx, modelStopDraft(manifest), 0)
		if err != nil {
			return err
		}
		if hold != nil {
			reason = "security_hold"
		} else if reason == "security_hold" {
			return fmt.Errorf("model security stop lacks a committed hold")
		}
		draft := modelStopDraft(manifest)
		draft.EventType, draft.Payload = "MODEL_STOP_REQUESTED", events.ModelStopRequest{ContextEventRef: contextEventRef, ReasonClass: reason, Hold: hold}
		request, err = appendEvent(ctx, tx, draft)
		if err != nil {
			return err
		}
		return events.ValidateModelStops(append(stream, request), freezes)
	})
	if err != nil {
		return events.Event{}, false, err
	}
	return request, completed, nil
}

func (l *SQLite) RecordModelStop(ctx context.Context, requestRef string, returned *events.ModelStopReturn) (events.Event, error) {
	if requestRef == "" || returned != nil && (returned.LocalState != "RETURNED" && returned.LocalState != "NOT_STARTED" || returned.LocalState == "RETURNED" && returned.ReturnedAt.IsZero() || returned.LocalState == "NOT_STARTED" && (!returned.ReturnedAt.IsZero() || returned.Usage != nil || returned.ProviderStop != nil) || returned.Usage != nil && !returned.Usage.Valid()) {
		return events.Event{}, fmt.Errorf("invalid model stop return")
	}
	var result events.Event
	err := l.withFreezeTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		request, found, err := eventByID(ctx, tx, requestRef)
		if err != nil {
			return err
		}
		if !found || request.EventType != "MODEL_STOP_REQUESTED" {
			return fmt.Errorf("model stop request is unavailable")
		}
		stream, freezes, err := modelStopHistory(ctx, tx, request)
		if err != nil {
			return err
		}
		if err := events.ValidateModelStops(stream, freezes); err != nil {
			return err
		}
		payload := events.ModelStopResult{StopRequestRef: requestRef}
		if returned != nil {
			payload.LocalState = returned.LocalState
			if returned.LocalState == "RETURNED" {
				at := returned.ReturnedAt.UTC()
				payload.ReturnedAt = &at
			}
			payload.ProviderStop = returned.ProviderStop
		}
		var usageEvent, priorResult events.Event
		for _, event := range stream {
			if event.EventType == "INFERENCE_USAGE_RECORDED" {
				var usage events.InferenceUsageRecordedPayload
				if usageEvent.EventID != "" || decodeExactJSONBytes(event.Payload, &usage) != nil || returned != nil && (returned.Usage == nil || !reflect.DeepEqual(usage, *returned.Usage)) {
					return fmt.Errorf("model stop has conflicting usage evidence")
				}
				usageEvent = event
			}
			if event.EventType == "MODEL_STOP_UNCERTAIN" || event.EventType == "MODEL_STOP_CONFIRMED" {
				priorResult = event
			}
		}
		if priorResult.EventType == "MODEL_STOP_CONFIRMED" {
			if returned != nil {
				payload.UsageEventRef = usageEvent.EventID
				var prior events.ModelStopResult
				if decodeExactJSONBytes(priorResult.Payload, &prior) != nil || !reflect.DeepEqual(prior, payload) {
					return fmt.Errorf("model stop confirmation has different local return evidence")
				}
			}
			result = priorResult
			return nil
		}
		if returned == nil && priorResult.EventID != "" {
			result = priorResult
			return nil
		}
		draft := modelStopDraft(request)
		draft.EventType = "MODEL_STOP_UNCERTAIN"
		if returned != nil {
			if returned.Usage != nil && usageEvent.EventID == "" {
				draft.EventType, draft.Payload = "INFERENCE_USAGE_RECORDED", returned.Usage
				usageEvent, err = appendEvent(ctx, tx, draft)
				if err != nil {
					return err
				}
				stream = append(stream, usageEvent)
			}
			payload.UsageEventRef = usageEvent.EventID
			draft.EventType = "MODEL_STOP_CONFIRMED"
		}
		draft.Payload = payload
		result, err = appendEvent(ctx, tx, draft)
		if err != nil {
			return err
		}
		return events.ValidateModelStops(append(stream, result), freezes)
	})
	if err != nil {
		return events.Event{}, err
	}
	return result, nil
}

func modelStopDraft(event events.Event) events.TrustedDraft {
	return events.TrustedDraft{OrganizationID: event.OrganizationID, TaskID: event.TaskID, CorrelationID: event.CorrelationID, SourceExecutionID: event.SourceExecutionID, SourceActorID: "runtime"}
}

func modelStopHistory(ctx context.Context, tx *sql.Tx, event events.Event) ([]events.Event, []events.OrganizationFreezeAdmission, error) {
	stream, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE correlation_id=? AND organization_id=? AND (source_execution_id=? OR (event_type='PLANNING_FAILED' AND json_extract(payload,'$.evidence_event_ref') IN (SELECT event_id FROM events WHERE organization_id=? AND correlation_id=? AND source_execution_id=? AND event_type='PLANNING_CONTEXT_MANIFESTED'))) ORDER BY sequence`, event.CorrelationID, event.OrganizationID, event.SourceExecutionID, event.OrganizationID, event.CorrelationID, event.SourceExecutionID))
	if err != nil {
		return nil, nil, err
	}
	freezes, err := modelStopFreezes(ctx, tx, event.OrganizationID)
	return stream, freezes, err
}

func modelStopRunHistory(ctx context.Context, tx *sql.Tx, organization, correlation string) ([]events.Event, []events.OrganizationFreezeAdmission, error) {
	stream, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE correlation_id=? AND organization_id=? ORDER BY sequence`, correlation, organization))
	if err != nil {
		return nil, nil, err
	}
	freezes, err := modelStopFreezes(ctx, tx, organization)
	return stream, freezes, err
}

func modelStopFreezes(ctx context.Context, tx *sql.Tx, organization string) ([]events.OrganizationFreezeAdmission, error) {
	history, err := loadFreezeHistory(ctx, tx, organization)
	if err != nil {
		return nil, err
	}
	freezes := make([]events.OrganizationFreezeAdmission, 0, len(history.revisions))
	for _, revision := range history.revisions {
		freezes = append(freezes, events.OrganizationFreezeAdmission{OrganizationID: revision.state.OrganizationID, EventRef: revision.event.EventID, Sequence: revision.event.Sequence, Frozen: revision.state.Frozen, Version: revision.record.Version, Control: revision.state.Control})
	}
	return freezes, nil
}

// Hot admission is an exact invocation query, not a history replay. Typed
// mutations and startup replay validate evidence before granting any closure.
func validateModelNotStopped(ctx context.Context, queryer rowsQueryer, draft events.TrustedDraft) error {
	if draft.OrganizationID == "" || draft.CorrelationID == "" || draft.SourceExecutionID == "" {
		return nil
	}
	rows, err := queryer.QueryContext(ctx, `SELECT payload FROM events WHERE correlation_id=? AND organization_id=? AND source_execution_id=? AND event_type='MODEL_STOP_REQUESTED' LIMIT 1`, draft.CorrelationID, draft.OrganizationID, draft.SourceExecutionID)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	if rows.Next() {
		var body []byte
		var payload events.ModelStopRequest
		if rows.Scan(&body) != nil || decodeExactJSONBytes(body, &payload) != nil || !events.ValidModelStopReason(payload.ReasonClass) {
			return core.ErrContainmentUnavailable
		}
		if payload.ReasonClass == "security_hold" && payload.Hold != nil {
			return errors.Join(core.ErrExecutionStopped, *payload.Hold)
		}
		if payload.ReasonClass == "containment_unavailable" {
			return errors.Join(core.ErrExecutionStopped, core.ErrContainmentUnavailable)
		}
		return core.ErrExecutionStopped
	}
	return rows.Err()
}

// modelStopRetryFinishes returns local closures only for confirmed nonsecurity
// normalization stop. It is not not-sent evidence and never applies to planning.
func modelStopRetryFinishes(stream []events.Event) map[string]int64 {
	requests := map[string]events.ModelStopRequest{}
	finishes := map[string]int64{}
	for _, event := range stream {
		if event.EventType == "MODEL_STOP_REQUESTED" {
			var request events.ModelStopRequest
			if json.Unmarshal(event.Payload, &request) == nil {
				requests[event.EventID] = request
			}
		}
		if event.EventType == "MODEL_STOP_CONFIRMED" {
			var result events.ModelStopResult
			if json.Unmarshal(event.Payload, &result) != nil {
				continue
			}
			request, found := requests[result.StopRequestRef]
			if found && request.ReasonClass != "security_hold" && request.ReasonClass != "containment_unavailable" {
				finishes[event.SourceExecutionID] = event.Sequence
			}
		}
	}
	return finishes
}

func validateModelSuspension(ctx context.Context, tx *sql.Tx, draft events.TrustedDraft) error {
	requests, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE organization_id=? AND correlation_id=? AND source_execution_id=? AND event_type='MODEL_STOP_REQUESTED'`, draft.OrganizationID, draft.CorrelationID, draft.SourceExecutionID))
	if err != nil {
		return err
	}
	if len(requests) == 0 {
		return validateExecutionPublication(ctx, tx, draft)
	}
	if len(requests) != 1 {
		return fmt.Errorf("duplicate model stop requests")
	}
	stream, freezes, err := modelStopHistory(ctx, tx, requests[0])
	if err != nil {
		return err
	}
	if err := events.ValidateModelStops(stream, freezes); err != nil {
		return err
	}
	body, err := json.Marshal(draft.Payload)
	if err != nil {
		return err
	}
	return events.ValidateModelStopSuspension(events.Event{OrganizationID: draft.OrganizationID, TaskID: draft.TaskID, CorrelationID: draft.CorrelationID, SourceExecutionID: draft.SourceExecutionID, SourceActorID: draft.SourceActorID, EventType: draft.EventType, RecipientScope: draft.RecipientScope, RecipientID: draft.RecipientID, AuthorizationRefs: draft.AuthorizationRefs, ArtifactRefs: draft.ArtifactRefs, Payload: body}, requests[0])
}

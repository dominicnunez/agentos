package ledger

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

// RequestExecutionStop makes the exact running task non-runnable before the
// runtime waits for an acknowledgement. A stop request is never a stop proof.
func (l *SQLite) RequestExecutionStop(ctx context.Context, organization, taskID, correlation, executionID, reason string) (events.Event, error) {
	if organization == "" || taskID == "" || correlation == "" || executionID == "" ||
		(reason != "security_hold" && reason != "containment_unavailable" && reason != "execution_cancelled") {
		return events.Event{}, fmt.Errorf("execution stop requires exact identity and a runtime reason")
	}
	var request events.Event
	err := l.withFreezeTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		request, err = requestExecutionStop(ctx, tx, organization, taskID, correlation, executionID, reason)
		return err
	})
	return request, err
}

func requestExecutionStop(ctx context.Context, tx *sql.Tx, organization, taskID, correlation, executionID, reason string) (events.Event, error) {
	record, task, found, err := latestProjectionRevision[core.Task](ctx, tx, "task", taskID)
	if err != nil {
		return events.Event{}, err
	}
	if !found || record.CorrelationID != correlation {
		return events.Event{}, fmt.Errorf("execution stop crosses its task boundary")
	}
	if task.Status == core.TaskBlocked {
		suspension, err := exactProjectionTransition(ctx, tx, "TASK_EXECUTION_SUSPENDED", record)
		if err != nil {
			return events.Event{}, err
		}
		if suspension.OrganizationID != organization {
			return events.Event{}, fmt.Errorf("execution stop crosses its task organization")
		}
		payload, admitted, err := events.AdmittedProjection(suspension)
		var detail events.ExecutionSuspension
		if err != nil || !admitted || decodeExactJSONBytes(payload.Detail, &detail) != nil || detail.StopRequestRef == "" {
			return events.Event{}, fmt.Errorf("suspended task lacks a matching stop request")
		}
		request, found, err := eventByID(ctx, tx, detail.StopRequestRef)
		if err != nil {
			return events.Event{}, err
		}
		if !found || request.EventType != "EXECUTION_STOP_REQUESTED" || request.OrganizationID != organization || request.TaskID != taskID || request.CorrelationID != correlation || request.SourceExecutionID != executionID {
			return events.Event{}, fmt.Errorf("suspended task belongs to another stop request")
		}
		return request, validateStopHistory(ctx, tx, request)
	}
	if task.Status != core.TaskRunning {
		return events.Event{}, fmt.Errorf("only a running execution can be stopped")
	}
	start, err := exactProjectionTransition(ctx, tx, "EXECUTION_STARTED", record)
	if err != nil {
		return events.Event{}, err
	}
	actualExecution, err := events.ContainmentExecutionID(start)
	if err != nil || start.OrganizationID != organization || actualExecution != executionID {
		return events.Event{}, fmt.Errorf("execution stop does not match the admitted start")
	}
	var completed bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE organization_id=? AND task_id=? AND correlation_id=? AND source_execution_id=? AND sequence>? AND event_type='COMPLETION_VERIFIED')`, organization, taskID, correlation, executionID, start.Sequence).Scan(&completed); err != nil {
		return events.Event{}, err
	}
	if completed {
		return events.Event{}, fmt.Errorf("execution has already crossed its completion boundary")
	}
	hold, err := executionIntervalHoldThrough(ctx, tx, events.TrustedDraft{OrganizationID: organization, TaskID: taskID, CorrelationID: correlation, SourceExecutionID: executionID}, 0)
	if err != nil {
		return events.Event{}, err
	}
	if hold != nil {
		reason = "security_hold"
	} else if reason == "security_hold" {
		return events.Event{}, fmt.Errorf("security stop lacks a committed hold")
	}
	request, err := appendEvent(ctx, tx, events.TrustedDraft{
		OrganizationID: organization, TaskID: taskID, CorrelationID: correlation,
		SourceActorID: "runtime", SourceExecutionID: executionID, EventType: "EXECUTION_STOP_REQUESTED",
		Payload: events.ExecutionStopRequest{ExecutionStartRef: start.EventID, ReasonClass: reason, Hold: hold},
	})
	if err != nil {
		return events.Event{}, err
	}
	task.Status = core.TaskBlocked
	item, err := prepareProjection(events.ProjectionDraft{
		Event: events.TrustedDraft{OrganizationID: organization, TaskID: taskID, CorrelationID: correlation,
			SourceActorID: "runtime", EventType: "TASK_EXECUTION_SUSPENDED",
			Payload: events.ExecutionSuspension{StopRequestRef: request.EventID, ExecutionStartRef: start.EventID}},
		ProjectionKind: "task", RecordID: taskID, Version: record.Version + 1, Value: task,
	}, false, false)
	if err != nil {
		return events.Event{}, err
	}
	if _, err := appendPreparedProjection(ctx, tx, item); err != nil {
		return events.Event{}, err
	}
	return request, validateStopHistory(ctx, tx, request)
}

// RecordExecutionStop commits accounting and a local finish only after the
// handler has returned. A missing acknowledgement is explicitly uncertain.
func (l *SQLite) RecordExecutionStop(ctx context.Context, requestRef string, outcome *core.ToolOutcome, usage *events.InferenceUsageRecordedPayload) (events.Event, error) {
	if requestRef == "" || (outcome == nil && usage != nil) || (usage != nil && !usage.Valid()) {
		return events.Event{}, fmt.Errorf("invalid execution stop result")
	}
	var result events.Event
	err := l.withFreezeTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		request, found, err := eventByID(ctx, tx, requestRef)
		if err != nil {
			return err
		}
		if !found || request.EventType != "EXECUTION_STOP_REQUESTED" {
			return fmt.Errorf("execution stop request is unavailable")
		}
		if err := validateStopHistory(ctx, tx, request); err != nil {
			return err
		}
		stream, err := executionStopHistory(ctx, tx, request)
		if err != nil {
			return err
		}
		var uncertain events.Event
		for _, event := range stream {
			if event.SourceExecutionID != request.SourceExecutionID || !events.RequiresExecutionStopAdmission(event.EventType) || event.EventType == "EXECUTION_STOP_REQUESTED" {
				continue
			}
			var payload events.ExecutionStopResult
			if decodeExactJSONBytes(event.Payload, &payload) != nil || payload.StopRequestRef != requestRef {
				return fmt.Errorf("execution stop result does not match its request")
			}
			if event.EventType == "EXECUTION_STOP_CONFIRMED" {
				if outcome != nil {
					prior, found, err := eventByID(ctx, tx, payload.OutcomeEventRef)
					var priorOutcome core.ToolOutcome
					if err != nil || !found || decodeExactJSONBytes(prior.Payload, &priorOutcome) != nil || !sameStopOutcome(priorOutcome, *outcome) {
						return fmt.Errorf("confirmed execution stop has different outcome evidence")
					}
					if (usage == nil) != (payload.UsageEventRef == "") {
						return fmt.Errorf("confirmed execution stop has different usage evidence")
					}
					if usage != nil {
						prior, found, err := eventByID(ctx, tx, payload.UsageEventRef)
						var priorUsage events.InferenceUsageRecordedPayload
						if err != nil || !found || decodeExactJSONBytes(prior.Payload, &priorUsage) != nil || !reflect.DeepEqual(priorUsage, *usage) {
							return fmt.Errorf("confirmed execution stop has different usage evidence")
						}
					}
				}
				result = event
				return nil
			}
			uncertain = event
		}
		if outcome == nil && uncertain.EventID != "" {
			result = uncertain
			return nil
		}
		draft := events.TrustedDraft{OrganizationID: request.OrganizationID, TaskID: request.TaskID,
			SourceActorID: "runtime", SourceExecutionID: request.SourceExecutionID, CorrelationID: request.CorrelationID}
		payload := events.ExecutionStopResult{StopRequestRef: requestRef}
		if outcome == nil {
			draft.EventType = "EXECUTION_STOP_UNCERTAIN"
		} else {
			draft.EventType, draft.Payload = "TOOL_OUTCOME_RECORDED", outcome
			if err := events.ValidateOrdinaryEventPayload(outcome); err != nil {
				return err
			}
			outcomeEvent, err := appendEvent(ctx, tx, draft)
			if err != nil {
				return err
			}
			payload.OutcomeEventRef = outcomeEvent.EventID
			if usage != nil {
				for _, event := range stream {
					if event.EventType != "INFERENCE_USAGE_RECORDED" || event.SourceExecutionID != request.SourceExecutionID {
						continue
					}
					var prior events.InferenceUsageRecordedPayload
					if payload.UsageEventRef != "" || decodeExactJSONBytes(event.Payload, &prior) != nil || !reflect.DeepEqual(prior, *usage) {
						return fmt.Errorf("execution already has different usage evidence")
					}
					payload.UsageEventRef = event.EventID
				}
				if payload.UsageEventRef == "" {
					draft.EventType, draft.Payload = "INFERENCE_USAGE_RECORDED", usage
					usageEvent, err := appendEvent(ctx, tx, draft)
					if err != nil {
						return err
					}
					payload.UsageEventRef = usageEvent.EventID
				}
			}
			draft.EventType, draft.Payload = "EXECUTION_FINISHED", map[string]any{"status": outcome.Status}
			finish, err := appendEvent(ctx, tx, draft)
			if err != nil {
				return err
			}
			payload.FinishEventRef = finish.EventID
			draft.EventType = "EXECUTION_STOP_CONFIRMED"
		}
		draft.Payload = payload
		result, err = appendEvent(ctx, tx, draft)
		if err != nil {
			return err
		}
		return validateStopHistory(ctx, tx, request)
	})
	return result, err
}

func sameStopOutcome(a, b core.ToolOutcome) bool {
	left, leftErr := json.Marshal(a)
	right, rightErr := json.Marshal(b)
	if leftErr != nil || rightErr != nil {
		return false
	}
	var normalizedLeft, normalizedRight any
	leftDecoder, rightDecoder := json.NewDecoder(bytes.NewReader(left)), json.NewDecoder(bytes.NewReader(right))
	leftDecoder.UseNumber()
	rightDecoder.UseNumber()
	return leftDecoder.Decode(&normalizedLeft) == nil && rightDecoder.Decode(&normalizedRight) == nil && reflect.DeepEqual(normalizedLeft, normalizedRight)
}

func executionStopHistory(ctx context.Context, tx *sql.Tx, request events.Event) ([]events.Event, error) {
	return collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE organization_id=? AND task_id=? AND correlation_id=? ORDER BY sequence`, request.OrganizationID, request.TaskID, request.CorrelationID))
}

func validateStopHistory(ctx context.Context, tx *sql.Tx, request events.Event) error {
	stream, err := executionStopHistory(ctx, tx, request)
	if err != nil {
		return err
	}
	history, err := loadFreezeHistory(ctx, tx, request.OrganizationID)
	if err != nil {
		return err
	}
	freezes := make([]events.OrganizationFreezeAdmission, 0, len(history.revisions))
	for _, revision := range history.revisions {
		freezes = append(freezes, events.OrganizationFreezeAdmission{OrganizationID: revision.state.OrganizationID,
			EventRef: revision.event.EventID, Sequence: revision.event.Sequence, Frozen: revision.state.Frozen,
			Version: revision.record.Version, Control: revision.state.Control})
	}
	return events.ValidateExecutionStops(stream, freezes)
}

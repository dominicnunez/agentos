package events

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/dominicnunez/agentos/internal/core"
)

const (
	executionStopRequested = iota + 1
	executionStopUncertain
	executionStopConfirmed
)

type executionStopBinding struct {
	organization string
	taskID       string
	correlation  string
	executionID  string
}

type executionStopHistoryState struct {
	request       Event
	payload       ExecutionStopRequest
	startSequence int64
	state         int
}

type executionStopEventIndex struct {
	event     Event
	duplicate bool
}

// ValidateExecutionStops validates the complete execution-stop lifecycle from
// an already-verified ledger snapshot. It indexes referenced events and freeze
// admissions once so each stop does not rescan the full history.
func ValidateExecutionStops(stream []Event, freezes []OrganizationFreezeAdmission) error {
	eventsByID := make(map[string]executionStopEventIndex, len(stream))
	for _, event := range stream {
		if event.EventID == "" {
			continue
		}
		indexed, found := eventsByID[event.EventID]
		if found {
			indexed.duplicate = true
			eventsByID[event.EventID] = indexed
			continue
		}
		eventsByID[event.EventID] = executionStopEventIndex{event: event}
	}

	freezeIndex := indexExecutionStopFreezes(freezes)
	states := make(map[string]*executionStopHistoryState)
	requestsByExecution := make(map[executionStopBinding]string)
	suspendedTasks := make(map[struct{ organization, taskID string }]int64)
	stopAudit := make(map[string]Event)
	usedStopAudit := make(map[string]struct{})

	for index, event := range stream {
		projection, taskProjection, err := decodeExecutionStopTaskProjection(event)
		if err != nil {
			return fmt.Errorf("invalid task projection %s: %w", event.EventID, err)
		}
		if taskProjection {
			taskKey := struct{ organization, taskID string }{event.OrganizationID, projection.Projection.RecordID}
			if sequence := suspendedTasks[taskKey]; sequence != 0 {
				return fmt.Errorf("task projection %s follows suspended revision at sequence %d", event.EventID, sequence)
			}
			if event.EventType == "TASK_EXECUTION_SUSPENDED" {
				if err := validateExecutionStopSuspensionReference(event, projection, states); err != nil {
					return err
				}
				suspendedTasks[taskKey] = event.Sequence
			}
		} else if event.EventType == "TASK_EXECUTION_SUSPENDED" {
			// Metadata-free legacy suspension events remain valid replay input,
			// but still close the task against every later projection revision.
			if event.OrganizationID == "" || event.TaskID == "" {
				return fmt.Errorf("legacy task suspension %s lacks task identity", event.EventID)
			}
			taskKey := struct{ organization, taskID string }{event.OrganizationID, event.TaskID}
			if sequence := suspendedTasks[taskKey]; sequence != 0 {
				return fmt.Errorf("task suspension %s follows suspended revision at sequence %d", event.EventID, sequence)
			}
			suspendedTasks[taskKey] = event.Sequence
		}

		switch event.EventType {
		case "EXECUTION_STOP_REQUESTED":
			state, binding, err := validateExecutionStopRequest(stream, index, event, eventsByID, freezeIndex)
			if err != nil {
				return err
			}
			if _, duplicate := states[event.EventID]; duplicate {
				return fmt.Errorf("execution stop request %s is duplicated", event.EventID)
			}
			if prior := requestsByExecution[binding]; prior != "" {
				return fmt.Errorf("execution stop request %s duplicates request %s", event.EventID, prior)
			}
			states[event.EventID] = state
			requestsByExecution[binding] = event.EventID
		case "EXECUTION_STOP_UNCERTAIN", "EXECUTION_STOP_CONFIRMED":
			used, err := validateExecutionStopResult(event, eventsByID, states)
			if err != nil {
				return err
			}
			for _, ref := range used {
				usedStopAudit[ref] = struct{}{}
			}
		}

		binding := executionStopBinding{event.OrganizationID, event.TaskID, event.CorrelationID, event.SourceExecutionID}
		requestRef := requestsByExecution[binding]
		if event.EventType == "TOOL_OUTCOME_RECORDED" {
			var outcome core.ToolOutcome
			if decodeExactPayload(event.Payload, &outcome) == nil && HasExecutionStopEvidence(outcome) &&
				(requestRef == "" || event.Sequence <= states[requestRef].request.Sequence) {
				return fmt.Errorf("execution stop outcome %s lacks its prior request", event.EventID)
			}
		}
		if requestRef == "" || event.Sequence <= states[requestRef].request.Sequence {
			continue
		}
		switch event.EventType {
		case "EXECUTION_STOP_REQUESTED", "EXECUTION_STOP_UNCERTAIN", "EXECUTION_STOP_CONFIRMED":
		case "TOOL_OUTCOME_RECORDED":
			if err := validateExecutionStopOutcome(event, *states[requestRef]); err != nil {
				return err
			}
			stopAudit[event.EventID] = event
		case "INFERENCE_USAGE_RECORDED":
			if err := validateExecutionStopUsage(event); err != nil {
				return err
			}
			stopAudit[event.EventID] = event
		case "EXECUTION_FINISHED":
			if err := validateExecutionStopFinish(event); err != nil {
				return err
			}
			stopAudit[event.EventID] = event
		case "INFERENCE_RECONCILED", "INFERENCE_NOT_SENT":
			// Independent inference replay validators own these accounting
			// contracts. They may settle after local stop was requested.
		default:
			return fmt.Errorf("event %s publishes ordinary execution activity after stop request %s", event.EventID, requestRef)
		}
	}

	for eventRef := range stopAudit {
		if _, used := usedStopAudit[eventRef]; !used {
			return fmt.Errorf("execution stop audit event %s lacks confirmed stop binding", eventRef)
		}
	}
	return nil
}

func indexExecutionStopFreezes(freezes []OrganizationFreezeAdmission) map[string][]OrganizationFreezeAdmission {
	indexed := make(map[string][]OrganizationFreezeAdmission)
	for _, freeze := range freezes {
		if freeze.Frozen {
			organization := string(freeze.OrganizationID)
			indexed[organization] = append(indexed[organization], freeze)
		}
	}
	for organization := range indexed {
		sort.Slice(indexed[organization], func(left, right int) bool {
			return indexed[organization][left].Sequence < indexed[organization][right].Sequence
		})
	}
	return indexed
}

func validateExecutionStopRequest(stream []Event, index int, request Event, eventsByID map[string]executionStopEventIndex, freezes map[string][]OrganizationFreezeAdmission) (*executionStopHistoryState, executionStopBinding, error) {
	var payload ExecutionStopRequest
	if request.EventID == "" || request.SourceActorID != "runtime" || request.OrganizationID == "" || request.TaskID == "" || request.CorrelationID == "" || request.SourceExecutionID == "" ||
		request.RecipientScope != "" || request.RecipientID != "" || len(request.AuthorizationRefs) != 0 || len(request.ArtifactRefs) != 0 ||
		decodeExactPayload(request.Payload, &payload) != nil || payload.ExecutionStartRef == "" || !validExecutionStopReason(payload.ReasonClass) {
		return nil, executionStopBinding{}, fmt.Errorf("execution stop request %s crosses its runtime execution boundary", request.EventID)
	}
	start, err := exactExecutionStopEvent(eventsByID, payload.ExecutionStartRef, "EXECUTION_STARTED")
	if err != nil {
		return nil, executionStopBinding{}, fmt.Errorf("execution stop request %s: %w", request.EventID, err)
	}
	if start.Sequence >= request.Sequence || start.OrganizationID != request.OrganizationID || start.TaskID != request.TaskID || start.CorrelationID != request.CorrelationID {
		return nil, executionStopBinding{}, fmt.Errorf("execution stop request %s does not bind its exact prior start", request.EventID)
	}
	executionID, err := ContainmentExecutionID(start)
	if err != nil || executionID != request.SourceExecutionID {
		return nil, executionStopBinding{}, fmt.Errorf("execution stop request %s does not bind its containment execution: %w", request.EventID, err)
	}

	first := firstExecutionStopFreeze(freezes[request.OrganizationID], start.Sequence, request.Sequence)
	if first == nil {
		if payload.Hold != nil || payload.ReasonClass == "security_hold" {
			return nil, executionStopBinding{}, fmt.Errorf("execution stop request %s has no matching committed hold", request.EventID)
		}
	} else if payload.ReasonClass != "security_hold" || payload.Hold == nil || !sameExecutionStopHold(*payload.Hold, *first) {
		return nil, executionStopBinding{}, fmt.Errorf("execution stop request %s does not bind its earliest committed hold", request.EventID)
	}

	if index+1 >= len(stream) || stream[index+1].Sequence != request.Sequence+1 {
		return nil, executionStopBinding{}, fmt.Errorf("execution stop request %s is not immediately followed by suspension", request.EventID)
	}
	if err := validateExecutionStopSuspension(stream[index+1], request, start); err != nil {
		return nil, executionStopBinding{}, err
	}
	binding := executionStopBinding{request.OrganizationID, request.TaskID, request.CorrelationID, request.SourceExecutionID}
	return &executionStopHistoryState{request: request, payload: payload, startSequence: start.Sequence, state: executionStopRequested}, binding, nil
}

func validExecutionStopReason(reason string) bool {
	return reason == "security_hold" || reason == "containment_unavailable" || reason == "execution_cancelled"
}

func firstExecutionStopFreeze(freezes []OrganizationFreezeAdmission, startSequence, requestSequence int64) *OrganizationFreezeAdmission {
	index := sort.Search(len(freezes), func(index int) bool { return freezes[index].Sequence > startSequence })
	if index == len(freezes) || freezes[index].Sequence >= requestSequence {
		return nil
	}
	return &freezes[index]
}

func sameExecutionStopHold(hold core.SecurityHoldCause, freeze OrganizationFreezeAdmission) bool {
	return hold.OrganizationID == freeze.OrganizationID && hold.EventRef != "" && hold.EventRef == freeze.EventRef && hold.Sequence > 0 && hold.Sequence == freeze.Sequence
}

func validateExecutionStopSuspension(suspension, request, start Event) error {
	projection, present, err := AdmittedProjection(suspension)
	if err != nil || !present {
		return fmt.Errorf("execution stop request %s lacks admitted task suspension: %w", request.EventID, err)
	}
	var detail ExecutionSuspension
	var task core.Task
	startProjection, _, startErr := AdmittedProjection(start)
	if suspension.EventType != "TASK_EXECUTION_SUSPENDED" || suspension.Sequence != request.Sequence+1 ||
		suspension.OrganizationID != request.OrganizationID || suspension.SourceActorID != "runtime" || suspension.SourceExecutionID != "" ||
		suspension.RecipientScope != "" || suspension.RecipientID != "" || suspension.TaskID != request.TaskID ||
		len(suspension.AuthorizationRefs) != 0 || len(suspension.ArtifactRefs) != 0 || suspension.CorrelationID != request.CorrelationID ||
		projection.Projection.ProjectionKind != "task" || projection.Projection.RecordID != request.TaskID || startErr != nil ||
		projection.Projection.Version != startProjection.Projection.Version+1 || decodeExactPayload(projection.Detail, &detail) != nil ||
		detail.StopRequestRef != request.EventID || detail.ExecutionStartRef != start.EventID ||
		decodeExactPayload(projection.Projection.Value, &task) != nil || task.ID != core.ID(request.TaskID) || task.Status != core.TaskBlocked {
		return fmt.Errorf("execution stop request %s lacks its exact adjacent task suspension", request.EventID)
	}
	return nil
}

func validateExecutionStopSuspensionReference(event Event, projection ProjectionEventPayload, states map[string]*executionStopHistoryState) error {
	var fields map[string]json.RawMessage
	if json.Unmarshal(projection.Detail, &fields) != nil {
		return fmt.Errorf("task suspension %s has malformed detail", event.EventID)
	}
	if _, newContract := fields["stop_request_ref"]; !newContract {
		return nil
	}
	var detail ExecutionSuspension
	if decodeExactPayload(projection.Detail, &detail) != nil || detail.StopRequestRef == "" || detail.ExecutionStartRef == "" {
		return fmt.Errorf("task suspension %s has invalid execution stop references", event.EventID)
	}
	state := states[detail.StopRequestRef]
	if state == nil || state.payload.ExecutionStartRef != detail.ExecutionStartRef || state.request.Sequence+1 != event.Sequence {
		return fmt.Errorf("task suspension %s does not bind its preceding stop request", event.EventID)
	}
	return nil
}

func decodeExecutionStopTaskProjection(event Event) (ProjectionEventPayload, bool, error) {
	var fields map[string]json.RawMessage
	containsProjection := json.Unmarshal(event.Payload, &fields) == nil && hasReservedProjectionField(fields)
	if !containsProjection {
		return ProjectionEventPayload{}, false, nil
	}
	projection, present, err := AdmittedProjection(event)
	if err != nil || !present {
		return projection, false, err
	}
	return projection, projection.Projection.ProjectionKind == "task", nil
}

func validateExecutionStopResult(result Event, eventsByID map[string]executionStopEventIndex, states map[string]*executionStopHistoryState) ([]string, error) {
	var payload ExecutionStopResult
	if result.SourceActorID != "runtime" || result.OrganizationID == "" || result.TaskID == "" || result.CorrelationID == "" || result.SourceExecutionID == "" ||
		result.RecipientScope != "" || result.RecipientID != "" || len(result.AuthorizationRefs) != 0 || len(result.ArtifactRefs) != 0 ||
		decodeExactPayload(result.Payload, &payload) != nil || payload.StopRequestRef == "" {
		return nil, fmt.Errorf("execution stop result %s crosses its runtime execution boundary", result.EventID)
	}
	state := states[payload.StopRequestRef]
	if state == nil || result.Sequence <= state.request.Sequence || result.OrganizationID != state.request.OrganizationID || result.TaskID != state.request.TaskID ||
		result.CorrelationID != state.request.CorrelationID || result.SourceExecutionID != state.request.SourceExecutionID {
		return nil, fmt.Errorf("execution stop result %s does not bind its request", result.EventID)
	}
	if result.EventType == "EXECUTION_STOP_UNCERTAIN" {
		if payload.OutcomeEventRef != "" || payload.UsageEventRef != "" || payload.FinishEventRef != "" || state.state != executionStopRequested {
			return nil, fmt.Errorf("execution stop uncertainty %s is duplicate or carries acknowledgement evidence", result.EventID)
		}
		state.state = executionStopUncertain
		return nil, nil
	}
	if state.state == executionStopConfirmed || payload.OutcomeEventRef == "" || payload.FinishEventRef == "" {
		return nil, fmt.Errorf("execution stop confirmation %s is duplicate or incomplete", result.EventID)
	}
	outcome, err := exactExecutionStopEvent(eventsByID, payload.OutcomeEventRef, "TOOL_OUTCOME_RECORDED")
	if err != nil {
		return nil, fmt.Errorf("execution stop confirmation %s: %w", result.EventID, err)
	}
	finish, err := exactExecutionStopEvent(eventsByID, payload.FinishEventRef, "EXECUTION_FINISHED")
	if err != nil {
		return nil, fmt.Errorf("execution stop confirmation %s: %w", result.EventID, err)
	}
	if err := validateExecutionStopOutcome(outcome, *state); err != nil {
		return nil, err
	}
	if err := validateExecutionStopFinish(finish); err != nil {
		return nil, err
	}
	used := []string{outcome.EventID, finish.EventID}
	if payload.UsageEventRef != "" {
		usage, err := exactExecutionStopEvent(eventsByID, payload.UsageEventRef, "INFERENCE_USAGE_RECORDED")
		if err != nil {
			return nil, fmt.Errorf("execution stop confirmation %s: %w", result.EventID, err)
		}
		if err := validateExecutionStopUsage(usage); err != nil {
			return nil, err
		}
		if !sameExecutionStopEventBinding(usage, state.request) || usage.Sequence <= state.startSequence || usage.Sequence >= result.Sequence {
			return nil, fmt.Errorf("execution stop usage %s has invalid identity or order", usage.EventID)
		}
		used = append(used, usage.EventID)
	}
	if !sameExecutionStopEventBinding(outcome, state.request) || !sameExecutionStopEventBinding(finish, state.request) ||
		outcome.Sequence <= state.request.Sequence || finish.Sequence <= outcome.Sequence || finish.Sequence >= result.Sequence {
		return nil, fmt.Errorf("execution stop confirmation %s has invalid evidence identity or order", result.EventID)
	}
	state.state = executionStopConfirmed
	return used, nil
}

func validateExecutionStopOutcome(event Event, state executionStopHistoryState) error {
	var outcome core.ToolOutcome
	var evidence core.ExecutionInterruptionEvidence
	if event.SourceActorID != "runtime" || event.RecipientScope != "" || event.RecipientID != "" || len(event.AuthorizationRefs) != 0 || len(event.ArtifactRefs) != 0 ||
		decodeExactPayload(event.Payload, &outcome) != nil || !outcome.Valid() || outcome.ToolID != "runtime-containment" ||
		outcome.Status != core.OutcomeFailed || outcome.PostconditionStatus != core.PostconditionNotChecked || outcome.Retryability != core.NotRetryable ||
		len(outcome.ArtifactRefs) != 0 || outcome.ErrorClass != state.payload.ReasonClass || decodeExactPayload(outcome.ObservedEffect, &evidence) != nil ||
		evidence.StopRequestRef != state.request.EventID || !evidence.LocalExecutionStopped || evidence.ExternalEffectsStatus != "REQUIRES_RECONCILIATION" ||
		!sameOptionalExecutionStopHold(evidence.Hold, state.payload.Hold) || !validExecutionProviderStop(evidence.ProviderStop) {
		return fmt.Errorf("execution stop outcome %s is not truthful local interruption evidence", event.EventID)
	}
	return nil
}

func validExecutionProviderStop(evidence *core.ProviderStopEvidence) bool {
	if evidence == nil {
		return true
	}
	return evidence.RemoteStatus == "UNCERTAIN" && (!evidence.LocalProcessStopped || evidence.LocalProcessStopAttempted && evidence.LocalTurnStopped)
}

func validateExecutionStopUsage(event Event) error {
	var usage InferenceUsageRecordedPayload
	if event.SourceActorID != "runtime" || event.RecipientScope != "" || event.RecipientID != "" || len(event.AuthorizationRefs) != 0 || len(event.ArtifactRefs) != 0 ||
		decodeExactPayload(event.Payload, &usage) != nil || !usage.Valid() {
		return fmt.Errorf("execution stop usage %s is invalid", event.EventID)
	}
	return nil
}

func validateExecutionStopFinish(event Event) error {
	var payload struct {
		Status core.ToolOutcomeStatus `json:"status"`
	}
	if event.SourceActorID != "runtime" || event.RecipientScope != "" || event.RecipientID != "" || len(event.AuthorizationRefs) != 0 || len(event.ArtifactRefs) != 0 ||
		decodeExactPayload(event.Payload, &payload) != nil || payload.Status != core.OutcomeFailed {
		return fmt.Errorf("execution stop finish %s is invalid", event.EventID)
	}
	return nil
}

func sameExecutionStopEventBinding(event, request Event) bool {
	return event.OrganizationID == request.OrganizationID && event.TaskID == request.TaskID && event.CorrelationID == request.CorrelationID && event.SourceExecutionID == request.SourceExecutionID
}

func sameOptionalExecutionStopHold(left, right *core.SecurityHoldCause) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func exactExecutionStopEvent(eventsByID map[string]executionStopEventIndex, eventRef, eventType string) (Event, error) {
	indexed, found := eventsByID[eventRef]
	if !found || indexed.duplicate || indexed.event.EventType != eventType {
		return Event{}, fmt.Errorf("reference %s is not one exact %s event", eventRef, eventType)
	}
	return indexed.event, nil
}

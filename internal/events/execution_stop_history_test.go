package events

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestValidateExecutionStopsAcceptsConfirmedStop(t *testing.T) {
	stream, freezes := executionStopHistory(t, "org-1", "task-1", 10, "security_hold", true, true)
	if err := ValidateExecutionStops(stream, freezes); err != nil {
		t.Fatalf("valid confirmed execution stop rejected: %v", err)
	}
}

func TestValidateExecutionStopsRejectsMixedCause(t *testing.T) {
	stream, freezes := executionStopHistory(t, "org-1", "task-1", 10, "security_hold", true, true)
	requestIndex := executionStopEventIndexByType(t, stream, "EXECUTION_STOP_REQUESTED")
	var request ExecutionStopRequest
	if err := json.Unmarshal(stream[requestIndex].Payload, &request); err != nil {
		t.Fatal(err)
	}
	request.ReasonClass = "execution_cancelled"
	stream[requestIndex].Payload, _ = json.Marshal(request)
	for index, event := range stream {
		if event.EventType != "TOOL_OUTCOME_RECORDED" {
			continue
		}
		var outcome core.ToolOutcome
		if err := json.Unmarshal(event.Payload, &outcome); err != nil {
			t.Fatal(err)
		}
		outcome.ErrorClass = "execution_cancelled"
		stream[index].Payload, _ = json.Marshal(outcome)
	}
	if err := ValidateExecutionStops(stream, freezes); err == nil {
		t.Fatal("replay accepted non-security cause for committed hold")
	}
}

func TestValidateExecutionStopsRejectsOrphanOutcome(t *testing.T) {
	stream, _ := executionStopHistory(t, "org-1", "task-1", 10, "execution_cancelled", false, false)
	orphan := []Event{stream[0]}
	for _, event := range stream {
		if event.EventType == "TOOL_OUTCOME_RECORDED" {
			orphan = append(orphan, event)
		}
	}
	if err := ValidateExecutionStops(orphan, nil); err == nil {
		t.Fatal("replay accepted claimed stop evidence without its request")
	}
}

func TestValidateExecutionStopsAcceptsUncertainThenConfirmed(t *testing.T) {
	stream, freezes := executionStopHistory(t, "org-1", "task-1", 10, "execution_cancelled", false, false)
	request := stream[1]
	for index := 3; index < len(stream); index++ {
		stream[index].Sequence++
		stream[index].CreatedAt = time.Unix(stream[index].Sequence, 0).UTC()
	}
	uncertain := executionStopEvent("org-1", request.CorrelationID, request.TaskID, request.SourceExecutionID, request.Sequence+2, "stop-uncertain-org-1", "EXECUTION_STOP_UNCERTAIN", ExecutionStopResult{StopRequestRef: request.EventID})
	stream = append(stream[:3], append([]Event{uncertain}, stream[3:]...)...)
	if err := ValidateExecutionStops(stream, freezes); err != nil {
		t.Fatalf("uncertain-to-confirmed stop rejected: %v", err)
	}
}

func TestValidateExecutionStopsAllowsIndependentLateInferenceAccounting(t *testing.T) {
	for _, eventType := range []string{"INFERENCE_RECONCILED", "INFERENCE_NOT_SENT"} {
		t.Run(eventType, func(t *testing.T) {
			stream, freezes := executionStopHistory(t, "org-1", "task-1", 10, "execution_cancelled", false, false)
			request := stream[1]
			stream = stream[:3]
			stream = append(stream, executionStopEvent("org-1", request.CorrelationID, request.TaskID, request.SourceExecutionID, request.Sequence+2, "late-accounting", eventType, map[string]any{"state": "UNCERTAIN"}))
			if err := ValidateExecutionStops(stream, freezes); err != nil {
				t.Fatalf("independently validated late accounting rejected: %v", err)
			}
		})
	}
}

func TestValidateExecutionStopsAcceptsUsageRecordedBeforeLateRequest(t *testing.T) {
	stream, freezes := executionStopHistory(t, "org-1", "task-1", 10, "execution_cancelled", false, true)
	request := stream[1]
	usageIndex := executionStopEventIndexByType(t, stream, "INFERENCE_USAGE_RECORDED")
	usage := stream[usageIndex]
	usage.Sequence = request.Sequence
	usage.CreatedAt = time.Unix(usage.Sequence, 0).UTC()
	request.Sequence++
	request.CreatedAt = time.Unix(request.Sequence, 0).UTC()
	stream[1] = request
	stream[2] = executionStopTaskProjection(t, "org-1", request.CorrelationID, request.TaskID, request.Sequence+1, "TASK_EXECUTION_SUSPENDED", 3, core.TaskBlocked, ExecutionSuspension{StopRequestRef: request.EventID, ExecutionStartRef: stream[0].EventID})
	for index := 3; index < len(stream); index++ {
		if index == usageIndex {
			continue
		}
		stream[index].Sequence++
		stream[index].CreatedAt = time.Unix(stream[index].Sequence, 0).UTC()
	}
	withoutUsage := append([]Event(nil), stream[:usageIndex]...)
	withoutUsage = append(withoutUsage, stream[usageIndex+1:]...)
	stream = append([]Event{stream[0], usage}, withoutUsage[1:]...)
	if err := ValidateExecutionStops(stream, freezes); err != nil {
		t.Fatalf("usage admitted before late stop request rejected: %v", err)
	}
}

func TestValidateExecutionStopsKeepsOriginalCauseWhenHoldCommitsAfterRequest(t *testing.T) {
	stream, _ := executionStopHistory(t, "org-1", "task-1", 10, "containment_unavailable", false, false)
	for index := 3; index < len(stream); index++ {
		stream[index].Sequence++
		stream[index].CreatedAt = time.Unix(stream[index].Sequence, 0).UTC()
	}
	freezeSequence := stream[2].Sequence + 1
	freeze := OrganizationFreezeAdmission{OrganizationID: "org-1", EventRef: "late-freeze", Frozen: true, Sequence: freezeSequence, Version: 1}
	stream = append(stream[:3], append([]Event{executionStopEvent("org-1", "", "", "", freezeSequence, freeze.EventRef, "FREEZE_SET", map[string]any{"frozen": true})}, stream[3:]...)...)
	if err := ValidateExecutionStops(stream, []OrganizationFreezeAdmission{freeze}); err != nil {
		t.Fatalf("later hold reinterpreted the immutable stop cause: %v", err)
	}
}

func TestValidateExecutionStopsPreservesLegacySuspensionAndTenantIsolation(t *testing.T) {
	legacy := executionStopLegacySuspension(t, "org-a", "task-1", 10)
	if err := ValidateExecutionStops(legacy, nil); err != nil {
		t.Fatalf("legacy suspension rejected: %v", err)
	}

	stream, freezes := executionStopHistory(t, "org-a", "task-1", 20, "security_hold", true, false)
	request := stream[2]
	stream = stream[:4]
	stream = append(stream, executionStopEvent("org-b", "work-org-b", request.TaskID, request.SourceExecutionID, request.Sequence+2, "other-tenant-result", "RESULT_PUBLISHED", map[string]any{"summary": "independent"}))
	if err := ValidateExecutionStops(stream, freezes); err != nil {
		t.Fatalf("one tenant's stop rejected another tenant's event: %v", err)
	}
}

func TestValidateExecutionStopsPreservesUnrelatedLegacyEvents(t *testing.T) {
	legacy := []Event{
		{EventID: "legacy-start", Sequence: 1, OrganizationID: "org-1", EventType: "EXECUTION_STARTED", TaskID: "task-1", Payload: nil},
		{EventID: "legacy-result", Sequence: 2, OrganizationID: "org-1", EventType: "RESULT_PUBLISHED", TaskID: "task-1", Payload: json.RawMessage(`{"summary":"legacy"}`)},
	}
	if err := ValidateExecutionStops(legacy, nil); err != nil {
		t.Fatalf("unrelated legacy history rejected: %v", err)
	}
}

func TestValidateExecutionStopsRejectsBadEarlierEvidenceDespitePlausibleLaterHistory(t *testing.T) {
	bad, badFreezes := executionStopHistory(t, "org-a", "task-a", 10, "security_hold", true, false)
	requestIndex := executionStopEventIndexByType(t, bad, "EXECUTION_STOP_REQUESTED")
	bad[requestIndex].Payload = json.RawMessage(`{"execution_start_ref":"` + bad[0].EventID + `","reason_class":"other"}`)
	good, goodFreezes := executionStopHistory(t, "org-b", "task-b", 100, "security_hold", true, false)
	if err := ValidateExecutionStops(append(bad, good...), append(badFreezes, goodFreezes...)); err == nil {
		t.Fatal("plausible later stop history masked invalid earlier evidence")
	}
}

func TestValidateExecutionStopsRejectsWrongIdentityReferencesAndOrdering(t *testing.T) {
	tests := map[string]func(*testing.T, []Event){
		"unknown request field": func(t *testing.T, stream []Event) {
			request := &stream[executionStopEventIndexByType(t, stream, "EXECUTION_STOP_REQUESTED")]
			request.Payload = json.RawMessage(`{"execution_start_ref":"` + stream[0].EventID + `","reason_class":"execution_cancelled","extra":true}`)
		},
		"wrong start reference": func(t *testing.T, stream []Event) {
			request := &stream[executionStopEventIndexByType(t, stream, "EXECUTION_STOP_REQUESTED")]
			request.Payload = executionStopJSON(ExecutionStopRequest{ExecutionStartRef: "missing", ReasonClass: "execution_cancelled"})
		},
		"wrong request execution": func(t *testing.T, stream []Event) {
			stream[executionStopEventIndexByType(t, stream, "EXECUTION_STOP_REQUESTED")].SourceExecutionID = "other-execution"
		},
		"wrong suspension request": func(t *testing.T, stream []Event) {
			request := stream[executionStopEventIndexByType(t, stream, "EXECUTION_STOP_REQUESTED")]
			stream[executionStopEventIndexByType(t, stream, "TASK_EXECUTION_SUSPENDED")] = executionStopTaskProjection(t, request.OrganizationID, request.CorrelationID, request.TaskID, request.Sequence+1, "TASK_EXECUTION_SUSPENDED", 3, core.TaskBlocked, ExecutionSuspension{StopRequestRef: "other-request", ExecutionStartRef: stream[0].EventID})
		},
		"wrong outcome identity": func(t *testing.T, stream []Event) {
			stream[executionStopEventIndexByType(t, stream, "TOOL_OUTCOME_RECORDED")].TaskID = "other-task"
		},
		"wrong finish reference type": func(t *testing.T, stream []Event) {
			confirmed := &stream[executionStopEventIndexByType(t, stream, "EXECUTION_STOP_CONFIRMED")]
			var result ExecutionStopResult
			if err := json.Unmarshal(confirmed.Payload, &result); err != nil {
				t.Fatal(err)
			}
			result.FinishEventRef = result.OutcomeEventRef
			confirmed.Payload = executionStopJSON(result)
		},
		"incomplete confirmation": func(t *testing.T, stream []Event) {
			confirmed := &stream[executionStopEventIndexByType(t, stream, "EXECUTION_STOP_CONFIRMED")]
			confirmed.Payload = executionStopJSON(ExecutionStopResult{StopRequestRef: stream[1].EventID})
		},
		"unknown confirmation field": func(t *testing.T, stream []Event) {
			confirmed := &stream[executionStopEventIndexByType(t, stream, "EXECUTION_STOP_CONFIRMED")]
			confirmed.Payload = append(append(json.RawMessage(nil), confirmed.Payload[:len(confirmed.Payload)-1]...), []byte(`,"extra":true}`)...)
		},
		"finish before outcome": func(t *testing.T, stream []Event) {
			outcome := stream[executionStopEventIndexByType(t, stream, "TOOL_OUTCOME_RECORDED")].Sequence
			stream[executionStopEventIndexByType(t, stream, "EXECUTION_FINISHED")].Sequence = outcome
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			stream, freezes := executionStopHistory(t, "org-1", "task-1", 10, "execution_cancelled", false, false)
			mutate(t, stream)
			if err := ValidateExecutionStops(stream, freezes); err == nil {
				t.Fatal("invalid stop identity, reference, or ordering was accepted")
			}
		})
	}
}

func TestValidateExecutionStopsRejectsMissingSuspensionAndInvalidStateTransitions(t *testing.T) {
	t.Run("missing suspension", func(t *testing.T) {
		stream, freezes := executionStopHistory(t, "org-1", "task-1", 10, "execution_cancelled", false, false)
		stream = append(stream[:2], stream[3:]...)
		if err := ValidateExecutionStops(stream, freezes); err == nil {
			t.Fatal("stop request without its adjacent suspension was accepted")
		}
	})

	t.Run("duplicate uncertain", func(t *testing.T) {
		stream, _ := executionStopHistory(t, "org-1", "task-1", 10, "execution_cancelled", false, false)
		request := stream[1]
		stream = stream[:3]
		for index := 0; index < 2; index++ {
			stream = append(stream, executionStopEvent("org-1", request.CorrelationID, request.TaskID, request.SourceExecutionID, request.Sequence+2+int64(index), "uncertain-"+string(rune('a'+index)), "EXECUTION_STOP_UNCERTAIN", ExecutionStopResult{StopRequestRef: request.EventID}))
		}
		if err := ValidateExecutionStops(stream, nil); err == nil {
			t.Fatal("duplicate uncertainty was accepted")
		}
	})

	t.Run("confirmed then uncertain", func(t *testing.T) {
		stream, freezes := executionStopHistory(t, "org-1", "task-1", 10, "execution_cancelled", false, false)
		request := stream[1]
		last := stream[len(stream)-1]
		stream = append(stream, executionStopEvent("org-1", request.CorrelationID, request.TaskID, request.SourceExecutionID, last.Sequence+1, "late-uncertain", "EXECUTION_STOP_UNCERTAIN", ExecutionStopResult{StopRequestRef: request.EventID}))
		if err := ValidateExecutionStops(stream, freezes); err == nil {
			t.Fatal("confirmed stop reversed to uncertain")
		}
	})

	t.Run("duplicate confirmed", func(t *testing.T) {
		stream, freezes := executionStopHistory(t, "org-1", "task-1", 10, "execution_cancelled", false, false)
		duplicate := stream[len(stream)-1]
		duplicate.EventID = "duplicate-confirmed"
		duplicate.Sequence++
		stream = append(stream, duplicate)
		if err := ValidateExecutionStops(stream, freezes); err == nil {
			t.Fatal("duplicate confirmation was accepted")
		}
	})
}

func TestValidateExecutionStopsRejectsTaskProjectionSuccessors(t *testing.T) {
	t.Run("new suspension", func(t *testing.T) {
		stream, freezes := executionStopHistory(t, "org-1", "task-1", 10, "execution_cancelled", false, false)
		stream = stream[:3]
		stream = append(stream, executionStopTaskProjection(t, "org-1", "work-org-1", "task-1", stream[len(stream)-1].Sequence+1, "TASK_RESUMED", 4, core.TaskPending, map[string]string{"reason": "ordinary resume"}))
		if err := ValidateExecutionStops(stream, freezes); err == nil {
			t.Fatal("ordinary resume followed a new stop suspension")
		}
	})

	t.Run("legacy suspension", func(t *testing.T) {
		stream := executionStopLegacySuspension(t, "org-1", "task-1", 10)
		stream = append(stream, executionStopTaskProjection(t, "org-1", "work-org-1", "task-1", 12, "TASK_RESUMED", 4, core.TaskPending, map[string]string{"reason": "ordinary resume"}))
		if err := ValidateExecutionStops(stream, nil); err == nil {
			t.Fatal("ordinary resume followed a legacy stop suspension")
		}
	})
}

func TestValidateExecutionStopsRejectsOrdinaryPostRequestActivity(t *testing.T) {
	for _, eventType := range []string{"RESULT_PUBLISHED", "CANDIDATE_COMPLETE", "EVIDENCE_PUBLISHED", "MESSAGE"} {
		t.Run(eventType, func(t *testing.T) {
			stream, freezes := executionStopHistory(t, "org-1", "task-1", 10, "execution_cancelled", false, false)
			request := stream[1]
			stream = stream[:3]
			stream = append(stream, executionStopEvent("org-1", request.CorrelationID, request.TaskID, request.SourceExecutionID, request.Sequence+2, "ordinary-"+eventType, eventType, map[string]any{"value": "forbidden"}))
			if err := ValidateExecutionStops(stream, freezes); err == nil {
				t.Fatal("ordinary execution activity followed stop request")
			}
		})
	}
}

func TestValidateExecutionStopsRequiresTruthfulLocalInterruptionEvidence(t *testing.T) {
	tests := map[string]func(*core.ExecutionInterruptionEvidence){
		"missing request binding": func(evidence *core.ExecutionInterruptionEvidence) { evidence.StopRequestRef = "" },
		"remote stop claimed": func(evidence *core.ExecutionInterruptionEvidence) {
			evidence.ProviderStop = &core.ProviderStopEvidence{LocalTurnStopped: true, LocalProcessStopAttempted: true, LocalProcessStopped: true, RemoteStatus: "STOPPED"}
		},
		"process stopped without attempt": func(evidence *core.ExecutionInterruptionEvidence) {
			evidence.ProviderStop = &core.ProviderStopEvidence{LocalTurnStopped: true, LocalProcessStopped: true, RemoteStatus: "UNCERTAIN"}
		},
		"process stopped without local turn": func(evidence *core.ExecutionInterruptionEvidence) {
			evidence.ProviderStop = &core.ProviderStopEvidence{LocalProcessStopAttempted: true, LocalProcessStopped: true, RemoteStatus: "UNCERTAIN"}
		},
		"external effects resolved": func(evidence *core.ExecutionInterruptionEvidence) { evidence.ExternalEffectsStatus = "STOPPED" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			stream, freezes := executionStopHistory(t, "org-1", "task-1", 10, "execution_cancelled", false, false)
			outcomeEvent := &stream[executionStopEventIndexByType(t, stream, "TOOL_OUTCOME_RECORDED")]
			var outcome core.ToolOutcome
			if err := json.Unmarshal(outcomeEvent.Payload, &outcome); err != nil {
				t.Fatal(err)
			}
			body := executionStopJSON(outcome.ObservedEffect)
			var evidence core.ExecutionInterruptionEvidence
			if err := json.Unmarshal(body, &evidence); err != nil {
				t.Fatal(err)
			}
			mutate(&evidence)
			outcome.ObservedEffect = evidence
			outcomeEvent.Payload = executionStopJSON(outcome)
			if err := ValidateExecutionStops(stream, freezes); err == nil {
				t.Fatal("untruthful interruption evidence was accepted")
			}
		})
	}
}

func TestValidateExecutionStopsAcceptsBoundedProviderStopEvidence(t *testing.T) {
	stream, freezes := executionStopHistory(t, "org-1", "task-1", 10, "execution_cancelled", false, false)
	outcomeEvent := &stream[executionStopEventIndexByType(t, stream, "TOOL_OUTCOME_RECORDED")]
	var outcome core.ToolOutcome
	if err := json.Unmarshal(outcomeEvent.Payload, &outcome); err != nil {
		t.Fatal(err)
	}
	var evidence core.ExecutionInterruptionEvidence
	if err := json.Unmarshal(executionStopJSON(outcome.ObservedEffect), &evidence); err != nil {
		t.Fatal(err)
	}
	evidence.ProviderStop = &core.ProviderStopEvidence{LocalTurnStopped: true, LocalProcessStopAttempted: true, LocalProcessStopped: true, RemoteStatus: "UNCERTAIN"}
	outcome.ObservedEffect = evidence
	outcomeEvent.Payload = executionStopJSON(outcome)
	if err := ValidateExecutionStops(stream, freezes); err != nil {
		t.Fatalf("bounded local provider-stop evidence rejected: %v", err)
	}
}

func TestValidateExecutionStopsRejectsUnboundStopAudit(t *testing.T) {
	stream, freezes := executionStopHistory(t, "org-1", "task-1", 10, "execution_cancelled", false, false)
	stream = stream[:4]
	if err := ValidateExecutionStops(stream, freezes); err == nil {
		t.Fatal("interruption outcome without a confirmed stop binding was accepted")
	}
}

func TestValidateExecutionStopsBindsEarliestHold(t *testing.T) {
	stream, freezes := executionStopHistory(t, "org-1", "task-1", 10, "security_hold", true, false)
	requestIndex := executionStopEventIndexByType(t, stream, "EXECUTION_STOP_REQUESTED")
	for index := requestIndex; index < len(stream); index++ {
		stream[index].Sequence++
		stream[index].CreatedAt = time.Unix(stream[index].Sequence, 0).UTC()
	}
	requestValue := stream[requestIndex]
	stream[requestIndex+1] = executionStopTaskProjection(t, "org-1", requestValue.CorrelationID, requestValue.TaskID, requestValue.Sequence+1, "TASK_EXECUTION_SUSPENDED", 3, core.TaskBlocked, ExecutionSuspension{StopRequestRef: requestValue.EventID, ExecutionStartRef: stream[0].EventID})
	later := OrganizationFreezeAdmission{OrganizationID: "org-1", EventRef: "freeze-later", Frozen: true, Sequence: freezes[0].Sequence + 1, Version: 2}
	stream = append(stream[:requestIndex], append([]Event{executionStopEvent("org-1", "", "", "", later.Sequence, later.EventRef, "FREEZE_SET", map[string]any{"frozen": true})}, stream[requestIndex:]...)...)
	request := &stream[executionStopEventIndexByType(t, stream, "EXECUTION_STOP_REQUESTED")]
	request.Payload = executionStopJSON(ExecutionStopRequest{ExecutionStartRef: stream[0].EventID, ReasonClass: "security_hold", Hold: &core.SecurityHoldCause{OrganizationID: "org-1", EventRef: later.EventRef, Sequence: later.Sequence}})
	if err := ValidateExecutionStops(stream, append(freezes, later)); err == nil {
		t.Fatal("later hold replaced earliest committed hold")
	}
}

func TestValidateExecutionStopsRequiresCommittedHoldForSecurityReason(t *testing.T) {
	stream, _ := executionStopHistory(t, "org-1", "task-1", 10, "security_hold", true, false)
	if err := ValidateExecutionStops(stream, nil); err == nil {
		t.Fatal("security-hold stop without its committed freeze admission was accepted")
	}
}

func executionStopLegacySuspension(t *testing.T, organization, taskID string, startSequence int64) []Event {
	t.Helper()
	correlation := "work-" + organization
	start := executionStopTaskProjection(t, organization, correlation, taskID, startSequence, "EXECUTION_STARTED", 2, core.TaskRunning, ExecutionStartDetail{InboxCutoffSequence: 0})
	suspension := executionStopTaskProjection(t, organization, correlation, taskID, startSequence+1, "TASK_EXECUTION_SUSPENDED", 3, core.TaskBlocked, map[string]any{
		"execution_start_ref": start.EventID, "outcome_event_ref": "legacy-outcome", "reason": "legacy security reconciliation required",
	})
	return []Event{start, suspension}
}

func executionStopEventIndexByType(t *testing.T, stream []Event, eventType string) int {
	t.Helper()
	for index := range stream {
		if stream[index].EventType == eventType {
			return index
		}
	}
	t.Fatalf("event %s not found", eventType)
	return -1
}

func executionStopHistory(t *testing.T, organization, taskID string, startSequence int64, reason string, withHold, withUsage bool) ([]Event, []OrganizationFreezeAdmission) {
	t.Helper()
	correlation := "work-" + organization
	start := executionStopTaskProjection(t, organization, correlation, taskID, startSequence, "EXECUTION_STARTED", 2, core.TaskRunning, ExecutionStartDetail{InboxCutoffSequence: 0})
	executionID, err := ContainmentExecutionID(start)
	if err != nil {
		t.Fatal(err)
	}
	stream := []Event{start}
	requestSequence := startSequence + 1
	var hold *core.SecurityHoldCause
	var freezes []OrganizationFreezeAdmission
	if withHold {
		freezeSequence := requestSequence
		requestSequence++
		hold = &core.SecurityHoldCause{OrganizationID: core.ID(organization), EventRef: "freeze-" + organization, Sequence: freezeSequence}
		stream = append(stream, executionStopEvent(organization, correlation, taskID, "", freezeSequence, hold.EventRef, "FREEZE_SET", map[string]any{"frozen": true}))
		freezes = []OrganizationFreezeAdmission{{OrganizationID: core.ID(organization), EventRef: hold.EventRef, Frozen: true, Sequence: freezeSequence, Version: 1}}
	}
	request := executionStopEvent(organization, correlation, taskID, executionID, requestSequence, "stop-request-"+organization, "EXECUTION_STOP_REQUESTED", ExecutionStopRequest{
		ExecutionStartRef: start.EventID, ReasonClass: reason, Hold: hold,
	})
	suspension := executionStopTaskProjection(t, organization, correlation, taskID, requestSequence+1, "TASK_EXECUTION_SUSPENDED", 3, core.TaskBlocked, ExecutionSuspension{
		StopRequestRef: request.EventID, ExecutionStartRef: start.EventID,
	})
	stream = append(stream, request, suspension)

	outcomeSequence := requestSequence + 2
	outcome := core.ToolOutcome{
		ToolInvocationID: core.ID("held-" + executionID), ToolID: "runtime-containment",
		Status: core.OutcomeFailed, PostconditionStatus: core.PostconditionNotChecked, Retryability: core.NotRetryable,
		ErrorClass: reason, ErrorDetail: "execution interrupted before result admission",
		ObservedEffect: core.ExecutionInterruptionEvidence{StopRequestRef: request.EventID, Hold: hold, LocalExecutionStopped: true, ExternalEffectsStatus: "REQUIRES_RECONCILIATION"},
		StartedAt:      time.Unix(outcomeSequence-1, 0).UTC(), FinishedAt: time.Unix(outcomeSequence, 0).UTC(),
	}
	outcomeEvent := executionStopEvent(organization, correlation, taskID, executionID, outcomeSequence, "stop-outcome-"+organization, "TOOL_OUTCOME_RECORDED", outcome)
	stream = append(stream, outcomeEvent)

	result := ExecutionStopResult{StopRequestRef: request.EventID, OutcomeEventRef: outcomeEvent.EventID}
	nextSequence := outcomeSequence + 1
	if withUsage {
		usage := InferenceUsageRecordedPayload{Source: "model", Provider: "provider", Model: "model", InputTokens: 2, OutputTokens: 3, TotalTokens: 5}
		usageEvent := executionStopEvent(organization, correlation, taskID, executionID, nextSequence, "stop-usage-"+organization, "INFERENCE_USAGE_RECORDED", usage)
		result.UsageEventRef = usageEvent.EventID
		stream = append(stream, usageEvent)
		nextSequence++
	}
	finish := executionStopEvent(organization, correlation, taskID, executionID, nextSequence, "stop-finish-"+organization, "EXECUTION_FINISHED", struct {
		Status core.ToolOutcomeStatus `json:"status"`
	}{Status: core.OutcomeFailed})
	result.FinishEventRef = finish.EventID
	confirmed := executionStopEvent(organization, correlation, taskID, executionID, nextSequence+1, "stop-confirmed-"+organization, "EXECUTION_STOP_CONFIRMED", result)
	stream = append(stream, finish, confirmed)
	return stream, freezes
}

func executionStopEvent(organization, correlation, taskID, executionID string, sequence int64, eventID, eventType string, payload any) Event {
	actor := "runtime"
	if eventType == "FREEZE_SET" {
		actor = "owner"
	}
	return Event{
		EventID: eventID, Sequence: sequence, OrganizationID: organization, EventType: eventType,
		SourceActorID: actor, SourceExecutionID: executionID, TaskID: taskID, CorrelationID: correlation,
		CreatedAt: time.Unix(sequence, 0).UTC(), SchemaVersion: SchemaVersion, Payload: executionStopJSON(payload),
	}
}

func executionStopTaskProjection(t *testing.T, organization, correlation, taskID string, sequence int64, eventType string, version int, status core.TaskStatus, detail any) Event {
	t.Helper()
	task := core.Task{
		ID: core.ID(taskID), WorkID: core.ID(correlation), Description: "bounded task",
		ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden,
		TaskContractVersion: "1", Status: status,
	}
	event := Event{
		EventID:  "task-event-" + organization + "-" + taskID + "-" + eventType,
		Sequence: sequence, OrganizationID: organization, EventType: eventType, SourceActorID: "runtime",
		TaskID: taskID, CorrelationID: correlation, CreatedAt: time.Unix(sequence, 0).UTC(), SchemaVersion: SchemaVersion,
	}
	sealed, err := SealProjectionEvent(event, ProjectionRecord{
		ProjectionKind: "task", RecordID: taskID, Version: version, CorrelationID: correlation, Value: executionStopJSON(task),
	}, executionStopJSON(detail))
	if err != nil {
		t.Fatal(err)
	}
	event.Payload = executionStopJSON(sealed)
	return event
}

func executionStopJSON(value any) json.RawMessage {
	body, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return body
}

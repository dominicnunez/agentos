package events

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/dominicnunez/agentos/internal/core"
)

// HasExecutionStopEvidence identifies new runtime stop claims. Legacy outcomes
// without these fields retain their historical admission contract.
func HasExecutionStopEvidence(outcome core.ToolOutcome) bool {
	if outcome.ToolID != "runtime-containment" {
		return false
	}
	body, err := json.Marshal(outcome.ObservedEffect)
	if err != nil {
		return false
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil {
		return false
	}
	_, request := fields["stop_request_ref"]
	_, provider := fields["provider_stop"]
	return request || provider
}

// ExecutionStopRequest describes runtime stop intent, not acknowledgement by a
// handler or a remote destination. Event metadata binds it to one execution.
type ExecutionStopRequest struct {
	ExecutionStartRef string                  `json:"execution_start_ref"`
	ReasonClass       string                  `json:"reason_class"`
	Hold              *core.SecurityHoldCause `json:"hold,omitempty"`
}

// ExecutionSuspension binds the non-runnable task revision to its stop request.
// Local stop can remain uncertain while this task is already suspended.
type ExecutionSuspension struct {
	StopRequestRef    string `json:"stop_request_ref"`
	ExecutionStartRef string `json:"execution_start_ref"`
}

// ExecutionStopResult is durable acknowledgement evidence. UNCERTAIN carries
// only the request reference. CONFIRMED also binds the final local handler
// outcome and finish, plus usage when present; it does not prove remote stop.
type ExecutionStopResult struct {
	StopRequestRef  string `json:"stop_request_ref"`
	OutcomeEventRef string `json:"outcome_event_ref,omitempty"`
	UsageEventRef   string `json:"usage_event_ref,omitempty"`
	FinishEventRef  string `json:"finish_event_ref,omitempty"`
}

type executionStopStore interface {
	RequestExecutionStop(context.Context, string, string, string, string, string) (Event, error)
	RecordExecutionStop(context.Context, string, *core.ToolOutcome, *InferenceUsageRecordedPayload) (Event, error)
}

// RequestExecutionStop atomically records the request and suspends the exact
// running task. Only the runtime can request this transition; it grants no work.
func (g *Gateway) RequestExecutionStop(ctx context.Context, organization, taskID, correlation, executionID, reason string) (Event, error) {
	store, ok := g.ledger.(executionStopStore)
	if !ok {
		return Event{}, fmt.Errorf("durable execution stop is unavailable")
	}
	return store.RequestExecutionStop(ctx, organization, taskID, correlation, executionID, reason)
}

// RecordExecutionStop records uncertainty when outcome is nil. A returned
// handler's interrupted outcome, accounting and local finish are otherwise
// persisted atomically with confirmation, without publishing ordinary results.
func (g *Gateway) RecordExecutionStop(ctx context.Context, requestRef string, outcome *core.ToolOutcome, usage *InferenceUsageRecordedPayload) (Event, error) {
	store, ok := g.ledger.(executionStopStore)
	if !ok {
		return Event{}, fmt.Errorf("durable execution stop is unavailable")
	}
	return store.RecordExecutionStop(ctx, requestRef, outcome, usage)
}

func RequiresExecutionStopAdmission(eventType string) bool {
	return eventType == "EXECUTION_STOP_REQUESTED" || eventType == "EXECUTION_STOP_UNCERTAIN" || eventType == "EXECUTION_STOP_CONFIRMED"
}

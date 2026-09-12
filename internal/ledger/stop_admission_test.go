package ledger

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

type stopAdmissionRecordingLedger struct {
	appendCalled bool
}

func (l *stopAdmissionRecordingLedger) Append(context.Context, events.TrustedDraft) (events.Event, error) {
	l.appendCalled = true
	return events.Event{}, nil
}

func (*stopAdmissionRecordingLedger) Events(context.Context, string) ([]events.Event, error) {
	return nil, nil
}

func TestGatewayRejectsGenericExecutionStopEvents(t *testing.T) {
	ledger := &stopAdmissionRecordingLedger{}
	gateway := events.NewGateway(ledger)
	for _, eventType := range []string{"EXECUTION_STOP_REQUESTED", "EXECUTION_STOP_UNCERTAIN", "EXECUTION_STOP_CONFIRMED"} {
		t.Run(eventType, func(t *testing.T) {
			ledger.appendCalled = false
			if _, err := gateway.PublishTrusted(t.Context(), events.TrustedDraft{EventType: eventType, Payload: map[string]any{"unexpected": true}}); err == nil || !strings.Contains(err.Error(), "typed execution stop admission") {
				t.Fatalf("generic stop event denial = %v", err)
			}
			if ledger.appendCalled {
				t.Fatal("gateway forwarded a reserved stop event")
			}
		})
	}
}

func TestSQLiteRejectsGenericExecutionStopEventsAndSuspensionProjection(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	for _, eventType := range []string{"EXECUTION_STOP_REQUESTED", "EXECUTION_STOP_UNCERTAIN", "EXECUTION_STOP_CONFIRMED"} {
		if _, err := store.Append(t.Context(), events.TrustedDraft{EventType: eventType, Payload: map[string]any{"unexpected": true}}); err == nil || !strings.Contains(err.Error(), "typed execution stop admission") {
			t.Fatalf("generic SQLite %s denial = %v", eventType, err)
		}
	}
	if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{EventType: "TASK_EXECUTION_SUSPENDED"}}); err == nil || !strings.Contains(err.Error(), "typed execution stop admission") {
		t.Fatalf("generic suspension projection denial = %v", err)
	}
	if count := stopAdmissionEventCount(t, store); count != 0 {
		t.Fatalf("reserved generic admission wrote %d events", count)
	}
}

func TestStoppedExecutionRejectsGenericPublication(t *testing.T) {
	store, request := setupStoppedTaskInference(t)
	before := stopAdmissionEventCount(t, store)
	drafts := []events.TrustedDraft{
		{
			OrganizationID: request.Scope.OrganizationID, EventType: "RESULT_PUBLISHED", SourceActorID: "runtime",
			SourceExecutionID: request.Scope.ExecutionID, TaskID: request.Scope.TaskID, CorrelationID: request.Scope.CorrelationID,
			Payload: map[string]any{"summary": "forbidden output"},
		},
		{
			OrganizationID: request.Scope.OrganizationID, EventType: "TOOL_OUTCOME_RECORDED", SourceActorID: "runtime",
			SourceExecutionID: request.Scope.ExecutionID, TaskID: request.Scope.TaskID, CorrelationID: request.Scope.CorrelationID,
			Payload: core.ToolOutcome{
				ToolInvocationID: "late-tool", ToolID: "deterministic", Status: core.OutcomeFailed,
				PostconditionStatus: core.PostconditionNotChecked, Retryability: core.NotRetryable,
				StartedAt: time.Unix(1, 0).UTC(), FinishedAt: time.Unix(2, 0).UTC(),
			},
		},
		{
			OrganizationID: request.Scope.OrganizationID, EventType: "INFERENCE_USAGE_RECORDED", SourceActorID: "runtime",
			SourceExecutionID: request.Scope.ExecutionID, TaskID: request.Scope.TaskID, CorrelationID: request.Scope.CorrelationID,
			Payload: events.InferenceUsageRecordedPayload{Source: "provider", Provider: "provider", Model: "model", InputTokens: 1, OutputTokens: 1, TotalTokens: 2},
		},
		{
			OrganizationID: request.Scope.OrganizationID, EventType: "EXECUTION_FINISHED", SourceActorID: "runtime",
			SourceExecutionID: request.Scope.ExecutionID, TaskID: request.Scope.TaskID, CorrelationID: request.Scope.CorrelationID,
			Payload: map[string]any{"status": core.OutcomeFailed},
		},
	}
	for _, draft := range drafts {
		t.Run(draft.EventType, func(t *testing.T) {
			if _, err := store.Append(t.Context(), draft); !errors.Is(err, core.ErrExecutionStopped) {
				t.Fatalf("stopped execution publication error = %v", err)
			}
		})
	}
	if after := stopAdmissionEventCount(t, store); after != before {
		t.Fatalf("denied audit publication changed event count from %d to %d", before, after)
	}
}

func TestStoppedExecutionRejectsNewTaskInferenceWithoutReservation(t *testing.T) {
	store, request := setupStoppedTaskInference(t)
	if err := store.CheckExecutionContainment(t.Context(), request.Scope.OrganizationID, request.Scope.TaskID, request.Scope.CorrelationID, request.Scope.ExecutionID); !errors.Is(err, core.ErrExecutionStopped) || errors.Is(err, core.ErrContainmentUnavailable) {
		t.Fatalf("durable stop was misclassified as an authority read failure: %v", err)
	}
	if _, err := store.ReserveInference(t.Context(), request); !errors.Is(err, core.ErrExecutionStopped) {
		t.Fatalf("stopped task inference error = %v", err)
	}
	var reservations int
	if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM inference_reservations WHERE organization_id=? AND execution_id=?`, request.Scope.OrganizationID, request.Scope.ExecutionID).Scan(&reservations); err != nil {
		t.Fatal(err)
	}
	if reservations != 0 {
		t.Fatalf("stopped execution admitted %d inference reservations", reservations)
	}
}

func TestStoppedTaskRejectsReplacementInference(t *testing.T) {
	store, request := setupStoppedTaskInference(t)
	request.Scope.ExecutionID = "replacement-execution"
	request.Scope.RequestID = request.Scope.ExecutionID
	if _, err := store.ReserveInference(t.Context(), request); err == nil {
		t.Fatal("replacement inference bypassed task execution admission")
	}
	var count int
	if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM inference_reservations WHERE task_id=?`, request.Scope.TaskID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("stopped task reserved inference: count=%d err=%v", count, err)
	}
}

func TestGenericOutcomeCannotClaimStop(t *testing.T) {
	store, request := setupAdmittedTaskInference(t)
	before := stopAdmissionEventCount(t, store)
	now := time.Now().UTC()
	outcome := core.ToolOutcome{ToolInvocationID: "invented", ToolID: "runtime-containment", Status: core.OutcomeFailed, PostconditionStatus: core.PostconditionNotChecked, Retryability: core.NotRetryable, ErrorClass: "execution_cancelled", StartedAt: now, FinishedAt: now, ObservedEffect: core.ExecutionInterruptionEvidence{StopRequestRef: "invented", LocalExecutionStopped: true, ExternalEffectsStatus: "REQUIRES_RECONCILIATION"}}
	_, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: request.Scope.OrganizationID, TaskID: request.Scope.TaskID, CorrelationID: request.Scope.CorrelationID, SourceExecutionID: request.Scope.ExecutionID, SourceActorID: "runtime", EventType: "TOOL_OUTCOME_RECORDED", Payload: outcome})
	if err == nil || stopAdmissionEventCount(t, store) != before {
		t.Fatal("generic outcome forged a stop claim")
	}
}

func TestExecutionStopAdmissionUsesExactExecutionAndTenant(t *testing.T) {
	store, request := setupStoppedTaskInference(t)
	tests := []events.TrustedDraft{
		{
			OrganizationID: request.Scope.OrganizationID, EventType: "EXECUTION_FINISHED", SourceActorID: "runtime",
			SourceExecutionID: "other-execution", TaskID: request.Scope.TaskID, CorrelationID: request.Scope.CorrelationID,
			Payload: map[string]any{"status": core.OutcomeFailed},
		},
		{
			OrganizationID: "other-organization", EventType: "EXECUTION_FINISHED", SourceActorID: "runtime",
			SourceExecutionID: request.Scope.ExecutionID, TaskID: request.Scope.TaskID, CorrelationID: request.Scope.CorrelationID,
			Payload: map[string]any{"status": core.OutcomeFailed},
		},
	}
	for _, draft := range tests {
		if _, err := store.Append(t.Context(), draft); err != nil {
			t.Fatalf("unrelated execution %s/%s was denied: %v", draft.OrganizationID, draft.SourceExecutionID, err)
		}
	}
}

func setupStoppedTaskInference(t *testing.T) (*SQLite, inference.InferenceRequest) {
	t.Helper()
	store, request := setupAdmittedTaskInference(t)
	if _, err := store.RequestExecutionStop(t.Context(), request.Scope.OrganizationID, request.Scope.TaskID, request.Scope.CorrelationID, request.Scope.ExecutionID, "execution_cancelled"); err != nil {
		t.Fatal(err)
	}
	return store, request
}

func stopAdmissionEventCount(t *testing.T, store *SQLite) int {
	t.Helper()
	var count int
	if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM events`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

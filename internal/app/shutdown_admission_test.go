package app

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/projections"
)

type stopAdmissionLedger struct {
	*ledger.SQLite
	eventType string
	after     bool
	stop      func()
}

func (l *stopAdmissionLedger) trigger(eventType string, after bool) {
	if l.stop != nil && l.eventType == eventType && l.after == after {
		stop := l.stop
		l.stop = nil
		stop()
	}
}

func (l *stopAdmissionLedger) Append(ctx context.Context, draft events.TrustedDraft) (events.Event, error) {
	l.trigger(draft.EventType, false)
	event, err := l.SQLite.Append(ctx, draft)
	if err == nil {
		l.trigger(draft.EventType, true)
	}
	return event, err
}

func (l *stopAdmissionLedger) AppendProjection(ctx context.Context, draft events.ProjectionDraft) (events.Event, error) {
	l.trigger(draft.Event.EventType, false)
	event, err := l.SQLite.AppendProjection(ctx, draft)
	if err == nil {
		l.trigger(draft.Event.EventType, true)
	}
	return event, err
}

func TestShutdownDuringResultAdmission(t *testing.T) {
	for _, kind := range []core.ExecutionKind{core.ExecutionDeterministic, core.ExecutionAgent} {
		stages := []string{"TOOL_OUTCOME_RECORDED", "EXECUTION_FINISHED", "RESULT_PUBLISHED", "CANDIDATE_COMPLETE"}
		if kind == core.ExecutionAgent {
			stages = append(stages, "INFERENCE_USAGE_RECORDED", "COMPLETION_REVIEW_REQUESTED", "TASK_BLOCKED")
		} else {
			stages = append(stages, "COMPLETION_VERIFIED", "TASK_VERIFIED_COMPLETE")
		}
		for _, stage := range stages {
			for _, after := range []bool{false, true} {
				// Terminal task writes have already ended the execution. Test their
				// admission, not an unrelated shutdown after that durable boundary.
				if after && (stage == "TASK_BLOCKED" || stage == "TASK_VERIFIED_COMPLETE") {
					continue
				}
				t.Run(fmt.Sprintf("%s/%s/after=%t", kind, stage, after), func(t *testing.T) {
					store, err := ledger.Open(":memory:")
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = store.Close() })
					writer := &stopAdmissionLedger{SQLite: store, eventType: stage, after: after}
					gateway := events.NewGateway(writer)
					service := NewWithModel(gateway, describedModel{})
					var stopSequence int64
					writer.stop = func() {
						stream, err := store.Events(t.Context(), "")
						if err != nil || len(stream) == 0 {
							t.Fatalf("read shutdown boundary: %v", err)
						}
						stopSequence = stream[len(stream)-1].Sequence
						service.StopExecutions()
					}
					_, submitErr := service.Submit(t.Context(), Submit{RequestID: "stop-admission", OrganizationID: "org-a", Statement: "echo bounded work", Kind: kind})
					if writer.stop != nil {
						t.Fatalf("shutdown boundary was not reached: %v", submitErr)
					}
					ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
					defer cancel()
					if err := service.WaitForStops(ctx); err != nil {
						t.Fatal(err)
					}
					stream, err := store.Events(t.Context(), "")
					if err != nil {
						t.Fatal(err)
					}
					verified := stage == "TASK_VERIFIED_COMPLETE" || (stage == "COMPLETION_VERIFIED" && after)
					wantStops, wantStatus := 1, core.TaskBlocked
					if verified {
						wantStops, wantStatus = 0, core.TaskCompleted
					}
					if countEventType(stream, "EXECUTION_STOP_REQUESTED") != wantStops || countEventType(stream, "EXECUTION_STOP_CONFIRMED") != wantStops {
						t.Fatalf("stop evidence missing or completion was reversed: requested=%d confirmed=%d want=%d submit=%v", countEventType(stream, "EXECUTION_STOP_REQUESTED"), countEventType(stream, "EXECUTION_STOP_CONFIRMED"), wantStops, submitErr)
					}
					for _, event := range stream {
						if event.Sequence <= stopSequence {
							continue
						}
						switch event.EventType {
						case "RESULT_PUBLISHED", "CANDIDATE_COMPLETE", "COMPLETION_VERIFIED", "COMPLETION_REVIEW_REQUESTED":
							t.Errorf("ordinary %s admitted after shutdown", event.EventType)
						}
					}
					if kind == core.ExecutionAgent && countEventType(stream, "INFERENCE_USAGE_RECORDED") != 1 {
						t.Fatal("shutdown lost or duplicated actual usage")
					}
					if _, err := New(events.NewGateway(store)).Recover(t.Context()); err != nil {
						t.Fatalf("recover shutdown boundary: %v", err)
					}
					snapshot, err := projections.New(events.NewGateway(store)).Rebuild(t.Context())
					if err != nil {
						t.Fatal(err)
					}
					for _, task := range snapshot.Tasks {
						if task.Value.Status != wantStatus {
							t.Fatalf("recovered task=%s want=%s", task.Value.Status, wantStatus)
						}
					}
				})
			}
		}
	}
}

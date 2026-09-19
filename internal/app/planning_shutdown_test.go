package app

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/planning"
	"github.com/dominicnunez/agentos/internal/projections"
)

type shutdownPlanningPlanner struct {
	started   chan struct{}
	cancelled chan struct{}
	release   chan struct{}
	once      sync.Once
}

func completedPlanningResult() planning.Result {
	usage := events.InferenceUsageRecordedPayload{
		Source: "test", Provider: "test-provider", Model: "test-model",
		InputTokens: 1, OutputTokens: 1, TotalTokens: 2,
	}
	return planning.Result{
		Tasks: []core.PlanTask{{
			Key: "root", Description: "prepare the requested note", ExecutionKind: core.ExecutionAgent,
			ModelInferencePolicy: core.InferenceRequired,
		}},
		Usage: &usage,
	}
}

func (p *shutdownPlanningPlanner) Descriptor() (planning.Descriptor, bool) {
	return planning.Descriptor{
		PromptVersion:           "shutdown-planning-v1",
		Provider:                "test-provider",
		Model:                   "test-model",
		ExecutionProfileVersion: "test-profile-v1",
	}, true
}

func (p *shutdownPlanningPlanner) Build(ctx context.Context, input planning.Input, kind core.ExecutionKind) (planning.Result, error) {
	if kind == core.ExecutionDeterministic {
		return (planning.SingleTaskPlanner{}).Build(ctx, input, kind)
	}
	close(p.started)
	select {
	case <-ctx.Done():
		close(p.cancelled)
	case <-p.release:
	}
	<-p.release
	return completedPlanningResult(), nil
}

func TestHeldPlannerReleasesOtherTenant(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	planner := &shutdownPlanningPlanner{started: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{})}
	service := NewWithModelAndPlanner(events.NewGateway(store), execution.FakeModel{}, planner)
	finished := make(chan error, 1)
	go func() {
		_, err := service.Submit(t.Context(), Submit{RequestID: "held-model", OrganizationID: "org-1", Statement: "prepare a note", Kind: core.ExecutionAgent})
		finished <- err
	}()
	t.Cleanup(func() {
		planner.finish()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := service.WaitForStops(ctx); err != nil {
			t.Error(err)
		}
	})
	select {
	case <-planner.started:
	case <-time.After(5 * time.Second):
		t.Fatal("planner did not start")
	}
	setAppTestFreeze(t, t.Context(), store, "org-1", 1, true)
	setAppTestFreeze(t, t.Context(), store, "org-1", 2, false)
	select {
	case err := <-finished:
		if !errors.Is(err, core.ErrOrganizationFrozen) {
			t.Fatalf("hold cause lost: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("held model retained the submission permit")
	}
	// Check the admission boundary directly. Completing the unrelated Task
	// also exercises its ordinary ledger work, which is not a stop deadline.
	select {
	case service.permit <- struct{}{}:
		service.release()
	default:
		t.Fatal("held model retained the submission permit after returning")
	}
	otherDone := make(chan error, 1)
	go func() {
		result, err := service.Submit(t.Context(), Submit{RequestID: "other-model", OrganizationID: "org-2", Statement: "echo unaffected", Kind: core.ExecutionDeterministic})
		if err == nil && result.Task.Status != core.TaskCompleted {
			err = errors.New("unrelated work did not complete")
		}
		otherDone <- err
	}()
	select {
	case err := <-otherDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("unrelated task did not finish after admission was released")
	}
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if countEventType(stream, "MODEL_STOP_REQUESTED") != 1 || countEventType(stream, "MODEL_STOP_UNCERTAIN") != 1 || countEventType(stream, "MODEL_STOP_CONFIRMED") != 0 {
		t.Fatal("blocked planner acquired false local acknowledgement")
	}
	planner.finish()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := service.WaitForStops(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := projections.New(events.NewGateway(store)).Rebuild(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func (p *shutdownPlanningPlanner) finish() {
	p.once.Do(func() { close(p.release) })
}

func TestPlanningDeadlineRetainsStop(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	planner := &shutdownPlanningPlanner{started: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{})}
	service := NewWithModelAndPlanner(events.NewGateway(store), execution.FakeModel{}, planner)
	service.modelTurnTimeout = 200 * time.Millisecond
	t.Cleanup(func() {
		planner.finish()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := service.WaitForStops(ctx); err != nil {
			t.Error(err)
		}
	})
	finished := make(chan error, 1)
	go func() {
		_, err := service.Submit(t.Context(), Submit{RequestID: "planning-deadline", OrganizationID: "org-1", Statement: "prepare a note", Kind: core.ExecutionAgent})
		finished <- err
	}()
	select {
	case <-planner.started:
	case <-time.After(5 * time.Second):
		t.Fatal("planner did not start")
	}
	select {
	case err := <-finished:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("deadline cause lost: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("planning deadline waited for blocked callback")
	}
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if countEventType(stream, "MODEL_STOP_UNCERTAIN") != 1 || countEventType(stream, "PLANNING_FAILED") != 0 {
		t.Fatal("deadline lost uncertainty or published ordinary failure")
	}
	for _, event := range stream {
		if event.EventType != "MODEL_STOP_REQUESTED" {
			continue
		}
		var detail events.ModelStopRequest
		if json.Unmarshal(event.Payload, &detail) != nil || detail.ReasonClass != "deadline_exceeded" {
			t.Fatalf("wrong stop cause: %+v", detail)
		}
	}
	planner.finish()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := service.WaitForStops(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := projections.New(events.NewGateway(store)).Rebuild(t.Context()); err != nil {
		t.Fatal(err)
	}
}

type failedModelStopLedger struct {
	*ledger.SQLite
	failure error
}

func (l failedModelStopLedger) RequestModelStop(context.Context, string, string) (events.Event, bool, error) {
	return events.Event{}, false, l.failure
}

func TestPlanningStopWriteFailureKeepsCallOwned(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	failure := errors.New("stop writer unavailable")
	planner := &shutdownPlanningPlanner{started: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{})}
	service := NewWithModelAndPlanner(events.NewGateway(failedModelStopLedger{SQLite: store, failure: failure}), execution.FakeModel{}, planner)
	finished := make(chan error, 1)
	go func() {
		_, err := service.Submit(t.Context(), Submit{RequestID: "failed-stop-write", OrganizationID: "org-1", Statement: "prepare a note", Kind: core.ExecutionAgent})
		finished <- err
	}()
	t.Cleanup(func() {
		planner.finish()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := service.WaitForStops(ctx); err != nil && !errors.Is(err, failure) {
			t.Error(err)
		}
	})
	select {
	case <-planner.started:
	case <-time.After(5 * time.Second):
		t.Fatal("planner did not start")
	}
	service.StopExecutions()
	select {
	case err := <-finished:
		if !errors.Is(err, failure) || !errors.Is(err, core.ErrExecutionStopped) {
			t.Fatalf("stop failure lost: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("failed evidence write retained caller")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	err = service.WaitForStops(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("failed write released unreturned call ownership: %v", err)
	}
	planner.finish()
	ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := service.WaitForStops(ctx); !errors.Is(err, failure) {
		t.Fatalf("drain hid persistence failure: %v", err)
	}
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if countEventType(stream, "MODEL_STOP_CONFIRMED") != 0 || countEventType(stream, "PLAN_CREATED") != 0 || countEventType(stream, "PLANNING_FAILED") != 0 {
		t.Fatal("failed stop persistence admitted a result or false acknowledgement")
	}
}

type completedPlanningPlanner struct {
	calls int
	err   error
}

func (p *completedPlanningPlanner) Descriptor() (planning.Descriptor, bool) {
	return (&shutdownPlanningPlanner{}).Descriptor()
}

func (p *completedPlanningPlanner) Build(context.Context, planning.Input, core.ExecutionKind) (planning.Result, error) {
	p.calls++
	return completedPlanningResult(), p.err
}

func TestShutdownPreservesCommittedPlanningDecision(t *testing.T) {
	for _, boundary := range []string{"PLAN_CREATED", "PLANNING_FAILED"} {
		t.Run(boundary, func(t *testing.T) {
			store, err := ledger.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			writer := &stopAdmissionLedger{SQLite: store, eventType: boundary, after: true}
			planner := &completedPlanningPlanner{}
			if boundary == "PLANNING_FAILED" {
				planner.err = errors.New("provider rejected request")
			}
			service := NewWithModelAndPlanner(events.NewGateway(writer), execution.FakeModel{}, planner)
			writer.stop = service.StopExecutions
			_, _ = service.Submit(t.Context(), Submit{RequestID: "committed-planning", OrganizationID: "org-1", Statement: "prepare a note", Kind: core.ExecutionAgent})
			if writer.stop != nil {
				t.Fatal("committed decision boundary was not reached")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if err := service.WaitForStops(ctx); err != nil {
				t.Fatal(err)
			}
			stream, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if countEventType(stream, boundary) != 1 || countEventType(stream, "MODEL_STOP_REQUESTED") != 0 {
				t.Fatal("shutdown reclassified an already committed planning decision")
			}
			if _, err := projections.New(events.NewGateway(store)).Rebuild(t.Context()); err != nil {
				t.Fatal(err)
			}
			recovered := NewWithModelAndPlanner(events.NewGateway(store), execution.FakeModel{}, planner)
			if _, err := recovered.Recover(t.Context()); err != nil {
				t.Fatal(err)
			}
			if planner.calls != 1 {
				t.Fatal("recovery repeated a completed planning call")
			}
		})
	}
}

func TestShutdownCancelsActiveAdaptivePlanning(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	gateway := events.NewGateway(store)
	planner := &shutdownPlanningPlanner{
		started: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{}),
	}
	service := NewWithModelAndPlanner(gateway, execution.FakeModel{}, planner)
	finished := make(chan error, 1)
	submitDone := make(chan struct{})
	go func() {
		defer close(submitDone)
		_, submitErr := service.Submit(t.Context(), Submit{
			RequestID: "shutdown-planning", OrganizationID: "org-1", Statement: "prepare a note", Kind: core.ExecutionAgent,
		})
		finished <- submitErr
	}()
	t.Cleanup(func() {
		planner.finish()
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelCleanup()
		select {
		case <-submitDone:
		case <-cleanupCtx.Done():
			t.Error("planning submission did not finish")
		}
		if err := service.WaitForStops(cleanupCtx); err != nil {
			t.Error(err)
		}
	})

	select {
	case <-planner.started:
	case <-time.After(5 * time.Second):
		t.Fatal("adaptive planning did not start")
	}
	service.StopExecutions()
	select {
	case <-planner.cancelled:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel active adaptive planning")
	}

	drainCtx, cancelDrain := context.WithTimeout(t.Context(), 500*time.Millisecond)
	err = service.WaitForStops(drainCtx)
	cancelDrain()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown falsely drained an unreturned planner: %v", err)
	}
	stopping, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if countEventType(stopping, "MODEL_STOP_REQUESTED") != 1 || countEventType(stopping, "MODEL_STOP_UNCERTAIN") != 1 || countEventType(stopping, "MODEL_STOP_CONFIRMED") != 0 {
		t.Fatal("planning shutdown did not preserve uncertain stop intent before handler return")
	}

	var submitErr error
	select {
	case submitErr = <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled adaptive planning did not return")
	}
	if !errors.Is(submitErr, core.ErrExecutionStopped) {
		t.Fatalf("planning shutdown error=%v", submitErr)
	}
	planner.finish()
	stopCtx, cancelStop := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelStop()
	if err := service.WaitForStops(stopCtx); err != nil {
		t.Fatal(err)
	}

	snapshot, err := projections.New(gateway).Rebuild(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Tasks) != 0 || len(snapshot.Works) != 1 {
		t.Fatalf("shutdown planning materialized work: tasks=%d works=%+v", len(snapshot.Tasks), snapshot.Works)
	}
	for _, work := range snapshot.Works {
		if work.Value.Status != core.WorkActive {
			t.Fatalf("shutdown terminalized interrupted planning: %+v", work)
		}
	}
	stream, err := service.ExternalEvents(t.Context(), "org-1", "shutdown-planning")
	if err != nil {
		t.Fatal(err)
	}
	if countEventType(stream, "PLANNING_CONTEXT_MANIFESTED") != 1 || countEventType(stream, "INFERENCE_USAGE_RECORDED") != 1 ||
		countEventType(stream, "MODEL_STOP_CONFIRMED") != 1 ||
		countEventType(stream, "PLAN_CREATED") != 0 || countEventType(stream, "PLANNING_FAILED") != 0 || countEventType(stream, "WORK_PLANNING_FAILED") != 0 {
		t.Fatal("shutdown planning lost accounting, admitted a result, or published ordinary failure")
	}
}

func TestShutdownDuringAdaptivePlanningAdmission(t *testing.T) {
	for _, test := range []struct {
		name, eventType       string
		after                 bool
		wantCalls             int
		wantManifest, wantUse int
	}{
		{name: "before context manifest", eventType: "PLANNING_CONTEXT_MANIFESTED", wantCalls: 0},
		{name: "after context manifest", eventType: "PLANNING_CONTEXT_MANIFESTED", after: true, wantManifest: 1},
		{name: "after usage accounting", eventType: "INFERENCE_USAGE_RECORDED", after: true, wantCalls: 1, wantManifest: 1, wantUse: 1},
		{name: "before plan admission", eventType: "PLAN_CREATED", wantCalls: 1, wantManifest: 1, wantUse: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := ledger.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			writer := &stopAdmissionLedger{SQLite: store, eventType: test.eventType, after: test.after}
			planner := &completedPlanningPlanner{}
			service := NewWithModelAndPlanner(events.NewGateway(writer), execution.FakeModel{}, planner)
			writer.stop = service.StopExecutions

			_, submitErr := service.Submit(t.Context(), Submit{
				RequestID: "shutdown-planning-admission", OrganizationID: "org-1", Statement: "prepare a note", Kind: core.ExecutionAgent,
			})
			if writer.stop != nil {
				t.Fatalf("shutdown boundary was not reached: %v", submitErr)
			}
			if !errors.Is(submitErr, core.ErrExecutionStopped) {
				t.Fatalf("planning shutdown error=%v", submitErr)
			}
			stopCtx, cancelStop := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancelStop()
			if err := service.WaitForStops(stopCtx); err != nil {
				t.Fatal(err)
			}
			stream, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if planner.calls != test.wantCalls || countEventType(stream, "PLANNING_CONTEXT_MANIFESTED") != test.wantManifest ||
				countEventType(stream, "INFERENCE_USAGE_RECORDED") != test.wantUse || countEventType(stream, "PLAN_CREATED") != 0 ||
				countEventType(stream, "PLANNING_FAILED") != 0 || countEventType(stream, "WORK_PLANNING_FAILED") != 0 {
				t.Fatalf("shutdown planning admission mismatch: calls=%d events=%+v", planner.calls, stream)
			}
			if countEventType(stream, "MODEL_STOP_REQUESTED") != test.wantManifest || countEventType(stream, "MODEL_STOP_CONFIRMED") != test.wantManifest {
				t.Fatal("manifested interruption lacks its exact local stop proof")
			}
			for _, event := range stream {
				if event.EventType != "MODEL_STOP_CONFIRMED" {
					continue
				}
				var detail events.ModelStopResult
				if err := json.Unmarshal(event.Payload, &detail); err != nil {
					t.Fatal(err)
				}
				want := "RETURNED"
				if test.wantCalls == 0 {
					want = "NOT_STARTED"
				}
				if detail.LocalState != want || (detail.ReturnedAt == nil) != (test.wantCalls == 0) {
					t.Fatalf("false local-return proof: %+v", detail)
				}
			}
			snapshot, err := projections.New(events.NewGateway(store)).Rebuild(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Tasks) != 0 || len(snapshot.Works) != 1 {
				t.Fatalf("shutdown materialized planning: tasks=%d works=%+v", len(snapshot.Tasks), snapshot.Works)
			}
			for _, work := range snapshot.Works {
				if work.Value.Status != core.WorkActive {
					t.Fatalf("shutdown terminalized planning: %+v", work)
				}
			}
			if test.name == "after context manifest" {
				recovered := NewWithModelAndPlanner(events.NewGateway(store), execution.FakeModel{}, planner)
				if _, err := recovered.Recover(t.Context()); err != nil {
					t.Fatal(err)
				}
				if planner.calls != 1 {
					t.Fatalf("unstarted attempt was not safely recoverable: calls=%d", planner.calls)
				}
				stream, err := store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				if countEventType(stream, "PLANNING_CONTEXT_MANIFESTED") != 2 || countEventType(stream, "INFERENCE_NOT_SENT") != 0 {
					t.Fatal("recovery reused the stopped attempt or fabricated guard proof")
				}
			}
		})
	}
}

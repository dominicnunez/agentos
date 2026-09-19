package app

import (
	"context"
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

func (p *shutdownPlanningPlanner) Build(ctx context.Context, _ planning.Input, _ core.ExecutionKind) (planning.Result, error) {
	close(p.started)
	select {
	case <-ctx.Done():
		close(p.cancelled)
	case <-p.release:
	}
	<-p.release
	return completedPlanningResult(), nil
}

func (p *shutdownPlanningPlanner) finish() {
	p.once.Do(func() { close(p.release) })
}

type completedPlanningPlanner struct{ calls int }

func (p *completedPlanningPlanner) Descriptor() (planning.Descriptor, bool) {
	return (&shutdownPlanningPlanner{}).Descriptor()
}

func (p *completedPlanningPlanner) Build(context.Context, planning.Input, core.ExecutionKind) (planning.Result, error) {
	p.calls++
	return completedPlanningResult(), nil
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
		select {
		case <-submitDone:
		case <-time.After(5 * time.Second):
			t.Error("planning submission did not finish")
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

	drainCtx, cancelDrain := context.WithTimeout(t.Context(), 50*time.Millisecond)
	err = service.WaitForStops(drainCtx)
	cancelDrain()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown falsely drained an unreturned planner: %v", err)
	}

	planner.finish()
	var submitErr error
	select {
	case submitErr = <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled adaptive planning did not return")
	}
	if !errors.Is(submitErr, core.ErrExecutionStopped) {
		t.Fatalf("planning shutdown error=%v", submitErr)
	}
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
		})
	}
}

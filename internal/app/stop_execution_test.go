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
	"github.com/dominicnunez/agentos/internal/modelinput"
	"github.com/dominicnunez/agentos/internal/projections"
)

type stubbornModel struct {
	execution.FakeModel
	started   chan struct{}
	cancelled chan struct{}
	release   chan struct{}
	once      sync.Once
}

func TestShutdownTracksActiveExecution(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	model := &stubbornModel{started: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{})}
	gateway := events.NewGateway(store)
	service := NewWithModel(gateway, model)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_, _ = service.Submit(t.Context(), Submit{RequestID: "shutdown-active", OrganizationID: "org-a", Statement: "prepare a note", Kind: core.ExecutionAgent})
	}()
	t.Cleanup(func() {
		model.finish()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		select {
		case <-finished:
		case <-ctx.Done():
			t.Error("active submission did not finish")
		}
		if err := service.WaitForStops(ctx); err != nil {
			t.Error(err)
		}
	})
	select {
	case <-model.started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}
	service.StopExecutions()
	select {
	case <-model.cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not cancel active handler")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	err = service.WaitForStops(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown falsely drained unresponsive handler: %v", err)
	}
	stream, err := gateway.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if countEventType(stream, "EXECUTION_STOP_REQUESTED") != 1 || countEventType(stream, "EXECUTION_STOP_UNCERTAIN") != 1 || countEventType(stream, "EXECUTION_STOP_CONFIRMED") != 0 {
		t.Fatal("active shutdown did not preserve uncertain stop intent")
	}
	if _, err := service.Submit(t.Context(), Submit{RequestID: "after-shutdown", OrganizationID: "org-b", Statement: "echo later", Kind: core.ExecutionDeterministic}); !errors.Is(err, core.ErrExecutionStopped) {
		t.Fatalf("shutdown admitted later work: %v", err)
	}
	model.finish()
	ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := service.WaitForStops(ctx); err != nil {
		t.Fatal(err)
	}
	stream, err = gateway.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if countEventType(stream, "EXECUTION_STOP_CONFIRMED") != 1 || countEventType(stream, "RESULT_PUBLISHED") != 0 {
		t.Fatal("shutdown lost local confirmation or published work")
	}
	replayed, err := projections.New(gateway).Rebuild(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range replayed.Tasks {
		if task.Value.Status != core.TaskBlocked {
			t.Fatal("replay revived stopped task")
		}
	}
}

func (m *stubbornModel) finish() { m.once.Do(func() { close(m.release) }) }

func (m *stubbornModel) CompleteRequest(ctx context.Context, request modelinput.Request) (execution.ModelResponse, error) {
	close(m.started)
	select {
	case <-ctx.Done():
		close(m.cancelled)
		<-m.release
	case <-m.release:
	}
	return m.FakeModel.CompleteRequest(ctx, request)
}

func TestHeldHandlerAllowsOtherTenant(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	model := &stubbornModel{
		started: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{}),
	}
	gateway := events.NewGateway(store)
	service := NewWithModel(gateway, model)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_, _ = service.Submit(t.Context(), Submit{
			RequestID: "held-handler", OrganizationID: "org-a", Statement: "prepare a note", Kind: core.ExecutionAgent,
		})
	}()
	t.Cleanup(func() {
		model.finish()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("submission did not finish after releasing the test handler")
		}
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := service.WaitForStops(stopCtx); err != nil {
			t.Error(err)
		}
	})
	select {
	case <-model.started:
	case <-finished:
		t.Fatal("submission ended before the model started")
	case <-time.After(5 * time.Second):
		t.Fatal("model did not start")
	}
	before, err := projections.New(gateway).Load(t.Context())
	if err != nil || len(before.Tasks) != 1 {
		t.Fatalf("expected one running task before containment: tasks=%d err=%v", len(before.Tasks), err)
	}
	var heldTaskID core.ID
	for id, state := range before.Tasks {
		if state.Value.Status != core.TaskRunning {
			t.Fatalf("model started without a running task: %s", state.Value.Status)
		}
		heldTaskID = id
	}
	setAppTestFreeze(t, t.Context(), store, "org-a", 1, true)
	select {
	case <-model.cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("committed hold did not reach the model")
	}
	otherCtx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	other, err := service.Submit(otherCtx, Submit{
		RequestID: "independent-handler", OrganizationID: "org-b", Statement: "echo independent", Kind: core.ExecutionDeterministic,
	})
	if err != nil || other.Task.Status != core.TaskCompleted {
		t.Fatalf("handler ignoring cancellation blocked another organization: task=%s err=%v", other.Task.Status, err)
	}
	snapshot, err := projections.New(gateway).Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	state, found := snapshot.Tasks[heldTaskID]
	if !found || state.Value.Status != core.TaskBlocked {
		t.Fatalf("held execution is not durably suspended while stop remains uncertain: found=%t status=%s", found, state.Value.Status)
	}
	stream, err := gateway.Events(t.Context(), state.CorrelationID)
	if err != nil {
		t.Fatal(err)
	}
	if countEventType(stream, "EXECUTION_STOP_REQUESTED") != 1 || countEventType(stream, "EXECUTION_STOP_UNCERTAIN") != 1 ||
		countEventType(stream, "EXECUTION_STOP_CONFIRMED") != 0 || countEventType(stream, "EXECUTION_FINISHED") != 0 || countEventType(stream, "RESULT_PUBLISHED") != 0 {
		t.Fatal("stop evidence does not distinguish requested/uncertain from confirmed local stop")
	}
	// Recovery must preserve the stopped task even after its organization is
	// released, while the original handler is still unresponsive.
	setAppTestFreeze(t, t.Context(), store, "org-a", 2, false)
	if recovered, err := New(gateway).Recover(t.Context()); err != nil || recovered.TasksExecuted != 0 {
		t.Fatalf("restart resumed unacknowledged work: %+v err=%v", recovered, err)
	}
	model.finish()
	stopCtx, stopCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer stopCancel()
	if err := service.WaitForStops(stopCtx); err != nil {
		t.Fatal(err)
	}
	stream, err = gateway.Events(t.Context(), state.CorrelationID)
	if err != nil {
		t.Fatal(err)
	}
	if countEventType(stream, "EXECUTION_STOP_CONFIRMED") != 1 || countEventType(stream, "EXECUTION_FINISHED") != 1 ||
		countEventType(stream, "INFERENCE_USAGE_RECORDED") != 1 || countEventType(stream, "RESULT_PUBLISHED") != 0 || countEventType(stream, "CANDIDATE_COMPLETE") != 0 {
		t.Fatal("late return lost accounting or published an ordinary result")
	}
	for _, event := range stream {
		if event.EventType != "TOOL_OUTCOME_RECORDED" {
			continue
		}
		var outcome core.ToolOutcome
		if err := json.Unmarshal(event.Payload, &outcome); err != nil {
			t.Fatal(err)
		}
		body, err := json.Marshal(outcome.ObservedEffect)
		if err != nil {
			t.Fatal(err)
		}
		var evidence core.ExecutionInterruptionEvidence
		if err := json.Unmarshal(body, &evidence); err != nil {
			t.Fatal(err)
		}
		if !evidence.LocalExecutionStopped || evidence.ProviderStop != nil || evidence.StopRequestRef == "" || evidence.ExternalEffectsStatus != "REQUIRES_RECONCILIATION" {
			t.Fatalf("late local return invented provider stop evidence: %+v", evidence)
		}
	}
	if recovered, err := New(gateway).Recover(t.Context()); err != nil || recovered.TasksExecuted != 0 {
		t.Fatalf("restart resumed confirmed but unreconciled work: %+v err=%v", recovered, err)
	}
}

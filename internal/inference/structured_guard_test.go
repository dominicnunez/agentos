package inference

import (
	"context"
	"errors"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/modelinput"
)

type structuredGuardModel struct {
	guardModel
	request modelinput.Request
}

func (m *structuredGuardModel) CompleteRequest(_ context.Context, request modelinput.Request) (execution.ModelResponse, error) {
	m.called = true
	m.request = request
	return m.response, m.err
}

func structuredGuardRequest() modelinput.Request {
	request := modelinput.Request{Version: modelinput.Version, Messages: []modelinput.Message{{Role: modelinput.User, Text: "data", Source: modelinput.Source{Kind: modelinput.TaskContext, Reference: "task-1", Digest: modelinput.TextDigest("data")}}}}
	scope, err := modelinput.InvocationScope("organization-1", "execution-1")
	if err != nil {
		panic(err)
	}
	binding, err := modelinput.Bind(scope, request)
	if err != nil {
		panic(err)
	}
	return binding.Request()
}

func TestStructuredGuardBindsCanonicalInputAndReconciles(t *testing.T) {
	store := &guardStore{}
	model := &structuredGuardModel{guardModel: guardModel{response: execution.ModelResponse{Text: "answer", Usage: events.InferenceUsageRecordedPayload{Source: "provider", Provider: "provider", Model: "model", InputTokens: 1, OutputTokens: 1, TotalTokens: 2}}}}
	adapter, err := NewGuardedAdapter(store, model)
	if err != nil {
		t.Fatal(err)
	}
	request := structuredGuardRequest()
	fingerprint, err := request.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.CompleteRequest(guardedContext(t), request); err != nil {
		t.Fatal(err)
	}
	if !model.called || store.result != ReconciliationCompleted || store.reservation.Request.PromptSHA256 != fingerprint {
		t.Fatal("structured request escaped reservation or reconciliation")
	}
	request.Messages[0].Text = "caller changed storage"
	if model.request.Messages[0].Text != "data" {
		t.Fatal("adapter retained caller-owned message storage")
	}
}

func TestStructuredGuardRejectsMissingCapabilityAndInvalidInput(t *testing.T) {
	store := &guardStore{}
	model := &guardModel{}
	adapter, _ := NewGuardedAdapter(store, model)
	if _, err := adapter.CompleteRequest(guardedContext(t), structuredGuardRequest()); err == nil || !execution.WasRequestNotSent(err) || model.called || store.reservation.ID != "" {
		t.Fatal("flattened into legacy adapter")
	}
	structured := &structuredGuardModel{}
	adapter, _ = NewGuardedAdapter(store, structured)
	request := structuredGuardRequest()
	request.Messages[0].Role = modelinput.System
	if _, err := adapter.CompleteRequest(guardedContext(t), request); err == nil || !execution.WasRequestNotSent(err) || structured.called || store.reservation.ID != "" {
		t.Fatal("promoted source before admission")
	}
	store.reserveErr = errors.New("budget exhausted")
	if _, err := adapter.CompleteRequest(guardedContext(t), structuredGuardRequest()); err == nil || !execution.WasRequestNotSent(err) || structured.called {
		t.Fatal("called provider without budget")
	}
}

func TestStructuredGuardUsesAdmittedScopeForSourceBinding(t *testing.T) {
	for _, mutation := range []string{"missing handle", "changed reference", "foreign invocation context"} {
		t.Run(mutation, func(t *testing.T) {
			store := &guardStore{}
			model := &structuredGuardModel{}
			adapter, err := NewGuardedAdapter(store, model)
			if err != nil {
				t.Fatal(err)
			}
			ctx := guardedContext(t)
			request := structuredGuardRequest()
			switch mutation {
			case "missing handle":
				request.Messages[0].Source.Handle = ""
			case "changed reference":
				request.Messages[0].Source.Reference = "task-2"
			case "foreign invocation context":
				ctx, err = modelinput.WithInvocation(ctx, "other-org", "other-execution")
				if err != nil {
					t.Fatal(err)
				}
				request.Messages[0].Source.Handle = ""
				binding, err := modelinput.BindContext(ctx, request)
				if err != nil {
					t.Fatal(err)
				}
				request = binding.Request()
			}
			if _, err := adapter.CompleteRequest(ctx, request); err == nil || !execution.WasRequestNotSent(err) || model.called || store.reservation.ID != "" {
				t.Fatal("foreign or stale source binding reached admission")
			}
		})
	}
}

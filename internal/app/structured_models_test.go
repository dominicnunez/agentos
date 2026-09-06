package app

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/modelinput"
	"github.com/dominicnunez/agentos/internal/planning"
)

func TestLongConversationCompletesStructuredRootAndChildExecution(t *testing.T) {
	for _, decomposed := range []bool{false, true} {
		t.Run(fmt.Sprint(decomposed), func(t *testing.T) {
			store, err := ledger.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			model := &organizationLoopModel{plan: `{"tasks":[]}`}
			if decomposed {
				model.plan = `{"tasks":[{"key":"` + strings.Repeat("k", 64) + `","description":"bounded child work","execution_kind":"AGENT","model_inference_policy":"REQUIRED","depends_on":[]}]}`
			}
			service := NewWithModelAndPlanner(events.NewGateway(store), model, newOrganizationPlanner(t, model))
			submission := Submit{RequestID: strings.Repeat("x", 256), OrganizationID: "org-1", Statement: "prepare a verified briefing", Kind: core.ExecutionAgent}
			result, err := service.Submit(t.Context(), submission)
			if err != nil || result.Task.Status != core.TaskCompleted || result.Work.Status != "COMPLETED" {
				t.Fatalf("admitted long conversation did not complete: %v %+v", err, result.Task)
			}
			calls := len(model.prompts)
			if calls != 2 && !decomposed || calls != 3 && decomposed {
				t.Fatalf("unexpected model calls: %d", calls)
			}
			replayed, err := service.Submit(t.Context(), submission)
			if err != nil || replayed.Task.ID != result.Task.ID || len(model.prompts) != calls {
				t.Fatal("long source identity did not replay exactly")
			}
		})
	}
}

func executionIDFromTestStream(t *testing.T, stream []events.Event, taskID core.ID) core.ID {
	t.Helper()
	for _, event := range stream {
		if event.EventType == "EXECUTION_CONTEXT_MANIFESTED" && event.TaskID == string(taskID) {
			var manifest core.ExecutionContextManifest
			if err := json.Unmarshal(event.Payload, &manifest); err != nil {
				t.Fatal(err)
			}
			return manifest.ExecutionID
		}
	}
	t.Fatal("execution manifest missing from test stream")
	return ""
}

func structuredTestInput(organizationID, executionID core.ID, input core.AgentExecutionInputContext) (string, error) {
	binding, err := core.BindAgentExecutionInput(organizationID, executionID, input)
	if err != nil {
		return "", err
	}
	body, err := binding.Request().Canonical()
	return string(body), err
}

// Synthetic models echo canonical input so deterministic completion verifies the
// manifested bytes; production adapters must preserve native message roles.
func (m *organizationLoopModel) CompleteRequest(ctx context.Context, request modelinput.Request) (execution.ModelResponse, error) {
	if len(request.Messages) > 0 && request.Messages[0].Source.Kind == modelinput.RuntimeContract && request.Messages[0].Source.Reference == planning.PromptVersion {
		return m.Complete(ctx, request.Messages[0].Text)
	}
	body, err := request.Canonical()
	if err != nil {
		return execution.ModelResponse{}, err
	}
	return m.Complete(ctx, string(body))
}

func (m delayedOrganizationModel) CompleteRequest(ctx context.Context, request modelinput.Request) (execution.ModelResponse, error) {
	if len(request.Messages) > 0 && request.Messages[0].Source.Kind == modelinput.RuntimeContract && request.Messages[0].Source.Reference == planning.PromptVersion {
		return m.Complete(ctx, request.Messages[0].Text)
	}
	body, err := request.Canonical()
	if err != nil {
		return execution.ModelResponse{}, err
	}
	return m.Complete(ctx, string(body))
}

func (m timeoutExecutionModel) CompleteRequest(ctx context.Context, request modelinput.Request) (execution.ModelResponse, error) {
	body, err := request.Canonical()
	if err != nil {
		return execution.ModelResponse{}, err
	}
	return m.Complete(ctx, string(body))
}

func (m *failingExecutionModel) CompleteRequest(ctx context.Context, request modelinput.Request) (execution.ModelResponse, error) {
	body, err := request.Canonical()
	if err != nil {
		return execution.ModelResponse{}, err
	}
	return m.Complete(ctx, string(body))
}

func (m describedModel) CompleteRequest(ctx context.Context, request modelinput.Request) (execution.ModelResponse, error) {
	body, err := request.Canonical()
	if err != nil {
		return execution.ModelResponse{}, err
	}
	return m.Complete(ctx, string(body))
}

func (m *failRevisionExecutionModel) CompleteRequest(ctx context.Context, request modelinput.Request) (execution.ModelResponse, error) {
	body, err := request.Canonical()
	if err != nil {
		return execution.ModelResponse{}, err
	}
	return m.Complete(ctx, string(body))
}

func (m changedDescriptorModel) CompleteRequest(ctx context.Context, request modelinput.Request) (execution.ModelResponse, error) {
	body, err := request.Canonical()
	if err != nil {
		return execution.ModelResponse{}, err
	}
	return m.Complete(ctx, string(body))
}

func (m replacementModel) CompleteRequest(ctx context.Context, request modelinput.Request) (execution.ModelResponse, error) {
	body, err := request.Canonical()
	if err != nil {
		return execution.ModelResponse{}, err
	}
	return m.Complete(ctx, string(body))
}

func (m *diagnosticFailureModel) CompleteRequest(ctx context.Context, request modelinput.Request) (execution.ModelResponse, error) {
	body, err := request.Canonical()
	if err != nil {
		return execution.ModelResponse{}, err
	}
	return m.Complete(ctx, string(body))
}

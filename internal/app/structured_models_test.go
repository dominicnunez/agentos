package app

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/modelinput"
	"github.com/dominicnunez/agentos/internal/planning"
)

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

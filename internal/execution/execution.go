package execution

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

type Handler interface {
	Execute(context.Context, core.Task, core.ExecutionContextManifest) (core.ToolOutcome, error)
}

type Deterministic struct{}

func (Deterministic) Execute(_ context.Context, task core.Task, _ core.ExecutionContextManifest) (core.ToolOutcome, error) {
	started := time.Now().UTC()
	const prefix = "echo "
	if !strings.HasPrefix(task.Description, prefix) {
		return core.ToolOutcome{ToolInvocationID: core.ID("tool-" + string(task.ID)), ToolID: "builtin.echo", Status: core.OutcomeFailed, PostconditionStatus: core.PostconditionVerified, Retryability: core.NotRetryable, ErrorClass: "unsupported_operation", ErrorDetail: "expected echo prefix", StartedAt: started, FinishedAt: time.Now().UTC()}, nil
	}
	value := strings.TrimPrefix(task.Description, prefix)
	return core.ToolOutcome{ToolInvocationID: core.ID("tool-" + string(task.ID)), ToolID: "builtin.echo", ToolVersion: "v1", Status: core.OutcomeSucceeded, ObservedEffect: value, PostconditionStatus: core.PostconditionVerified, Retryability: core.NotRetryable, StartedAt: started, FinishedAt: time.Now().UTC()}, nil
}

type ModelAdapter interface {
	Name() string
	Complete(context.Context, string) (string, error)
}
type FakeModel struct{}

func (FakeModel) Name() string { return "fake-model/v1" }
func (FakeModel) Complete(_ context.Context, prompt string) (string, error) {
	return "fake-model: " + prompt, nil
}

type AgentExecution struct{ model ModelAdapter }

func NewAgentExecution(model ModelAdapter) *AgentExecution { return &AgentExecution{model: model} }
func (a *AgentExecution) Execute(ctx context.Context, task core.Task, _ core.ExecutionContextManifest) (core.ToolOutcome, error) {
	started := time.Now().UTC()
	if task.ModelInferencePolicy == core.InferenceForbidden {
		return core.ToolOutcome{}, fmt.Errorf("model inference forbidden for task %s", task.ID)
	}
	text, err := a.model.Complete(ctx, task.Description)
	if err != nil {
		return core.ToolOutcome{ToolInvocationID: core.ID("model-" + string(task.ID)), ToolID: a.model.Name(), Status: core.OutcomeFailed, PostconditionStatus: core.PostconditionNotChecked, Retryability: core.Retryable, ErrorDetail: err.Error(), StartedAt: started, FinishedAt: time.Now().UTC()}, err
	}
	return core.ToolOutcome{ToolInvocationID: core.ID("model-" + string(task.ID)), ToolID: a.model.Name(), Status: core.OutcomeSucceeded, ObservedEffect: text, PostconditionStatus: core.PostconditionVerified, Retryability: core.NotRetryable, StartedAt: started, FinishedAt: time.Now().UTC()}, nil
}

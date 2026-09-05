package execution

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

type Handler interface {
	Execute(context.Context, core.Task, core.ExecutionContextManifest) (Result, error)
}

type Result struct {
	Outcome        core.ToolOutcome                      `json:"outcome"`
	InferenceUsage *events.InferenceUsageRecordedPayload `json:"inference_usage,omitempty"`
}

type Deterministic struct{}

func (Deterministic) Execute(_ context.Context, task core.Task, _ core.ExecutionContextManifest) (Result, error) {
	started := time.Now().UTC()
	const prefix = "echo "
	if !strings.HasPrefix(task.Description, prefix) {
		return Result{Outcome: core.ToolOutcome{ToolInvocationID: core.ID("tool-" + string(task.ID)), ToolID: "builtin.echo", Status: core.OutcomeFailed, PostconditionStatus: core.PostconditionVerified, Retryability: core.NotRetryable, ErrorClass: "unsupported_operation", ErrorDetail: "expected echo prefix", StartedAt: started, FinishedAt: time.Now().UTC()}}, nil
	}
	value := strings.TrimPrefix(task.Description, prefix)
	return Result{Outcome: core.ToolOutcome{ToolInvocationID: core.ID("tool-" + string(task.ID)), ToolID: "builtin.echo", ToolVersion: "v1", Status: core.OutcomeSucceeded, ObservedEffect: value, PostconditionStatus: core.PostconditionNotChecked, Retryability: core.NotRetryable, StartedAt: started, FinishedAt: time.Now().UTC()}}, nil
}

type ModelAdapter interface {
	Name() string
	Descriptor() ModelDescriptor
	Complete(context.Context, string) (ModelResponse, error)
}
type ModelDescriptor struct {
	Provider                string
	Model                   string
	ExecutionProfileVersion string
}
type ModelResponse struct {
	Text  string
	Usage events.InferenceUsageRecordedPayload
}

type requestNotSentError struct{ err error }

func (e requestNotSentError) Error() string { return e.err.Error() }
func (e requestNotSentError) Unwrap() error { return e.err }

// RequestNotSent marks a definite local rejection before a model provider was
// contacted. Ambiguous transport or provider failures must not use this mark.
func RequestNotSent(err error) error {
	if err == nil {
		return nil
	}
	return requestNotSentError{err: err}
}

func WasRequestNotSent(err error) bool {
	var target requestNotSentError
	return errors.As(err, &target)
}

type FakeModel struct{}

func (FakeModel) Name() string { return "fake-model/v1" }
func (FakeModel) Descriptor() ModelDescriptor {
	return ModelDescriptor{Provider: "fake", Model: "fake-model/v1", ExecutionProfileVersion: "v1-fake"}
}
func (FakeModel) Complete(_ context.Context, prompt string) (ModelResponse, error) {
	zero := 0.0
	return ModelResponse{
		Text: "fake-model: " + prompt,
		Usage: events.InferenceUsageRecordedPayload{
			Source:   "fake_adapter",
			Provider: "fake",
			Model:    "fake-model/v1",
			CostUSD:  &zero,
		},
	}, nil
}

// ReviewFakeModel is a non-network release-test adapter. Unlike FakeModel, its
// Tool ID has no deterministic verifier, so it exercises the exact completion
// review path without sending data to a real provider.
type ReviewFakeModel struct{}

func (ReviewFakeModel) Name() string { return "fake-review-model/v1" }
func (ReviewFakeModel) Descriptor() ModelDescriptor {
	return ModelDescriptor{Provider: "fake-review", Model: "fake-review-model/v1", ExecutionProfileVersion: "v1-fake-review"}
}
func (ReviewFakeModel) Complete(_ context.Context, prompt string) (ModelResponse, error) {
	zero := 0.0
	return ModelResponse{
		Text: "fake-review-model: " + prompt,
		Usage: events.InferenceUsageRecordedPayload{
			Source:   "fake_review_adapter",
			Provider: "fake-review",
			Model:    "fake-review-model/v1",
			CostUSD:  &zero,
		},
	}, nil
}

type AgentExecution struct {
	model      ModelAdapter
	descriptor ModelDescriptor
}

func NewAgentExecution(model ModelAdapter) *AgentExecution {
	return &AgentExecution{model: model, descriptor: model.Descriptor()}
}
func (a *AgentExecution) Descriptor() ModelDescriptor { return a.descriptor }
func (a *AgentExecution) Execute(ctx context.Context, task core.Task, manifest core.ExecutionContextManifest) (Result, error) {
	started := time.Now().UTC()
	if task.ModelInferencePolicy == core.InferenceForbidden {
		return Result{}, fmt.Errorf("model inference forbidden for task %s", task.ID)
	}
	if manifest.Provider != a.descriptor.Provider || manifest.Model != a.descriptor.Model || manifest.ExecutionProfileVersion != a.descriptor.ExecutionProfileVersion {
		err := fmt.Errorf("execution context model identity does not match the configured adapter")
		return Result{Outcome: core.ToolOutcome{ToolInvocationID: core.ID("model-" + string(task.ID)), ToolID: a.model.Name(), Status: core.OutcomeFailed, PostconditionStatus: core.PostconditionNotChecked, Retryability: core.NotRetryable, ErrorClass: "provider_contract", ErrorDetail: err.Error(), StartedAt: started, FinishedAt: time.Now().UTC()}}, err
	}
	prompt := task.Description
	if task.ExecutionBrief != "" {
		prompt = task.ExecutionBrief
	}
	response, err := a.model.Complete(ctx, prompt)
	if err != nil {
		err = SafeModelError(ModelCallFailed, err)
		return Result{Outcome: core.ToolOutcome{ToolInvocationID: core.ID("model-" + string(task.ID)), ToolID: a.model.Name(), Status: core.OutcomeFailed, PostconditionStatus: core.PostconditionNotChecked, Retryability: core.Retryable, ErrorClass: ModelErrorClass(err), ErrorDetail: err.Error(), StartedAt: started, FinishedAt: time.Now().UTC()}}, err
	}
	if !response.Usage.Valid() || response.Usage.Provider != a.descriptor.Provider || response.Usage.Model != a.descriptor.Model {
		err := fmt.Errorf("model usage identity does not match the configured adapter")
		return Result{Outcome: core.ToolOutcome{ToolInvocationID: core.ID("model-" + string(task.ID)), ToolID: a.model.Name(), Status: core.OutcomeFailed, PostconditionStatus: core.PostconditionNotChecked, Retryability: core.NotRetryable, ErrorClass: "provider_contract", ErrorDetail: err.Error(), StartedAt: started, FinishedAt: time.Now().UTC()}}, err
	}
	return Result{
		Outcome:        core.ToolOutcome{ToolInvocationID: core.ID("model-" + string(task.ID)), ToolID: a.model.Name(), Status: core.OutcomeSucceeded, ObservedEffect: response.Text, PostconditionStatus: core.PostconditionNotChecked, Retryability: core.NotRetryable, StartedAt: started, FinishedAt: time.Now().UTC()},
		InferenceUsage: &response.Usage,
	}, nil
}

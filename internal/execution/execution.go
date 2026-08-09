package execution

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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

// OpenAICompatible is the V1 real-provider adapter. Credentials are resolved
// at call time and placed only in the outbound adapter request.
type OpenAICompatible struct {
	Endpoint, Model string
	APIKey          func(context.Context) (string, error)
	Client          *http.Client
}

func (a OpenAICompatible) Name() string { return "openai-compatible/" + a.Model }
func (a OpenAICompatible) Complete(ctx context.Context, prompt string) (string, error) {
	if a.Endpoint == "" || a.Model == "" || a.APIKey == nil {
		return "", fmt.Errorf("real model adapter is not configured")
	}
	key, err := a.APIKey(ctx)
	if err != nil {
		return "", err
	}
	body, _ := json.Marshal(map[string]any{"model": a.Model, "messages": []map[string]string{{"role": "user", "content": prompt}}})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.Endpoint, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)
	client := a.Client
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("model provider returned %s", resp.Status)
	}
	var decoded struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err = json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return "", err
	}
	if len(decoded.Choices) == 0 {
		return "", fmt.Errorf("model provider returned no choices")
	}
	return decoded.Choices[0].Message.Content, nil
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

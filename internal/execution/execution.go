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
	return Result{Outcome: core.ToolOutcome{ToolInvocationID: core.ID("tool-" + string(task.ID)), ToolID: "builtin.echo", ToolVersion: "v1", Status: core.OutcomeSucceeded, ObservedEffect: value, PostconditionStatus: core.PostconditionVerified, Retryability: core.NotRetryable, StartedAt: started, FinishedAt: time.Now().UTC()}}, nil
}

type ModelAdapter interface {
	Name() string
	Complete(context.Context, string) (ModelResponse, error)
}
type ModelResponse struct {
	Text  string
	Usage events.InferenceUsageRecordedPayload
}
type FakeModel struct{}

func (FakeModel) Name() string { return "fake-model/v1" }
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

// OpenAICompatible is the V1 real-provider adapter. Credentials are resolved
// at call time and placed only in the outbound adapter request.
type OpenAICompatible struct {
	Endpoint, Model                           string
	APIKey                                    func(context.Context) (string, error)
	Client                                    *http.Client
	PricingKnown                              bool
	InputCostPerMillion, OutputCostPerMillion float64
}

func (a OpenAICompatible) Name() string { return "openai-compatible/" + a.Model }
func (a OpenAICompatible) Complete(ctx context.Context, prompt string) (ModelResponse, error) {
	if a.Endpoint == "" || a.Model == "" || a.APIKey == nil {
		return ModelResponse{}, fmt.Errorf("real model adapter is not configured")
	}
	key, err := a.APIKey(ctx)
	if err != nil {
		return ModelResponse{}, err
	}
	body, _ := json.Marshal(map[string]any{"model": a.Model, "messages": []map[string]string{{"role": "user", "content": prompt}}})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.Endpoint, bytes.NewReader(body))
	if err != nil {
		return ModelResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)
	client := a.Client
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return ModelResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return ModelResponse{}, fmt.Errorf("model provider returned %s", resp.Status)
	}
	var decoded struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Usage *struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			TotalTokens      int `json:"total_tokens"`
		} `json:"usage"`
	}
	if err = json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return ModelResponse{}, err
	}
	if len(decoded.Choices) == 0 {
		return ModelResponse{}, fmt.Errorf("model provider returned no choices")
	}
	if decoded.Usage == nil {
		return ModelResponse{}, fmt.Errorf("model provider returned no usage")
	}
	usage := events.InferenceUsageRecordedPayload{
		Source:       "provider_response",
		Provider:     "openai-compatible",
		Model:        a.Model,
		InputTokens:  decoded.Usage.PromptTokens,
		OutputTokens: decoded.Usage.CompletionTokens,
		TotalTokens:  decoded.Usage.TotalTokens,
	}
	if usage.TotalTokens == 0 {
		usage.TotalTokens = usage.InputTokens + usage.OutputTokens
	}
	if a.PricingKnown {
		if a.InputCostPerMillion < 0 || a.OutputCostPerMillion < 0 {
			return ModelResponse{}, fmt.Errorf("model pricing cannot be negative")
		}
		cost := (float64(usage.InputTokens)*a.InputCostPerMillion + float64(usage.OutputTokens)*a.OutputCostPerMillion) / 1_000_000
		usage.CostUSD = &cost
	}
	if !usage.Valid() {
		return ModelResponse{}, fmt.Errorf("model provider returned invalid usage")
	}
	return ModelResponse{Text: decoded.Choices[0].Message.Content, Usage: usage}, nil
}

type AgentExecution struct{ model ModelAdapter }

func NewAgentExecution(model ModelAdapter) *AgentExecution { return &AgentExecution{model: model} }
func (a *AgentExecution) Execute(ctx context.Context, task core.Task, _ core.ExecutionContextManifest) (Result, error) {
	started := time.Now().UTC()
	if task.ModelInferencePolicy == core.InferenceForbidden {
		return Result{}, fmt.Errorf("model inference forbidden for task %s", task.ID)
	}
	response, err := a.model.Complete(ctx, task.Description)
	if err != nil {
		return Result{Outcome: core.ToolOutcome{ToolInvocationID: core.ID("model-" + string(task.ID)), ToolID: a.model.Name(), Status: core.OutcomeFailed, PostconditionStatus: core.PostconditionNotChecked, Retryability: core.Retryable, ErrorDetail: err.Error(), StartedAt: started, FinishedAt: time.Now().UTC()}}, err
	}
	return Result{
		Outcome:        core.ToolOutcome{ToolInvocationID: core.ID("model-" + string(task.ID)), ToolID: a.model.Name(), Status: core.OutcomeSucceeded, ObservedEffect: response.Text, PostconditionStatus: core.PostconditionVerified, Retryability: core.NotRetryable, StartedAt: started, FinishedAt: time.Now().UTC()},
		InferenceUsage: &response.Usage,
	}, nil
}

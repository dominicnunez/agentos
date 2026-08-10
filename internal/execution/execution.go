package execution

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
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
	AllowedHosts                              []string
	PricingKnown                              bool
	InputCostPerMillion, OutputCostPerMillion float64
}

func (a OpenAICompatible) Name() string { return "openai-compatible/" + a.Model }
func (a OpenAICompatible) Complete(ctx context.Context, prompt string) (ModelResponse, error) {
	if a.Endpoint == "" || a.Model == "" || a.APIKey == nil {
		return ModelResponse{}, fmt.Errorf("real model adapter is not configured")
	}
	endpoint, err := url.Parse(a.Endpoint)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil || endpoint.Fragment != "" {
		return ModelResponse{}, fmt.Errorf("model endpoint must be an absolute HTTPS URL without user info or fragment")
	}
	if !allowedProviderHost(endpoint.Hostname(), a.AllowedHosts) {
		return ModelResponse{}, fmt.Errorf("model endpoint host is not allowlisted")
	}
	key, err := a.APIKey(ctx)
	if err != nil {
		return ModelResponse{}, err
	}
	if key == "" || strings.TrimSpace(key) != key || strings.ContainsAny(key, "\r\n") {
		return ModelResponse{}, fmt.Errorf("model credential is invalid")
	}
	body, _ := json.Marshal(map[string]any{"model": a.Model, "messages": []map[string]string{{"role": "user", "content": prompt}}})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.Endpoint, bytes.NewReader(body))
	if err != nil {
		return ModelResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)
	baseClient := a.Client
	if baseClient == nil {
		baseClient = &http.Client{}
	}
	client := *baseClient
	if client.Timeout <= 0 || client.Timeout > 60*time.Second {
		client.Timeout = 60 * time.Second
	}
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		return ModelResponse{}, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode/100 != 2 {
		return ModelResponse{}, fmt.Errorf("model provider returned %s", resp.Status)
	}
	mediaType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return ModelResponse{}, fmt.Errorf("model provider must return application/json")
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
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil {
		return ModelResponse{}, err
	}
	if len(responseBody) > 1<<20 {
		return ModelResponse{}, fmt.Errorf("model provider response exceeds 1048576 bytes")
	}
	decoder := json.NewDecoder(bytes.NewReader(responseBody))
	if err = decoder.Decode(&decoded); err != nil {
		return ModelResponse{}, err
	}
	if err = decoder.Decode(&struct{}{}); err != io.EOF {
		return ModelResponse{}, fmt.Errorf("model provider returned trailing content")
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

func allowedProviderHost(host string, allowed []string) bool {
	if host == "" || len(allowed) == 0 {
		return false
	}
	for _, candidate := range allowed {
		if strings.EqualFold(host, candidate) {
			return true
		}
	}
	return false
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
		Outcome:        core.ToolOutcome{ToolInvocationID: core.ID("model-" + string(task.ID)), ToolID: a.model.Name(), Status: core.OutcomeSucceeded, ObservedEffect: response.Text, PostconditionStatus: core.PostconditionNotChecked, Retryability: core.NotRetryable, StartedAt: started, FinishedAt: time.Now().UTC()},
		InferenceUsage: &response.Usage,
	}, nil
}

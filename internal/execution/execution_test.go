package execution

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestOpenAICompatibleRecordsProviderUsageAndConfiguredCost(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Fatalf("authorization=%q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"answer"}}],"usage":{"prompt_tokens":100,"completion_tokens":50,"total_tokens":150}}`))
	}))
	t.Cleanup(server.Close)
	adapter := OpenAICompatible{
		Endpoint:             server.URL,
		Model:                "test-model",
		APIKey:               func(context.Context) (string, error) { return "secret", nil },
		Client:               server.Client(),
		AllowedHosts:         []string{"127.0.0.1"},
		PricingKnown:         true,
		InputCostPerMillion:  10,
		OutputCostPerMillion: 20,
	}
	response, err := adapter.Complete(context.Background(), "prompt")
	if err != nil {
		t.Fatal(err)
	}
	if response.Text != "answer" || !response.Usage.Valid() || response.Usage.TotalTokens != 150 || response.Usage.CostUSD == nil || *response.Usage.CostUSD != 0.002 {
		t.Fatalf("response=%+v", response)
	}
}

func TestAgentExecutionReturnsSeparateUsageContract(t *testing.T) {
	executor := NewAgentExecution(FakeModel{})
	descriptor := executor.Descriptor()
	if descriptor.Provider != "fake" || descriptor.Model != "fake-model/v1" || descriptor.ExecutionProfileVersion != "v1-fake" {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	result, err := executor.Execute(context.Background(), core.Task{ID: "task-1", Description: "work", ModelInferencePolicy: core.InferenceAllowed}, core.ExecutionContextManifest{Provider: descriptor.Provider, Model: descriptor.Model, ExecutionProfileVersion: descriptor.ExecutionProfileVersion})
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome.ObservedEffect != "fake-model: work" || result.InferenceUsage == nil || !result.InferenceUsage.Valid() || result.InferenceUsage.CostUSD == nil || *result.InferenceUsage.CostUSD != 0 {
		t.Fatalf("result=%+v", result)
	}
}

type mismatchedUsageModel struct{}

func (mismatchedUsageModel) Name() string { return "configured/model" }
func (mismatchedUsageModel) Descriptor() ModelDescriptor {
	return ModelDescriptor{Provider: "configured", Model: "model", ExecutionProfileVersion: "profile-v1"}
}
func (mismatchedUsageModel) Complete(context.Context, string) (ModelResponse, error) {
	return ModelResponse{Text: "untrusted response", Usage: events.InferenceUsageRecordedPayload{Source: "provider_response", Provider: "different", Model: "model", InputTokens: 1, TotalTokens: 1}}, nil
}

func TestAgentExecutionRejectsMismatchedProviderUsageIdentity(t *testing.T) {
	executor := NewAgentExecution(mismatchedUsageModel{})
	descriptor := executor.Descriptor()
	result, err := executor.Execute(context.Background(), core.Task{ID: "task-1", Description: "work", ModelInferencePolicy: core.InferenceAllowed}, core.ExecutionContextManifest{Provider: descriptor.Provider, Model: descriptor.Model, ExecutionProfileVersion: descriptor.ExecutionProfileVersion})
	if err == nil || result.Outcome.Status != core.OutcomeFailed || result.Outcome.ErrorClass != "provider_contract" || result.InferenceUsage != nil {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

type manifestTrackingModel struct{ called bool }

func (*manifestTrackingModel) Name() string { return "fake-model/v1" }
func (*manifestTrackingModel) Descriptor() ModelDescriptor {
	return FakeModel{}.Descriptor()
}
func (m *manifestTrackingModel) Complete(context.Context, string) (ModelResponse, error) {
	m.called = true
	return FakeModel{}.Complete(context.Background(), "work")
}

func TestAgentExecutionRejectsMismatchedManifestIdentityBeforeCallingProvider(t *testing.T) {
	model := &manifestTrackingModel{}
	result, err := NewAgentExecution(model).Execute(context.Background(), core.Task{ID: "task-1", Description: "work", ModelInferencePolicy: core.InferenceAllowed}, core.ExecutionContextManifest{Provider: "other", Model: "fake-model/v1", ExecutionProfileVersion: "v1-fake"})
	if err == nil || model.called || result.Outcome.Status != core.OutcomeFailed || result.Outcome.ErrorClass != "provider_contract" || result.InferenceUsage != nil {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestOpenAICompatibleRejectsMissingUsage(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"answer"}}]}`))
	}))
	t.Cleanup(server.Close)
	adapter := OpenAICompatible{Endpoint: server.URL, Model: "test-model", APIKey: func(context.Context) (string, error) { return "secret", nil }, Client: server.Client(), AllowedHosts: []string{"127.0.0.1"}}
	if _, err := adapter.Complete(context.Background(), "prompt"); err == nil {
		t.Fatal("provider response without usage was accepted")
	}
}

func TestOpenAICompatibleFailsClosedAtEgressBoundary(t *testing.T) {
	for _, adapter := range []OpenAICompatible{
		{Endpoint: "http://provider.example/v1", Model: "model", APIKey: func(context.Context) (string, error) { return "secret", nil }, AllowedHosts: []string{"provider.example"}},
		{Endpoint: "https://unreviewed.example/v1", Model: "model", APIKey: func(context.Context) (string, error) { return "secret", nil }, AllowedHosts: []string{"provider.example"}},
	} {
		if _, err := adapter.Complete(context.Background(), "prompt"); err == nil {
			t.Fatal("unsafe provider endpoint was accepted")
		}
	}

	redirectTarget := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"redirected"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	}))
	t.Cleanup(redirectTarget.Close)
	redirector := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", redirectTarget.URL)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirector.Close)
	adapter := OpenAICompatible{Endpoint: redirector.URL, Model: "model", APIKey: func(context.Context) (string, error) { return "secret", nil }, Client: redirector.Client(), AllowedHosts: []string{"127.0.0.1"}}
	if _, err := adapter.Complete(context.Background(), "prompt"); err == nil {
		t.Fatal("provider redirect was followed")
	}
}

func TestOpenAICompatibleBoundsProviderResponse(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"` + strings.Repeat("x", (1<<20)+1) + `"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	}))
	t.Cleanup(server.Close)
	adapter := OpenAICompatible{Endpoint: server.URL, Model: "model", APIKey: func(context.Context) (string, error) { return "secret", nil }, Client: server.Client(), AllowedHosts: []string{"127.0.0.1"}}
	if _, err := adapter.Complete(context.Background(), "prompt"); err == nil {
		t.Fatal("oversized provider response was accepted")
	}
}

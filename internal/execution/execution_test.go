package execution

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestOpenAICompatibleRecordsProviderUsageAndConfiguredCost(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	result, err := NewAgentExecution(FakeModel{}).Execute(context.Background(), core.Task{ID: "task-1", Description: "work", ModelInferencePolicy: core.InferenceAllowed}, core.ExecutionContextManifest{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome.ObservedEffect != "fake-model: work" || result.InferenceUsage == nil || !result.InferenceUsage.Valid() || result.InferenceUsage.CostUSD == nil || *result.InferenceUsage.CostUSD != 0 {
		t.Fatalf("result=%+v", result)
	}
}

func TestOpenAICompatibleRejectsMissingUsage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"answer"}}]}`))
	}))
	t.Cleanup(server.Close)
	adapter := OpenAICompatible{Endpoint: server.URL, Model: "test-model", APIKey: func(context.Context) (string, error) { return "secret", nil }, Client: server.Client()}
	if _, err := adapter.Complete(context.Background(), "prompt"); err == nil {
		t.Fatal("provider response without usage was accepted")
	}
}

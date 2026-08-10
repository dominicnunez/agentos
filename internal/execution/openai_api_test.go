package execution

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

const openAITestModel = "gpt-test-2026-01-01"

func TestOpenAIAPISendsConstrainedResponsesRequestAndRecordsUsage(t *testing.T) {
	var credentialResolutions int
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/v1/responses" || request.URL.RawQuery != "" {
			t.Errorf("request=%s %s", request.Method, request.URL.String())
		}
		if request.Header.Get("Authorization") != "Bearer test-secret" || request.Header.Get("Accept") != "application/json" || request.Header.Get("Content-Type") != "application/json" {
			t.Error("required OpenAI API headers were not set exactly")
		}
		if clientRequestID := request.Header.Get("X-Client-Request-Id"); !strings.HasPrefix(clientRequestID, "agentos-") || len(clientRequestID) != 40 {
			t.Errorf("client request id=%q", clientRequestID)
		}
		var input openAIRequest
		if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
			t.Error(err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if input.Model != openAITestModel || input.Input != "draft the update" || input.Instructions != openAIInstructions || input.ToolChoice != "none" || input.Tools == nil || len(input.Tools) != 0 || input.MaxOutputTokens != openAIMaximumOutputTokens || input.Store || input.Truncation != "disabled" {
			t.Errorf("request=%+v", input)
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("X-Request-Id", "req_test")
		_, _ = writer.Write(openAITestResponse("answer"))
	}))
	t.Cleanup(server.Close)
	adapter, err := newOpenAIAPI(context.Background(), OpenAIAPIConfig{
		Model: openAITestModel,
		APIKey: func(context.Context) (string, error) {
			credentialResolutions++
			return "test-secret", nil
		},
	}, server.URL+"/v1/responses", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	response, err := adapter.Complete(context.Background(), "draft the update")
	if err != nil {
		t.Fatal(err)
	}
	if credentialResolutions != 2 {
		t.Fatalf("credential resolutions=%d", credentialResolutions)
	}
	if descriptor := adapter.Descriptor(); descriptor.Provider != openAIAPIProvider || descriptor.Model != openAITestModel || descriptor.ExecutionProfileVersion != openAIAPIProfile {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	if response.Text != "answer" || response.Usage.Source != "provider_api" || response.Usage.Provider != openAIAPIProvider || response.Usage.Model != openAITestModel || response.Usage.InputTokens != 12 || response.Usage.OutputTokens != 3 || response.Usage.TotalTokens != 15 || response.Usage.CostUSD != nil {
		t.Fatalf("response=%+v", response)
	}
}

func TestOpenAIAPIAcceptsReasoningMetadataButRejectsAuthorityBearingOutput(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{
			name: "tool call",
			mutate: func(response map[string]any) {
				response["output"] = []any{map[string]any{"type": "function_call", "name": "send_email", "arguments": `{}`}}
			},
		},
		{
			name: "annotation",
			mutate: func(response map[string]any) {
				openAITestTextContent(response)["annotations"] = []any{map[string]any{"type": "url_citation", "url": "https://example.test"}}
			},
		},
		{
			name: "refusal",
			mutate: func(response map[string]any) {
				response["output"] = []any{map[string]any{"type": "message", "role": "assistant", "status": "completed", "content": []any{map[string]any{"type": "refusal", "refusal": "no"}}}}
			},
		},
		{
			name: "profile changed",
			mutate: func(response map[string]any) {
				response["tool_choice"] = "auto"
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := openAITestResponseObject("answer")
			test.mutate(response)
			adapter := openAITestAdapter(t, response)
			if _, err := adapter.Complete(context.Background(), "prompt"); err == nil {
				t.Fatal("untrusted response escaped the model-only contract")
			}
		})
	}
}

func TestOpenAIAPIFailsClosedOnInvalidResponseContracts(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "wrong model", mutate: func(response map[string]any) { response["model"] = "other-model" }},
		{name: "incomplete", mutate: func(response map[string]any) { response["status"] = "incomplete" }},
		{name: "missing usage", mutate: func(response map[string]any) { delete(response, "usage") }},
		{name: "inconsistent usage", mutate: func(response map[string]any) { openAITestUsage(response)["total_tokens"] = 14 }},
		{name: "zero usage", mutate: func(response map[string]any) {
			response["usage"] = map[string]any{"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
		}},
		{name: "unbounded usage", mutate: func(response map[string]any) {
			response["usage"] = map[string]any{"input_tokens": openAIMaximumUsageTokens, "output_tokens": 1, "total_tokens": openAIMaximumUsageTokens + 1}
		}},
		{name: "stored", mutate: func(response map[string]any) { response["store"] = true }},
		{name: "oversized output", mutate: func(response map[string]any) {
			openAITestTextContent(response)["text"] = strings.Repeat("x", openAIMaximumResponseBytes+1)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := openAITestResponseObject("answer")
			test.mutate(response)
			adapter := openAITestAdapter(t, response)
			if _, err := adapter.Complete(context.Background(), "prompt"); err == nil {
				t.Fatal("invalid provider response was accepted")
			}
		})
	}
}

func TestOpenAIAPIBoundsFullProviderBodyAndPrompt(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"padding":"` + strings.Repeat("x", openAIMaximumBodyBytes) + `"}`))
	}))
	t.Cleanup(server.Close)
	adapter, err := newOpenAIAPI(context.Background(), openAITestConfig(), server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.Complete(context.Background(), "prompt"); err == nil {
		t.Fatal("oversized provider body was accepted")
	}
	for _, prompt := range []string{"   ", strings.Repeat("x", openAIMaximumPromptBytes+1)} {
		if _, err := adapter.Complete(context.Background(), prompt); err == nil {
			t.Fatal("invalid prompt was accepted")
		}
	}
}

func TestOpenAIAPIDoesNotFollowRedirects(t *testing.T) {
	var redirectTargetCalled atomic.Bool
	redirectTarget := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		redirectTargetCalled.Store(true)
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write(openAITestResponse("redirected"))
	}))
	t.Cleanup(redirectTarget.Close)
	redirector := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", redirectTarget.URL)
		writer.WriteHeader(http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirector.Close)
	adapter, err := newOpenAIAPI(context.Background(), openAITestConfig(), redirector.URL, redirector.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.Complete(context.Background(), "prompt"); err == nil || redirectTargetCalled.Load() {
		t.Fatalf("err=%v redirectTargetCalled=%t", err, redirectTargetCalled.Load())
	}
}

func TestOpenAIAPIErrorsDoNotExposeSecretsOrProviderBodies(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("X-Request-Id", "req_safe")
		writer.WriteHeader(http.StatusTooManyRequests)
		_, _ = writer.Write([]byte(`{"error":{"message":"private prompt test-secret"}}`))
	}))
	t.Cleanup(server.Close)
	adapter, err := newOpenAIAPI(context.Background(), openAITestConfig(), server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Complete(context.Background(), "private prompt")
	if err == nil || strings.Contains(err.Error(), "private prompt") || strings.Contains(err.Error(), "test-secret") || !strings.Contains(err.Error(), "req_safe") {
		t.Fatalf("error=%q", err)
	}
}

func TestOpenAIAPIUsesFixedBoundedDirectClient(t *testing.T) {
	adapter, err := NewOpenAIAPI(context.Background(), openAITestConfig())
	if err != nil {
		t.Fatal(err)
	}
	if adapter.endpoint != openAIResponsesEndpoint || adapter.client.Timeout != openAIAPITimeout || adapter.client.CheckRedirect == nil {
		t.Fatalf("adapter=%+v", adapter)
	}
	transport, ok := adapter.client.Transport.(*http.Transport)
	if !ok || transport.Proxy != nil || transport.TLSClientConfig == nil || transport.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Fatalf("transport=%+v", adapter.client.Transport)
	}
}

func TestOpenAIAPIValidatesConfigurationAndRotatedCredentials(t *testing.T) {
	for _, test := range []struct {
		name     string
		config   OpenAIAPIConfig
		endpoint string
	}{
		{name: "missing model", config: OpenAIAPIConfig{APIKey: openAITestConfig().APIKey}, endpoint: "https://example.test/v1/responses"},
		{name: "model whitespace", config: OpenAIAPIConfig{Model: " model", APIKey: openAITestConfig().APIKey}, endpoint: "https://example.test/v1/responses"},
		{name: "mutable model alias", config: OpenAIAPIConfig{Model: "gpt-5", APIKey: openAITestConfig().APIKey}, endpoint: "https://example.test/v1/responses"},
		{name: "invalid snapshot date", config: OpenAIAPIConfig{Model: "gpt-5-2026-02-31", APIKey: openAITestConfig().APIKey}, endpoint: "https://example.test/v1/responses"},
		{name: "missing resolver", config: OpenAIAPIConfig{Model: openAITestModel}, endpoint: "https://example.test/v1/responses"},
		{name: "invalid key", config: OpenAIAPIConfig{Model: openAITestModel, APIKey: func(context.Context) (string, error) { return "secret\n", nil }}, endpoint: "https://example.test/v1/responses"},
		{name: "plaintext endpoint", config: openAITestConfig(), endpoint: "http://example.test/v1/responses"},
		{name: "endpoint query", config: openAITestConfig(), endpoint: "https://example.test/v1/responses?redirect=true"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := newOpenAIAPI(context.Background(), test.config, test.endpoint, &http.Client{}); err == nil {
				t.Fatal("invalid configuration was accepted")
			}
		})
	}
	for _, model := range []string{"gpt-test-2026-01-01", "ft:gpt-test-2026-01-01:organization:name:id"} {
		config := openAITestConfig()
		config.Model = model
		if _, err := newOpenAIAPI(context.Background(), config, "https://example.test/v1/responses", &http.Client{}); err != nil {
			t.Fatalf("dated model %q was rejected: %v", model, err)
		}
	}

	valid := true
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write(openAITestResponse("answer"))
	}))
	t.Cleanup(server.Close)
	adapter, err := newOpenAIAPI(context.Background(), OpenAIAPIConfig{
		Model: openAITestModel,
		APIKey: func(context.Context) (string, error) {
			if valid {
				return "test-secret", nil
			}
			return "rotated\ninvalid", nil
		},
	}, server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	valid = false
	if _, err := adapter.Complete(context.Background(), "prompt"); err == nil {
		t.Fatal("invalid rotated credential was accepted")
	}
}

func openAITestAdapter(t *testing.T, response map[string]any) *OpenAIAPI {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(response); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(server.Close)
	adapter, err := newOpenAIAPI(context.Background(), openAITestConfig(), server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func openAITestConfig() OpenAIAPIConfig {
	return OpenAIAPIConfig{Model: openAITestModel, APIKey: func(context.Context) (string, error) { return "test-secret", nil }}
}

func openAITestResponse(text string) []byte {
	encoded, err := json.Marshal(openAITestResponseObject(text))
	if err != nil {
		panic(err)
	}
	return encoded
}

func openAITestResponseObject(text string) map[string]any {
	return map[string]any{
		"id":                 "resp_test",
		"object":             "response",
		"status":             "completed",
		"error":              nil,
		"incomplete_details": nil,
		"model":              openAITestModel,
		"output": []any{
			map[string]any{"id": "reasoning_test", "type": "reasoning", "status": "completed", "summary": []any{}},
			map[string]any{"id": "message_test", "type": "message", "role": "assistant", "status": "completed", "content": []any{map[string]any{"type": "output_text", "text": text, "annotations": []any{}}}},
		},
		"store":             false,
		"tool_choice":       "none",
		"tools":             []any{},
		"truncation":        "disabled",
		"max_output_tokens": openAIMaximumOutputTokens,
		"usage":             map[string]any{"input_tokens": 12, "output_tokens": 3, "total_tokens": 15},
	}
}

func openAITestTextContent(response map[string]any) map[string]any {
	output, ok := response["output"].([]any)
	if !ok || len(output) < 2 {
		panic("invalid OpenAI test output")
	}
	message, ok := output[1].(map[string]any)
	if !ok {
		panic("invalid OpenAI test message")
	}
	content, ok := message["content"].([]any)
	if !ok || len(content) == 0 {
		panic("invalid OpenAI test content")
	}
	text, ok := content[0].(map[string]any)
	if !ok {
		panic("invalid OpenAI test text")
	}
	return text
}

func openAITestUsage(response map[string]any) map[string]any {
	usage, ok := response["usage"].(map[string]any)
	if !ok {
		panic("invalid OpenAI test usage")
	}
	return usage
}

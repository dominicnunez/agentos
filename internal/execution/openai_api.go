package execution

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/modelid"
)

const (
	openAIAPIProvider          = "openai-api"
	openAIAPIProfile           = "v1-openai-responses-model-only"
	openAIResponsesEndpoint    = "https://api.openai.com/v1/responses"
	openAIAPITimeout           = 20 * time.Second
	openAIMaximumPromptBytes   = 256 << 10
	openAIMaximumResponseBytes = 256 << 10
	openAIMaximumBodyBytes     = 1 << 20
	openAIMaximumModelBytes    = 128
	openAIMaximumKeyBytes      = 16 << 10
	openAIMaximumOutputTokens  = 4096
	openAIMaximumOutputItems   = 64
	openAIMaximumUsageTokens   = 1_000_000
	openAIInstructions         = "Return only the requested final text. Do not request, invoke, or describe tool calls."
)

type OpenAIAPIConfig struct {
	Model  string
	APIKey func(context.Context) (string, error)
}

// OpenAIAPI is a model-only adapter for the official OpenAI Responses API.
// It intentionally exposes no provider tools or arbitrary endpoint setting.
type OpenAIAPI struct {
	model    string
	apiKey   func(context.Context) (string, error)
	endpoint string
	client   *http.Client
}

func NewOpenAIAPI(ctx context.Context, config OpenAIAPIConfig) (*OpenAIAPI, error) {
	return newOpenAIAPI(ctx, config, openAIResponsesEndpoint, nil)
}

func newOpenAIAPI(ctx context.Context, config OpenAIAPIConfig, endpoint string, client *http.Client) (*OpenAIAPI, error) {
	if ctx == nil {
		return nil, fmt.Errorf("OpenAI API context is required")
	}
	if err := validateOpenAIModel(config.Model); err != nil {
		return nil, err
	}
	if config.APIKey == nil {
		return nil, fmt.Errorf("OpenAI API credential resolver is required")
	}
	if err := validateOpenAIEndpoint(endpoint); err != nil {
		return nil, err
	}
	key, err := config.APIKey(ctx)
	if err != nil {
		return nil, fmt.Errorf("OpenAI API credential is unavailable")
	}
	if err := validateOpenAIKey(key); err != nil {
		return nil, err
	}
	return &OpenAIAPI{model: config.Model, apiKey: config.APIKey, endpoint: endpoint, client: boundedOpenAIClient(client)}, nil
}

func (a *OpenAIAPI) Name() string { return openAIAPIProvider + "/" + a.model }

func (a *OpenAIAPI) Descriptor() ModelDescriptor {
	return ModelDescriptor{Provider: openAIAPIProvider, Model: a.model, ExecutionProfileVersion: openAIAPIProfile}
}

func (a *OpenAIAPI) Complete(ctx context.Context, prompt string) (ModelResponse, error) {
	if ctx == nil {
		return ModelResponse{}, fmt.Errorf("OpenAI API context is required")
	}
	if !utf8.ValidString(prompt) || strings.TrimSpace(prompt) == "" || len(prompt) > openAIMaximumPromptBytes {
		return ModelResponse{}, fmt.Errorf("OpenAI API prompt must contain 1 to %d valid UTF-8 bytes", openAIMaximumPromptBytes)
	}
	key, err := a.apiKey(ctx)
	if err != nil {
		return ModelResponse{}, fmt.Errorf("OpenAI API credential is unavailable")
	}
	if err := validateOpenAIKey(key); err != nil {
		return ModelResponse{}, err
	}
	clientRequestID, err := newOpenAIClientRequestID()
	if err != nil {
		return ModelResponse{}, fmt.Errorf("create OpenAI API request identity: %w", err)
	}
	body, err := json.Marshal(openAIRequest{
		Model:           a.model,
		Input:           prompt,
		Instructions:    openAIInstructions,
		ToolChoice:      "none",
		Tools:           []struct{}{},
		MaxOutputTokens: openAIMaximumOutputTokens,
		Store:           false,
		Truncation:      "disabled",
	})
	if err != nil {
		return ModelResponse{}, fmt.Errorf("encode OpenAI API request: %w", err)
	}
	requestCtx, cancel := context.WithTimeout(ctx, openAIAPITimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, a.endpoint, bytes.NewReader(body))
	if err != nil {
		return ModelResponse{}, fmt.Errorf("create OpenAI API request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+key)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Client-Request-Id", clientRequestID)

	response, err := a.client.Do(request)
	if err != nil {
		if requestCtx.Err() != nil {
			return ModelResponse{}, fmt.Errorf("OpenAI API request did not complete (%s): %w", clientRequestID, requestCtx.Err())
		}
		return ModelResponse{}, fmt.Errorf("OpenAI API request failed (%s)", clientRequestID)
	}
	defer func() { _ = response.Body.Close() }()
	providerRequestID := canonicalOpenAIRequestID(response.Header.Get("X-Request-Id"))
	if response.StatusCode != http.StatusOK {
		return ModelResponse{}, openAIStatusError(response.StatusCode, clientRequestID, providerRequestID)
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return ModelResponse{}, fmt.Errorf("OpenAI API response was not application/json (%s)", requestReference(clientRequestID, providerRequestID))
	}
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, openAIMaximumBodyBytes+1))
	if err != nil {
		return ModelResponse{}, fmt.Errorf("read OpenAI API response (%s)", requestReference(clientRequestID, providerRequestID))
	}
	if len(responseBody) > openAIMaximumBodyBytes {
		return ModelResponse{}, fmt.Errorf("OpenAI API response exceeded %d bytes (%s)", openAIMaximumBodyBytes, requestReference(clientRequestID, providerRequestID))
	}
	if !utf8.Valid(responseBody) {
		return ModelResponse{}, fmt.Errorf("OpenAI API response was not valid UTF-8 (%s)", requestReference(clientRequestID, providerRequestID))
	}
	decoded, err := decodeOpenAIResponse(responseBody)
	if err != nil {
		return ModelResponse{}, fmt.Errorf("invalid OpenAI API response (%s): %w", requestReference(clientRequestID, providerRequestID), err)
	}
	validated, err := a.validateResponse(decoded)
	if err != nil {
		return ModelResponse{}, fmt.Errorf("invalid OpenAI API response (%s): %w", requestReference(clientRequestID, providerRequestID), err)
	}
	return validated, nil
}

type openAIRequest struct {
	Model           string     `json:"model"`
	Input           string     `json:"input"`
	Instructions    string     `json:"instructions"`
	ToolChoice      string     `json:"tool_choice"`
	Tools           []struct{} `json:"tools"`
	MaxOutputTokens int        `json:"max_output_tokens"`
	Store           bool       `json:"store"`
	Truncation      string     `json:"truncation"`
}

type openAIResponse struct {
	ID                string             `json:"id"`
	Object            string             `json:"object"`
	Status            string             `json:"status"`
	Error             *json.RawMessage   `json:"error"`
	IncompleteDetails *json.RawMessage   `json:"incomplete_details"`
	Model             string             `json:"model"`
	Output            []openAIOutputItem `json:"output"`
	Store             *bool              `json:"store"`
	ToolChoice        string             `json:"tool_choice"`
	Tools             []json.RawMessage  `json:"tools"`
	Truncation        string             `json:"truncation"`
	MaxOutputTokens   *int               `json:"max_output_tokens"`
	Usage             *struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
		TotalTokens  int `json:"total_tokens"`
	} `json:"usage"`
}

type openAIOutputItem struct {
	Type    string                `json:"type"`
	Role    string                `json:"role"`
	Status  string                `json:"status"`
	Content []openAIOutputContent `json:"content"`
}

type openAIOutputContent struct {
	Type        string            `json:"type"`
	Text        string            `json:"text"`
	Refusal     string            `json:"refusal"`
	Annotations []json.RawMessage `json:"annotations"`
}

func decodeOpenAIResponse(body []byte) (openAIResponse, error) {
	var response openAIResponse
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&response); err != nil {
		return openAIResponse{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return openAIResponse{}, fmt.Errorf("trailing JSON content")
	}
	return response, nil
}

func (a *OpenAIAPI) validateResponse(response openAIResponse) (ModelResponse, error) {
	if response.Object != "response" || response.ID == "" || !canonicalASCII(response.ID, 512) {
		return ModelResponse{}, fmt.Errorf("response identity is invalid")
	}
	if response.Status != "completed" || response.Error != nil || response.IncompleteDetails != nil {
		return ModelResponse{}, fmt.Errorf("response did not complete")
	}
	if response.Model != a.model {
		return ModelResponse{}, fmt.Errorf("response model does not match the configured snapshot")
	}
	if response.Store == nil || *response.Store || response.ToolChoice != "none" || response.Tools == nil || len(response.Tools) != 0 || response.Truncation != "disabled" || response.MaxOutputTokens == nil || *response.MaxOutputTokens != openAIMaximumOutputTokens {
		return ModelResponse{}, fmt.Errorf("response did not preserve the model-only execution profile")
	}
	if len(response.Output) == 0 || len(response.Output) > openAIMaximumOutputItems {
		return ModelResponse{}, fmt.Errorf("response output item count is invalid")
	}
	var output strings.Builder
	for _, item := range response.Output {
		if item.Status != "" && item.Status != "completed" {
			return ModelResponse{}, fmt.Errorf("response contained an incomplete output item")
		}
		switch item.Type {
		case "reasoning":
			continue
		case "message":
			if item.Role != "assistant" || len(item.Content) == 0 {
				return ModelResponse{}, fmt.Errorf("response message contract is invalid")
			}
		default:
			return ModelResponse{}, fmt.Errorf("response contained disallowed output item type %q", item.Type)
		}
		for _, content := range item.Content {
			if content.Type == "refusal" {
				return ModelResponse{}, fmt.Errorf("model refused the request")
			}
			if content.Type != "output_text" || content.Refusal != "" || len(content.Annotations) != 0 || !utf8.ValidString(content.Text) {
				return ModelResponse{}, fmt.Errorf("response contained disallowed message content")
			}
			if content.Text == "" || output.Len() > openAIMaximumResponseBytes-len(content.Text) {
				return ModelResponse{}, fmt.Errorf("OpenAI API output must contain 1 to %d bytes", openAIMaximumResponseBytes)
			}
			_, _ = output.WriteString(content.Text)
		}
	}
	if output.Len() == 0 {
		return ModelResponse{}, fmt.Errorf("response contained no output text")
	}
	if response.Usage == nil {
		return ModelResponse{}, fmt.Errorf("response contained no token usage")
	}
	usage := events.InferenceUsageRecordedPayload{
		Source:       "provider_api",
		Provider:     openAIAPIProvider,
		Model:        a.model,
		InputTokens:  response.Usage.InputTokens,
		OutputTokens: response.Usage.OutputTokens,
		TotalTokens:  response.Usage.TotalTokens,
	}
	if usage.InputTokens <= 0 || usage.OutputTokens <= 0 || usage.TotalTokens > openAIMaximumUsageTokens || !usage.Valid() {
		return ModelResponse{}, fmt.Errorf("response contained invalid token usage")
	}
	return ModelResponse{Text: output.String(), Usage: usage}, nil
}

func validateOpenAIModel(model string) error {
	if len(model) == 0 || len(model) > openAIMaximumModelBytes || strings.TrimSpace(model) != model || !canonicalASCII(model, openAIMaximumModelBytes) {
		return fmt.Errorf("OpenAI API model must be a canonical identifier of at most %d bytes", openAIMaximumModelBytes)
	}
	if !modelid.HasDatedSnapshot(model) {
		return fmt.Errorf("OpenAI API model must identify an exact dated snapshot")
	}
	return nil
}

func validateOpenAIKey(key string) error {
	if len(key) == 0 || len(key) > openAIMaximumKeyBytes || !canonicalASCII(key, openAIMaximumKeyBytes) {
		return fmt.Errorf("OpenAI API credential is invalid")
	}
	return nil
}

func canonicalASCII(value string, maximum int) bool {
	if len(value) == 0 || len(value) > maximum {
		return false
	}
	for index := range len(value) {
		if value[index] < 0x21 || value[index] > 0x7e {
			return false
		}
	}
	return true
}

func validateOpenAIEndpoint(endpoint string) error {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("OpenAI API endpoint must be an absolute HTTPS URL without user info, query, or fragment")
	}
	return nil
}

func boundedOpenAIClient(base *http.Client) *http.Client {
	if base == nil {
		transport, _ := http.DefaultTransport.(*http.Transport)
		if transport != nil {
			transport = transport.Clone()
		} else {
			transport = &http.Transport{}
		}
		transport.Proxy = nil
		if transport.TLSClientConfig == nil {
			transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		} else {
			transport.TLSClientConfig = transport.TLSClientConfig.Clone()
			transport.TLSClientConfig.MinVersion = tls.VersionTLS12
		}
		base = &http.Client{Transport: transport}
	}
	client := *base
	client.Timeout = openAIAPITimeout
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	return &client
}

func newOpenAIClientRequestID() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return "agentos-" + hex.EncodeToString(random[:]), nil
}

func canonicalOpenAIRequestID(value string) string {
	if !canonicalASCII(value, 512) {
		return ""
	}
	return value
}

func openAIStatusError(status int, clientRequestID, providerRequestID string) error {
	return fmt.Errorf("OpenAI API returned HTTP %d (%s)", status, requestReference(clientRequestID, providerRequestID))
}

func requestReference(clientRequestID, providerRequestID string) string {
	if providerRequestID == "" {
		return "client_request_id=" + clientRequestID
	}
	return "client_request_id=" + clientRequestID + ", request_id=" + providerRequestID
}

package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/dominicnunez/agentos-a2a-go/executionkind"
	"github.com/dominicnunez/agentos-a2a-go/intentconfirmation"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/intake"
)

const (
	a2aRoleUser           = string(a2a.MessageRoleUser)
	a2aStateInputRequired = string(a2a.TaskStateInputRequired)
	a2aStateCompleted     = string(a2a.TaskStateCompleted)
	intentConfirmationURI = intentconfirmation.URI
)

type strictJSONRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type strictSendMessageRequest struct {
	Tenant        string            `json:"tenant,omitempty"`
	Configuration *json.RawMessage  `json:"configuration,omitempty"`
	Message       *strictA2AMessage `json:"message"`
	Metadata      map[string]any    `json:"metadata,omitempty"`
}

type strictGetTaskRequest struct {
	Tenant        string `json:"tenant,omitempty"`
	ID            string `json:"id"`
	HistoryLength *int   `json:"historyLength,omitempty"`
}

type strictA2AMessage struct {
	MessageID      string          `json:"messageId"`
	ContextID      string          `json:"contextId,omitempty"`
	Extensions     []string        `json:"extensions,omitempty"`
	Metadata       map[string]any  `json:"metadata,omitempty"`
	Parts          []strictA2APart `json:"parts"`
	ReferenceTasks []string        `json:"referenceTaskIds,omitempty"`
	Role           string          `json:"role"`
	TaskID         string          `json:"taskId,omitempty"`
}

type strictA2APart struct {
	Text      *string         `json:"text,omitempty"`
	Raw       json.RawMessage `json:"raw,omitempty"`
	Data      json.RawMessage `json:"data,omitempty"`
	URL       string          `json:"url,omitempty"`
	Filename  string          `json:"filename,omitempty"`
	MediaType string          `json:"mediaType,omitempty"`
	Metadata  map[string]any  `json:"metadata,omitempty"`
}

func validateA2ARequest(body []byte) error {
	var envelope strictJSONRPCRequest
	if err := decodeStrictJSON(body, &envelope); err != nil {
		return err
	}
	if envelope.JSONRPC != "2.0" || !validRPCID(envelope.ID) || envelope.Method == "" {
		return errors.New("valid JSON-RPC 2.0 id and method are required")
	}
	switch envelope.Method {
	case "SendMessage":
		return validateSendMessageParams(envelope.Params)
	case "GetTask":
		return validateGetTaskParams(envelope.Params)
	default:
		return nil
	}
}

func validateSendMessageParams(raw json.RawMessage) error {
	var params strictSendMessageRequest
	if err := decodeStrictJSON(raw, &params); err != nil {
		return fmt.Errorf("invalid SendMessage parameters: %w", err)
	}
	if params.Tenant != "" || params.Configuration != nil || len(params.Metadata) != 0 {
		return errors.New("tenant, configuration, and request metadata are outside the V1 boundary")
	}
	message := params.Message
	if message == nil || message.MessageID == "" || message.Role != a2aRoleUser || len(message.Parts) != 1 || len(message.ReferenceTasks) != 0 {
		return errors.New("one ROLE_USER text message with messageId is required")
	}
	if len(message.Extensions) > 2 || len(message.Metadata) > 2 {
		return errors.New("only the declared Agent OS extensions are supported")
	}
	declared := make(map[string]struct{}, len(message.Extensions))
	for _, extension := range message.Extensions {
		if extension != executionkind.URI && extension != intentConfirmationURI {
			return errors.New("message declares an unsupported extension")
		}
		if _, duplicate := declared[extension]; duplicate {
			return errors.New("message declares a duplicate extension")
		}
		declared[extension] = struct{}{}
	}
	for key := range message.Metadata {
		if key != executionkind.URI && key != intentConfirmationURI {
			return errors.New("message contains unsupported metadata")
		}
		if _, ok := declared[key]; !ok {
			return errors.New("message metadata requires its extension declaration")
		}
	}
	part := message.Parts[0]
	if part.Text == nil || strings.TrimSpace(*part.Text) == "" || len(part.Raw) != 0 || len(part.Data) != 0 || part.URL != "" || part.Filename != "" || len(part.Metadata) != 0 || (part.MediaType != "" && part.MediaType != "text/plain") {
		return errors.New("one non-empty text/plain part is required")
	}
	return nil
}

func validateGetTaskParams(raw json.RawMessage) error {
	var params strictGetTaskRequest
	if err := decodeStrictJSON(raw, &params); err != nil {
		return fmt.Errorf("invalid GetTask parameters: %w", err)
	}
	if params.ID == "" || params.Tenant != "" || params.HistoryLength != nil {
		return errors.New("GetTask supports only a non-empty id in V1")
	}
	return nil
}

func decodeStrictJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("content must contain one JSON object")
	}
	return nil
}

func validRPCID(id json.RawMessage) bool {
	if len(id) == 0 || len(id) > 256 || string(id) == "null" {
		return false
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(id))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return false
	}
	switch value.(type) {
	case string, json.Number:
		return true
	default:
		return false
	}
}

type a2aRequestHandler struct {
	service *intake.Service
}

var _ a2asrv.RequestHandler = (*a2aRequestHandler)(nil)

func (h *a2aRequestHandler) SendMessage(ctx context.Context, request *a2a.SendMessageRequest) (a2a.SendMessageResult, error) {
	principal, ok := a2aPrincipalFrom(ctx)
	if !ok {
		return nil, a2a.ErrUnauthenticated
	}
	if request == nil || request.Message == nil || len(request.Message.Parts) != 1 {
		return nil, a2a.ErrInvalidParams
	}
	message := request.Message
	contextID := message.ContextID
	confirmation, confirms, err := intentconfirmation.Get(message)
	if err != nil {
		return nil, a2a.NewError(a2a.ErrInvalidParams, "intent-confirmation extension is invalid")
	}
	if confirms {
		if message.TaskID == "" || contextID == "" {
			return nil, a2a.NewError(a2a.ErrInvalidParams, "intent confirmation requires durable taskId and contextId")
		}
		for _, extension := range message.Extensions {
			if extension == executionkind.URI {
				return nil, a2a.NewError(a2a.ErrInvalidParams, "intent confirmation cannot carry an execution-routing hint")
			}
		}
		view, confirmErr := h.service.ConfirmIntent(ctx, principal, intake.IntentConfirmation{ConversationID: contextID, TaskID: string(message.TaskID), MessageID: message.ID, Fingerprint: confirmation.Fingerprint})
		if confirmErr != nil {
			return nil, intakeA2AError(confirmErr)
		}
		return projectA2ATask(view), nil
	}
	if message.TaskID != "" {
		durable, err := h.service.Get(ctx, principal, string(message.TaskID))
		if err != nil {
			return nil, intakeA2AError(err)
		}
		if contextID != "" && contextID != durable.ConversationID {
			return nil, a2a.NewError(a2a.ErrInvalidParams, "contextId does not match durable task state")
		}
		contextID = durable.ConversationID
	} else if contextID == "" {
		contextID = generatedA2AContextID(principal, message.ID)
	}
	kind, present, err := executionkind.Get(message)
	if err != nil {
		return nil, a2a.NewError(a2a.ErrInvalidParams, "execution-kind extension is invalid")
	}
	requestedKind := core.ExecutionKind("")
	if present {
		requestedKind = core.ExecutionKind(kind)
	}
	view, err := h.service.Handle(ctx, principal, intake.Message{
		ConversationID: contextID,
		TaskID:         string(message.TaskID),
		MessageID:      message.ID,
		Text:           message.Parts[0].Text(),
		RequestedKind:  requestedKind,
	})
	if err != nil {
		return nil, intakeA2AError(err)
	}
	return projectA2ATask(view), nil
}

func generatedA2AContextID(principal intake.Principal, messageID string) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte("agentos-a2a-context-v1\x00"))
	_, _ = hash.Write([]byte(principal.OrganizationID))
	_, _ = hash.Write([]byte{'\x00'})
	_, _ = hash.Write([]byte(principal.ID))
	_, _ = hash.Write([]byte{'\x00'})
	_, _ = hash.Write([]byte(messageID))
	return "ctx-" + base64.RawURLEncoding.EncodeToString(hash.Sum(nil))
}

func (h *a2aRequestHandler) GetTask(ctx context.Context, request *a2a.GetTaskRequest) (*a2a.Task, error) {
	principal, ok := a2aPrincipalFrom(ctx)
	if !ok {
		return nil, a2a.ErrUnauthenticated
	}
	if request == nil || request.ID == "" {
		return nil, a2a.ErrInvalidParams
	}
	view, err := h.service.Get(ctx, principal, string(request.ID))
	if err != nil {
		return nil, intakeA2AError(err)
	}
	return projectA2ATask(view), nil
}

func intakeA2AError(err error) error {
	switch {
	case errors.Is(err, intake.ErrForbidden):
		return a2a.NewError(a2a.ErrUnauthorized, "operator capability required")
	case errors.Is(err, intake.ErrNotFound):
		return a2a.ErrTaskNotFound
	case errors.Is(err, intake.ErrInvalid), errors.Is(err, intake.ErrConflict):
		return a2a.NewError(a2a.ErrInvalidParams, "operator message is invalid or conflicts with durable work")
	default:
		return a2a.NewError(a2a.ErrServerError, "operator work is unavailable")
	}
}

func projectA2ATask(view intake.View) *a2a.Task {
	task := &a2a.Task{
		ID:        a2a.TaskID(view.TaskID),
		ContextID: view.ConversationID,
		Status:    a2a.TaskStatus{State: a2aState(view.State)},
	}
	if !view.UpdatedAt.IsZero() {
		updatedAt := view.UpdatedAt.UTC()
		task.Status.Timestamp = &updatedAt
	}
	if view.Prompt != "" {
		task.Status.Message = statusMessage(view.ConversationID, view.TaskID, "status-"+view.TaskID, view.Prompt)
	}
	if view.Result != "" {
		part := a2a.NewTextPart(view.Result)
		part.MediaType = "text/plain"
		task.Artifacts = []*a2a.Artifact{{
			ID: a2a.ArtifactID("result-" + view.TaskID), Name: "Agent OS result", Parts: a2a.ContentParts{part},
		}}
	}
	if view.Intent != nil {
		review := map[string]any{
			"state": view.State, "fingerprint": view.Intent.Fingerprint, "version": view.Intent.Version,
			"mode": view.Intent.Mode, "objective": view.Intent.Objective, "context": view.Intent.Context, "deliverables": view.Intent.Deliverables,
			"completion_criteria": view.Intent.CompletionCriteria, "constraints": view.Intent.Constraints,
			"resolved_decisions": view.Intent.ResolvedDecisions, "consequence_candidates": view.Intent.ConsequenceCandidates,
			"missing_user_inputs": view.Intent.MissingUserInputs,
		}
		if view.Intent.Goal != nil {
			review["goal"] = *view.Intent.Goal
		}
		task.Metadata = map[string]any{intentConfirmationURI: review}
	}
	return task
}

func a2aState(state string) a2a.TaskState {
	switch state {
	case intake.StateWorking:
		return a2a.TaskStateWorking
	case intake.StateInputRequired:
		return a2a.TaskStateInputRequired
	case intake.StateAwaitingConfirmation:
		return a2a.TaskStateInputRequired
	case intake.StateCompleted:
		return a2a.TaskStateCompleted
	case intake.StateFailed:
		return a2a.TaskStateFailed
	default:
		return a2a.TaskStateFailed
	}
}

func statusMessage(contextID, taskID, messageID, text string) *a2a.Message {
	part := a2a.NewTextPart(text)
	part.MediaType = "text/plain"
	return &a2a.Message{
		ID: messageID, ContextID: contextID, TaskID: a2a.TaskID(taskID),
		Role: a2a.MessageRoleAgent, Parts: a2a.ContentParts{part},
	}
}

func (*a2aRequestHandler) ListTasks(context.Context, *a2a.ListTasksRequest) (*a2a.ListTasksResponse, error) {
	return nil, a2a.ErrUnsupportedOperation
}

func (*a2aRequestHandler) CancelTask(context.Context, *a2a.CancelTaskRequest) (*a2a.Task, error) {
	return nil, a2a.ErrUnsupportedOperation
}

func (*a2aRequestHandler) SubscribeToTask(context.Context, *a2a.SubscribeToTaskRequest) iter.Seq2[a2a.Event, error] {
	return unsupportedA2AStream()
}

func (*a2aRequestHandler) SendStreamingMessage(context.Context, *a2a.SendMessageRequest) iter.Seq2[a2a.Event, error] {
	return unsupportedA2AStream()
}

func unsupportedA2AStream() iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		yield(nil, a2a.ErrUnsupportedOperation)
	}
}

func (*a2aRequestHandler) GetTaskPushConfig(context.Context, *a2a.GetTaskPushConfigRequest) (*a2a.PushConfig, error) {
	return nil, a2a.ErrPushNotificationNotSupported
}

func (*a2aRequestHandler) ListTaskPushConfigs(context.Context, *a2a.ListTaskPushConfigRequest) (*a2a.ListTaskPushConfigResponse, error) {
	return nil, a2a.ErrPushNotificationNotSupported
}

func (*a2aRequestHandler) CreateTaskPushConfig(context.Context, *a2a.PushConfig) (*a2a.PushConfig, error) {
	return nil, a2a.ErrPushNotificationNotSupported
}

func (*a2aRequestHandler) DeleteTaskPushConfig(context.Context, *a2a.DeleteTaskPushConfigRequest) error {
	return a2a.ErrPushNotificationNotSupported
}

func (*a2aRequestHandler) GetExtendedAgentCard(context.Context, *a2a.GetExtendedAgentCardRequest) (*a2a.AgentCard, error) {
	return nil, a2a.ErrExtendedCardNotConfigured
}

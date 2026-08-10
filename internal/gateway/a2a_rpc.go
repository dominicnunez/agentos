package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/intake"
)

const (
	a2aRoleUser             = "ROLE_USER"
	a2aRoleAgent            = "ROLE_AGENT"
	a2aStateWorking         = "TASK_STATE_WORKING"
	a2aStateInputRequired   = "TASK_STATE_INPUT_REQUIRED"
	a2aStateCompleted       = "TASK_STATE_COMPLETED"
	a2aStateFailed          = "TASK_STATE_FAILED"
	rpcInvalidRequest       = -32600
	rpcMethodNotFound       = -32601
	rpcInvalidParams        = -32602
	rpcTaskNotFound         = -32001
	rpcWorkUnavailable      = -32002
	agentOSExecutionKindKey = "agentos.execution_kind"
)

type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type jsonRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *jsonRPCError   `json:"error,omitempty"`
}

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type sendMessageParams struct {
	Message a2aMessage `json:"message"`
}

type getTaskParams struct {
	ID string `json:"id"`
}

type a2aMessage struct {
	MessageID string                     `json:"messageId"`
	ContextID string                     `json:"contextId"`
	TaskID    string                     `json:"taskId,omitempty"`
	Role      string                     `json:"role"`
	Parts     []a2aPart                  `json:"parts"`
	Metadata  map[string]json.RawMessage `json:"metadata,omitempty"`
}

type a2aPart struct {
	Text      string `json:"text"`
	MediaType string `json:"mediaType,omitempty"`
}

type a2aTask struct {
	ID        string        `json:"id"`
	ContextID string        `json:"contextId"`
	Status    a2aTaskStatus `json:"status"`
	Artifacts []a2aArtifact `json:"artifacts,omitempty"`
}

type a2aTaskStatus struct {
	State     string      `json:"state"`
	Message   *a2aMessage `json:"message,omitempty"`
	Timestamp string      `json:"timestamp,omitempty"`
}

type a2aArtifact struct {
	ArtifactID string    `json:"artifactId"`
	Name       string    `json:"name,omitempty"`
	Parts      []a2aPart `json:"parts"`
}

type sendMessageResponse struct {
	Task a2aTask `json:"task"`
}

func (a *A2A) serveJSONRPC(w http.ResponseWriter, r *http.Request, principal intake.Principal) {
	defer func() {
		_ = r.Body.Close()
	}()
	var request jsonRPCRequest
	if err := decodeWorkContent(w, r, &request); err != nil {
		writeRPCError(w, json.RawMessage("null"), rpcInvalidRequest, err.Error())
		return
	}
	if request.JSONRPC != "2.0" || !validRPCID(request.ID) || request.Method == "" {
		writeRPCError(w, json.RawMessage("null"), rpcInvalidRequest, "valid JSON-RPC 2.0 id and method are required")
		return
	}
	switch request.Method {
	case "SendMessage":
		a.sendMessage(w, r, request, principal)
	case "GetTask":
		a.getTask(w, r, request, principal)
	default:
		writeRPCError(w, request.ID, rpcMethodNotFound, "A2A method is not supported")
	}
}

func (a *A2A) sendMessage(w http.ResponseWriter, r *http.Request, request jsonRPCRequest, principal intake.Principal) {
	var params sendMessageParams
	if err := decodeStrictRaw(request.Params, &params); err != nil {
		writeRPCError(w, request.ID, rpcInvalidParams, "params.message is required")
		return
	}
	message := params.Message
	if message.MessageID == "" || message.ContextID == "" || message.Role != a2aRoleUser || len(message.Parts) != 1 || message.Parts[0].Text == "" || len(message.Metadata) > 16 || (message.Parts[0].MediaType != "" && message.Parts[0].MediaType != "text/plain") {
		writeRPCError(w, request.ID, rpcInvalidParams, "one ROLE_USER text/plain message part with messageId and contextId is required")
		return
	}
	kind, err := requestedExecutionKind(message.Metadata)
	if err != nil {
		writeRPCError(w, request.ID, rpcInvalidParams, err.Error())
		return
	}
	view, err := a.service.Handle(r.Context(), principal, intake.Message{
		ConversationID: message.ContextID, MessageID: message.MessageID,
		Text: message.Parts[0].Text, RequestedKind: kind,
	})
	if err != nil {
		a.writeIntakeError(w, request.ID, err)
		return
	}
	writeRPCResult(w, request.ID, sendMessageResponse{Task: projectA2ATask(view)})
}

func (a *A2A) getTask(w http.ResponseWriter, r *http.Request, request jsonRPCRequest, principal intake.Principal) {
	var params getTaskParams
	if err := decodeStrictRaw(request.Params, &params); err != nil || params.ID == "" {
		writeRPCError(w, request.ID, rpcInvalidParams, "params.id is required")
		return
	}
	view, err := a.service.Get(r.Context(), principal, params.ID)
	if err != nil {
		a.writeIntakeError(w, request.ID, err)
		return
	}
	writeRPCResult(w, request.ID, projectA2ATask(view))
}

func (a *A2A) writeIntakeError(w http.ResponseWriter, id json.RawMessage, err error) {
	switch {
	case errors.Is(err, intake.ErrForbidden):
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "operator capability required"})
	case errors.Is(err, intake.ErrNotFound):
		writeRPCError(w, id, rpcTaskNotFound, "task not found")
	case errors.Is(err, intake.ErrInvalid), errors.Is(err, intake.ErrConflict):
		writeRPCError(w, id, rpcInvalidParams, "operator message is invalid or conflicts with durable work")
	default:
		writeRPCError(w, id, rpcWorkUnavailable, "operator work is unavailable")
	}
}

func requestedExecutionKind(metadata map[string]json.RawMessage) (core.ExecutionKind, error) {
	var kind core.ExecutionKind
	for key, raw := range metadata {
		if key != agentOSExecutionKindKey {
			continue
		}
		if err := json.Unmarshal(raw, &kind); err != nil {
			return "", fmt.Errorf("%s must be a string", agentOSExecutionKindKey)
		}
	}
	return kind, nil
}

func validRPCID(id json.RawMessage) bool {
	if len(id) == 0 || len(id) > 256 || string(id) == "null" {
		return false
	}
	var value any
	if err := json.Unmarshal(id, &value); err != nil {
		return false
	}
	switch value.(type) {
	case string, float64:
		return true
	default:
		return false
	}
}

func decodeStrictRaw(raw json.RawMessage, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("content must contain one JSON object")
	}
	return nil
}

func projectA2ATask(view intake.View) a2aTask {
	task := a2aTask{ID: view.TaskID, ContextID: view.ConversationID, Status: a2aTaskStatus{State: a2aState(view.State)}}
	if !view.UpdatedAt.IsZero() {
		task.Status.Timestamp = view.UpdatedAt.UTC().Format(time.RFC3339Nano)
	}
	if view.Prompt != "" {
		task.Status.Message = statusMessage(view.ConversationID, view.TaskID, "status-"+view.TaskID, view.Prompt)
	}
	if view.Result != "" {
		task.Artifacts = []a2aArtifact{{ArtifactID: "result-" + view.TaskID, Name: "Agent OS result", Parts: []a2aPart{{Text: view.Result, MediaType: "text/plain"}}}}
	}
	return task
}

func a2aState(state string) string {
	switch state {
	case intake.StateWorking:
		return a2aStateWorking
	case intake.StateInputRequired:
		return a2aStateInputRequired
	case intake.StateCompleted:
		return a2aStateCompleted
	case intake.StateFailed:
		return a2aStateFailed
	default:
		return a2aStateFailed
	}
}

func statusMessage(contextID, taskID, messageID, text string) *a2aMessage {
	return &a2aMessage{MessageID: messageID, ContextID: contextID, TaskID: taskID, Role: a2aRoleAgent, Parts: []a2aPart{{Text: text, MediaType: "text/plain"}}}
}

func writeRPCResult(w http.ResponseWriter, id json.RawMessage, result any) {
	writeJSON(w, http.StatusOK, jsonRPCResponse{JSONRPC: "2.0", ID: id, Result: result})
}

func writeRPCError(w http.ResponseWriter, id json.RawMessage, code int, message string) {
	writeJSON(w, http.StatusOK, jsonRPCResponse{JSONRPC: "2.0", ID: id, Error: &jsonRPCError{Code: code, Message: message}})
}

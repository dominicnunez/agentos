package gateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

const (
	a2aRoleUser             = "ROLE_USER"
	a2aRoleAgent            = "ROLE_AGENT"
	a2aStateInputRequired   = "TASK_STATE_INPUT_REQUIRED"
	a2aStateCompleted       = "TASK_STATE_COMPLETED"
	a2aStateFailed          = "TASK_STATE_FAILED"
	rpcInvalidRequest       = -32600
	rpcMethodNotFound       = -32601
	rpcInvalidParams        = -32602
	rpcTaskNotFound         = -32001
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

func (a *A2A) serveJSONRPC(w http.ResponseWriter, r *http.Request) {
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
		a.sendMessage(w, r, request)
	case "GetTask":
		a.getTask(w, r, request)
	default:
		writeRPCError(w, request.ID, rpcMethodNotFound, "A2A method is not supported")
	}
}

func (a *A2A) sendMessage(w http.ResponseWriter, r *http.Request, request jsonRPCRequest) {
	var params sendMessageParams
	if err := json.Unmarshal(request.Params, &params); err != nil {
		writeRPCError(w, request.ID, rpcInvalidParams, "params.message is required")
		return
	}
	message := params.Message
	if message.MessageID == "" || message.ContextID == "" || message.Role != a2aRoleUser || len(message.Parts) != 1 || message.Parts[0].Text == "" || (message.Parts[0].MediaType != "" && message.Parts[0].MediaType != "text/plain") {
		writeRPCError(w, request.ID, rpcInvalidParams, "one ROLE_USER text/plain message part with messageId and contextId is required")
		return
	}
	kind, err := executionKind(message.Metadata)
	if err != nil {
		writeRPCError(w, request.ID, rpcInvalidParams, err.Error())
		return
	}
	stream, err := a.service.ExternalEvents(r.Context(), a.actor.OrganizationID, message.ContextID)
	if err != nil {
		writeRPCError(w, request.ID, rpcInvalidParams, "work stream is unavailable")
		return
	}
	if len(stream) == 0 {
		if !a.allowed("submit_work") {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "capability submit_work required"})
			return
		}
		_, submitErr := a.service.Submit(r.Context(), app.Submit{RequestID: message.ContextID, OrganizationID: a.actor.OrganizationID, Statement: message.Parts[0].Text, Kind: kind})
		stream, err = a.service.ExternalEvents(r.Context(), a.actor.OrganizationID, message.ContextID)
		if err != nil || (submitErr != nil && len(stream) == 0) {
			writeRPCError(w, request.ID, rpcInvalidParams, "work could not be submitted")
			return
		}
	} else if externalState(stream) == a2aStateInputRequired && !matchesOriginalInstruction(stream, message.Parts[0].Text) {
		if !a.allowed("provide_input") {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "capability provide_input required"})
			return
		}
		taskID := streamTaskID(stream)
		if taskID == "" {
			writeRPCError(w, request.ID, rpcInvalidParams, "blocked work has no task mapping")
			return
		}
		if err := a.service.ProvideExternalInput(r.Context(), a.actor.OrganizationID, a.actor.ID, message.ContextID, taskID, message.MessageID, message.Parts[0].Text); err != nil {
			writeRPCError(w, request.ID, rpcInvalidParams, "input could not continue the task")
			return
		}
		stream, err = a.service.ExternalEvents(r.Context(), a.actor.OrganizationID, message.ContextID)
		if err != nil {
			writeRPCError(w, request.ID, rpcInvalidParams, "continued work stream is unavailable")
			return
		}
	} else if externalState(stream) != a2aStateInputRequired {
		if !matchesOriginalInstruction(stream, message.Parts[0].Text) && !matchesDurableInput(stream, message.MessageID, message.Parts[0].Text) {
			writeRPCError(w, request.ID, rpcInvalidParams, "contextId is already bound to different work")
			return
		}
	}
	if streamTaskID(stream) == "" {
		writeRPCError(w, request.ID, rpcInvalidParams, "work did not create a task")
		return
	}
	writeRPCResult(w, request.ID, sendMessageResponse{Task: projectA2ATask(message.ContextID, stream, a.allowed("read_result"))})
}

func (a *A2A) getTask(w http.ResponseWriter, r *http.Request, request jsonRPCRequest) {
	if !a.allowed("read_status") {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "capability read_status required"})
		return
	}
	var params getTaskParams
	if err := json.Unmarshal(request.Params, &params); err != nil || params.ID == "" {
		writeRPCError(w, request.ID, rpcInvalidParams, "params.id is required")
		return
	}
	contextID := strings.TrimPrefix(params.ID, "task-")
	stream, err := a.service.ExternalEvents(r.Context(), a.actor.OrganizationID, contextID)
	if err != nil {
		writeRPCError(w, request.ID, rpcInvalidParams, "work stream is unavailable")
		return
	}
	if len(stream) == 0 || streamTaskID(stream) != params.ID {
		writeRPCError(w, request.ID, rpcTaskNotFound, "task not found")
		return
	}
	writeRPCResult(w, request.ID, projectA2ATask(contextID, stream, a.allowed("read_result")))
}

func executionKind(metadata map[string]json.RawMessage) (core.ExecutionKind, error) {
	kind := core.ExecutionDeterministic
	for key, raw := range metadata {
		if key != agentOSExecutionKindKey {
			continue
		}
		if err := json.Unmarshal(raw, &kind); err != nil {
			return "", fmt.Errorf("%s must be a string", agentOSExecutionKindKey)
		}
	}
	switch kind {
	case core.ExecutionDeterministic, core.ExecutionAgent, core.ExecutionHuman:
		return kind, nil
	default:
		return "", fmt.Errorf("%s is not a supported execution kind", agentOSExecutionKindKey)
	}
}

func validRPCID(id json.RawMessage) bool {
	if len(id) == 0 || string(id) == "null" {
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

func projectA2ATask(contextID string, stream []events.Event, includeResult bool) a2aTask {
	taskID := streamTaskID(stream)
	task := a2aTask{ID: taskID, ContextID: contextID, Status: a2aTaskStatus{State: externalState(stream)}}
	if len(stream) > 0 && !stream[len(stream)-1].CreatedAt.IsZero() {
		task.Status.Timestamp = stream[len(stream)-1].CreatedAt.UTC().Format(time.RFC3339Nano)
	}
	switch task.Status.State {
	case a2aStateInputRequired:
		text := blockedStatusText(stream)
		task.Status.Message = statusMessage(contextID, taskID, "input-required-"+taskID, text)
	case a2aStateFailed:
		task.Status.Message = statusMessage(contextID, taskID, "failed-"+taskID, "Agent OS could not complete the task.")
	case a2aStateCompleted:
		if includeResult {
			if result, ok := publishedResult(stream); ok {
				task.Artifacts = []a2aArtifact{{ArtifactID: "result-" + taskID, Name: "Agent OS result", Parts: []a2aPart{{Text: result.Summary, MediaType: "text/plain"}}}}
			}
		}
	}
	return task
}

func statusMessage(contextID, taskID, messageID, text string) *a2aMessage {
	return &a2aMessage{MessageID: messageID, ContextID: contextID, TaskID: taskID, Role: a2aRoleAgent, Parts: []a2aPart{{Text: text, MediaType: "text/plain"}}}
}

func streamTaskID(stream []events.Event) string {
	for i := len(stream) - 1; i >= 0; i-- {
		if stream[i].TaskID != "" {
			return stream[i].TaskID
		}
	}
	return ""
}

func matchesOriginalInstruction(stream []events.Event, statement string) bool {
	for _, event := range stream {
		if event.EventType != "INTENT_CREATED" {
			continue
		}
		var payload events.ProjectionEventPayload
		var intent core.Intent
		if json.Unmarshal(event.Payload, &payload) == nil && json.Unmarshal(payload.Projection.Value, &intent) == nil {
			return intent.OriginalInstruction == statement
		}
	}
	return false
}

func matchesDurableInput(stream []events.Event, messageID, text string) bool {
	for _, event := range stream {
		if event.EventType != "A2A_INPUT_RECEIVED" {
			continue
		}
		var input events.A2AInputReceivedPayload
		if json.Unmarshal(event.Payload, &input) == nil {
			return input.MessageID == messageID && input.Text == text
		}
	}
	return false
}

func blockedStatusText(stream []events.Event) string {
	for i := len(stream) - 1; i >= 0; i-- {
		if stream[i].EventType != "TASK_BLOCKED" {
			continue
		}
		var projection events.ProjectionEventPayload
		var blocked events.TaskBlockedPayload
		if json.Unmarshal(stream[i].Payload, &projection) == nil && json.Unmarshal(projection.Detail, &blocked) == nil && blocked.Missing != "" {
			return blocked.Reason + " Missing: " + blocked.Missing + " Why needed: " + blocked.WhyNeeded
		}
	}
	return "Agent OS requires additional input to continue this task."
}

func publishedResult(stream []events.Event) (events.ResultPublishedPayload, bool) {
	for i := len(stream) - 1; i >= 0; i-- {
		if stream[i].EventType != "RESULT_PUBLISHED" {
			continue
		}
		var result events.ResultPublishedPayload
		if json.Unmarshal(stream[i].Payload, &result) == nil && result.ValidFor(stream[i].ArtifactRefs) {
			return result, true
		}
	}
	return events.ResultPublishedPayload{}, false
}

func writeRPCResult(w http.ResponseWriter, id json.RawMessage, result any) {
	writeJSON(w, http.StatusOK, jsonRPCResponse{JSONRPC: "2.0", ID: id, Result: result})
}

func writeRPCError(w http.ResponseWriter, id json.RawMessage, code int, message string) {
	writeJSON(w, http.StatusOK, jsonRPCResponse{JSONRPC: "2.0", ID: id, Error: &jsonRPCError{Code: code, Message: message}})
}

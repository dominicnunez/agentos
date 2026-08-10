package gateway

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/intake"
	"github.com/dominicnunez/agentos/internal/trustconfig"
)

type Human struct {
	service *intake.Service
	actors  *HumanActorRegistry
}

func NewHuman(service *intake.Service, actors *HumanActorRegistry) *Human {
	return &Human{service: service, actors: actors}
}

type humanMessageRequest struct {
	ConversationID string             `json:"conversation_id"`
	MessageID      string             `json:"message_id"`
	Text           string             `json:"text"`
	ExecutionKind  core.ExecutionKind `json:"execution_kind,omitempty"`
}

type humanTaskResponse struct {
	TaskID         string `json:"task_id"`
	ConversationID string `json:"conversation_id"`
	State          string `json:"state"`
	Prompt         string `json:"prompt,omitempty"`
	Result         string `json:"result,omitempty"`
	UpdatedAt      string `json:"updated_at,omitempty"`
}

type humanReviewDecisionRequest struct {
	ReviewID    string                        `json:"review_id"`
	Fingerprint string                        `json:"fingerprint"`
	Decision    core.CompletionReviewDecision `json:"decision"`
	Feedback    string                        `json:"feedback,omitempty"`
}

type humanReviewResponse struct {
	ReviewID     string                     `json:"review_id"`
	TaskID       string                     `json:"task_id"`
	TaskVersion  int                        `json:"task_version"`
	Fingerprint  string                     `json:"fingerprint"`
	State        string                     `json:"state"`
	Objective    string                     `json:"objective"`
	Result       string                     `json:"candidate_result"`
	Criteria     []core.CompletionCriterion `json:"criteria"`
	EvidenceRefs []string                   `json:"evidence_refs"`
	UpdatedAt    string                     `json:"updated_at"`
}

func (h *Human) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	token, ok := bearerCredential(r.Header.Get("Authorization"))
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authenticated human operator required"})
		return
	}
	session, err := h.actors.Acquire(token)
	if errors.Is(err, ErrOperatorLimited) {
		w.Header().Set("Retry-After", "60")
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "human operator request limit reached"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authenticated human operator required"})
		return
	}
	defer session.Release()
	if r.Method == http.MethodPost && r.URL.Path == "/v1/human/messages" {
		h.handleMessage(w, r, session.Principal)
		return
	}
	const taskPrefix = "/v1/human/tasks/"
	if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, taskPrefix) && len(r.URL.Path) > len(taskPrefix) {
		h.handleGetTask(w, r, session.Principal, strings.TrimPrefix(r.URL.Path, taskPrefix))
		return
	}
	const reviewPrefix = "/v1/human/reviews/"
	if strings.HasPrefix(r.URL.Path, reviewPrefix) && len(r.URL.Path) > len(reviewPrefix) {
		taskID := strings.TrimPrefix(r.URL.Path, reviewPrefix)
		if strings.Contains(taskID, "/") {
			http.NotFound(w, r)
			return
		}
		switch r.Method {
		case http.MethodGet:
			h.handleGetReview(w, r, session.Principal, taskID)
		case http.MethodPost:
			h.handleReviewDecision(w, r, session.Principal, taskID)
		default:
			http.NotFound(w, r)
		}
		return
	}
	http.NotFound(w, r)
}

func (h *Human) handleMessage(w http.ResponseWriter, r *http.Request, principal intake.Principal) {
	if !hasJSONContentType(r.Header.Get("Content-Type")) {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "human messages require application/json"})
		return
	}
	defer func() {
		_ = r.Body.Close()
	}()
	var request humanMessageRequest
	if err := decodeWorkContent(w, r, &request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	view, err := h.service.Handle(r.Context(), principal, intake.Message{
		ConversationID: request.ConversationID, MessageID: request.MessageID,
		Text: request.Text, RequestedKind: request.ExecutionKind,
	})
	h.writeView(w, view, err)
}

func (h *Human) handleGetTask(w http.ResponseWriter, r *http.Request, principal intake.Principal, taskID string) {
	view, err := h.service.Get(r.Context(), principal, taskID)
	h.writeView(w, view, err)
}

func (h *Human) handleGetReview(w http.ResponseWriter, r *http.Request, principal intake.Principal, taskID string) {
	view, err := h.service.GetCompletionReview(r.Context(), principal, taskID)
	h.writeReviewView(w, view, err)
}

func (h *Human) handleReviewDecision(w http.ResponseWriter, r *http.Request, principal intake.Principal, taskID string) {
	if !hasJSONContentType(r.Header.Get("Content-Type")) {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "completion reviews require application/json"})
		return
	}
	defer func() {
		_ = r.Body.Close()
	}()
	var request humanReviewDecisionRequest
	reader := http.MaxBytesReader(w, r.Body, intake.MaximumReviewFeedbackBytes+4096)
	if err := trustconfig.DecodeObject(reader, "completion review decision", &request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	view, err := h.service.DecideCompletionReview(r.Context(), principal, intake.CompletionReviewDecision{
		TaskID: taskID, ReviewID: request.ReviewID, Fingerprint: request.Fingerprint,
		Decision: request.Decision, Feedback: request.Feedback,
	})
	h.writeReviewView(w, view, err)
}

func (h *Human) writeReviewView(w http.ResponseWriter, view intake.CompletionReviewView, err error) {
	w.Header().Set("Cache-Control", "no-store")
	if err != nil {
		h.writeIntakeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, humanReviewResponse{
		ReviewID: view.ReviewID, TaskID: view.TaskID, TaskVersion: view.TaskVersion,
		Fingerprint: view.Fingerprint, State: view.State, Objective: view.Objective, Result: view.Result,
		Criteria: view.Criteria, EvidenceRefs: view.EvidenceRefs,
		UpdatedAt: view.UpdatedAt.UTC().Format(time.RFC3339Nano),
	})
}

func (h *Human) writeView(w http.ResponseWriter, view intake.View, err error) {
	if err != nil {
		h.writeIntakeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, humanResponse(view))
}

func (h *Human) writeIntakeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, intake.ErrForbidden):
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "operator capability required"})
	case errors.Is(err, intake.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "task not found"})
	case errors.Is(err, intake.ErrInvalid):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "operator message is invalid"})
	case errors.Is(err, intake.ErrConflict):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "operator message conflicts with durable work"})
	default:
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "operator work is unavailable"})
	}
}

func humanResponse(view intake.View) humanTaskResponse {
	response := humanTaskResponse{
		TaskID: view.TaskID, ConversationID: view.ConversationID,
		State: view.State, Prompt: view.Prompt, Result: view.Result,
	}
	if !view.UpdatedAt.IsZero() {
		response.UpdatedAt = view.UpdatedAt.UTC().Format(time.RFC3339Nano)
	}
	return response
}

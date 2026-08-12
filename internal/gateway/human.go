package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/dominicnunez/agentos/internal/artifacts"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/intake"
	"github.com/dominicnunez/agentos/internal/trustconfig"
)

type Human struct {
	service   *intake.Service
	owner     LocalHuman
	limits    *localHumanLimits
	artifacts *artifacts.Store
}

type LocalHuman struct {
	UID               int
	ID                core.ID
	OrganizationID    core.ID
	MaxConcurrent     int
	RequestsPerMinute int
}

type peerUIDContextKey struct{}

// ContextWithPeerUID is used only by the Unix-domain listener after it reads
// kernel-provided peer credentials. Network handlers must never call it.
func ContextWithPeerUID(ctx context.Context, uid int) context.Context {
	return context.WithValue(ctx, peerUIDContextKey{}, uid)
}

func NewHuman(service *intake.Service, owner LocalHuman, stores ...artifacts.Store) (*Human, error) {
	if service == nil || owner.UID < 0 || owner.ID == "" || owner.OrganizationID == "" {
		return nil, fmt.Errorf("local user service, Linux UID, identity, and organization are required")
	}
	if owner.MaxConcurrent == 0 {
		owner.MaxConcurrent = 8
	}
	if owner.RequestsPerMinute == 0 {
		owner.RequestsPerMinute = 240
	}
	if owner.MaxConcurrent < 1 || owner.MaxConcurrent > 64 || owner.RequestsPerMinute < 1 || owner.RequestsPerMinute > 10_000 {
		return nil, fmt.Errorf("local user request limits are invalid")
	}
	human := &Human{service: service, owner: owner, limits: &localHumanLimits{slots: make(chan struct{}, owner.MaxConcurrent), requestsPerMinute: owner.RequestsPerMinute}}
	if len(stores) > 1 {
		return nil, fmt.Errorf("only one artifact store may be configured")
	}
	if len(stores) == 1 {
		human.artifacts = &stores[0]
	}
	return human, nil
}

type localHumanLimits struct {
	slots             chan struct{}
	requestsPerMinute int
	mu                sync.Mutex
	windowStart       time.Time
	requests          int
}

type humanMessageRequest struct {
	ConversationID string             `json:"conversation_id"`
	MessageID      string             `json:"message_id"`
	Text           string             `json:"text"`
	ExecutionKind  core.ExecutionKind `json:"execution_kind,omitempty"`
}

type humanTaskResponse struct {
	TaskID             string                   `json:"task_id"`
	ConversationID     string                   `json:"conversation_id"`
	State              string                   `json:"state"`
	Prompt             string                   `json:"prompt,omitempty"`
	Result             string                   `json:"result,omitempty"`
	UpdatedAt          string                   `json:"updated_at,omitempty"`
	CompletionContract *core.CompletionContract `json:"completion_contract,omitempty"`
}

type humanCompletionRequest struct {
	MessageID string             `json:"message_id"`
	Fields    map[string]string  `json:"fields"`
	Artifacts []artifacts.Upload `json:"artifacts,omitempty"`
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
	principal, release, err := h.acquire(r.Context())
	if errors.Is(err, ErrOperatorLimited) {
		w.Header().Set("Retry-After", "60")
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "user request limit reached"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "local user owner required"})
		return
	}
	defer release()
	if r.Method == http.MethodPost && r.URL.Path == "/v1/user/messages" {
		h.handleMessage(w, r, principal)
		return
	}
	const taskPrefix = "/v1/user/tasks/"
	if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, taskPrefix) && strings.HasSuffix(r.URL.Path, "/completion") {
		taskID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, taskPrefix), "/completion")
		if taskID == "" || strings.Contains(taskID, "/") {
			http.NotFound(w, r)
			return
		}
		h.handleTaskCompletion(w, r, principal, taskID)
		return
	}
	if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, taskPrefix) && len(r.URL.Path) > len(taskPrefix) {
		h.handleGetTask(w, r, principal, strings.TrimPrefix(r.URL.Path, taskPrefix))
		return
	}
	const reviewPrefix = "/v1/user/reviews/"
	if strings.HasPrefix(r.URL.Path, reviewPrefix) && len(r.URL.Path) > len(reviewPrefix) {
		taskID := strings.TrimPrefix(r.URL.Path, reviewPrefix)
		if strings.Contains(taskID, "/") {
			http.NotFound(w, r)
			return
		}
		switch r.Method {
		case http.MethodGet:
			h.handleGetReview(w, r, principal, taskID)
		case http.MethodPost:
			h.handleReviewDecision(w, r, principal, taskID)
		default:
			http.NotFound(w, r)
		}
		return
	}
	http.NotFound(w, r)
}

func (h *Human) handleTaskCompletion(w http.ResponseWriter, r *http.Request, principal intake.Principal, taskID string) {
	if h.artifacts == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "user artifact storage is unavailable"})
		return
	}
	if !hasJSONContentType(r.Header.Get("Content-Type")) {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "user task completion requires application/json"})
		return
	}
	defer func() { _ = r.Body.Close() }()
	reader := http.MaxBytesReader(w, r.Body, 48<<20)
	var request humanCompletionRequest
	if err := trustconfig.DecodeObject(reader, "user task completion", &request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if len(request.Artifacts) > 32 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "user task completion has too many artifacts"})
		return
	}
	total := 0
	evidence := make([]core.ArtifactEvidence, 0, len(request.Artifacts))
	for _, upload := range request.Artifacts {
		total += len(upload.Data)
		if total > 32<<20 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "user task artifacts exceed 33554432 bytes"})
			return
		}
		stored, _, err := h.artifacts.Put(principal.OrganizationID, taskID, principal.ID, upload)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		evidence = append(evidence, stored)
	}
	view, err := h.service.CompleteHumanTask(r.Context(), principal, taskID, core.HumanTaskSubmission{MessageID: request.MessageID, Fields: request.Fields, Artifacts: evidence})
	h.writeView(w, view, err)
}

func (h *Human) acquire(ctx context.Context) (intake.Principal, func(), error) {
	uid, ok := ctx.Value(peerUIDContextKey{}).(int)
	if !ok || uid != h.owner.UID {
		return intake.Principal{}, nil, ErrOperatorUnauthorized
	}
	select {
	case h.limits.slots <- struct{}{}:
	default:
		return intake.Principal{}, nil, ErrOperatorLimited
	}
	now := time.Now().UTC()
	h.limits.mu.Lock()
	if h.limits.windowStart.IsZero() || now.Sub(h.limits.windowStart) >= time.Minute {
		h.limits.windowStart = now
		h.limits.requests = 0
	}
	if h.limits.requests >= h.limits.requestsPerMinute {
		h.limits.mu.Unlock()
		<-h.limits.slots
		return intake.Principal{}, nil, ErrOperatorLimited
	}
	h.limits.requests++
	h.limits.mu.Unlock()
	principal := operatorPrincipal(string(h.owner.ID), core.PrincipalHuman, string(h.owner.OrganizationID), intake.ChannelHumanDirect, []string{
		intake.CapabilitySubmitWork, intake.CapabilityReadStatus, intake.CapabilityReadResult,
		intake.CapabilityProvideInput, intake.CapabilityReviewCompletion,
	}, intake.WorkScopeOrganization)
	return principal, func() { <-h.limits.slots }, nil
}

func (h *Human) handleMessage(w http.ResponseWriter, r *http.Request, principal intake.Principal) {
	if !hasJSONContentType(r.Header.Get("Content-Type")) {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "user messages require application/json"})
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
	response.CompletionContract = view.CompletionContract
	if !view.UpdatedAt.IsZero() {
		response.UpdatedAt = view.UpdatedAt.UTC().Format(time.RFC3339Nano)
	}
	return response
}

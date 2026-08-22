package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dominicnunez/agentos/internal/artifacts"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/intake"
	"github.com/dominicnunez/agentos/internal/trustconfig"
)

type Human struct {
	service   *intake.Service
	owner     LocalHuman
	access    *localUserAccess
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
	human := &Human{service: service, owner: owner, access: newLocalUserAccess(owner.UID, owner.MaxConcurrent, owner.RequestsPerMinute)}
	if len(stores) > 1 {
		return nil, fmt.Errorf("only one artifact store may be configured")
	}
	if len(stores) == 1 {
		human.artifacts = &stores[0]
	}
	return human, nil
}

type humanMessageRequest struct {
	ConversationID string             `json:"conversation_id"`
	MessageID      string             `json:"message_id"`
	Text           string             `json:"text"`
	ExecutionKind  core.ExecutionKind `json:"execution_kind,omitempty"`
	GoalID         core.ID            `json:"goal_id,omitempty"`
}

type humanTaskResponse struct {
	TaskID                     string                   `json:"task_id"`
	WorkID                     string                   `json:"work_id,omitempty"`
	ConversationID             string                   `json:"conversation_id"`
	SelectedGoalID             core.ID                  `json:"selected_goal_id,omitempty"`
	State                      string                   `json:"state"`
	Prompt                     string                   `json:"prompt,omitempty"`
	Result                     string                   `json:"result,omitempty"`
	Mode                       core.IntentMode          `json:"mode,omitempty"`
	TrustLabel                 string                   `json:"trust_label,omitempty"`
	UpdatedAt                  string                   `json:"updated_at,omitempty"`
	CompletionContract         *core.CompletionContract `json:"completion_contract,omitempty"`
	ReviewRequired             bool                     `json:"review_required,omitempty"`
	UserInputAllowed           bool                     `json:"user_input_allowed,omitempty"`
	InputRecoveryRequired      bool                     `json:"input_recovery_required,omitempty"`
	CompletionRecoveryRequired bool                     `json:"completion_recovery_required,omitempty"`
	Intent                     *core.IntentDraft        `json:"intent,omitempty"`
}

type humanIntentConfirmationRequest struct {
	MessageID   string `json:"message_id"`
	Fingerprint string `json:"fingerprint"`
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

func (h *Human) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	release, ok := acquireLocalUserRequest(w, r, h.access, "user request limit reached")
	if !ok {
		return
	}
	defer release()
	principal := h.principal()
	if r.Method == http.MethodPost && r.URL.Path == "/v1/user/messages" {
		h.handleMessage(w, r, principal)
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/v1/user/intents/active" {
		view, err := h.service.ActiveIntent(r.Context(), principal)
		h.writeView(w, view, err)
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/v1/user/organization" && r.URL.RawQuery == "" {
		view, err := h.service.OrganizationState(r.Context(), principal)
		if err != nil {
			h.writeIntakeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, view)
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/v1/user/tasks/recent" {
		view, err := h.service.LatestTask(r.Context(), principal)
		h.writeView(w, view, err)
		return
	}
	const intentPrefix = "/v1/user/intents/"
	if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, intentPrefix) && strings.HasSuffix(r.URL.Path, "/confirm") {
		conversationID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, intentPrefix), "/confirm")
		if conversationID == "" || strings.Contains(conversationID, "/") {
			http.NotFound(w, r)
			return
		}
		h.handleIntentConfirmation(w, r, principal, conversationID)
		return
	}
	const taskPrefix = "/v1/user/tasks/"
	if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, taskPrefix) && strings.HasSuffix(r.URL.Path, "/input/recover") {
		taskID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, taskPrefix), "/input/recover")
		if taskID == "" || strings.Contains(taskID, "/") || r.URL.RawQuery != "" || r.ContentLength > 0 {
			http.NotFound(w, r)
			return
		}
		h.handleTaskInputRecovery(w, r, principal, taskID)
		return
	}
	if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, taskPrefix) && strings.HasSuffix(r.URL.Path, "/completion/recover") {
		taskID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, taskPrefix), "/completion/recover")
		if taskID == "" || strings.Contains(taskID, "/") || r.URL.RawQuery != "" || r.ContentLength > 0 {
			http.NotFound(w, r)
			return
		}
		h.handleTaskCompletionRecovery(w, r, principal, taskID)
		return
	}
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
	if r.Method == http.MethodGet && r.URL.Path == "/v1/user/reviews" {
		h.handleListReviews(w, r, principal)
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/v1/user/reviews/recent" {
		h.handleListRecentReviews(w, r, principal)
		return
	}
	if strings.HasPrefix(r.URL.Path, reviewPrefix) && len(r.URL.Path) > len(reviewPrefix) {
		remainder := strings.TrimPrefix(r.URL.Path, reviewPrefix)
		parts := strings.Split(remainder, "/")
		if r.Method == http.MethodGet && len(parts) == 3 && parts[0] != "" && parts[1] == "records" && parts[2] != "" {
			view, err := h.service.GetCompletionReviewRecord(r.Context(), principal, parts[0], parts[2])
			h.writeReviewView(w, view, err)
			return
		}
		taskID := remainder
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

func (h *Human) handleTaskCompletionRecovery(w http.ResponseWriter, r *http.Request, principal intake.Principal, taskID string) {
	defer func() { _ = r.Body.Close() }()
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1))
	if err != nil || len(body) != 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "user completion recovery requires an empty body"})
		return
	}
	view, err := h.service.RecoverHumanCompletion(r.Context(), principal, taskID)
	h.writeView(w, view, err)
}

func (h *Human) handleTaskInputRecovery(w http.ResponseWriter, r *http.Request, principal intake.Principal, taskID string) {
	defer func() { _ = r.Body.Close() }()
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1))
	if err != nil || len(body) != 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "user input recovery requires an empty body"})
		return
	}
	view, err := h.service.RecoverHumanInput(r.Context(), principal, taskID)
	h.writeView(w, view, err)
}

func (h *Human) handleIntentConfirmation(w http.ResponseWriter, r *http.Request, principal intake.Principal, conversationID string) {
	if !hasJSONContentType(r.Header.Get("Content-Type")) {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "intent confirmation requires application/json"})
		return
	}
	defer func() { _ = r.Body.Close() }()
	reader := http.MaxBytesReader(w, r.Body, 4096)
	var request humanIntentConfirmationRequest
	if err := trustconfig.DecodeObject(reader, "intent confirmation", &request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	view, err := h.service.ConfirmIntent(r.Context(), principal, intake.IntentConfirmation{ConversationID: conversationID, MessageID: request.MessageID, Fingerprint: request.Fingerprint})
	h.writeView(w, view, err)
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
	if err := intake.ValidateIdentifier("message", request.MessageID); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "user task completion message identity is invalid"})
		return
	}
	task, err := h.service.Get(r.Context(), principal, taskID)
	if err != nil {
		h.writeIntakeError(w, err)
		return
	}
	evidence, err := inspectHumanArtifacts(principal.ID, request.Artifacts)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if task.CompletionContract == nil {
		view, err := h.service.CompleteHumanTask(r.Context(), principal, taskID, core.HumanTaskSubmission{MessageID: request.MessageID, Fields: request.Fields, Artifacts: evidence})
		h.writeView(w, view, err)
		return
	}
	if err := validateHumanCompletion(*task.CompletionContract, request, evidence); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	for index, upload := range request.Artifacts {
		stored, _, err := h.artifacts.Put(principal.OrganizationID, taskID, principal.ID, upload)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if stored != evidence[index] {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "user task artifact evidence changed during persistence"})
			return
		}
	}
	view, err := h.service.CompleteHumanTask(r.Context(), principal, taskID, core.HumanTaskSubmission{MessageID: request.MessageID, Fields: request.Fields, Artifacts: evidence})
	h.writeView(w, view, err)
}

func inspectHumanArtifacts(principalID string, uploads []artifacts.Upload) ([]core.ArtifactEvidence, error) {
	if len(uploads) > 32 {
		return nil, fmt.Errorf("user task completion has too many artifacts")
	}
	total := 0
	evidence := make([]core.ArtifactEvidence, 0, len(uploads))
	for _, upload := range uploads {
		total += len(upload.Data)
		if total > 32<<20 {
			return nil, fmt.Errorf("user task artifacts exceed 33554432 bytes")
		}
		inspected, err := artifacts.Inspect(principalID, upload)
		if err != nil {
			return nil, err
		}
		evidence = append(evidence, inspected)
	}
	return evidence, nil
}

func inspectHumanCompletion(principalID string, contract *core.CompletionContract, request humanCompletionRequest) ([]core.ArtifactEvidence, error) {
	if contract == nil {
		return nil, fmt.Errorf("user task has no completion contract")
	}
	evidence, err := inspectHumanArtifacts(principalID, request.Artifacts)
	if err != nil {
		return nil, err
	}
	if err := validateHumanCompletion(*contract, request, evidence); err != nil {
		return nil, err
	}
	return evidence, nil
}

func validateHumanCompletion(contract core.CompletionContract, request humanCompletionRequest, evidence []core.ArtifactEvidence) error {
	result := core.EvaluateHumanTaskCompletion(contract, core.HumanTaskSubmission{
		MessageID: request.MessageID, Fields: request.Fields, Artifacts: evidence,
	})
	if !result.Complete {
		return fmt.Errorf("user task completion does not satisfy its contract: %s", strings.Join(result.Reasons, "; "))
	}
	return nil
}

func (h *Human) principal() intake.Principal {
	return operatorPrincipal(string(h.owner.ID), core.PrincipalHuman, string(h.owner.OrganizationID), intake.ChannelHumanDirect, []string{
		intake.CapabilitySubmitWork, intake.CapabilityConfirmIntent, intake.CapabilityReadStatus, intake.CapabilityReadResult,
		intake.CapabilityProvideInput, intake.CapabilityReviewCompletion,
	}, intake.WorkScopeOrganization)
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
		Text: request.Text, RequestedKind: request.ExecutionKind, SelectedGoalID: request.GoalID,
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

func (h *Human) handleListReviews(w http.ResponseWriter, r *http.Request, principal intake.Principal) {
	query := r.URL.Query()
	for key, values := range query {
		if (key != "after" && key != "limit") || len(values) != 1 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "review list query is invalid"})
			return
		}
	}
	limit := 50
	if raw := query.Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "review list limit is invalid"})
			return
		}
		limit = parsed
	}
	page, err := h.service.ListCompletionReviews(r.Context(), principal, query.Get("after"), limit)
	w.Header().Set("Cache-Control", "no-store")
	if err != nil {
		h.writeIntakeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (h *Human) handleListRecentReviews(w http.ResponseWriter, r *http.Request, principal intake.Principal) {
	if r.URL.RawQuery != "limit=20" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "recent review query is invalid"})
		return
	}
	page, err := h.service.ListRecentCompletionReviews(r.Context(), principal, 20)
	if err != nil {
		h.writeIntakeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
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
	writeJSON(w, http.StatusOK, view)
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
	case errors.Is(err, intake.ErrConfirmationMismatch):
		writeJSON(w, http.StatusPreconditionFailed, map[string]string{"error": "intent confirmation differs from durable state"})
	default:
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "operator work is unavailable"})
	}
}

func humanResponse(view intake.View) humanTaskResponse {
	response := humanTaskResponse{
		TaskID: view.TaskID, WorkID: view.WorkID, ConversationID: view.ConversationID,
		SelectedGoalID: view.SelectedGoalID,
		State:          view.State, Prompt: view.Prompt, Result: view.Result, Mode: view.Mode, TrustLabel: view.TrustLabel,
		ReviewRequired: view.ReviewRequired, UserInputAllowed: view.UserInputAllowed, InputRecoveryRequired: view.InputRecoveryRequired,
		CompletionRecoveryRequired: view.CompletionRecoveryRequired,
	}
	response.CompletionContract = view.CompletionContract
	response.Intent = view.Intent
	if !view.UpdatedAt.IsZero() {
		response.UpdatedAt = view.UpdatedAt.UTC().Format(time.RFC3339Nano)
	}
	return response
}

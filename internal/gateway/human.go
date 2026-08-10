package gateway

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/intake"
)

type HumanActor struct {
	ID             string
	OrganizationID string
	BearerToken    string
	Capabilities   []string
}

type Human struct {
	service    *intake.Service
	principal  intake.Principal
	tokenHash  [sha256.Size]byte
	configured bool
}

func NewHuman(service *intake.Service, actor HumanActor) *Human {
	return &Human{
		service: service,
		principal: operatorPrincipal(
			actor.ID, core.PrincipalHuman, actor.OrganizationID, intake.ChannelHumanDirect, actor.Capabilities, intake.WorkScopeOrganization,
		),
		tokenHash: sha256.Sum256([]byte(actor.BearerToken)), configured: actor.BearerToken != "",
	}
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

func (h *Human) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	token, ok := bearerCredential(r.Header.Get("Authorization"))
	provided := sha256.Sum256([]byte(token))
	if !ok || !h.configured || h.principal.ID == "" || h.principal.OrganizationID == "" || subtle.ConstantTimeCompare(provided[:], h.tokenHash[:]) != 1 {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authenticated human operator required"})
		return
	}
	if r.Method == http.MethodPost && r.URL.Path == "/v1/human/messages" {
		h.handleMessage(w, r)
		return
	}
	const taskPrefix = "/v1/human/tasks/"
	if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, taskPrefix) && len(r.URL.Path) > len(taskPrefix) {
		h.handleGetTask(w, r, strings.TrimPrefix(r.URL.Path, taskPrefix))
		return
	}
	http.NotFound(w, r)
}

func (h *Human) handleMessage(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "application/json") {
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
	view, err := h.service.Handle(r.Context(), h.principal, intake.Message{
		ConversationID: request.ConversationID, MessageID: request.MessageID,
		Text: request.Text, RequestedKind: request.ExecutionKind,
	})
	h.writeView(w, view, err)
}

func (h *Human) handleGetTask(w http.ResponseWriter, r *http.Request, taskID string) {
	view, err := h.service.Get(r.Context(), h.principal, taskID)
	h.writeView(w, view, err)
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

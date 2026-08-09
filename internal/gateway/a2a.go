package gateway

import (
	"encoding/json"
	"net/http"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
)

type ExternalActor struct {
	ID             string
	OrganizationID string
	BearerToken    string
}
type A2A struct {
	service *app.Service
	actor   ExternalActor
}

func NewA2A(service *app.Service, actor ExternalActor) *A2A {
	return &A2A{service: service, actor: actor}
}

type part struct {
	Type string `json:"type"`
	Text string `json:"text"`
}
type request struct {
	ID      string `json:"id"`
	Message struct {
		Role  string `json:"role"`
		Parts []part `json:"parts"`
	} `json:"message"`
	Metadata struct {
		OrganizationID string             `json:"organization_id"`
		ExecutionKind  core.ExecutionKind `json:"execution_kind"`
	} `json:"metadata"`
}

func (a *A2A) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" && r.URL.Path == "/.well-known/agent-card.json" {
		writeJSON(w, http.StatusOK, map[string]any{"name": "Agent OS Operator Gateway", "description": "Inbound work-level gateway for Agent OS v4.2", "url": "/a2a/v1/tasks/send", "version": "0.1.0", "protocolVersion": "1.0", "capabilities": map[string]bool{"streaming": false, "pushNotifications": false}, "skills": []map[string]any{{"id": "submit-work", "name": "Submit organizational work", "tags": []string{"agent-os", "operator"}}}})
		return
	}
	if r.Method != "POST" || r.URL.Path != "/a2a/v1/tasks/send" {
		http.NotFound(w, r)
		return
	}
	if a.actor.BearerToken == "" || r.Header.Get("Authorization") != "Bearer "+a.actor.BearerToken {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authenticated external actor required"})
		return
	}
	defer r.Body.Close()
	var req request
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if req.Message.Role != "user" || len(req.Message.Parts) != 1 || req.Message.Parts[0].Type != "text" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "one user text part is required"})
		return
	}
	if req.Metadata.ExecutionKind == "" {
		req.Metadata.ExecutionKind = core.ExecutionDeterministic
	}
	if req.Metadata.OrganizationID != "" && req.Metadata.OrganizationID != a.actor.OrganizationID {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "external actor is not authorized for organization"})
		return
	}
	result, err := a.service.Submit(r.Context(), app.Submit{RequestID: req.ID, OrganizationID: a.actor.OrganizationID, Statement: req.Message.Parts[0].Text, Kind: req.Metadata.ExecutionKind})
	if err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, result)
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

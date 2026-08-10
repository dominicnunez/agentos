package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

type ExternalActor struct {
	ID             string
	OrganizationID string
	BearerToken    string
	Capabilities   []string
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

var forbiddenAuthorityFields = map[string]struct{}{
	"approval":           {},
	"approvalref":        {},
	"approvalstatus":     {},
	"approved":           {},
	"authorizationrefs":  {},
	"capabilities":       {},
	"capabilityrefs":     {},
	"effectobligation":   {},
	"effectobligationid": {},
	"freeze":             {},
	"humanapproval":      {},
	"policyoverride":     {},
	"unfreeze":           {},
}

func decodeWorkContent(w http.ResponseWriter, r *http.Request, target any) error {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		return err
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		return err
	}
	if err := rejectAuthorityFields(envelope); err != nil {
		return err
	}
	if metadata, ok := envelope["metadata"]; ok {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(metadata, &fields); err != nil {
			return fmt.Errorf("metadata must be an object: %w", err)
		}
		if err := rejectAuthorityFields(fields); err != nil {
			return err
		}
	}
	return json.Unmarshal(body, target)
}

func rejectAuthorityFields(fields map[string]json.RawMessage) error {
	for field := range fields {
		normalized := strings.NewReplacer("_", "", "-", "").Replace(strings.ToLower(field))
		if _, forbidden := forbiddenAuthorityFields[normalized]; forbidden {
			return fmt.Errorf("A2A work content cannot carry authority field %q", field)
		}
	}
	return nil
}

func (a *A2A) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" && r.URL.Path == "/.well-known/agent-card.json" {
		writeJSON(w, http.StatusOK, map[string]any{"name": "Agent OS Operator Gateway", "description": "Inbound work-level gateway for Agent OS V1", "url": "/a2a/v1/tasks/send", "version": "1.0.0-dev", "protocolVersion": "1.0", "capabilities": map[string]bool{"streaming": false, "pushNotifications": false}, "skills": []map[string]any{{"id": "submit-work", "name": "Submit organizational work", "tags": []string{"agent-os", "operator"}}}})
		return
	}
	if r.URL.Path == "/.well-known/agent-card.json" {
		http.NotFound(w, r)
		return
	}
	if a.actor.BearerToken == "" || r.Header.Get("Authorization") != "Bearer "+a.actor.BearerToken {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authenticated external actor required"})
		return
	}
	if r.Method == "GET" && len(r.URL.Path) > len("/a2a/v1/tasks/") && r.URL.Path[:len("/a2a/v1/tasks/")] == "/a2a/v1/tasks/" {
		if !a.allowed("read_status") {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "capability read_status required"})
			return
		}
		id := r.URL.Path[len("/a2a/v1/tasks/"):]
		es, err := a.service.ExternalEvents(r.Context(), a.actor.OrganizationID, id)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if len(es) == 0 {
			http.NotFound(w, r)
			return
		}
		status := projectStatus(id, es)
		if a.allowed("read_result") && status.State == "completed" {
			status.Result = projectResult(es)
		}
		writeJSON(w, http.StatusOK, status)
		return
	}
	if r.Method == "POST" && len(r.URL.Path) > len("/a2a/v1/tasks/") && r.URL.Path[len(r.URL.Path)-6:] == "/input" {
		if !a.allowed("provide_input") {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "capability provide_input required"})
			return
		}
		id := r.URL.Path[len("/a2a/v1/tasks/") : len(r.URL.Path)-6]
		defer func() {
			_ = r.Body.Close()
		}()
		var in struct {
			TaskID string `json:"task_id"`
			Text   string `json:"text"`
		}
		if err := decodeWorkContent(w, r, &in); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if err := a.service.ProvideExternalInput(r.Context(), a.actor.OrganizationID, a.actor.ID, id, in.TaskID, in.Text); err != nil {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
			return
		}
		es, err := a.service.ExternalEvents(r.Context(), a.actor.OrganizationID, id)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		status := projectStatus(id, es)
		if a.allowed("read_result") && status.State == "completed" {
			status.Result = projectResult(es)
		}
		responseStatus := http.StatusAccepted
		if status.State == "completed" || status.State == "failed" {
			responseStatus = http.StatusOK
		}
		writeJSON(w, responseStatus, status)
		return
	}
	if r.Method != "POST" || r.URL.Path != "/a2a/v1/tasks/send" {
		http.NotFound(w, r)
		return
	}
	if !a.allowed("submit_work") {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "capability submit_work required"})
		return
	}
	defer func() {
		_ = r.Body.Close()
	}()
	var req request
	if err := decodeWorkContent(w, r, &req); err != nil {
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
func (a *A2A) allowed(want string) bool {
	for _, v := range a.actor.Capabilities {
		if v == want {
			return true
		}
	}
	return false
}

// externalStatus is an intentionally narrow A2A projection. Ledger events and
// their internal payloads are never part of the status-capability response.
type externalStatus struct {
	ID     string `json:"id"`
	State  string `json:"state"`
	TaskID string `json:"task_id,omitempty"`
	Result any    `json:"result,omitempty"`
}

func projectStatus(id string, es []events.Event) externalStatus {
	s := externalStatus{ID: id, State: externalState(es)}
	for _, e := range es {
		if e.TaskID != "" {
			s.TaskID = e.TaskID
		}
	}
	return s
}

func projectResult(es []events.Event) any {
	for i := len(es) - 1; i >= 0; i-- {
		if es[i].EventType == "RESULT_PUBLISHED" {
			var result events.ResultPublishedPayload
			if json.Unmarshal(es[i].Payload, &result) == nil && result.ValidFor(es[i].ArtifactRefs) {
				return result
			}
		}
	}
	return nil
}

func externalState(es []events.Event) string {
	state := "working"
	for _, e := range es {
		switch e.EventType {
		case "TASK_BLOCKED":
			state = "input-required"
		case "TASK_RESUMED":
			state = "working"
		case "TASK_VERIFIED_COMPLETE":
			state = "completed"
		case "COMPLETION_REJECTED":
			state = "failed"
		}
	}
	return state
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

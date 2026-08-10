package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/intake"
)

type ExternalActor struct {
	ID             string
	OrganizationID string
	BearerToken    string
	PublicURL      string
	Capabilities   []string
}

type A2A struct {
	service     *intake.Service
	principal   intake.Principal
	publicURL   string
	bearerToken string
}

func NewA2A(service *intake.Service, actor ExternalActor) *A2A {
	return &A2A{
		service: service,
		principal: intake.Principal{
			ID: actor.ID, Kind: core.PrincipalExternalAgent, OrganizationID: actor.OrganizationID,
			Channel: intake.ChannelA2A, Capabilities: actor.Capabilities,
		},
		publicURL: actor.PublicURL, bearerToken: actor.BearerToken,
	}
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
	var content any
	if err := json.Unmarshal(body, &content); err != nil {
		return err
	}
	if err := rejectAuthorityContent(content); err != nil {
		return err
	}
	return json.Unmarshal(body, target)
}

func rejectAuthorityContent(content any) error {
	switch value := content.(type) {
	case map[string]any:
		for field, nested := range value {
			if _, forbidden := forbiddenAuthorityFields[canonicalWorkField(field)]; forbidden {
				return fmt.Errorf("operator work content cannot carry authority field %q", field)
			}
			if err := rejectAuthorityContent(nested); err != nil {
				return err
			}
		}
	case []any:
		for _, nested := range value {
			if err := rejectAuthorityContent(nested); err != nil {
				return err
			}
		}
	}
	return nil
}

func canonicalWorkField(field string) string {
	return strings.NewReplacer("_", "", "-", "").Replace(strings.ToLower(field))
}

func (a *A2A) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Path == "/.well-known/agent-card.json" {
		writeJSON(w, http.StatusOK, a.agentCard(r))
		return
	}
	if r.Method != http.MethodPost || r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	if a.principal.ID == "" || a.principal.OrganizationID == "" || a.bearerToken == "" || r.Header.Get("Authorization") != "Bearer "+a.bearerToken {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authenticated external actor required"})
		return
	}
	if !strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "application/json") {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "A2A JSON-RPC requires application/json"})
		return
	}
	a.serveJSONRPC(w, r)
}

func (a *A2A) agentCard(r *http.Request) map[string]any {
	endpoint := strings.TrimRight(a.publicURL, "/") + "/"
	if a.publicURL == "" {
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		endpoint = scheme + "://" + r.Host + "/"
	}
	return map[string]any{
		"name":        "Agent OS Operator Gateway",
		"description": "Inbound work-level gateway for Agent OS V1",
		"version":     "1.0.0-dev",
		"provider": map[string]string{
			"organization": "Agent OS",
			"url":          endpoint,
		},
		"supportedInterfaces": []map[string]string{{
			"url":             endpoint,
			"protocolBinding": "JSONRPC",
			"protocolVersion": "1.0",
		}},
		"capabilities": map[string]bool{
			"streaming":              false,
			"pushNotifications":      false,
			"stateTransitionHistory": false,
			"extendedAgentCard":      false,
		},
		"defaultInputModes":  []string{"text/plain"},
		"defaultOutputModes": []string{"text/plain"},
		"skills": []map[string]any{{
			"id":          "submit-work",
			"name":        "Submit organizational work",
			"description": "Submit or continue bounded organizational work through Agent OS.",
			"tags":        []string{"agent-os", "operator"},
		}},
		"securitySchemes": map[string]any{
			"bearer": map[string]string{"type": "http", "scheme": "bearer"},
		},
		"security": []map[string][]string{{"bearer": {}}},
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

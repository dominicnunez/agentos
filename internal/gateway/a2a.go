package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/dominicnunez/agentos/internal/intake"
)

type A2A struct {
	service   *intake.Service
	actors    *ExternalActorRegistry
	publicURL string
}

func NewA2A(service *intake.Service, actors *ExternalActorRegistry, publicURL string) *A2A {
	return &A2A{service: service, actors: actors, publicURL: publicURL}
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
	token, ok := bearerCredential(r.Header.Get("Authorization"))
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authenticated external actor required"})
		return
	}
	session, err := a.actors.Acquire(token)
	if errors.Is(err, ErrActorLimited) {
		w.Header().Set("Retry-After", "60")
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "external actor request limit reached"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authenticated external actor required"})
		return
	}
	defer session.Release()
	if !strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "application/json") {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "A2A JSON-RPC requires application/json"})
		return
	}
	a.serveJSONRPC(w, r, session.Principal)
}

func bearerCredential(header string) (string, bool) {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return "", false
	}
	token := strings.TrimPrefix(header, prefix)
	return token, token != "" && !strings.ContainsAny(token, " \t\r\n")
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

package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/dominicnunez/agentos-a2a-go/executionkind"
	"github.com/dominicnunez/agentos/internal/intake"
)

const maximumA2ARequestBytes = 256 << 10

type A2A struct {
	service   *intake.Service
	actors    *ExternalActorRegistry
	publicURL string
	version   string
	transport http.Handler
}

func NewA2A(service *intake.Service, actors *ExternalActorRegistry, publicURL, version string) *A2A {
	handler := &a2aRequestHandler{service: service}
	return &A2A{
		service: service, actors: actors, publicURL: publicURL, version: version,
		transport: a2asrv.NewJSONRPCHandler(handler, a2asrv.WithTransportPanicHandler(func(any) error {
			return a2a.ErrInternalError
		})),
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

func readA2AContent(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maximumA2ARequestBytes))
	if err != nil {
		return nil, err
	}
	var content any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&content); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, errors.New("A2A request must contain one JSON object")
	}
	if err := rejectAuthorityContent(content); err != nil {
		return nil, err
	}
	if err := validateA2ARequest(body); err != nil {
		return nil, err
	}
	return body, nil
}

func hasExactJSONContentType(value string) bool {
	mediaType, parameters, err := mime.ParseMediaType(value)
	return err == nil && strings.EqualFold(mediaType, "application/json") && len(parameters) == 0
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
		card, err := a.agentCard(r)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Agent Card endpoint is not configured"})
			return
		}
		writeJSON(w, http.StatusOK, card)
		return
	}
	if r.Method != http.MethodPost || r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	token, ok := bearerCredential(r.Header.Get("Authorization"))
	if !ok || a.actors == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authenticated external actor required"})
		return
	}
	session, err := a.actors.Acquire(token)
	if errors.Is(err, ErrOperatorLimited) {
		w.Header().Set("Retry-After", "60")
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "external actor request limit reached"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authenticated external actor required"})
		return
	}
	defer session.Release()
	if !hasExactJSONContentType(r.Header.Get("Content-Type")) {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "A2A JSON-RPC requires exactly application/json"})
		return
	}
	body, err := readA2AContent(w, r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid A2A request"})
		return
	}
	ctx := withA2APrincipal(r.Context(), session.Principal)
	r.Body = io.NopCloser(bytes.NewReader(body))
	a.transport.ServeHTTP(w, r.WithContext(ctx))
}

func bearerCredential(header string) (string, bool) {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return "", false
	}
	token := strings.TrimPrefix(header, prefix)
	return token, token != "" && !strings.ContainsAny(token, " \t\r\n")
}

func (a *A2A) agentCard(r *http.Request) (a2a.AgentCard, error) {
	endpoint := strings.TrimRight(a.publicURL, "/") + "/"
	if a.publicURL == "" {
		parsedHost, err := url.Parse("//" + r.Host)
		if err != nil || parsedHost.Hostname() == "" || parsedHost.User != nil ||
			parsedHost.Path != "" || parsedHost.RawQuery != "" || parsedHost.Fragment != "" {
			return a2a.AgentCard{}, errors.New("request host is invalid")
		}
		hostname := parsedHost.Hostname()
		address := net.ParseIP(hostname)
		if !strings.EqualFold(hostname, "localhost") && (address == nil || !address.IsLoopback()) {
			return a2a.AgentCard{}, errors.New("request host is not loopback")
		}
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		endpoint = scheme + "://" + r.Host + "/"
	}
	bearer := a2a.SecuritySchemeName("bearer")
	return a2a.AgentCard{
		Name:        "Agent OS Operator Gateway",
		Description: "Inbound work-level gateway for Agent OS V1",
		Version:     a.version,
		Provider:    &a2a.AgentProvider{Org: "Agent OS", URL: endpoint},
		SupportedInterfaces: []*a2a.AgentInterface{{
			URL: endpoint, ProtocolBinding: a2a.TransportProtocolJSONRPC, ProtocolVersion: a2a.Version,
		}},
		Capabilities: a2a.AgentCapabilities{Extensions: []a2a.AgentExtension{
			{URI: executionkind.URI, Required: false, Description: "Optional untrusted execution-routing hint; it grants no authority."},
			{URI: intentConfirmationURI, Required: false, Description: "Version-bound confirmation of a reviewed Agent OS Intent; it grants no effect authority."},
		}},
		DefaultInputModes:  []string{"text/plain"},
		DefaultOutputModes: []string{"text/plain"},
		Skills: []a2a.AgentSkill{{
			ID: "submit-work", Name: "Submit organizational work",
			Description: "Submit or continue bounded organizational work through Agent OS.",
			Tags:        []string{"agent-os", "operator"},
		}},
		SecuritySchemes: a2a.NamedSecuritySchemes{
			bearer: a2a.HTTPAuthSecurityScheme{Scheme: "bearer", Description: "Reviewed external-Agent bearer credential."},
		},
		SecurityRequirements: a2a.SecurityRequirementsOptions{{bearer: {}}},
	}, nil
}

type a2aPrincipalContextKey struct{}

func withA2APrincipal(ctx context.Context, principal intake.Principal) context.Context {
	return context.WithValue(ctx, a2aPrincipalContextKey{}, principal)
}

func a2aPrincipalFrom(ctx context.Context) (intake.Principal, bool) {
	principal, ok := ctx.Value(a2aPrincipalContextKey{}).(intake.Principal)
	return principal, ok
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

// Package effectstatus implements read-only external effect status adapters.
package effectstatus

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/effects"
	"github.com/dominicnunez/agentos/internal/trustconfig"
)

const reconciliationResponseLimit = 64 << 10

type ReconcilerBindingStatus string

const (
	ReconcilerBindingActive ReconcilerBindingStatus = "ACTIVE"
)

type HTTPReconcilerBinding struct {
	OrganizationID core.ID                 `json:"organization_id"`
	Action         string                  `json:"action"`
	Resource       string                  `json:"resource"`
	Status         ReconcilerBindingStatus `json:"status"`
	StatusURL      string                  `json:"status_url"`
	TokenRef       string                  `json:"token_ref"`
	ReviewRef      string                  `json:"review_ref"`
	ExpiresAt      *time.Time              `json:"expires_at"`
	BearerToken    string                  `json:"-"`
}

type HTTPReconcilerConfig struct {
	Reconcilers []HTTPReconcilerBinding `json:"reconcilers"`
}

type registeredHTTPReconciler struct {
	status     ReconcilerBindingStatus
	expiresAt  time.Time
	reconciler *httpStatusReconciler
}

type HTTPReconcilerRegistry struct {
	byScope map[string]registeredHTTPReconciler
	now     func() time.Time
}

type httpStatusReconciler struct {
	client      *http.Client
	statusURL   string
	bearerToken string
}

type reconciliationResponse struct {
	EffectObligationID string                      `json:"effect_obligation_id"`
	IdempotencyKey     string                      `json:"idempotency_key"`
	EffectFingerprint  string                      `json:"effect_fingerprint"`
	State              effects.ReconciliationState `json:"state"`
	EvidenceRefs       []string                    `json:"evidence_refs"`
}

func DecodeHTTPReconcilerConfig(reader io.Reader) ([]HTTPReconcilerBinding, error) {
	var config HTTPReconcilerConfig
	return trustconfig.DecodeEntries(reader, "effect reconciler registry", "binding", &config, &config.Reconcilers)
}

func NewHTTPReconcilerRegistry(bindings []HTTPReconcilerBinding, client *http.Client) (*HTTPReconcilerRegistry, error) {
	if len(bindings) == 0 {
		return nil, fmt.Errorf("at least one effect reconciler binding is required")
	}
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	} else {
		clone := *client
		client = &clone
		if client.Timeout <= 0 || client.Timeout > 30*time.Second {
			client.Timeout = 10 * time.Second
		}
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	registry := &HTTPReconcilerRegistry{byScope: make(map[string]registeredHTTPReconciler, len(bindings)), now: time.Now}
	for _, binding := range bindings {
		if err := validateHTTPReconcilerBinding(binding); err != nil {
			return nil, fmt.Errorf("effect reconciler %s/%s/%s: %w", binding.OrganizationID, binding.Action, binding.Resource, err)
		}
		key := reconciliationScopeKey(binding.OrganizationID, binding.Action, binding.Resource)
		if _, exists := registry.byScope[key]; exists {
			return nil, fmt.Errorf("effect reconciler binding %s/%s/%s is duplicated", binding.OrganizationID, binding.Action, binding.Resource)
		}
		registry.byScope[key] = registeredHTTPReconciler{
			status: binding.Status, expiresAt: binding.ExpiresAt.UTC(),
			reconciler: &httpStatusReconciler{client: client, statusURL: binding.StatusURL, bearerToken: binding.BearerToken},
		}
	}
	return registry, nil
}

func (r *HTTPReconcilerRegistry) ReconcilerFor(obligation core.EffectObligation) (effects.Reconciler, bool) {
	if r == nil {
		return nil, false
	}
	registered, ok := r.byScope[reconciliationScopeKey(obligation.OrganizationID, obligation.Action, obligation.Resource)]
	if !ok || registered.status != ReconcilerBindingActive || !r.now().UTC().Before(registered.expiresAt) {
		return nil, false
	}
	return registered.reconciler, true
}

func (r *httpStatusReconciler) Check(ctx context.Context, obligation core.EffectObligation) (effects.ReconciliationObservation, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, r.statusURL, nil)
	if err != nil {
		return effects.ReconciliationObservation{}, fmt.Errorf("create reconciliation request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+r.bearerToken)
	request.Header.Set("X-AgentOS-Effect-Obligation-ID", string(obligation.ID))
	request.Header.Set("X-AgentOS-Idempotency-Key", obligation.IdempotencyKey)
	request.Header.Set("X-AgentOS-Effect-Fingerprint", obligation.EffectFingerprint)
	response, err := r.client.Do(request)
	if err != nil {
		return effects.ReconciliationObservation{}, fmt.Errorf("query reconciliation status: %w", err)
	}
	defer func() {
		_ = response.Body.Close()
	}()
	if response.StatusCode != http.StatusOK {
		return effects.ReconciliationObservation{}, fmt.Errorf("reconciliation status endpoint returned %d", response.StatusCode)
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return effects.ReconciliationObservation{}, fmt.Errorf("reconciliation status endpoint must return application/json")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, reconciliationResponseLimit+1))
	if err != nil {
		return effects.ReconciliationObservation{}, fmt.Errorf("read reconciliation response: %w", err)
	}
	if len(body) > reconciliationResponseLimit {
		return effects.ReconciliationObservation{}, fmt.Errorf("reconciliation response exceeds %d bytes", reconciliationResponseLimit)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var result reconciliationResponse
	if err := decoder.Decode(&result); err != nil {
		return effects.ReconciliationObservation{}, fmt.Errorf("decode reconciliation response: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return effects.ReconciliationObservation{}, fmt.Errorf("reconciliation response must contain one JSON object")
	}
	if result.EffectObligationID != string(obligation.ID) || result.IdempotencyKey != obligation.IdempotencyKey || result.EffectFingerprint != obligation.EffectFingerprint {
		return effects.ReconciliationObservation{}, fmt.Errorf("reconciliation response identity does not match effect obligation")
	}
	return effects.ReconciliationObservation{State: result.State, EvidenceRefs: result.EvidenceRefs}, nil
}

func validateHTTPReconcilerBinding(binding HTTPReconcilerBinding) error {
	if binding.OrganizationID == "" || binding.Action == "" || binding.Resource == "" || binding.TokenRef == "" || binding.ReviewRef == "" {
		return fmt.Errorf("organization_id, action, resource, token_ref, and review_ref are required")
	}
	if err := trustconfig.ValidateCredentialLifecycle(string(binding.Status), binding.BearerToken, binding.ExpiresAt); err != nil {
		return err
	}
	parsed, err := url.Parse(binding.StatusURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return fmt.Errorf("status_url must be an absolute HTTPS URL without user information or fragment")
	}
	return nil
}

func reconciliationScopeKey(organizationID core.ID, action, resource string) string {
	return string(organizationID) + "\x00" + action + "\x00" + resource
}

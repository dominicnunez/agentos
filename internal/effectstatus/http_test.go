package effectstatus

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/effects"
)

const testReconcilerToken = "effect-reconciler-token-000000000001"

func TestHTTPReconcilerUsesExactReadOnlyStatusContract(t *testing.T) {
	obligation := attemptedEffect("effect-http")
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer "+testReconcilerToken || r.Header.Get("X-AgentOS-Effect-Obligation-ID") != string(obligation.ID) || r.Header.Get("X-AgentOS-Idempotency-Key") != obligation.IdempotencyKey || r.Header.Get("X-AgentOS-Effect-Fingerprint") != obligation.EffectFingerprint {
			t.Errorf("unexpected status request method=%s headers=%v", r.Method, r.Header)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(reconciliationResponse{
			EffectObligationID: string(obligation.ID), IdempotencyKey: obligation.IdempotencyKey,
			EffectFingerprint: obligation.EffectFingerprint, State: effects.ReconciliationConfirmed,
			EvidenceRefs: []string{"receipt-http-1"},
		}); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(server.Close)
	registry, err := NewHTTPReconcilerRegistry([]HTTPReconcilerBinding{testReconcilerBinding(server.URL)}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	reconciler, ok := registry.ReconcilerFor(obligation)
	if !ok {
		t.Fatal("exact reconciler binding was not resolved")
	}
	observation, err := reconciler.Check(context.Background(), obligation)
	if err != nil || observation.State != effects.ReconciliationConfirmed || len(observation.EvidenceRefs) != 1 || observation.EvidenceRefs[0] != "receipt-http-1" {
		t.Fatalf("observation=%+v err=%v", observation, err)
	}
	wrongOrganization := obligation
	wrongOrganization.OrganizationID = "other-org"
	if _, ok := registry.ReconcilerFor(wrongOrganization); ok {
		t.Fatal("reconciler crossed organization binding")
	}
}

func TestHTTPReconcilerRejectsRedirectAndMismatchedIdentity(t *testing.T) {
	obligation := attemptedEffect("effect-http")
	var redirectTargetCalled atomic.Bool
	redirectTarget := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirectTargetCalled.Store(true)
	}))
	t.Cleanup(redirectTarget.Close)
	redirect := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, redirectTarget.URL, http.StatusFound)
	}))
	t.Cleanup(redirect.Close)
	registry, err := NewHTTPReconcilerRegistry([]HTTPReconcilerBinding{testReconcilerBinding(redirect.URL)}, redirect.Client())
	if err != nil {
		t.Fatal(err)
	}
	reconciler, _ := registry.ReconcilerFor(obligation)
	if _, err := reconciler.Check(context.Background(), obligation); err == nil || redirectTargetCalled.Load() {
		t.Fatalf("redirect followed=%t err=%v", redirectTargetCalled.Load(), err)
	}

	mismatch := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(reconciliationResponse{
			EffectObligationID: "different-effect", IdempotencyKey: obligation.IdempotencyKey,
			EffectFingerprint: obligation.EffectFingerprint, State: effects.ReconciliationConfirmed,
			EvidenceRefs: []string{"unbound-receipt"},
		})
	}))
	t.Cleanup(mismatch.Close)
	registry, err = NewHTTPReconcilerRegistry([]HTTPReconcilerBinding{testReconcilerBinding(mismatch.URL)}, mismatch.Client())
	if err != nil {
		t.Fatal(err)
	}
	reconciler, _ = registry.ReconcilerFor(obligation)
	if _, err := reconciler.Check(context.Background(), obligation); err == nil {
		t.Fatal("mismatched destination response was trusted")
	}
}

func TestHTTPReconcilerRegistryFailsClosedOnUntrustedConfiguration(t *testing.T) {
	binding := testReconcilerBinding("https://status.example/effects")
	for _, test := range []struct {
		name     string
		bindings []HTTPReconcilerBinding
	}{
		{name: "weak credential", bindings: []HTTPReconcilerBinding{withReconcilerToken(binding, "short")}},
		{name: "non HTTPS endpoint", bindings: []HTTPReconcilerBinding{withReconcilerURL(binding, "http://status.example/effects")}},
		{name: "unknown status", bindings: []HTTPReconcilerBinding{withReconcilerStatus(binding, "UNKNOWN")}},
		{name: "duplicate scope", bindings: []HTTPReconcilerBinding{binding, binding}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewHTTPReconcilerRegistry(test.bindings, nil); err == nil {
				t.Fatal("untrusted reconciler configuration was accepted")
			}
		})
	}
	expired := binding
	past := time.Now().UTC().Add(-time.Minute)
	expired.ExpiresAt = &past
	registry, err := NewHTTPReconcilerRegistry([]HTTPReconcilerBinding{expired}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := registry.ReconcilerFor(attemptedEffect("expired")); ok {
		t.Fatal("expired reconciler binding was used")
	}
}

func TestDecodeHTTPReconcilerConfigIsStrict(t *testing.T) {
	valid := `{"reconcilers":[{"organization_id":"org-1","action":"send","resource":"destination-1","status":"ACTIVE","status_url":"https://status.example/effects","token_ref":"STATUS_TOKEN","authorization_ref":"review-1","expires_at":"2099-01-01T00:00:00Z"}]}`
	bindings, err := DecodeHTTPReconcilerConfig(strings.NewReader(valid))
	if err != nil || len(bindings) != 1 || bindings[0].TokenRef != "STATUS_TOKEN" {
		t.Fatalf("bindings=%+v err=%v", bindings, err)
	}
	for _, invalid := range []string{
		strings.Replace(valid, `"reconcilers"`, `"unknown":true,"reconcilers"`, 1),
		valid + `{}`,
	} {
		if _, err := DecodeHTTPReconcilerConfig(strings.NewReader(invalid)); err == nil {
			t.Fatal("invalid reconciler registry was accepted")
		}
	}
}

func testReconcilerBinding(statusURL string) HTTPReconcilerBinding {
	expires := time.Now().UTC().Add(time.Hour)
	return HTTPReconcilerBinding{
		OrganizationID: "org-1", Action: "send", Resource: "destination-1", Status: ReconcilerBindingActive,
		StatusURL: statusURL, TokenRef: "STATUS_TOKEN", AuthorizationRef: "review-1", ExpiresAt: &expires,
		BearerToken: testReconcilerToken,
	}
}

func withReconcilerToken(binding HTTPReconcilerBinding, token string) HTTPReconcilerBinding {
	binding.BearerToken = token
	return binding
}

func withReconcilerURL(binding HTTPReconcilerBinding, statusURL string) HTTPReconcilerBinding {
	binding.StatusURL = statusURL
	return binding
}

func withReconcilerStatus(binding HTTPReconcilerBinding, status ReconcilerBindingStatus) HTTPReconcilerBinding {
	binding.Status = status
	return binding
}

func attemptedEffect(id core.ID) core.EffectObligation {
	now := time.Now().UTC()
	return core.EffectObligation{
		ID: id, OrganizationID: "org-1", TaskID: "task-1", ActorID: "agent-1",
		Action: "send", Resource: "destination-1", Scope: "org-1", Descriptor: "send message",
		EffectFingerprint: "fingerprint-1", AuthorizationRefs: []string{"lease-1"}, ApprovalRef: "approval-1",
		IdempotencyKey: "idempotency-" + string(id), ReplayContext: map[string]string{"body": "hello"},
		Status: core.EffectAttempted, AttemptCount: 1, LastAttemptAt: &now, CreatedAt: now.Add(-time.Minute),
	}
}

var _ effects.Reconciler = (*httpStatusReconciler)(nil)
var _ effects.ReconcilerResolver = (*HTTPReconcilerRegistry)(nil)

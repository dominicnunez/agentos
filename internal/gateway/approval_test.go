package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/approvals"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/effects"
	"github.com/dominicnunez/agentos/internal/ledger"
)

type approvalNotifier struct{}

const testApprovalToken = "local-owner-test-marker"

func (approvalNotifier) Notify(context.Context, core.HumanApproval) error { return nil }

func TestApprovalControlShowsTrustedEffectAndEnforcesLifecycle(t *testing.T) {
	handler, fingerprint := newApprovalControlFixture(t)

	response := approvalRequest(t, handler, http.MethodGet, "/v1/control/approvals/approval-1", testApprovalToken, "")
	if response.Code != http.StatusOK {
		t.Fatalf("inspect status=%d body=%s", response.Code, response.Body.String())
	}
	var view approvalControlResponse
	if err := json.Unmarshal(response.Body.Bytes(), &view); err != nil {
		t.Fatal(err)
	}
	if view.EffectArguments["body"] != "complete exact content" || view.CanonicalEffectDescriptor != "send exact public message" || view.EffectFingerprint != fingerprint {
		t.Fatalf("trusted decision view omitted effect context: %+v", view)
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("approval response was cacheable")
	}

	for _, step := range []struct {
		path string
		body string
		want core.ApprovalStatus
	}{
		{path: "/v1/control/approvals/approval-1/acknowledge", body: `{"effect_fingerprint":"` + fingerprint + `"}`, want: core.ApprovalAcknowledged},
		{path: "/v1/control/approvals/approval-1/begin", body: `{"effect_fingerprint":"` + fingerprint + `"}`, want: core.ApprovalPendingDecision},
		{path: "/v1/control/approvals/approval-1/decision", body: `{"effect_fingerprint":"` + fingerprint + `","decision":"APPROVE"}`, want: core.ApprovalApproved},
	} {
		response = approvalRequest(t, handler, http.MethodPost, step.path, testApprovalToken, step.body)
		if response.Code != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", step.path, response.Code, response.Body.String())
		}
		if err := json.Unmarshal(response.Body.Bytes(), &view); err != nil {
			t.Fatal(err)
		}
		if view.Status != step.want {
			t.Fatalf("%s status=%s want=%s", step.path, view.Status, step.want)
		}
	}
}

func TestApprovalControlRejectsWorkCredentialsNonExactAuthorityAndUntrustedFields(t *testing.T) {
	handler, fingerprint := newApprovalControlFixture(t)
	for _, test := range []struct {
		name, token, body string
		want              int
	}{
		{name: "Agent credential", token: testExternalToken, body: `{"effect_fingerprint":"` + fingerprint + `"}`, want: http.StatusUnauthorized},
		{name: "different local user", token: "other-organization-approval-token-001", body: `{"effect_fingerprint":"` + fingerprint + `"}`, want: http.StatusUnauthorized},
		{name: "network credential", token: "wrong-risk-approval-control-token-01", body: `{"effect_fingerprint":"` + fingerprint + `"}`, want: http.StatusUnauthorized},
		{name: "stale fingerprint", token: testApprovalToken, body: `{"effect_fingerprint":"stale"}`, want: http.StatusConflict},
		{name: "authority field", token: testApprovalToken, body: `{"effect_fingerprint":"` + fingerprint + `","organization_id":"org-1"}`, want: http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := approvalRequest(t, handler, http.MethodPost, "/v1/control/approvals/approval-1/acknowledge", test.token, test.body)
			if response.Code != test.want {
				t.Fatalf("status=%d want=%d body=%s", response.Code, test.want, response.Body.String())
			}
		})
	}
}

func TestApprovalControlRejectsMalformedRequestsWithoutChangingState(t *testing.T) {
	handler, fingerprint := newApprovalControlFixture(t)
	initialResponse := approvalRequest(t, handler, http.MethodGet, "/v1/control/approvals/approval-1", testApprovalToken, "")
	var initialView approvalControlResponse
	if initialResponse.Code != http.StatusOK {
		t.Fatalf("initial inspect status=%d body=%s", initialResponse.Code, initialResponse.Body.String())
	}
	if err := json.Unmarshal(initialResponse.Body.Bytes(), &initialView); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name, method, path, token, contentType, body string
		want                                         int
	}{
		{name: "missing credential", method: http.MethodGet, path: "/v1/control/approvals/approval-1", want: http.StatusUnauthorized},
		{name: "unknown operation", method: http.MethodPost, path: "/v1/control/approvals/approval-1/approve", token: testApprovalToken, contentType: "application/json", body: `{}`, want: http.StatusNotFound},
		{name: "wrong method", method: http.MethodPut, path: "/v1/control/approvals/approval-1/acknowledge", token: testApprovalToken, contentType: "application/json", body: `{}`, want: http.StatusNotFound},
		{name: "wrong content type", method: http.MethodPost, path: "/v1/control/approvals/approval-1/acknowledge", token: testApprovalToken, contentType: "text/plain", body: `{}`, want: http.StatusUnsupportedMediaType},
		{name: "trailing object", method: http.MethodPost, path: "/v1/control/approvals/approval-1/acknowledge", token: testApprovalToken, contentType: "application/json", body: `{"effect_fingerprint":"` + fingerprint + `"}{}`, want: http.StatusBadRequest},
		{name: "oversized body", method: http.MethodPost, path: "/v1/control/approvals/approval-1/acknowledge", token: testApprovalToken, contentType: "application/json", body: `{"effect_fingerprint":"` + strings.Repeat("a", 4<<10) + `"}`, want: http.StatusBadRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequestWithContext(t.Context(), test.method, test.path, strings.NewReader(test.body))
			if test.token != "" {
				request = request.WithContext(ContextWithPeerUID(request.Context(), 1000))
			}
			if test.contentType != "" {
				request.Header.Set("Content-Type", test.contentType)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.want {
				t.Fatalf("status=%d want=%d body=%s", response.Code, test.want, response.Body.String())
			}
		})
	}

	response := approvalRequest(t, handler, http.MethodGet, "/v1/control/approvals/approval-1", testApprovalToken, "")
	var view approvalControlResponse
	if response.Code != http.StatusOK {
		t.Fatalf("inspect status=%d body=%s", response.Code, response.Body.String())
	}
	if err := json.Unmarshal(response.Body.Bytes(), &view); err != nil {
		t.Fatal(err)
	}
	if view.Status != initialView.Status {
		t.Fatalf("rejected request changed approval state from %s to %s", initialView.Status, view.Status)
	}
}

func newApprovalControlFixture(t *testing.T) (http.Handler, string) {
	t.Helper()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	expires := time.Now().UTC().Add(time.Hour)
	effect := core.EffectObligation{
		ID: "effect-1", OrganizationID: "org-1", TaskID: "task-1", ActorID: "agent-1",
		Action: "send", Resource: "public-channel", Scope: "org-1", ConsequenceBoundary: core.BoundaryPublicExternal,
		Descriptor: "send exact public message", ApprovalRef: "approval-1",
		IdempotencyKey: "effect-key-1", ReplayContext: map[string]string{"body": "complete exact content", "format": "plain"}, Status: core.EffectPending,
	}
	fingerprint := setGatewayEffectFingerprint(t, &effect)
	if err := store.AppendRecord(t.Context(), "org-1", "EFFECT_OBLIGATION_PREPARED", "agent-1", "task-1", nil, nil, "effect", "effect-1", 1, effect); err != nil {
		t.Fatal(err)
	}
	owner := LocalHuman{UID: 1000, ID: "local-uid-1000", OrganizationID: "org-1"}
	service := approvals.New(store, approvalNotifier{}, approvals.OwnerAuthorizer{OrganizationID: "org-1", HumanID: owner.ID})
	if _, err := service.Request(t.Context(), core.HumanApproval{
		ID: "approval-1", OrganizationID: "org-1", TaskID: "task-1", EffectObligationID: "effect-1",
		Action: "send", Resource: "public-channel", Boundary: core.BoundaryPublicExternal,
		Risk: "HIGH", Urgency: "MEDIUM", EffectFingerprint: fingerprint, ExpiresAt: &expires, SingleUse: true,
	}); err != nil {
		t.Fatal(err)
	}
	control, err := NewApprovalControl(service, owner)
	if err != nil {
		t.Fatal(err)
	}
	return control, fingerprint
}

func approvalRequest(t *testing.T, handler http.Handler, method, path, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	uid := 1000
	if token != testApprovalToken {
		uid = 1001
	}
	request := httptest.NewRequestWithContext(ContextWithPeerUID(t.Context(), uid), method, path, bytes.NewBufferString(body))
	if method == http.MethodPost {
		request.Header.Set("Content-Type", "application/json")
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func setGatewayEffectFingerprint(t *testing.T, obligation *core.EffectObligation) string {
	t.Helper()
	fingerprint, err := effects.Fingerprint(*obligation)
	if err != nil {
		t.Fatal(err)
	}
	obligation.EffectFingerprint = fingerprint
	return fingerprint
}

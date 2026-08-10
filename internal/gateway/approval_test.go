package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/approvals"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/effects"
	"github.com/dominicnunez/agentos/internal/ledger"
)

type approvalNotifier struct{}

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
		{name: "operator credential", token: testHumanToken, body: `{"effect_fingerprint":"` + fingerprint + `"}`, want: http.StatusUnauthorized},
		{name: "reviewer credential", token: testReviewerToken, body: `{"effect_fingerprint":"` + fingerprint + `"}`, want: http.StatusUnauthorized},
		{name: "Agent credential", token: testExternalToken, body: `{"effect_fingerprint":"` + fingerprint + `"}`, want: http.StatusUnauthorized},
		{name: "cross organization", token: "other-organization-approval-token-001", body: `{"effect_fingerprint":"` + fingerprint + `"}`, want: http.StatusNotFound},
		{name: "wrong risk", token: "wrong-risk-approval-control-token-01", body: `{"effect_fingerprint":"` + fingerprint + `"}`, want: http.StatusNotFound},
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

func newApprovalControlFixture(t *testing.T) (http.Handler, string) {
	t.Helper()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	expires := time.Now().UTC().Add(time.Hour)
	actors := []ApprovalActor{
		{ID: "approver-1", OrganizationID: "org-1", Status: OperatorActive, TokenRef: "APPROVAL", ReviewRef: "review-1", ExpiresAt: &expires, MaxConcurrent: 2, RequestsPerMinute: 100, BearerToken: testApprovalToken, Grants: []ApprovalGrant{{Boundary: core.BoundaryPublicExternal, Risk: "HIGH"}}},
		{ID: "approver-2", OrganizationID: "org-2", Status: OperatorActive, TokenRef: "OTHER_ORG", ReviewRef: "review-2", ExpiresAt: &expires, MaxConcurrent: 2, RequestsPerMinute: 100, BearerToken: "other-organization-approval-token-001", Grants: []ApprovalGrant{{Boundary: core.BoundaryPublicExternal, Risk: "HIGH"}}},
		{ID: "approver-3", OrganizationID: "org-1", Status: OperatorActive, TokenRef: "WRONG_RISK", ReviewRef: "review-3", ExpiresAt: &expires, MaxConcurrent: 2, RequestsPerMinute: 100, BearerToken: "wrong-risk-approval-control-token-01", Grants: []ApprovalGrant{{Boundary: core.BoundaryPublicExternal, Risk: "LOW"}}},
	}
	registry, err := NewApprovalActorRegistry(actors)
	if err != nil {
		t.Fatal(err)
	}
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
	service := approvals.New(store, approvalNotifier{}, registry)
	if _, err := service.Request(t.Context(), core.HumanApproval{
		ID: "approval-1", OrganizationID: "org-1", TaskID: "task-1", EffectObligationID: "effect-1",
		Action: "send", Resource: "public-channel", Boundary: core.BoundaryPublicExternal,
		Risk: "HIGH", Urgency: "MEDIUM", EffectFingerprint: fingerprint, ExpiresAt: &expires, SingleUse: true,
	}); err != nil {
		t.Fatal(err)
	}
	return NewApprovalControl(service, registry), fingerprint
}

func approvalRequest(t *testing.T, handler http.Handler, method, path, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequestWithContext(t.Context(), method, path, bytes.NewBufferString(body))
	request.Header.Set("Authorization", "Bearer "+token)
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

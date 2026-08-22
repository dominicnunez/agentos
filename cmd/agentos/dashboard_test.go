package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/bootstrap"
)

type dashboardRoundTripFunc func(*http.Request) (*http.Response, error)

func (f dashboardRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type dashboardTestAssets struct{}

func (dashboardTestAssets) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	_, _ = io.WriteString(w, "dashboard")
}

func (dashboardTestAssets) ScriptPolicy() string { return "'self' 'sha256-test'" }
func (dashboardTestAssets) StylePolicy() string  { return "'unsafe-hashes' 'sha256-test'" }

func TestDashboardBridgeUsesOneTimeSessionAndExactBrowserOrigin(t *testing.T) {
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	bridge := testDashboardBridge(t, &http.Client{}, func() time.Time { return now })

	response := dashboardRequest(bridge, http.MethodGet, "/", "", "", "")
	if response.Code != http.StatusOK || response.Body.String() != "dashboard" {
		t.Fatalf("asset response=%d %q", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Header().Get("Content-Security-Policy"), "frame-ancestors 'none'") {
		t.Fatalf("CSP=%q", response.Header().Get("Content-Security-Policy"))
	}

	response = dashboardRequest(bridge, http.MethodPost, "/api/session", "https://attacker.invalid", "application/json", `{"bootstrap_token":"bootstrap-secret"}`)
	if response.Code != http.StatusForbidden {
		t.Fatalf("cross-origin session=%d", response.Code)
	}
	token := dashboardSession(t, bridge)
	if token == "" || token == "bootstrap-secret" {
		t.Fatal("dashboard session token was not independently generated")
	}
	response = dashboardRequest(bridge, http.MethodPost, "/api/session", "http://127.0.0.1:41000", "application/json", `{"bootstrap_token":"bootstrap-secret"}`)
	if response.Code != http.StatusOK {
		t.Fatalf("unacknowledged bootstrap retry=%d", response.Code)
	}
	var retry map[string]string
	if err := json.Unmarshal(response.Body.Bytes(), &retry); err != nil || retry["session_token"] != token {
		t.Fatalf("unacknowledged bootstrap returned a different session: %v err=%v", retry, err)
	}
	response = dashboardAuthorizedRequest(bridge, http.MethodPost, "/api/session/ack", token, "")
	if response.Code != http.StatusNoContent {
		t.Fatalf("session acknowledgement=%d %s", response.Code, response.Body.String())
	}
	response = dashboardAuthorizedRequest(bridge, http.MethodPost, "/api/session/ack", token, "")
	if response.Code != http.StatusNoContent {
		t.Fatalf("session acknowledgement replay=%d %s", response.Code, response.Body.String())
	}
	response = dashboardRequest(bridge, http.MethodPost, "/api/session", "http://127.0.0.1:41000", "application/json", `{"bootstrap_token":"bootstrap-secret"}`)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("acknowledged bootstrap replay=%d", response.Code)
	}

	response = dashboardAuthorizedRequest(bridge, http.MethodGet, "/api/dashboard", token, "")
	if response.Code != http.StatusOK {
		t.Fatalf("dashboard identity=%d %s", response.Code, response.Body.String())
	}
	var identity map[string]string
	if err := json.Unmarshal(response.Body.Bytes(), &identity); err != nil || identity["organization"] != "organization-1" || identity["mode"] != "user" {
		t.Fatalf("identity=%v err=%v", identity, err)
	}
	response = dashboardRequest(bridge, http.MethodGet, "/api/dashboard", "http://127.0.0.1:41000", "", "")
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("missing session=%d", response.Code)
	}
}

func TestDashboardBridgeProxiesOnlyAllowlistedJSONWithoutBrowserCredential(t *testing.T) {
	var requests atomic.Int32
	upstream := &http.Client{Transport: dashboardRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests.Add(1)
		if request.URL.Path != "/v1/user/tasks/task?case@partner" || request.URL.RawQuery != "" {
			t.Fatalf("upstream target=%s?%s", request.URL.Path, request.URL.RawQuery)
		}
		if request.Header.Get("Authorization") != "" || request.Header.Get("Cookie") != "" {
			t.Fatal("browser credentials crossed the Unix gateway boundary")
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"reviews":[]}`)),
		}, nil
	})}
	bridge, err := newDashboardBridge(upstream, dashboardTestAssets{}, "organization-1", bootstrap.ModeUser, "1.0.0-dev", "127.0.0.1:41000", "bootstrap-secret", time.Now, func() {})
	if err != nil {
		t.Fatal(err)
	}
	token := dashboardSession(t, bridge)
	response := dashboardAuthorizedRequest(bridge, http.MethodGet, "/api/v1/user/tasks/task%3Fcase@partner", token, "")
	if response.Code != http.StatusOK || response.Body.String() != `{"reviews":[]}` {
		t.Fatalf("proxy=%d %q", response.Code, response.Body.String())
	}
	response = dashboardAuthorizedRequest(bridge, http.MethodPost, "/api/v1/control/approvals/approval-1/approve", token, "{}")
	if response.Code != http.StatusNotFound || requests.Load() != 1 {
		t.Fatalf("unsupported route=%d upstream_requests=%d", response.Code, requests.Load())
	}
}

func TestDashboardBridgeRejectsAuthorityBoundaryResponseWithJSONPrefix(t *testing.T) {
	upstream := &http.Client{Transport: dashboardRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/jsonp"}},
			Body:       io.NopCloser(strings.NewReader(`{"untrusted":true}`)),
		}, nil
	})}
	bridge := testDashboardBridge(t, upstream, time.Now)
	token := dashboardSession(t, bridge)
	response := dashboardAuthorizedRequest(bridge, http.MethodGet, "/api/v1/user/reviews", token, "")
	if response.Code != http.StatusBadGateway || strings.Contains(response.Body.String(), "untrusted") {
		t.Fatalf("prefixed JSON media type=%d %q", response.Code, response.Body.String())
	}
}

func TestDashboardSessionExpiresFailClosed(t *testing.T) {
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	bridge := testDashboardBridge(t, &http.Client{}, func() time.Time { return now })
	token := dashboardSession(t, bridge)
	now = now.Add(dashboardSessionLifetime)
	response := dashboardAuthorizedRequest(bridge, http.MethodGet, "/api/dashboard", token, "")
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("expired session=%d", response.Code)
	}
}

func TestAllowedDashboardRoute(t *testing.T) {
	tests := []struct {
		method, path, query string
		want                bool
	}{
		{http.MethodPost, "/v1/user/messages", "", true},
		{http.MethodPost, "/v1/user/intents/conversation-1/confirm", "", true},
		{http.MethodGet, "/v1/user/tasks/recent", "", true},
		{http.MethodPost, "/v1/user/tasks/task-1/completion", "", true},
		{http.MethodPost, "/v1/user/tasks/task-1/completion/recover", "", true},
		{http.MethodPost, "/v1/user/tasks/task-1/input/recover", "", true},
		{http.MethodGet, "/v1/user/tasks/task-case@partner", "", true},
		{http.MethodGet, "/v1/user/tasks/task-\u6848\u4ef6", "", true},
		{http.MethodGet, "/v1/user/reviews", "after=task-1&limit=10", true},
		{http.MethodGet, "/v1/user/reviews/recent", "limit=20", true},
		{http.MethodGet, "/v1/user/reviews/task-1/records/review-task-1-v1", "", true},
		{http.MethodGet, "/v1/user/reviews", "after=task-case%40partner&limit=10", true},
		{http.MethodPost, "/v1/control/approvals/approval-1/decision", "", true},
		{http.MethodGet, "/v1/control/approvals/recent", "limit=20", true},
		{http.MethodPost, "/v1/control/approvals/approval-1/approve", "", false},
		{http.MethodGet, "/v1/user/reviews", "limit=10&limit=20", false},
		{http.MethodGet, "/v1/user/reviews", "after=../../events&limit=10", false},
		{http.MethodGet, "/v1/user/reviews", "limit=101", false},
		{http.MethodGet, "/v1/user/tasks/../events", "", false},
		{http.MethodGet, "/v1/internal/events", "", false},
	}
	for _, test := range tests {
		if got := allowedDashboardRoute(test.method, test.path, test.query); got != test.want {
			t.Errorf("%s %s?%s=%t want %t", test.method, test.path, test.query, got, test.want)
		}
	}
}

func testDashboardBridge(t *testing.T, upstream *http.Client, now func() time.Time) *dashboardBridge {
	t.Helper()
	bridge, err := newDashboardBridge(upstream, dashboardTestAssets{}, "organization-1", bootstrap.ModeUser, "1.0.0-dev", "127.0.0.1:41000", "bootstrap-secret", now, func() {})
	if err != nil {
		t.Fatal(err)
	}
	return bridge
}

func dashboardSession(t *testing.T, bridge *dashboardBridge) string {
	t.Helper()
	response := dashboardRequest(bridge, http.MethodPost, "/api/session", "http://127.0.0.1:41000", "application/json", `{"bootstrap_token":"bootstrap-secret"}`)
	if response.Code != http.StatusOK {
		t.Fatalf("session=%d %s", response.Code, response.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return body["session_token"]
}

func dashboardAuthorizedRequest(bridge *dashboardBridge, method, target, token, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequestWithContext(context.Background(), method, target, bytes.NewBufferString(body))
	request.Host = "127.0.0.1:41000"
	request.Header.Set("Origin", "http://127.0.0.1:41000")
	request.Header.Set("Authorization", "Bearer "+token)
	if method == http.MethodPost {
		request.Header.Set("Content-Type", "application/json")
	}
	response := httptest.NewRecorder()
	bridge.ServeHTTP(response, request)
	return response
}

func dashboardRequest(bridge *dashboardBridge, method, target, origin, contentType, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequestWithContext(context.Background(), method, target, bytes.NewBufferString(body))
	request.Host = "127.0.0.1:41000"
	if origin != "" {
		request.Header.Set("Origin", origin)
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response := httptest.NewRecorder()
	bridge.ServeHTTP(response, request)
	return response
}

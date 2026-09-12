package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/authority"
	"github.com/dominicnunez/agentos/internal/core"
)

type gatewayFreezeStore struct {
	snapshot  authority.FreezeSnapshot
	readErr   error
	writeErr  error
	reads     int
	writes    int
	org       core.ID
	actorID   core.ID
	actorKind core.PrincipalKind
	change    authority.FreezeChange
}

func (s *gatewayFreezeStore) ReadFreeze(_ context.Context, organization core.ID) (authority.FreezeSnapshot, error) {
	s.reads++
	s.org = organization
	return s.snapshot, s.readErr
}

func (s *gatewayFreezeStore) SetFreeze(_ context.Context, organization, actorID core.ID, actorKind core.PrincipalKind, change authority.FreezeChange) (authority.FreezeSnapshot, error) {
	s.writes++
	s.org, s.actorID, s.actorKind, s.change = organization, actorID, actorKind, change
	return s.snapshot, s.writeErr
}

func TestFreezeControlReadsAndChangesAsConfiguredLocalOwner(t *testing.T) {
	store := &gatewayFreezeStore{snapshot: authority.FreezeSnapshot{
		State: authority.FreezeState{OrganizationID: "org-1", Frozen: true}, EventRef: "freeze-2", Version: 2,
	}}
	handler := newGatewayFreezeControl(t, store, LocalHuman{UID: 1000, ID: "owner-1", OrganizationID: "org-1"})

	response := freezeRequest(t, handler, http.MethodGet, "/v1/control/freeze", 1000, "")
	if response.Code != http.StatusOK {
		t.Fatalf("GET status=%d body=%s", response.Code, response.Body.String())
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("GET response was cacheable")
	}
	var snapshot authority.FreezeSnapshot
	if err := json.Unmarshal(response.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot != store.snapshot || store.org != "org-1" || store.reads != 1 {
		t.Fatalf("GET snapshot=%+v store org=%q reads=%d", snapshot, store.org, store.reads)
	}

	for _, test := range []struct {
		name             string
		body             string
		frozen           bool
		reason           string
		expectedEventRef string
		expectedVersion  int
	}{
		{name: "freeze", body: `{"frozen":true,"reason":"active incident","expected_event_ref":"freeze-1","expected_version":1}`, frozen: true, reason: "active incident", expectedEventRef: "freeze-1", expectedVersion: 1},
		{name: "release", body: `{"frozen":false,"reason":"incident resolved","expected_event_ref":"freeze-2","expected_version":2}`, frozen: false, reason: "incident resolved", expectedEventRef: "freeze-2", expectedVersion: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := freezeRequest(t, handler, http.MethodPost, "/v1/control/freeze", 1000, test.body)
			if response.Code != http.StatusOK {
				t.Fatalf("POST status=%d body=%s", response.Code, response.Body.String())
			}
			if response.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("POST response was cacheable")
			}
			if store.actorID != "owner-1" || store.actorKind != core.PrincipalHuman || store.org != "org-1" ||
				store.change.Frozen != test.frozen || store.change.Reason != test.reason ||
				store.change.ExpectedEventRef != test.expectedEventRef || store.change.ExpectedVersion != test.expectedVersion {
				t.Fatalf("store received org=%q actor=(%q,%q) change=%+v", store.org, store.actorID, store.actorKind, store.change)
			}
		})
	}
}

func TestFreezeControlRejectsUntrustedIdentityBeforeStore(t *testing.T) {
	for _, test := range []struct {
		name string
		uid  *int
	}{
		{name: "missing peer UID"},
		{name: "wrong peer UID", uid: intPointer(1001)},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &gatewayFreezeStore{}
			handler := newGatewayFreezeControl(t, store, LocalHuman{UID: 1000, ID: "owner-1", OrganizationID: "org-1"})
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/control/freeze", strings.NewReader(`{"frozen":true,"reason":"incident","expected_event_ref":"","expected_version":0}`))
			if test.uid != nil {
				request = request.WithContext(ContextWithPeerUID(request.Context(), *test.uid))
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("X-AgentOS-Actor-ID", "owner-1")
			request.Header.Set("X-AgentOS-Organization-ID", "org-1")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusForbidden {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			if store.reads != 0 || store.writes != 0 {
				t.Fatalf("denied request reached store: reads=%d writes=%d", store.reads, store.writes)
			}
		})
	}
}

func TestFreezeControlIgnoresImpostorAuthorityHeaders(t *testing.T) {
	store := &gatewayFreezeStore{}
	handler := newGatewayFreezeControl(t, store, LocalHuman{UID: 1000, ID: "owner-1", OrganizationID: "org-1"})
	request := httptest.NewRequestWithContext(ContextWithPeerUID(t.Context(), 1000), http.MethodPost, "/v1/control/freeze", strings.NewReader(`{"frozen":true,"reason":"incident","expected_event_ref":"","expected_version":0}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-AgentOS-Actor-ID", "attacker")
	request.Header.Set("X-AgentOS-Actor-Kind", "AGENT")
	request.Header.Set("X-AgentOS-Organization-ID", "other-org")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if store.org != "org-1" || store.actorID != "owner-1" || store.actorKind != core.PrincipalHuman {
		t.Fatalf("headers influenced authority: org=%q actor=(%q,%q)", store.org, store.actorID, store.actorKind)
	}
}

func TestFreezeControlRejectsMalformedAndAmbiguousCommandsWithoutWrite(t *testing.T) {
	tests := []struct {
		name, path, contentType, body string
		want                          int
	}{
		{name: "query parameter", path: "/v1/control/freeze?organization_id=org-1", contentType: "application/json", body: `{}`, want: http.StatusNotFound},
		{name: "wrong content type", path: "/v1/control/freeze", contentType: "text/plain", body: `{}`, want: http.StatusBadRequest},
		{name: "missing frozen", path: "/v1/control/freeze", contentType: "application/json", body: `{"reason":"incident","expected_event_ref":"","expected_version":0}`, want: http.StatusBadRequest},
		{name: "null frozen", path: "/v1/control/freeze", contentType: "application/json", body: `{"frozen":null,"reason":"incident","expected_event_ref":"","expected_version":0}`, want: http.StatusBadRequest},
		{name: "null reason", path: "/v1/control/freeze", contentType: "application/json", body: `{"frozen":true,"reason":null,"expected_event_ref":"","expected_version":0}`, want: http.StatusBadRequest},
		{name: "missing expected version", path: "/v1/control/freeze", contentType: "application/json", body: `{"frozen":true,"reason":"incident","expected_event_ref":""}`, want: http.StatusBadRequest},
		{name: "unknown field", path: "/v1/control/freeze", contentType: "application/json", body: `{"frozen":true,"reason":"incident","expected_event_ref":"","expected_version":0,"organization_id":"other"}`, want: http.StatusBadRequest},
		{name: "duplicate field", path: "/v1/control/freeze", contentType: "application/json", body: `{"frozen":true,"frozen":false,"reason":"incident","expected_event_ref":"","expected_version":0}`, want: http.StatusBadRequest},
		{name: "trailing object", path: "/v1/control/freeze", contentType: "application/json", body: `{"frozen":true,"reason":"incident","expected_event_ref":"","expected_version":0}{}`, want: http.StatusBadRequest},
		{name: "oversized", path: "/v1/control/freeze", contentType: "application/json", body: `{"frozen":true,"reason":"` + strings.Repeat("a", 4<<10) + `","expected_event_ref":"","expected_version":0}`, want: http.StatusBadRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &gatewayFreezeStore{}
			handler := newGatewayFreezeControl(t, store, LocalHuman{UID: 1000, ID: "owner-1", OrganizationID: "org-1"})
			request := httptest.NewRequestWithContext(ContextWithPeerUID(t.Context(), 1000), http.MethodPost, test.path, strings.NewReader(test.body))
			request.Header.Set("Content-Type", test.contentType)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.want {
				t.Fatalf("status=%d want=%d body=%s", response.Code, test.want, response.Body.String())
			}
			if store.writes != 0 {
				t.Fatalf("malformed request wrote %d times", store.writes)
			}
		})
	}
}

func TestFreezeControlMapsBoundedErrorsWithoutLeakingInternals(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want int
	}{
		{name: "invalid", err: authority.ErrFreezeInvalid, want: http.StatusBadRequest},
		{name: "identity", err: authority.ErrFreezeUnauthorized, want: http.StatusForbidden},
		{name: "conflict", err: authority.ErrFreezeConflict, want: http.StatusConflict},
		{name: "internal", err: errors.New("database password secret"), want: http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &gatewayFreezeStore{writeErr: test.err}
			handler := newGatewayFreezeControl(t, store, LocalHuman{UID: 1000, ID: "owner-1", OrganizationID: "org-1"})
			response := freezeRequest(t, handler, http.MethodPost, "/v1/control/freeze", 1000, `{"frozen":true,"reason":"incident","expected_event_ref":"","expected_version":0}`)
			if response.Code != test.want {
				t.Fatalf("status=%d want=%d body=%s", response.Code, test.want, response.Body.String())
			}
			if strings.Contains(response.Body.String(), "database password secret") {
				t.Fatalf("response leaked internal error: %s", response.Body.String())
			}
		})
	}
}

func TestNewFreezeControlRequiresExactServiceOwnerBinding(t *testing.T) {
	store := &gatewayFreezeStore{}
	service, err := authority.NewFreezeControl(store, "org-1", "owner-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, owner := range []LocalHuman{
		{UID: -1, ID: "owner-1", OrganizationID: "org-1"},
		{UID: 1000, ID: "owner-2", OrganizationID: "org-1"},
		{UID: 1000, ID: "owner-1", OrganizationID: "org-2"},
	} {
		if _, err := NewFreezeControl(service, owner); err == nil {
			t.Fatalf("NewFreezeControl accepted owner %+v", owner)
		}
	}
}

func newGatewayFreezeControl(t *testing.T, store authority.FreezeStore, owner LocalHuman) http.Handler {
	t.Helper()
	service, err := authority.NewFreezeControl(store, owner.OrganizationID, owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	control, err := NewFreezeControl(service, owner)
	if err != nil {
		t.Fatal(err)
	}
	return control
}

func freezeRequest(t *testing.T, handler http.Handler, method, path string, uid int, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequestWithContext(ContextWithPeerUID(t.Context(), uid), method, path, bytes.NewBufferString(body))
	if method == http.MethodPost {
		request.Header.Set("Content-Type", "application/json")
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

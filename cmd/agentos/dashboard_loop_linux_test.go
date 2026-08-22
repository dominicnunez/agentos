//go:build linux

package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/artifacts"
	"github.com/dominicnunez/agentos/internal/bootstrap"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/gateway"
	"github.com/dominicnunez/agentos/internal/intake"
	"github.com/dominicnunez/agentos/internal/ledger"
)

type dashboardLoopTask struct {
	TaskID         string            `json:"task_id"`
	WorkID         string            `json:"work_id"`
	ConversationID string            `json:"conversation_id"`
	State          string            `json:"state"`
	Result         string            `json:"result"`
	Intent         *core.IntentDraft `json:"intent"`
}

func TestDashboardCompletesDurableAgentWorkThroughKernelAuthenticatedGateway(t *testing.T) {
	uid, gid := syscall.Geteuid(), syscall.Getegid()
	root := t.TempDir()
	runtimeBase, err := os.MkdirTemp("", "aos-loop-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(runtimeBase) })
	if err := os.Chmod(runtimeBase, 0o700); err != nil {
		t.Fatal(err)
	}
	runtimeDir := filepath.Join(runtimeBase, "agentos")
	if err := os.Mkdir(runtimeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(runtimeDir, "user.sock")
	store, err := ledger.Open(filepath.Join(root, "agentos.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	runtime := app.New(events.NewGateway(store))
	operator := intake.New(runtime)
	owner := gateway.LocalHuman{
		UID: uid, ID: core.ID("local-uid-" + strconv.Itoa(uid)), OrganizationID: "org-1",
		MaxConcurrent: 4, RequestsPerMinute: 100,
	}
	userGateway, err := gateway.NewHuman(operator, owner, artifacts.Store{Root: filepath.Join(root, "artifacts")})
	if err != nil {
		t.Fatal(err)
	}
	userMux := http.NewServeMux()
	userMux.Handle("/v1/user/", userGateway)
	userServer := newHTTPServer("", userMux, nil)
	userServer.ConnContext = localConnContext
	listener, err := listenLocalHuman(t.Context(), socketPath, uid, gid)
	if err != nil {
		t.Fatal(err)
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- userServer.Serve(listener) }()
	t.Cleanup(func() {
		_ = userServer.Close()
		if err := <-serveErrors; err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, os.ErrClosed) {
			t.Errorf("serve local user gateway: %v", err)
		}
	})

	config := bootstrap.NewConfig(bootstrap.ModeUser, bootstrap.Owner{
		Username: "current-user", UID: uid, GID: gid,
	}, bootstrap.Paths{RuntimeDir: runtimeDir, UserSocket: socketPath}, time.Now())
	upstream, err := localHTTPClient(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { upstream.CloseIdleConnections() })
	bridge, err := newDashboardBridge(upstream, dashboardTestAssets{}, "org-1", bootstrap.ModeUser, "test-version", "127.0.0.1:41000", "bootstrap-secret", time.Now, func() {})
	if err != nil {
		t.Fatal(err)
	}
	session := dashboardSession(t, bridge)

	message := marshalDashboardLoopBody(t, map[string]string{
		"conversation_id": "dashboard-organization-loop",
		"message_id":      "message-1",
		"text":            "draft a private briefing",
	})
	response := dashboardAuthorizedRequest(bridge, http.MethodPost, "/api/v1/user/messages", session, message)
	var draft dashboardLoopTask
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &draft) != nil || draft.State != intake.StateAwaitingConfirmation || draft.Intent == nil {
		t.Fatalf("dashboard intent draft=%d %s", response.Code, response.Body.String())
	}
	if draft.TaskID == "" || draft.WorkID != "" {
		t.Fatalf("unconfirmed intent identity=%+v", draft)
	}
	preConfirmation, err := store.Events(t.Context(), "dashboard-organization-loop")
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range preConfirmation {
		switch event.EventType {
		case "WORK_CREATED", "PLAN_CREATED", "TASK_CREATED", "EXECUTION_STARTED":
			t.Fatalf("unconfirmed intent reached execution event %s", event.EventType)
		}
	}

	confirmation := marshalDashboardLoopBody(t, map[string]string{
		"message_id":  "confirm-message-1",
		"fingerprint": draft.Intent.Fingerprint,
	})
	response = dashboardAuthorizedRequest(bridge, http.MethodPost, "/api/v1/user/intents/dashboard-organization-loop/confirm", session, confirmation)
	var completed dashboardLoopTask
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &completed) != nil {
		t.Fatalf("dashboard confirmation=%d %s", response.Code, response.Body.String())
	}
	if completed.State != intake.StateCompleted || completed.TaskID != draft.TaskID || completed.WorkID == "" || completed.ConversationID != "dashboard-organization-loop" ||
		!strings.Contains(completed.Result, "fake-model:") || !strings.Contains(completed.Result, "draft a private briefing") {
		t.Fatalf("completed dashboard work=%+v", completed)
	}

	response = dashboardAuthorizedRequest(bridge, http.MethodGet, "/api/v1/user/tasks/"+completed.TaskID, session, "")
	var recovered dashboardLoopTask
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &recovered) != nil || recovered.TaskID != completed.TaskID || recovered.State != intake.StateCompleted || recovered.Result != completed.Result {
		t.Fatalf("dashboard durable task=%d %s", response.Code, response.Body.String())
	}

	stream, err := store.Events(t.Context(), "dashboard-organization-loop")
	if err != nil {
		t.Fatal(err)
	}
	assertDashboardOrganizationLoop(t, stream, owner.ID)
}

func marshalDashboardLoopBody(t *testing.T, value map[string]string) string {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func assertDashboardOrganizationLoop(t *testing.T, stream []events.Event, ownerID core.ID) {
	t.Helper()
	required := []string{
		"INTENT_DRAFTED", "INTENT_CONFIRMED", "PLAN_CREATED", "TASK_CREATED",
		"EXECUTION_CONTEXT_MANIFESTED", "RESULT_PUBLISHED", "TASK_VERIFIED_COMPLETE", "WORK_COMPLETED",
	}
	next := 0
	var manifest core.ExecutionContextManifest
	for _, event := range stream {
		if strings.HasPrefix(event.EventType, "APPROVAL_") || strings.HasPrefix(event.EventType, "EFFECT_") {
			t.Fatalf("private dashboard work created an authority event: %s", event.EventType)
		}
		if event.EventType == "INTENT_CONFIRMED" && event.SourceActorID != string(ownerID) {
			t.Fatalf("intent confirmation actor=%q want %q", event.SourceActorID, ownerID)
		}
		if event.EventType == "EXECUTION_CONTEXT_MANIFESTED" {
			if err := json.Unmarshal(event.Payload, &manifest); err != nil {
				t.Fatal(err)
			}
		}
		if next < len(required) && event.EventType == required[next] {
			next++
		}
	}
	if next != len(required) {
		t.Fatalf("durable organization loop stopped before %s: events=%v", required[next], dashboardEventTypes(stream))
	}
	if manifest.AgentID == "" || manifest.Provider != "fake" || manifest.Model != "fake-model/v1" || manifest.ExecutionProfileVersion != "v1-fake" {
		t.Fatalf("bounded Agent execution manifest=%+v", manifest)
	}
}

func dashboardEventTypes(stream []events.Event) []string {
	types := make([]string, 0, len(stream))
	for _, event := range stream {
		types = append(types, event.EventType)
	}
	return types
}

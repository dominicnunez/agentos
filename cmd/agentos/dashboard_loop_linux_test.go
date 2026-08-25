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
	"github.com/dominicnunez/agentos/internal/projections"
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
	runtimeBase, err := os.MkdirTemp("/tmp", "aos-")
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

	eventGateway := events.NewGateway(store)
	runtime := app.New(eventGateway)
	repository := projections.New(eventGateway)
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
	strategy := marshalDashboardLoopBody(t, map[string]any{
		"request_id": "strategy-dashboard", "mission_id": "mission-dashboard",
		"mission_statement": "Deliver governed outcomes", "goal_id": "goal-dashboard",
		"goal_objective": "Demonstrate the organization loop", "goal_mode": "TARGET",
		"success_criteria": []string{"the dashboard exposes durable organization state"},
	})
	response := dashboardAuthorizedRequest(bridge, http.MethodPost, "/api/v1/user/strategy/bootstrap", session, strategy)
	var direction app.OrganizationSnapshot
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &direction) != nil || len(direction.Missions) != 1 || len(direction.Goals) != 1 {
		t.Fatalf("dashboard strategy=%d %s", response.Code, response.Body.String())
	}

	message := marshalDashboardLoopBody(t, map[string]string{
		"conversation_id": "dashboard-organization-loop",
		"message_id":      "message-1",
		"text":            "draft a private briefing",
		"goal_id":         "goal-dashboard",
	})
	response = dashboardAuthorizedRequest(bridge, http.MethodPost, "/api/v1/user/messages", session, message)
	var draft dashboardLoopTask
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &draft) != nil || draft.State != intake.StateAwaitingConfirmation || draft.Intent == nil {
		t.Fatalf("dashboard intent draft=%d %s", response.Code, response.Body.String())
	}
	if draft.TaskID == "" || draft.WorkID != "" {
		t.Fatalf("unconfirmed intent identity=%+v", draft)
	}
	preConfirmation, err := runtime.ExternalEvents(t.Context(), "org-1", "dashboard-organization-loop")
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
	seedDashboardTeam(t, runtime, repository)
	response = dashboardAuthorizedRequest(bridge, http.MethodGet, "/api/v1/user/organization", session, "")
	var organization app.OrganizationSnapshot
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &organization) != nil || organization.Organization.ID != "org-1" ||
		len(organization.Missions) != 1 || organization.Missions[0].ID != "mission-dashboard" ||
		len(organization.Goals) != 1 || organization.Goals[0].ID != "goal-dashboard" || organization.Goals[0].MissionID != "mission-dashboard" ||
		len(organization.Works) != 1 || organization.Works[0].ID != core.ID(completed.WorkID) || organization.Works[0].GoalID != "goal-dashboard" ||
		len(organization.Tasks) != 1 || organization.Tasks[0].ID != core.ID(completed.TaskID) ||
		len(organization.Teams) != 1 || organization.Teams[0].ID != "team-dashboard" || len(organization.Teams[0].MemberAgentIDs) != 1 ||
		len(organization.Agents) != 1 || organization.Teams[0].MemberAgentIDs[0] != organization.Agents[0].ID {
		t.Fatalf("dashboard organization state=%d %s", response.Code, response.Body.String())
	}
	response = dashboardAuthorizedRequest(bridge, http.MethodGet, "/api/v1/user/aims/evidence", session, "")
	var aimsEvidence app.AIMSEvidencePackage
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &aimsEvidence) != nil || aimsEvidence.Organization.ID != "org-1" ||
		aimsEvidence.Claim.Certified || aimsEvidence.Claim.Status != "READINESS_WORK_IN_PROGRESS" || len(aimsEvidence.Fingerprint) != 64 ||
		len(aimsEvidence.Inventory.AISystems) != 1 || aimsEvidence.Inventory.Operations.Works != 1 || aimsEvidence.Inventory.Operations.Tasks != 1 {
		t.Fatalf("dashboard AIMS evidence=%d %s", response.Code, response.Body.String())
	}
	for _, forbidden := range []string{"Deliver governed outcomes", "draft a private briefing", "operating_instructions", "event_type", "payload", "effect_fingerprint"} {
		if strings.Contains(response.Body.String(), forbidden) {
			t.Fatalf("dashboard AIMS evidence leaked %q: %s", forbidden, response.Body.String())
		}
	}
	quickstartMessage := marshalDashboardLoopBody(t, map[string]string{
		"conversation_id": "dashboard-quickstart", "message_id": "quickstart-message-1",
		"text": "echo Agent OS completed reviewed work", "execution_kind": "DETERMINISTIC", "goal_id": "goal-dashboard",
	})
	response = dashboardAuthorizedRequest(bridge, http.MethodPost, "/api/v1/user/messages", session, quickstartMessage)
	var quickstartDraft dashboardLoopTask
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &quickstartDraft) != nil || quickstartDraft.State != intake.StateAwaitingConfirmation ||
		quickstartDraft.Intent == nil || quickstartDraft.Intent.Objective != "echo Agent OS completed reviewed work" || quickstartDraft.Intent.RequestedExecutionKind != core.ExecutionDeterministic {
		t.Fatalf("dashboard quickstart draft=%d %s", response.Code, response.Body.String())
	}
	quickstartConfirmation := marshalDashboardLoopBody(t, map[string]string{
		"message_id": "quickstart-confirmation-1", "fingerprint": quickstartDraft.Intent.Fingerprint,
	})
	response = dashboardAuthorizedRequest(bridge, http.MethodPost, "/api/v1/user/intents/dashboard-quickstart/confirm", session, quickstartConfirmation)
	var quickstartCompleted dashboardLoopTask
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &quickstartCompleted) != nil || quickstartCompleted.State != intake.StateCompleted ||
		quickstartCompleted.Result != "Agent OS completed reviewed work" {
		t.Fatalf("dashboard quickstart completion=%d %s", response.Code, response.Body.String())
	}
	quickstartCorrelation, found, err := store.ResolveExternalWork(t.Context(), "org-1", "dashboard-quickstart")
	if err != nil || !found {
		t.Fatalf("dashboard quickstart mapping=%q found=%t err=%v", quickstartCorrelation, found, err)
	}
	quickstartStream, err := store.Events(t.Context(), quickstartCorrelation)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range quickstartStream {
		if event.EventType == "INTENT_NORMALIZATION_CONTEXT_MANIFESTED" || event.EventType == "INFERENCE_USAGE_RECORDED" {
			t.Fatalf("dashboard deterministic quickstart used model inference: %+v", quickstartStream)
		}
	}

	correlationID, found, err := store.ResolveExternalWork(t.Context(), "org-1", "dashboard-organization-loop")
	if err != nil || !found || correlationID == "" {
		t.Fatalf("durable external Work mapping=%q found=%t err=%v", correlationID, found, err)
	}
	stream, err := store.Events(t.Context(), correlationID)
	if err != nil {
		t.Fatal(err)
	}
	assertDashboardOrganizationLoop(t, stream, owner.ID)
	strategyEvents, err := store.Events(t.Context(), "strategy-dashboard")
	if err != nil {
		t.Fatalf("dashboard strategy events=%+v err=%v", strategyEvents, err)
	}
	strategyCreation := map[string]int{
		"ORGANIZATION_CREATED": 0,
		"MISSION_CREATED":      0,
		"GOAL_CREATED":         0,
	}
	for _, event := range strategyEvents {
		if _, creation := strategyCreation[event.EventType]; creation {
			strategyCreation[event.EventType]++
			continue
		}
		if event.EventType != "GOAL_PROGRESS_EVALUATED" {
			t.Fatalf("unexpected strategy-correlated event=%+v", event)
		}
	}
	for eventType, count := range strategyCreation {
		if count != 1 {
			t.Fatalf("strategy creation event %s count=%d stream=%+v", eventType, count, strategyEvents)
		}
	}
	allEvents, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	assertDashboardCreatedNoAuthorityEvents(t, allEvents)
}

func seedDashboardTeam(t *testing.T, runtime *app.Service, repository *projections.Repository) {
	t.Helper()
	ctx := t.Context()
	current, found, err := runtime.OrganizationState(ctx, "org-1")
	if err != nil || !found || len(current.Agents) != 1 {
		t.Fatalf("dashboard Agent roster found=%t err=%v state=%+v", found, err, current)
	}
	now := time.Now().UTC()
	team := core.Team{
		ID: "team-dashboard", OrganizationID: "org-1", Name: "Delivery", Mission: "Deliver governed outcomes",
		MemberAgentIDs: []core.ID{current.Agents[0].ID}, Status: "ACTIVE", CreatedAt: now,
	}
	if err := repository.SaveTeam(ctx, "TEAM_CREATED", "runtime", "dashboard-team", 1, team, nil); err != nil {
		t.Fatal(err)
	}
}

func marshalDashboardLoopBody(t *testing.T, value any) string {
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

func assertDashboardCreatedNoAuthorityEvents(t *testing.T, stream []events.Event) {
	t.Helper()
	for _, event := range stream {
		if strings.HasPrefix(event.EventType, "APPROVAL_") || strings.HasPrefix(event.EventType, "EFFECT_") {
			t.Fatalf("private dashboard work created an authority event: %s", event.EventType)
		}
	}
}

func dashboardEventTypes(stream []events.Event) []string {
	types := make([]string, 0, len(stream))
	for _, event := range stream {
		types = append(types, event.EventType)
	}
	return types
}

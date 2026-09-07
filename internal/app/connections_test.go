package app

import (
	"context"
	"encoding/json"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/modelinput"
	"github.com/dominicnunez/agentos/internal/planning"
	"github.com/dominicnunez/agentos/internal/projections"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestConnectionConstructorRequiresCatalogTaskRequirements(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	model := &routedCountingModel{}
	metadata := inference.RouteMetadata{ConnectionID: "catalog", Descriptor: model.Descriptor(), Capabilities: []inference.Capability{inference.Text}, Local: true, ContextTokens: 11000, OutputTokens: 1000, DataClasses: []string{"internal"}, ValidUntil: time.Now().UTC().Add(time.Hour)}
	registry, err := inference.NewConnectionRegistry(store, []inference.Connection{{ID: "catalog", Adapter: model, Metadata: &metadata}, {ID: "legacy", Adapter: model}})
	if err != nil {
		t.Fatal(err)
	}
	requirements := modelinput.RouteRequirements{OrganizationID: "org", Capabilities: []modelinput.Capability{modelinput.Text}, InputTokens: 100, OutputTokens: 20, Locality: modelinput.LocalOnly, DataClass: "internal"}
	for _, tc := range []struct {
		name    string
		routing TaskConnectionRouting
		valid   bool
	}{
		{"catalog default missing", TaskConnectionRouting{Default: "catalog"}, false},
		{"specific rules cannot cover default", TaskConnectionRouting{Default: "catalog", TaskRequirements: map[string]modelinput.RouteRequirements{"research": requirements}}, false},
		{"catalog pin missing", TaskConnectionRouting{Default: "legacy", ByTaskKey: map[string]string{"research": "catalog"}}, false},
		{"legacy explicit", TaskConnectionRouting{Default: "legacy", ByTaskKey: map[string]string{"research": "legacy"}}, true},
		{"catalog default supplied", TaskConnectionRouting{Default: "catalog", Requirements: &requirements}, true},
		{"catalog pin inherits", TaskConnectionRouting{Default: "legacy", Requirements: &requirements, ByTaskKey: map[string]string{"research": "catalog"}}, true},
		{"catalog pin specific needs baseline", TaskConnectionRouting{Default: "legacy", ByTaskKey: map[string]string{"research": "catalog"}, TaskRequirements: map[string]modelinput.RouteRequirements{"research": requirements}}, false},
		{"broker pin to legacy", TaskConnectionRouting{Default: "catalog", Requirements: &requirements, ByTaskKey: map[string]string{"research": "legacy"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, err := NewWithConnections(events.NewGateway(store), registry, tc.routing, planning.SingleTaskPlanner{})
			if (err == nil) != tc.valid || (!tc.valid && service != nil) {
				t.Fatalf("constructor returned %v, %v", service, err)
			}
		})
	}
	stream, err := store.Events(t.Context(), "")
	if err != nil || len(stream) != 0 || model.calls.Load() != 0 {
		t.Fatalf("constructor persisted events or invoked model: events=%d calls=%d err=%v", len(stream), model.calls.Load(), err)
	}
}

func TestConnectionConstructorRejectsGovernedNoncatalogRoutes(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC()
	policy := inference.Policy{Version: inference.ConnectionPolicyVersion, ConnectionID: "governed", OrganizationID: "org-1", Provider: "fake", Model: "fake-model/v1", ExecutionProfileVersion: "v1-fake", Mode: inference.Local, MaxInputTokensPerRequest: 10000, MaxOutputTokensPerRequest: 1000, MaxTokensPerWindow: 100000, WindowDurationSeconds: 3600, MaxConcurrentRequests: 1, MaxAttemptsPerRequest: 1, AuthorizedBy: "operator", AuthorizedAt: now.Add(-time.Minute), AuthorizationExpiresAt: now.Add(time.Hour), OrganizationBudget: &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 100000, MaxConcurrentRequests: 2}, Routing: &inference.RoutePolicy{OrganizationID: "org-1", Locality: inference.LocalOnly, DataClasses: []string{"internal"}}}
	if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	model := &routedCountingModel{}
	registry, err := inference.NewConnectionRegistry(store, []inference.Connection{{ID: "governed", Adapter: model}, {ID: "legacy", Adapter: model}})
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	for _, routing := range []TaskConnectionRouting{{Default: "governed"}, {Default: "legacy", ByTaskKey: map[string]string{"research": "governed"}}} {
		if service, err := NewWithConnections(events.NewGateway(store), registry, routing, planning.SingleTaskPlanner{}); err == nil || service != nil {
			t.Fatal("governed route accepted without requirements")
		}
	}
	after, err := store.Events(t.Context(), "")
	if err != nil || len(before) != len(after) || model.calls.Load() != 0 {
		t.Fatal("rejected composition changed durable state or called provider", err)
	}
}

func TestServiceResolvesPinnedConnectionInsteadOfDefault(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	registry, err := inference.NewConnectionRegistry(store, []inference.Connection{{ID: "first", Adapter: execution.FakeModel{}}, {ID: "second", Adapter: execution.FakeModel{}}})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewWithConnections(events.NewGateway(store), registry, TaskConnectionRouting{Default: "first"}, planning.SingleTaskPlanner{})
	if err != nil {
		t.Fatal(err)
	}
	blueprint := core.AgentBlueprint{ID: "blueprint", OrganizationID: "org", Version: "v1", Status: "ACTIVE"}
	profile := core.ExecutionProfile{ID: "profile", OrganizationID: "org", ConnectionID: "second", Version: "v1-fake", ModelProvider: "fake", Model: "fake-model/v1", PromptVersion: defaultPromptVersion, ToolRefs: []string{}, Status: "ACTIVE"}
	agent := core.Agent{ID: "worker", OrganizationID: "org", BlueprintID: blueprint.ID, BlueprintVersion: blueprint.Version, ExecutionProfileID: profile.ID, ExecutionProfileVersion: profile.Version, RuntimeAdapter: localRuntimeAdapter, Status: "ACTIVE"}
	snapshot := projections.Snapshot{
		Agents:            map[core.ID]projections.Versioned[core.Agent]{agent.ID: {Value: agent}},
		AgentBlueprints:   map[core.ID]projections.Versioned[core.AgentBlueprint]{blueprint.ID: {Value: blueprint}},
		ExecutionProfiles: map[core.ID]projections.Versioned[core.ExecutionProfile]{profile.ID: {Value: profile}},
	}
	task := core.Task{ExecutionKind: core.ExecutionAgent, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: &core.AgentConfig{BlueprintID: blueprint.ID, BlueprintVersion: blueprint.Version, ProfileID: profile.ID, ProfileVersion: profile.Version, RuntimeAdapter: localRuntimeAdapter}}
	selection, err := service.resolveAssigned(snapshot, "org", task)
	if err != nil || selection.ExecutionProfile.ConnectionID != "second" {
		t.Fatalf("default substituted for pinned account: %+v, %v", selection, err)
	}
	if _, err := service.resolveAssigned(snapshot, "other-org", task); err == nil {
		t.Fatal("cross-organization task accepted")
	}
	delete(service.agentRoutes, "second")
	if _, err := service.resolveAssigned(snapshot, "org", task); err == nil {
		t.Fatal("missing account fell back to default")
	}
}

type routedTaskPlanner struct{}

func (routedTaskPlanner) Descriptor() (planning.Descriptor, bool) {
	return planning.Descriptor{}, false
}
func (routedTaskPlanner) Build(_ context.Context, input planning.Input, _ core.ExecutionKind) (planning.Result, error) {
	return planning.Result{Tasks: []core.PlanTask{
		{Key: "root", Description: input.Intent.Objective, ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, DependsOn: []string{"second"}},
		{Key: "second", Description: input.Intent.Objective, ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, DependsOn: []string{}},
	}}, nil
}

type routedCountingModel struct {
	execution.FakeModel
	calls atomic.Int32
}

func (m *routedCountingModel) CompleteRequest(ctx context.Context, request modelinput.Request) (execution.ModelResponse, error) {
	m.calls.Add(1)
	return m.FakeModel.CompleteRequest(ctx, request)
}
func TestServiceRoutesTwoPlannedTasksThroughDistinctAccounts(t *testing.T) {
	t.Run("explicit", func(t *testing.T) { testServiceRoutesTwoAccounts(t, false) })
	t.Run("broker", func(t *testing.T) { testServiceRoutesTwoAccounts(t, true) })
}

func testServiceRoutesTwoAccounts(t *testing.T, broker bool) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	gateway := events.NewGateway(store)
	seedTestGoal(t, t.Context(), projections.New(gateway), "org-1", "mission-1", "goal-1", core.GoalActive)
	first, second := &routedCountingModel{}, &routedCountingModel{}
	registry, err := inference.NewConnectionRegistry(store, []inference.Connection{{ID: "first", Adapter: first}, {ID: "second", Adapter: second}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	budget := &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 100000, MaxConcurrentRequests: 2}
	policy := inference.Policy{Version: inference.ConnectionPolicyVersion, ConnectionID: "first", OrganizationBudget: budget, OrganizationID: "org-1", Provider: "fake", Model: "fake-model/v1", ExecutionProfileVersion: "v1-fake", Mode: inference.Local, MaxInputTokensPerRequest: 10000, MaxOutputTokensPerRequest: 1000, MaxTokensPerWindow: 100000, WindowDurationSeconds: 3600, MaxConcurrentRequests: 1, MaxAttemptsPerRequest: 1, AuthorizedBy: "operator", AuthorizedAt: now.Add(-time.Minute), AuthorizationExpiresAt: now.Add(time.Hour)}
	if broker {
		policy.Routing = &inference.RoutePolicy{OrganizationID: "org-1", Locality: inference.LocalOnly, DataClasses: []string{"internal"}}
		policy.Catalog = &inference.CatalogDefinition{Capabilities: []inference.Capability{inference.Text}, Local: true, ContextTokens: 11000, OutputTokens: 1000, DataClasses: []string{"internal"}, ValidUntil: policy.AuthorizationExpiresAt}
	}
	other := policy
	other.ConnectionID = "second"
	if broker {
		firstMetadata, err := policy.Catalog.Metadata(policy)
		if err != nil {
			t.Fatal(err)
		}
		secondMetadata, err := other.Catalog.Metadata(other)
		if err != nil {
			t.Fatal(err)
		}
		registry, err = inference.NewConnectionRegistry(store, []inference.Connection{{ID: "first", Adapter: first, Metadata: &firstMetadata}, {ID: "second", Adapter: second, Metadata: &secondMetadata}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := store.ActivateInferencePolicies(t.Context(), []inference.Policy{policy, other}); err != nil {
		t.Fatal(err)
	}
	rules := map[string]string{"second": "second"}
	routing := TaskConnectionRouting{Default: "first", ByTaskKey: rules}
	if broker {
		routing.ByTaskKey = nil
		routing.Requirements = &modelinput.RouteRequirements{OrganizationID: "org-1", Capabilities: []modelinput.Capability{modelinput.Text}, InputTokens: 10000, OutputTokens: 1000, Locality: modelinput.LocalOnly, DataClass: "internal"}
		secondRequirements := routing.Requirements.Clone()
		// A model-authored key can choose preferences, but cannot weaken the
		// default's locality or estimates when selecting the second account.
		secondRequirements.Locality = modelinput.CloudAllowed
		secondRequirements.InputTokens, secondRequirements.OutputTokens = 1, 1
		secondRequirements.PreferredConnections = []string{"second"}
		routing.TaskRequirements = map[string]modelinput.RouteRequirements{"second": secondRequirements}
	}
	service, err := NewWithConnections(gateway, registry, routing, routedTaskPlanner{})
	if err != nil {
		t.Fatal(err)
	}
	if broker {
		planned := core.PlanTask{Key: "root", ExecutionKind: core.ExecutionAgent}
		if _, err := service.plannedAssignmentRoute(t.Context(), "other-org", planned); err == nil {
			t.Fatal("task selection accepted another organization's requirements")
		}
		for index, mutate := range []func(*modelinput.RouteRequirements){
			func(r *modelinput.RouteRequirements) { r.DataClass = "secret" },
			func(r *modelinput.RouteRequirements) {
				r.Capabilities = []modelinput.Capability{modelinput.Text, modelinput.Vision}
			},
			func(r *modelinput.RouteRequirements) { r.InputTokens = 11001 },
			func(r *modelinput.RouteRequirements) { r.DeniedProviders = []string{"fake"} },
		} {
			requirements := routing.Requirements.Clone()
			mutate(&requirements)
			denied, err := NewWithConnections(gateway, registry, TaskConnectionRouting{Default: "first", Requirements: &requirements}, routedTaskPlanner{})
			if err != nil {
				t.Fatal(err)
			}
			input := confirmedGoalSubmit(t, t.Context(), gateway, "denied-task-"+strconv.Itoa(index), "org-1", "goal-1", "perform governed work", core.ExecutionAgent)
			if _, err := denied.Submit(t.Context(), input); err == nil {
				t.Fatal("ineligible task was dispatched")
			}
			stream, err := denied.ExternalEvents(t.Context(), "org-1", input.RequestID)
			if err != nil {
				t.Fatal(err)
			}
			rejections := 0

			for _, event := range stream {
				if event.EventType == "INFERENCE_RESERVED" || event.EventType == "EXECUTION_CONTEXT_MANIFESTED" {
					t.Fatal("rejected task acquired inference authority")
				}
				if event.EventType != "INFERENCE_ROUTE_REJECTED" {
					continue
				}
				var payload events.InferenceRouteRejectedPayload
				if err := json.Unmarshal(event.Payload, &payload); err != nil {
					t.Fatal(err)
				}
				// The runtime supplies a default preference before selection.
				effective := requirements.Clone()
				effective.PreferredConnections = []string{"first"}
				fingerprint, _ := effective.Fingerprint()
				if payload.Purpose != "TASK_ASSIGNMENT" || payload.PlannedTaskKey != "root" || payload.RequirementsFingerprint != fingerprint || payload.OriginEventRef == "" {
					t.Fatal("task rejection lost attribution", payload)
				}
				rejections++
			}
			if rejections != 1 {
				t.Fatalf("task rejection count=%d", rejections)
			}
			if _, err := denied.plannedAssignmentRoute(t.Context(), "org-1", planned); err == nil {
				t.Fatal("task selection weakened requirements to use the default account")
			}
		}
		if first.calls.Load() != 0 || second.calls.Load() != 0 {
			t.Fatal("task selection called a provider")
		}
	}
	rules["second"] = "first"
	if broker {
		routing.Requirements.DataClass = "changed"
		routing.TaskRequirements["second"].PreferredConnections[0] = "first"
	}
	inputs := []Submit{
		confirmedGoalSubmit(t, t.Context(), gateway, "routed-tasks-a", "org-1", "goal-1", "prepare a governed result", core.ExecutionAgent),
		confirmedGoalSubmit(t, t.Context(), gateway, "routed-tasks-b", "org-1", "goal-1", "prepare another governed result", core.ExecutionAgent),
	}
	type outcome struct {
		result Result
		err    error
	}
	finished := make(chan outcome, len(inputs))
	for _, input := range inputs {
		go func(in Submit) { result, err := service.Submit(t.Context(), in); finished <- outcome{result, err} }(input)
	}
	for range inputs {
		completed := <-finished
		if completed.err != nil {
			t.Fatal(completed.err)
		}
		manifests, usage := map[string]int{}, map[string]int{}
		decisions := map[string]*modelinput.RouteDecision{}
		reservationDecisions := 0
		for _, event := range completed.result.Events {
			switch event.EventType {
			case "EXECUTION_CONTEXT_MANIFESTED":
				var payload core.ExecutionContextManifest
				if err := json.Unmarshal(event.Payload, &payload); err != nil {
					t.Fatal(err)
				}
				if broker && (payload.Routing == nil || payload.Routing.DataClass != "internal" || payload.Routing.Locality != modelinput.LocalOnly) {
					t.Fatal("manifest lost authoritative task requirements")
				}
				if broker {
					if payload.RoutingDecision == nil || payload.RoutingDecision.ValidateFor(*payload.Routing) != nil || payload.RoutingDecision.ConnectionID != payload.ConnectionID || payload.RoutingDecision.SnapshotSequence <= 0 || payload.RoutingDecision.SnapshotSequence >= event.Sequence {
						t.Fatal("manifest lost original routing decision")
					}
					decisions[payload.ConnectionID] = payload.RoutingDecision
				}
				manifests[payload.ConnectionID]++
			case "INFERENCE_RESERVED":
				if broker {
					var payload events.InferenceReservedPayload
					if err := json.Unmarshal(event.Payload, &payload); err != nil {
						t.Fatal(err)
					}
					if payload.RoutingDecision == nil || !modelinput.SameRouteDecision(decisions[payload.ConnectionID], payload.RoutingDecision) {
						t.Fatal("reservation changed or removed task routing decision")
					}
					reservationDecisions++
				}
			case "INFERENCE_USAGE_RECORDED":
				var payload events.InferenceUsageRecordedPayload
				if err := json.Unmarshal(event.Payload, &payload); err != nil {
					t.Fatal(err)
				}
				usage[payload.ConnectionID]++
			}
		}
		if len(manifests) != 2 || manifests["first"] != 1 || manifests["second"] != 1 || len(usage) != 2 || usage["first"] != 1 || usage["second"] != 1 {
			t.Fatalf("account attribution changed: manifests=%v usage=%v", manifests, usage)
		}
		if broker && reservationDecisions != 2 {
			t.Fatal("task decisions were not reserved exactly once")
		}
	}
	if first.calls.Load() != 2 || second.calls.Load() != 2 {
		t.Fatalf("calls first=%d second=%d", first.calls.Load(), second.calls.Load())
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatalf("routed history rejected: %v", err)
	}
}

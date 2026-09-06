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
	"sync/atomic"
	"testing"
	"time"
)

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
	other := policy
	other.ConnectionID = "second"
	if err := store.ActivateInferencePolicies(t.Context(), []inference.Policy{policy, other}); err != nil {
		t.Fatal(err)
	}
	rules := map[string]string{"second": "second"}
	service, err := NewWithConnections(gateway, registry, TaskConnectionRouting{Default: "first", ByTaskKey: rules}, routedTaskPlanner{})
	if err != nil {
		t.Fatal(err)
	}
	rules["second"] = "first"
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
		for _, event := range completed.result.Events {
			switch event.EventType {
			case "EXECUTION_CONTEXT_MANIFESTED":
				var payload core.ExecutionContextManifest
				if err := json.Unmarshal(event.Payload, &payload); err != nil {
					t.Fatal(err)
				}
				manifests[payload.ConnectionID]++
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
	}
	if first.calls.Load() != 2 || second.calls.Load() != 2 {
		t.Fatalf("calls first=%d second=%d", first.calls.Load(), second.calls.Load())
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatalf("routed history rejected: %v", err)
	}
}

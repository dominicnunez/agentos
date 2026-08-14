package app

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/planning"
)

type guardedPlanningModel struct{ adapter execution.ModelAdapter }

func (m guardedPlanningModel) Descriptor() planning.Descriptor {
	descriptor := m.adapter.Descriptor()
	return planning.Descriptor{Provider: descriptor.Provider, Model: descriptor.Model, ExecutionProfileVersion: descriptor.ExecutionProfileVersion}
}

func (m guardedPlanningModel) CompleteText(ctx context.Context, prompt string) (planning.TextCompletion, error) {
	response, err := m.adapter.Complete(ctx, prompt)
	return planning.TextCompletion{Text: response.Text, Usage: response.Usage}, err
}

func TestPlanningAndAgentExecutionUseDurableInferenceScope(t *testing.T) {
	store, err := ledger.Open(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC()
	policy := inference.Policy{
		Version: inference.PolicyVersion, OrganizationID: "org-1", Provider: "fake", Model: "fake-model/v1",
		ExecutionProfileVersion: "v1-fake", Mode: inference.Local,
		MaxInputTokensPerRequest: 262_144, MaxOutputTokensPerRequest: 262_144, MaxTokensPerWindow: 2_000_000,
		ContinuityReserveTokens: 200_000, WindowDurationSeconds: 3600, MaxConcurrentRequests: 1, MaxAttemptsPerRequest: 1,
		AuthorizedBy: "local-uid-1000", AuthorizedAt: now.Add(-time.Minute), AuthorizationExpiresAt: now.Add(time.Hour),
	}
	if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	raw := &organizationLoopModel{}
	guarded, err := inference.NewGuardedAdapter(store, raw)
	if err != nil {
		t.Fatal(err)
	}
	planner, err := planning.NewModelPlanner(guardedPlanningModel{adapter: guarded})
	if err != nil {
		t.Fatal(err)
	}
	service := NewWithModelAndPlanner(events.NewGateway(store), guarded, planner)
	result, err := service.Submit(t.Context(), Submit{RequestID: "guarded-loop", OrganizationID: "org-1", Statement: "prepare a verified briefing", Kind: core.ExecutionAgent})
	if err != nil {
		t.Fatal(err)
	}
	if result.Task.Status != core.TaskCompleted || len(raw.prompts) != 3 {
		t.Fatalf("organization loop did not complete behind the gate: task=%+v calls=%d", result.Task, len(raw.prompts))
	}
	reserved, reconciled := 0, 0
	for _, event := range result.Events {
		switch event.EventType {
		case "INFERENCE_RESERVED":
			reserved++
		case "INFERENCE_RECONCILED":
			reconciled++
		}
	}
	if reserved != 3 || reconciled != 3 {
		t.Fatalf("model calls were not exactly reserved and reconciled: reserved=%d reconciled=%d", reserved, reconciled)
	}
}

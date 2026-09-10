package app

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/modelinput"
	"github.com/dominicnunez/agentos/internal/planning"
)

type guardedPlanningModel struct {
	adapter execution.StructuredModelAdapter
}

func (m guardedPlanningModel) Descriptor() planning.Descriptor {
	descriptor := m.adapter.Descriptor()
	return planning.Descriptor{Provider: descriptor.Provider, Model: descriptor.Model, ExecutionProfileVersion: descriptor.ExecutionProfileVersion}
}

func (m guardedPlanningModel) CompleteRequest(ctx context.Context, request modelinput.Request) (planning.TextCompletion, error) {
	response, err := m.adapter.CompleteRequest(ctx, request)
	return planning.TextCompletion{Text: response.Text, Usage: response.Usage}, err
}

type initialPlanningDenialLedger struct {
	*ledger.SQLite
	deny           bool
	loseProof      bool
	loseSuspension bool
}

func (l *initialPlanningDenialLedger) BeginInferenceContext(ctx context.Context, organization string) (context.Context, func(), error) {
	if l.deny {
		l.deny = false
		return nil, nil, core.ErrContainmentUnavailable
	}
	return l.SQLite.BeginInferenceContext(ctx, organization)
}

func (l *initialPlanningDenialLedger) RecordInferenceNotSent(ctx context.Context, request inference.InferenceRequest) error {
	if l.loseProof {
		return errors.New("injected non-dispatch proof loss")
	}
	return l.SQLite.RecordInferenceNotSent(ctx, request)
}

func (l *initialPlanningDenialLedger) Append(ctx context.Context, draft events.TrustedDraft) (events.Event, error) {
	if l.loseSuspension && draft.EventType == "PLANNING_CONTAINMENT_SUSPENDED" {
		return events.Event{}, errors.New("injected suspension crash")
	}
	return l.SQLite.Append(ctx, draft)
}

func TestPlanningAndAgentExecutionUseDurableInferenceScope(t *testing.T) {
	for _, mode := range []string{"normal", "initial-denial", "lost-proof", "lost-suspension"} {
		t.Run(mode, func(t *testing.T) { testPlanningAndAgentExecutionUseDurableInferenceScope(t, mode) })
	}
}

func testPlanningAndAgentExecutionUseDurableInferenceScope(t *testing.T, mode string) {
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
	writer := &initialPlanningDenialLedger{SQLite: store, deny: mode != "normal", loseProof: mode == "lost-proof", loseSuspension: mode == "lost-suspension"}
	guarded, err := inference.NewGuardedAdapter(writer, raw)
	if err != nil {
		t.Fatal(err)
	}
	planner, err := planning.NewModelPlanner(guardedPlanningModel{adapter: guarded})
	if err != nil {
		t.Fatal(err)
	}
	service := NewWithModelAndPlanner(events.NewGateway(writer), guarded, planner)
	in := Submit{RequestID: "guarded-loop", OrganizationID: "org-1", Statement: "prepare a verified briefing", Kind: core.ExecutionAgent}
	if mode != "normal" {
		if _, err := service.Submit(t.Context(), in); !errors.Is(err, core.ErrContainmentUnavailable) || len(raw.prompts) != 0 {
			t.Fatalf("denial dispatched planning: calls=%d err=%v", len(raw.prompts), err)
		}
		service = NewWithModelAndPlanner(events.NewGateway(store), guarded, planner)
		for range 2 {
			if _, err := service.Recover(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
		if mode == "lost-proof" {
			if _, err := service.Submit(t.Context(), in); !errors.Is(err, core.ErrContainmentUnavailable) || len(raw.prompts) != 0 {
				t.Fatalf("unproven retry dispatched: calls=%d err=%v", len(raw.prompts), err)
			}
			return
		}
	}
	result, err := service.Submit(t.Context(), in)
	if err != nil {
		t.Fatal(err)
	}
	if result.Task.Status != core.TaskCompleted || len(raw.prompts) != 3 {
		t.Fatalf("organization loop did not complete behind the gate: task=%+v calls=%d", result.Task, len(raw.prompts))
	}
	wantAttempts, wantProof := 1, 0
	if mode != "normal" {
		wantAttempts, wantProof = 2, 1
	}
	if countEventType(result.Events, "PLANNING_CONTEXT_MANIFESTED") != wantAttempts || countEventType(result.Events, "INFERENCE_NOT_SENT") != wantProof {
		t.Fatal("planning retry lost exact attempt evidence")
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

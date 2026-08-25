package app

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
)

func TestBootstrapStrategyCreatesDurableDirectionAndReplaysExactly(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := New(events.NewGateway(store))
	input := StrategyBootstrapInput{
		OrganizationID: "org-1", RequestID: "strategy-1", RequestedByID: "local-uid-1000",
		RequestedByKind: core.PrincipalHuman, SourceChannel: "HUMAN_DIRECT",
		MissionID: "mission-1", MissionStatement: "Build a trustworthy artificial organization",
		GoalID: "goal-1", GoalObjective: "Complete one governed workflow",
		GoalMode: core.GoalTarget, SuccessCriteria: []string{"Work is completed", "Evidence is durable"},
	}
	view, err := service.BootstrapStrategy(ctx, input)
	if err != nil || view.Organization.ID != input.OrganizationID || len(view.Missions) != 1 || len(view.Goals) != 1 ||
		view.Missions[0].ID != input.MissionID || view.Goals[0].ID != input.GoalID || view.Goals[0].MissionID != input.MissionID {
		t.Fatalf("strategy view=%+v err=%v", view, err)
	}
	stream, err := store.Events(ctx, input.RequestID)
	if err != nil || len(stream) != 3 {
		t.Fatalf("strategy events=%+v err=%v", stream, err)
	}
	for _, event := range stream {
		var payload events.ProjectionEventPayload
		var detail events.StrategyBootstrapDetail
		if event.SourceActorID != "runtime" || event.SourceExecutionID != "" || event.TaskID != "" || len(event.AuthorizationRefs) != 0 ||
			json.Unmarshal(event.Payload, &payload) != nil || json.Unmarshal(payload.Detail, &detail) != nil || !detail.Valid() || detail.RequestedByID != input.RequestedByID {
			t.Fatalf("strategy event crossed its provenance boundary: %+v", event)
		}
		if strings.HasPrefix(event.EventType, "APPROVAL_") || strings.HasPrefix(event.EventType, "CAPABILITY_") || strings.HasPrefix(event.EventType, "EFFECT_") {
			t.Fatalf("strategy created authority: %+v", event)
		}
	}
	if _, err := service.BootstrapStrategy(ctx, input); err != nil {
		t.Fatalf("exact strategy replay: %v", err)
	}
	replayed, err := store.Events(ctx, input.RequestID)
	if err != nil || len(replayed) != len(stream) {
		t.Fatalf("strategy replay appended events=%d err=%v", len(replayed), err)
	}

	changed := input
	changed.GoalObjective = "A different outcome"
	if _, err := service.BootstrapStrategy(ctx, changed); !errors.Is(err, ErrStrategyConflict) {
		t.Fatalf("changed replay error=%v", err)
	}
	if _, err := New(events.NewGateway(store)).Recover(ctx); err != nil {
		t.Fatalf("recover bootstrapped strategy: %v", err)
	}
}

func TestBootstrapStrategyRejectsIncompleteOrAuthorityShapedInput(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := New(events.NewGateway(store))
	valid := StrategyBootstrapInput{
		OrganizationID: "org-1", RequestID: "strategy-1", RequestedByID: "local-uid-1000",
		RequestedByKind: core.PrincipalHuman, SourceChannel: "HUMAN_DIRECT",
		MissionID: "mission-1", MissionStatement: "Direction", GoalID: "goal-1",
		GoalObjective: "Outcome", GoalMode: core.GoalTarget, SuccessCriteria: []string{"Verified"},
	}
	tests := []StrategyBootstrapInput{
		func() StrategyBootstrapInput {
			value := valid
			value.RequestedByKind = core.PrincipalExternalAgent
			return value
		}(),
		func() StrategyBootstrapInput { value := valid; value.MissionID = "other"; return value }(),
		func() StrategyBootstrapInput { value := valid; value.GoalID = "goal/escape"; return value }(),
		func() StrategyBootstrapInput { value := valid; value.SuccessCriteria = nil; return value }(),
		func() StrategyBootstrapInput {
			value := valid
			value.SuccessCriteria = []string{"Verified", " Verified "}
			return value
		}(),
		func() StrategyBootstrapInput {
			value := valid
			value.RequestedByID = core.ID(strings.Repeat("a", 257))
			return value
		}(),
		func() StrategyBootstrapInput { value := valid; value.GoalObjective = "\x00"; return value }(),
	}
	for index, input := range tests {
		if _, err := service.BootstrapStrategy(context.Background(), input); !errors.Is(err, ErrStrategyInvalid) {
			t.Fatalf("invalid strategy %d error=%v", index, err)
		}
	}
	stream, err := store.Events(context.Background(), "")
	if err != nil || len(stream) != 0 {
		t.Fatalf("invalid strategy reached ledger=%+v err=%v", stream, err)
	}
}

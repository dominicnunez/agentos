package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/projections"
)

type boundedStrategyReadLedger struct {
	*ledger.SQLite
	unboundedStrategyReads int
}

func (l *boundedStrategyReadLedger) Events(ctx context.Context, correlationID string) ([]events.Event, error) {
	if correlationID == "strategy-1" {
		l.unboundedStrategyReads++
		return nil, errors.New("unbounded strategy stream read")
	}
	return l.SQLite.Events(ctx, correlationID)
}

func TestBootstrapStrategyCreatesDurableDirectionAndReplaysExactly(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	bounded := &boundedStrategyReadLedger{SQLite: store}
	service := New(events.NewGateway(bounded))
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
	restarted := New(events.NewGateway(store))
	if _, err := restarted.BootstrapStrategy(ctx, input); err != nil {
		t.Fatalf("exact strategy replay after process restart: %v", err)
	}
	duplicate := input
	duplicate.RequestID = "strategy-2"
	duplicate.MissionID = "mission-2"
	duplicate.GoalID = "goal-2"
	if _, err := restarted.BootstrapStrategy(ctx, duplicate); !errors.Is(err, ErrStrategyConflict) {
		t.Fatalf("second initial strategy after process restart error=%v", err)
	}
	duplicateEvents, err := store.StrategyCreationEvents(ctx, string(input.OrganizationID), duplicate.RequestID)
	if err != nil || len(duplicateEvents) != 0 {
		t.Fatalf("second initial strategy events=%+v err=%v", duplicateEvents, err)
	}
	for index := 0; index < 256; index++ {
		if _, err := store.Append(ctx, events.TrustedDraft{
			OrganizationID: string(input.OrganizationID), EventType: "STRATEGY_PROGRESS_OBSERVED", SourceActorID: "runtime",
			CorrelationID: input.RequestID, Payload: map[string]int{"index": index},
		}); err != nil {
			t.Fatalf("append strategy-correlated noise: %v", err)
		}
	}
	creationEvents, err := store.StrategyCreationEvents(ctx, string(input.OrganizationID), input.RequestID)
	if err != nil || len(creationEvents) != 3 {
		t.Fatalf("bounded strategy creations=%+v err=%v", creationEvents, err)
	}
	otherTenant, err := store.StrategyCreationEvents(ctx, "org-2", input.RequestID)
	if err != nil || len(otherTenant) != 0 {
		t.Fatalf("cross-tenant strategy creations=%+v err=%v", otherTenant, err)
	}

	revisedMission := core.Mission{
		ID: input.MissionID, OrganizationID: input.OrganizationID, Statement: "Refined durable direction",
		Status: core.MissionActive, CreatedAt: view.Missions[0].CreatedAt,
	}
	if err := service.state.SaveMission(ctx, "MISSION_REVISED", "runtime", "mission-revision", 2, revisedMission, map[string]string{"reason": "reviewed"}); err != nil {
		t.Fatalf("revise Mission: %v", err)
	}
	revisedGoal := core.Goal{
		ID: input.GoalID, OrganizationID: input.OrganizationID, MissionID: input.MissionID,
		Objective: "Refined verified outcome", Mode: input.GoalMode, Status: core.GoalActive, CreatedAt: view.Goals[0].CreatedAt,
		SuccessCriteria: []core.IntentValue{{Value: "Revised evidence is durable", Origin: "USER_CONFIRMED"}},
	}
	if err := service.state.SaveGoal(ctx, "GOAL_REFINED", "runtime", "goal-revision", 2, revisedGoal, map[string]string{"reason": "reviewed"}); err != nil {
		t.Fatalf("revise Goal: %v", err)
	}
	revisedView, err := service.BootstrapStrategy(ctx, input)
	if err != nil || revisedView.Missions[0].Statement != revisedMission.Statement || revisedView.Goals[0].Objective != revisedGoal.Objective {
		t.Fatalf("exact replay after revisions view=%+v err=%v", revisedView, err)
	}

	changed := input
	changed.GoalObjective = "A different outcome"
	if _, err := service.BootstrapStrategy(ctx, changed); !errors.Is(err, ErrStrategyConflict) {
		t.Fatalf("changed replay error=%v", err)
	}
	if _, err := New(events.NewGateway(store)).Recover(ctx); err != nil {
		t.Fatalf("recover bootstrapped strategy: %v", err)
	}
	if bounded.unboundedStrategyReads != 0 {
		t.Fatalf("strategy retries used %d unbounded correlation reads", bounded.unboundedStrategyReads)
	}
}

func TestPreflightStrategySnapshotRejectsRecordOverflow(t *testing.T) {
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "org-1", PolicyVersion: "v1", CreatedAt: now}
	snapshot := projections.Snapshot{
		Organizations: map[core.ID]projections.Versioned[core.Organization]{organization.ID: {Version: 1, Value: organization}},
		Missions:      make(map[core.ID]projections.Versioned[core.Mission], maximumOrganizationSnapshotRecords-1),
	}
	for index := 0; index < maximumOrganizationSnapshotRecords-1; index++ {
		id := core.ID(fmt.Sprintf("mission-existing-%05d", index))
		snapshot.Missions[id] = projections.Versioned[core.Mission]{Version: 1, Value: core.Mission{
			ID: id, OrganizationID: organization.ID, Statement: "existing", Status: core.MissionActive, CreatedAt: now,
		}}
	}
	mission := core.Mission{ID: "mission-new", OrganizationID: organization.ID, Statement: "new", Status: core.MissionActive, CreatedAt: now}
	goal := core.Goal{
		ID: "goal-new", OrganizationID: organization.ID, MissionID: mission.ID, Objective: "new",
		Mode: core.GoalTarget, Status: core.GoalActive, CreatedAt: now,
		SuccessCriteria: []core.IntentValue{{Value: "verified", Origin: "USER_CONFIRMED"}},
	}
	if _, err := preflightStrategySnapshot(snapshot, nil, mission, goal); !errors.Is(err, errOrganizationSnapshotLimit) {
		t.Fatalf("strategy record overflow error=%v", err)
	}
}

func TestBootstrapStrategyAdmissionIsTenantScoped(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := New(events.NewGateway(store))
	first := StrategyBootstrapInput{
		OrganizationID: "org-1", RequestID: "strategy-shared", RequestedByID: "local-uid-1000",
		RequestedByKind: core.PrincipalHuman, SourceChannel: "HUMAN_DIRECT",
		MissionID: "mission-org-1", MissionStatement: "First tenant direction",
		GoalID: "goal-org-1", GoalObjective: "First tenant outcome", GoalMode: core.GoalTarget,
		SuccessCriteria: []string{"First tenant evidence is durable"},
	}
	second := first
	second.OrganizationID = "org-2"
	second.RequestedByID = "local-uid-2000"
	second.MissionID = "mission-org-2"
	second.MissionStatement = "Second tenant direction"
	second.GoalID = "goal-org-2"
	second.GoalObjective = "Second tenant outcome"
	second.SuccessCriteria = []string{"Second tenant evidence is durable"}
	if _, err := service.BootstrapStrategy(ctx, first); err != nil {
		t.Fatalf("first tenant strategy: %v", err)
	}
	if _, err := service.BootstrapStrategy(ctx, second); err != nil {
		t.Fatalf("second tenant strategy with shared request ID: %v", err)
	}
	for _, input := range []StrategyBootstrapInput{first, second} {
		stream, err := store.StrategyCreationEvents(ctx, string(input.OrganizationID), input.RequestID)
		if err != nil || len(stream) != 3 {
			t.Fatalf("tenant %s strategy events=%+v err=%v", input.OrganizationID, stream, err)
		}
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
	forgedDetail := json.RawMessage(`{"request_id":"strategy-1","requested_by_id":"local-uid-1000","requested_by_kind":"HUMAN","source_channel":"HUMAN_DIRECT","approval_authority":true}`)
	if strategyCreationDetailMatches(forgedDetail, valid) {
		t.Fatal("authority-shaped strategy provenance was accepted")
	}
	stream, err := store.Events(context.Background(), "")
	if err != nil || len(stream) != 0 {
		t.Fatalf("invalid strategy reached ledger=%+v err=%v", stream, err)
	}
}

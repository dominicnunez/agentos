package ledger_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
	ledgerrecovery "github.com/dominicnunez/agentos/internal/ledger/recovery"
)

func TestRecoveryRejectsCausallyReorderedGoalEvidence(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "causal-goal.db")
	store, err := ledger.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	gateway := events.NewGateway(store)
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "organization-1"},
		ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization,
	}); err != nil {
		t.Fatal(err)
	}
	mission := core.Mission{ID: "mission-1", OrganizationID: organization.ID, Statement: "produce verified outcomes", Status: core.MissionActive, CreatedAt: now}
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "MISSION_CREATED", SourceActorID: "runtime", CorrelationID: "mission-1"},
		ProjectionKind: "mission", RecordID: string(mission.ID), Version: 1, Value: mission,
	}); err != nil {
		t.Fatal(err)
	}
	criterion := core.IntentValue{Value: "The requested outcome is produced and independently evaluated.", Origin: "RUNTIME_DEFAULT"}
	goal := core.Goal{
		ID: "goal-1", OrganizationID: organization.ID, MissionID: mission.ID, Objective: "produce a verified result",
		Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{criterion}, Status: core.GoalActive, CreatedAt: now,
	}
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "GOAL_CREATED", SourceActorID: "runtime", CorrelationID: "goal-1"},
		ProjectionKind: "goal", RecordID: string(goal.ID), Version: 1, Value: goal,
	}); err != nil {
		t.Fatal(err)
	}

	const requestID = "goal-recovery"
	const statement = "echo verified Goal result"
	correlationID, err := gateway.ReserveExternalWork(ctx, string(organization.ID), requestID)
	if err != nil {
		t.Fatal(err)
	}
	messageID := "message-" + requestID
	draft := core.IntentDraft{
		ID: core.ID("intent-" + correlationID), OrganizationID: organization.ID, Version: 1,
		Status: core.IntentStatusReadyForReview, Mode: core.IntentModeStandard, RequestedExecutionKind: core.ExecutionDeterministic,
		Goal: &core.IntentValue{Value: string(goal.ID), Origin: "EXPLICIT", SourceMessageID: messageID}, Objective: statement,
		Context:            []core.IntentValue{},
		Deliverables:       []core.IntentValue{{Value: "The submitted work is performed.", Origin: "RUNTIME_DEFAULT"}},
		CompletionCriteria: []core.IntentValue{criterion}, Constraints: []core.IntentValue{}, ResolvedDecisions: []core.IntentDecision{},
		ConsequenceCandidates: []string{}, MissingUserInputs: []core.IntentValue{}, CreatedAt: time.Unix(0, 0).UTC(),
	}
	draft.Fingerprint, err = core.FingerprintIntentDraft(draft)
	if err != nil {
		t.Fatal(err)
	}
	original := statement + " under " + string(goal.ID)
	taskID := "task-" + correlationID
	if _, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: string(organization.ID), EventType: "INTAKE_MESSAGE_RECORDED", SourceActorID: "user-1", TaskID: taskID, CorrelationID: correlationID,
		Payload: events.IntakeMessageRecordedPayload{MessageID: messageID, Text: original, SourcePrincipalID: "user-1", SourcePrincipalKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", RequestedExecutionKind: core.ExecutionDeterministic},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: string(organization.ID), EventType: "INTENT_DRAFTED", SourceActorID: "runtime", TaskID: taskID, CorrelationID: correlationID,
		Payload: events.IntentDraftedPayload{SourceMessageID: messageID, Draft: draft, Reply: "Review the proposed intent before work begins."},
	}); err != nil {
		t.Fatal(err)
	}
	confirmation := events.IntentConfirmedPayload{
		IntentID: string(draft.ID), GoalID: string(goal.ID), Version: draft.Version, Fingerprint: draft.Fingerprint,
		ConfirmingActorID: "user-1", ConfirmingActorKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", MessageID: "confirmation-" + requestID,
	}
	if _, err := gateway.PublishIntentConfirmation(ctx, events.TrustedDraft{
		OrganizationID: string(organization.ID), EventType: "INTENT_CONFIRMED", SourceActorID: "user-1", TaskID: taskID, CorrelationID: correlationID, Payload: confirmation,
	}, goal.ID); err != nil {
		t.Fatal(err)
	}
	submission := app.Submit{
		RequestID: requestID, OrganizationID: string(organization.ID), GoalID: goal.ID, Statement: original, Kind: core.ExecutionDeterministic,
		MessageID: messageID, SourcePrincipalID: "user-1", SourcePrincipalKind: core.PrincipalHuman, SourceChannel: "HUMAN_DIRECT", NormalizedIntent: &draft,
	}
	if _, err := app.New(gateway).Submit(ctx, submission); err != nil {
		t.Fatal(err)
	}
	retiredMission := mission
	retiredMission.Status = core.MissionRetired
	retirement, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: string(organization.ID), EventType: "MISSION_RETIRED", SourceActorID: "runtime", CorrelationID: "mission-retired"},
		ProjectionKind: "mission", RecordID: string(mission.ID), Version: 2, Value: retiredMission,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := app.New(gateway).Recover(ctx); err != nil {
		t.Fatalf("legitimate Mission retirement after Goal evaluation was rejected: %v", err)
	}

	stream, err := store.Events(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	var workTransitionID string
	var evaluationSequence int64
	for _, event := range stream {
		switch event.EventType {
		case "WORK_COMPLETED":
			var payload events.ProjectionEventPayload
			if json.Unmarshal(event.Payload, &payload) == nil && payload.Projection.CorrelationID == correlationID {
				workTransitionID = event.EventID
			}
		case "GOAL_PROGRESS_EVALUATED":
			var progress events.GoalProgressEvaluatedPayload
			if json.Unmarshal(event.Payload, &progress) == nil && progress.GoalID == goal.ID {
				evaluationSequence = event.Sequence
			}
		}
	}
	if workTransitionID == "" || evaluationSequence < 1 || retirement.EventID == "" {
		t.Fatal("causal Goal evidence was not found")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ledgerrecovery.Verify(ctx, databasePath); err != nil {
		t.Fatalf("recovery verification rejected legitimate Goal achievement evidence: %v", err)
	}
	raw, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := raw.BeginTx(ctx, nil)
	if err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	for _, statement := range []string{`UPDATE events SET sequence=-sequence`, `UPDATE events SET sequence=(-sequence)*2`} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			_ = tx.Rollback()
			_ = raw.Close()
			t.Fatal(err)
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE events SET sequence=? WHERE event_id=?`, evaluationSequence*2-1, retirement.EventID); err != nil {
		_ = tx.Rollback()
		_ = raw.Close()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	if corruptStore, err := ledger.Open(databasePath); err == nil {
		_ = corruptStore.Close()
		t.Fatal("startup admitted causally reordered projection events")
	}
}

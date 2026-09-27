package ledger_test

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
	ledgerrecovery "github.com/dominicnunez/agentos/internal/ledger/recovery"
)

func TestIncidentGoalDetailOwner(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "goal-detail.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
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
	}, goal.ID, ""); err != nil {
		t.Fatal(err)
	}
	submission := app.Submit{
		RequestID: requestID, OrganizationID: string(organization.ID), GoalID: goal.ID, Statement: original, Kind: core.ExecutionDeterministic,
		MessageID: messageID, SourcePrincipalID: "user-1", SourcePrincipalKind: core.PrincipalHuman, SourceChannel: "HUMAN_DIRECT", NormalizedIntent: &draft,
	}
	if _, err := app.New(gateway).Submit(ctx, submission); err != nil {
		t.Fatal(err)
	}

	selectedResult, err := app.New(gateway).Submit(ctx, app.Submit{RequestID: "selected-goal-detail", OrganizationID: "org-2", Statement: "echo selected", Kind: core.ExecutionDeterministic})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := store.Events(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	var selected, incoming events.Event
	for _, event := range stream {
		if event.OrganizationID == "org-1" && event.EventType == "GOAL_ACHIEVED" {
			incoming = event
		}
	}
	for _, event := range selectedResult.Events {
		if event.EventType == "WORK_COMPLETION_EVALUATED" {
			selected = event
		}
	}
	if selected.EventID == "" || incoming.EventID == "" {
		t.Fatal("writer did not admit required Goal fixture events")
	}
	if _, err := store.VerifiedIncidentEvents(ctx, "org-1", incoming.CorrelationID, 256); err != nil {
		t.Fatalf("valid achieved Goal incident: %v", err)
	}
	baseline, err := store.VerifiedIncidentEvents(ctx, "org-2", selected.CorrelationID, 256)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range baseline.DependencyEvents {
		if event.EventID == incoming.EventID {
			t.Fatal("unrelated Goal selected before reference changed")
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ledgerrecovery.Verify(ctx, path); err != nil {
		t.Fatalf("valid full Goal recovery: %v", err)
	}
	store, err = ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ledger.ChangeIncidentDetailForTest(t, store, incoming.EventID, "evidence_event_ref", selected.EventID)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ledgerrecovery.Verify(ctx, path); err == nil {
		t.Fatal("full recovery accepted mismatched Goal evidence")
	}
	store, err = ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.VerifiedIncidentEvents(ctx, "org-2", selected.CorrelationID, 256)
	if err == nil {
		t.Fatal("incident omitted incoming Goal achievement detail")
	}
	if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
		t.Fatal("failed incident returned partial evidence")
	}
}

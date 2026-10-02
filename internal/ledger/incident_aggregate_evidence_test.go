package ledger_test

import (
	"encoding/json"
	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
	ledgerrecovery "github.com/dominicnunez/agentos/internal/ledger/recovery"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestIncidentAggregateEvidence(t *testing.T) {
	testIncidentAggregateEvidence(t, false)
}

func TestIncidentAggregateMissingConsumer(t *testing.T) {
	testIncidentAggregateEvidence(t, true)
}

func testIncidentAggregateEvidence(t *testing.T, missingConsumer bool) {
	t.Helper()
	ledger.ParallelIncidentTestForTest(t)
	for _, field := range []string{"work_evidence_refs", "criteria.work_evidence_refs", "tasks.verification_event_ref", "tasks.completion_event_ref"} {
		t.Run(field, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "aggregate.db")
			store, err := ledger.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			appendAggregateGoal(t, store)
			result, err := app.New(events.NewGateway(store)).Submit(t.Context(), app.Submit{RequestID: "selected-aggregate", OrganizationID: "org-2", Statement: "echo selected", Kind: core.ExecutionDeterministic})
			if err != nil {
				t.Fatal(err)
			}
			var selected, incoming events.Event
			targetKind, incomingKind := "WORK_COMPLETION_EVALUATED", "GOAL_PROGRESS_EVALUATED"
			if strings.HasPrefix(field, "tasks.") {
				incomingKind = "WORK_COMPLETION_EVALUATED"
				targetKind = "COMPLETION_VERIFIED"
				if field == "tasks.completion_event_ref" {
					targetKind = "TASK_VERIFIED_COMPLETE"
				}
			}
			for _, event := range result.Events {
				if event.EventType == targetKind {
					selected = event
				}
			}
			stream, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range stream {
				if event.OrganizationID == "org-1" && event.EventType == incomingKind {
					incoming = event
				}
			}
			if selected.EventID == "" || incoming.EventID == "" {
				t.Fatal("writer did not admit required aggregate evidence")
			}
			graph, err := events.ValidateProjectionHistory(stream, nil, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := events.ValidateProjectionCompletions(graph, stream, nil); err != nil {
				t.Fatalf("healthy aggregate completion validation: %v", err)
			}
			own, err := store.VerifiedIncidentEvents(t.Context(), "org-1", incoming.CorrelationID, 256)
			if err != nil {
				t.Fatalf("healthy owner incident: %v", err)
			}
			found := false
			for _, part := range [][]events.Event{own.Work.Events, own.DependencyEvents} {
				for _, event := range part {
					found = found || event.EventID == incoming.EventID
				}
			}
			if !found {
				t.Fatal("valid owner incident omitted aggregate evaluation")
			}
			baseline, err := store.VerifiedIncidentEvents(t.Context(), "org-2", selected.CorrelationID, 256)
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range baseline.DependencyEvents {
				if event.EventID == incoming.EventID {
					t.Fatal("unrelated aggregate selected before mutation")
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := ledgerrecovery.Verify(t.Context(), path); err != nil {
				t.Fatalf("healthy full recovery: %v", err)
			}
			store, err = ledger.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			ledger.ChangeAggregateEvidenceForTest(t, store, incoming.EventID, field, selected.EventID)
			stream, err = store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if missingConsumer {
				var terminal string
				for _, event := range stream {
					if event.EventType != "WORK_COMPLETED" && event.EventType != "GOAL_ACHIEVED" {
						continue
					}
					payload, present, err := events.AdmittedProjection(event)
					if err != nil || !present {
						t.Fatalf("writer terminal admission: %v", err)
					}
					var detail struct {
						EvidenceEventRef string `json:"evidence_event_ref"`
					}
					if err := json.Unmarshal(payload.Detail, &detail); err != nil {
						t.Fatal(err)
					}
					if detail.EvidenceEventRef == incoming.EventID {
						terminal = event.EventID
					}
				}
				if terminal == "" {
					t.Fatal("aggregate has no actual consuming terminal")
				}
				ledger.ChangeIncidentSourceKindForTest(t, store, terminal, "missing-kind")
				stream, err = store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
			}
			graph, err = events.ValidateProjectionHistory(stream, nil, nil, nil)
			if missingConsumer {
				if err == nil {
					t.Fatal("full history accepted malformed consuming terminal")
				}
			} else {
				if err != nil {
					t.Fatalf("mutation must preserve projection admissions: %v", err)
				}
				if err := events.ValidateProjectionCompletions(graph, stream, nil); err == nil {
					t.Fatal("full completion owner accepted substituted aggregate evidence")
				} else {
					t.Logf("owner rejection: %v", err)
				}
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-2", selected.CorrelationID, 256)
			if err == nil {
				t.Fatal("incident omitted incoming aggregate evidence")
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("failed incident returned partial evidence")
			}
		})
	}
}

func appendAggregateGoal(t *testing.T, store *ledger.SQLite) {
	t.Helper()
	ctx := t.Context()
	var err error
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

}

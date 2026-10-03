package ledger_test

import (
	"encoding/json"
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

func TestIncidentUnusedAggregateClaims(t *testing.T) {
	ledger.ParallelIncidentTestForTest(t)
	for _, claim := range []string{"work_id", "intent_id", "tasks.task_id", "work.goal_id", "tasks.verification_event_ref", "tasks.completion_event_ref", "mission_id", "goal.goal_id", "work_evidence_refs", "criteria.work_evidence_refs", "unrelated-work", "unrelated-goal", "wrong-work-consumer", "wrong-goal-consumer", "consumed-work-goal", "consumed-goal-goal", "public-work", "public-goal"} {
		t.Run(claim, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "unused-aggregate.db")
			store, err := ledger.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			appendAggregateGoal(t, store)
			selected, err := app.New(events.NewGateway(store)).Submit(t.Context(), app.Submit{RequestID: "selected-unused", OrganizationID: "org-2", Statement: "echo selected", Kind: core.ExecutionDeterministic})
			if err != nil {
				t.Fatal(err)
			}
			mission := core.Mission{ID: "selected-mission", OrganizationID: "org-2", Statement: "selected mission", Status: core.MissionActive, CreatedAt: time.Now().UTC()}
			goal := core.Goal{ID: "selected-goal", OrganizationID: "org-2", MissionID: mission.ID, Objective: "selected goal", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "selected outcome", Origin: "EXPLICIT"}}, Status: core.GoalActive, CreatedAt: time.Now().UTC()}
			for _, draft := range []events.ProjectionDraft{
				{Event: events.TrustedDraft{OrganizationID: "org-2", EventType: "MISSION_CREATED", SourceActorID: "runtime", CorrelationID: "selected-goal"}, ProjectionKind: "mission", RecordID: string(mission.ID), Version: 1, Value: mission},
				{Event: events.TrustedDraft{OrganizationID: "org-2", EventType: "GOAL_CREATED", SourceActorID: "runtime", CorrelationID: "selected-goal"}, ProjectionKind: "goal", RecordID: string(goal.ID), Version: 1, Value: goal},
			} {
				if _, err := store.AppendProjection(t.Context(), draft); err != nil {
					t.Fatal(err)
				}
			}
			stream, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			var workSource, goalSource, verification, completion, workEvidence events.Event
			for _, event := range stream {
				if event.OrganizationID == "org-1" && event.EventType == "WORK_COMPLETION_EVALUATED" {
					workSource = event
				}
				if event.OrganizationID == "org-1" && event.EventType == "GOAL_PROGRESS_EVALUATED" {
					goalSource = event
				}
				if event.OrganizationID == "org-2" {
					switch event.EventType {
					case "COMPLETION_VERIFIED":
						verification = event
					case "TASK_VERIFIED_COMPLETE":
						completion = event
					case "WORK_COMPLETION_EVALUATED":
						workEvidence = event
					}
				}
			}
			var value any
			source := workSource
			correlation := selected.Events[0].CorrelationID
			if claim == "mission_id" || claim == "goal.goal_id" || claim == "work_evidence_refs" || claim == "criteria.work_evidence_refs" || claim == "unrelated-goal" || claim == "wrong-goal-consumer" || claim == "consumed-goal-goal" || claim == "public-goal" {
				source = goalSource
				var payload events.GoalProgressEvaluatedPayload
				if err := json.Unmarshal(source.Payload, &payload); err != nil {
					t.Fatal(err)
				}
				switch claim {
				case "mission_id", "wrong-goal-consumer", "public-goal":
					payload.MissionID = mission.ID
					correlation = "selected-goal"
				case "goal.goal_id", "consumed-goal-goal":
					payload.GoalID = goal.ID
					correlation = "selected-goal"
				case "work_evidence_refs":
					payload.WorkEvidenceRefs = []string{workEvidence.EventID}
				case "criteria.work_evidence_refs":
					payload.Criteria[0].WorkEvidenceRefs = []string{workEvidence.EventID}
				}
				payload.Fingerprint, err = payload.ExpectedFingerprint()
				value = payload
			} else {
				var payload events.WorkCompletionEvidencePayload
				if err := json.Unmarshal(source.Payload, &payload); err != nil {
					t.Fatal(err)
				}
				switch claim {
				case "work_id", "wrong-work-consumer", "public-work":
					payload.WorkID = selected.Work.ID
				case "intent_id":
					payload.IntentID = selected.Intent.ID
				case "tasks.task_id":
					payload.Tasks[0].TaskID = selected.Task.ID
				case "work.goal_id", "consumed-work-goal":
					payload.GoalID = goal.ID
					correlation = "selected-goal"
				case "tasks.verification_event_ref":
					payload.Tasks[0].VerificationEventRef = verification.EventID
				case "tasks.completion_event_ref":
					payload.Tasks[0].CompletionEventRef = completion.EventID
				}
				payload.Fingerprint, err = payload.ExpectedFingerprint()
				value = payload
			}
			if err != nil {
				t.Fatal(err)
			}
			if source.EventID == "" {
				t.Fatal("actual writer aggregate evidence missing")
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-2", correlation, 256); err != nil {
				t.Fatalf("healthy selected incident: %v", err)
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
			if claim == "consumed-work-goal" || claim == "consumed-goal-goal" {
				ledger.ChangeAggregateGoalClaimForTest(t, store, source, string(goal.ID))
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := ledgerrecovery.Verify(t.Context(), path); err == nil {
					t.Fatal("full recovery accepted consumed duplicate Goal claim")
				}
				store, err = ledger.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-2", correlation, 256)
				if err == nil {
					t.Fatal("incident omitted consumed later duplicate Goal claim")
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("invalid consumed aggregate returned partial snapshot")
				}
				return
			}
			var unused string
			if claim == "public-work" || claim == "public-goal" {
				unused = ledger.InsertUnusedAggregateForTest(t, store, source, value, "org-2", correlation)
			} else {
				unused = ledger.InsertUnusedAggregateForTest(t, store, source, value)
			}
			stream, err = store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			graph, err := events.ValidateProjectionHistory(stream, nil, nil, nil)
			if err != nil {
				t.Fatalf("unused evaluation changed full history: %v", err)
			}
			if err := events.ValidateProjectionCompletions(graph, stream, nil); err != nil {
				t.Fatalf("unused evaluation changed full aggregate owner: %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := ledgerrecovery.Verify(t.Context(), path); err != nil {
				t.Fatalf("unused evaluation changed full recovery: %v", err)
			}
			store, err = ledger.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if claim == "wrong-work-consumer" || claim == "wrong-goal-consumer" {
				ledger.InsertWrongAggregateConsumerForTest(t, store, source, unused)
				mutated, err := store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				if err := events.ValidateWorkCompletions(graph, mutated, nil, nil); err != nil {
					t.Fatalf("completion owner incorrectly consumed evaluation through wrong projection field: %v", err)
				}
				if err := events.ValidateGoalCompletions(graph, mutated, nil, nil); err != nil {
					t.Fatalf("Goal owner incorrectly consumed evaluation through wrong projection field: %v", err)
				}
				if _, err := events.ValidateProjectionHistory(mutated, nil, nil, nil); err == nil {
					t.Fatal("wrong lifecycle consumer fixture unexpectedly passed global projection replay")
				}
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-2", correlation, 256)
			if err != nil {
				t.Fatalf("unused aggregate claim poisoned selected incident: %v", err)
			}
			for _, event := range snapshot.DependencyEvents {
				if event.EventID == unused {
					t.Fatal("unused aggregate became incoming incident evidence")
				}
			}
			if claim == "public-work" || claim == "public-goal" {
				visible := false
				for _, event := range snapshot.Work.Events {
					visible = visible || event.EventID == unused
				}
				if !visible {
					t.Fatal("unused public evaluation disappeared from the incident timeline")
				}
			}
		})
	}
}

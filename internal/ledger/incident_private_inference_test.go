package ledger

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

func TestIncidentPrivateInferenceBacking(t *testing.T) {
	for _, mutation := range []string{"valid", "missing-accounting", "invalid-policy"} {
		t.Run(mutation, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "private-inference.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			for _, draft := range []events.ProjectionDraft{
				{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "organization", RecordID: "org-1", Version: 1, Value: core.Organization{ID: "org-1", Name: "Org", PolicyVersion: "v1", CreatedAt: now}},
				{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "MISSION_CREATED", SourceActorID: "runtime", CorrelationID: "mission"}, ProjectionKind: "mission", RecordID: "mission-1", Version: 1, Value: core.Mission{ID: "mission-1", OrganizationID: "org-1", Statement: "test", Status: core.MissionActive, CreatedAt: now}},
				{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "GOAL_CREATED", SourceActorID: "runtime", CorrelationID: "goal"}, ProjectionKind: "goal", RecordID: "goal-1", Version: 1, Value: core.Goal{ID: "goal-1", OrganizationID: "org-1", MissionID: "mission-1", Objective: "test", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "test", Origin: "RUNTIME_DEFAULT"}}, Status: core.GoalActive, CreatedAt: now}},
			} {
				if _, err := store.AppendProjection(t.Context(), draft); err != nil {
					t.Fatal(err)
				}
			}
			reviewed := appendReviewedGoalIntent(t, t.Context(), store, "org-1", "model-stop", "intent-model-stop", "goal-1", "test", core.ExecutionDeterministic, now)
			confirmation := events.IntentConfirmedPayload{IntentID: "intent-model-stop", GoalID: "goal-1", Version: 1, Fingerprint: reviewed.Fingerprint, ConfirmingActorID: "user-1", ConfirmingActorKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", MessageID: "confirm"}
			if _, err := store.AppendIntentConfirmation(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CONFIRMED", SourceActorID: "user-1", TaskID: "task-model-stop", Payload: confirmation, CorrelationID: "model-stop"}, "goal-1", ""); err != nil {
				t.Fatal(err)
			}
			for _, draft := range []events.ProjectionDraft{
				{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: "model-stop"}, ProjectionKind: "intent", RecordID: "intent-model-stop", Version: 1, Value: core.Intent{ID: "intent-model-stop", OrganizationID: "org-1", GoalID: "goal-1", OriginalInstruction: "test under goal-1", NormalizedObjective: "test", AcceptedFingerprint: reviewed.Fingerprint, ExternalRequestID: "private-request", SourcePrincipalID: "user-1", SourcePrincipalKind: core.PrincipalHuman, SourceChannel: "HUMAN_DIRECT", SourceMessageID: "source-model-stop", CreatedAt: now}},
				{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: "model-stop"}, ProjectionKind: "work", RecordID: "work-1", Version: 1, Value: core.Work{ID: "work-1", IntentID: "intent-model-stop", GoalID: "goal-1", Objective: "test", Status: core.WorkActive, CreatedAt: now}},
			} {
				if _, err := store.AppendProjection(t.Context(), draft); err != nil {
					t.Fatal(err)
				}
			}

			modelStopManifest(t, store, true, "private-call")
			policy := testInferencePolicy(time.Now().UTC())
			policy.OrganizationID, policy.Provider, policy.Model, policy.ExecutionProfileVersion = "org-1", "provider", "model", "v1"
			if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			request := testInferenceRequest("private-call")
			request.Scope.OrganizationID, request.Scope.CorrelationID, request.Scope.TaskID, request.Scope.IntentID = "org-1", "model-stop", "task-model-stop", "intent-model-stop"
			request.Scope.Purpose = inference.PurposeIntentNormalization
			request.Descriptor.Provider, request.Descriptor.Model, request.Descriptor.ExecutionProfileVersion = "provider", "model", "v1"
			if _, err := store.ReserveInference(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			baseline, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "goal", 256)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, event := range baseline.DependencyEvents {
				if event.EventType == "INFERENCE_RESERVED" {
					found = true
				}
			}
			if !found {
				t.Fatal("fixture did not include private inference")
			}
			switch mutation {
			case "missing-accounting":
				_, err = store.db.ExecContext(t.Context(), `DELETE FROM inference_reservations`)
			case "invalid-policy":
				_, err = store.db.ExecContext(t.Context(), `UPDATE inference_policies SET body='{}'`)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			_, err = store.VerifiedIncidentEvents(t.Context(), "org-1", "goal", 256)
			if mutation == "valid" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("invalid private inference backing accepted")
			}
		})
	}
}

package completion

import (
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestGoalEvidenceBindsAcceptedIntentPlanAndVerifiedTasks(t *testing.T) {
	goal, intent, plan, tasks := goalEvidenceFixture()
	record, err := NewGoalEvidence(goal, 2, intent, plan, tasks, time.Date(2026, 8, 13, 3, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if !record.Valid() || !record.MatchesCurrent(goal, 2, intent, plan, tasks) {
		t.Fatalf("valid evidence was rejected: %+v", record)
	}
	if record.Tasks[0].TaskID != "task-a" || record.Tasks[1].TaskID != "task-b" {
		t.Fatalf("task evidence is not canonical: %+v", record.Tasks)
	}
	if len(record.ArtifactRefs) != 2 || record.ArtifactRefs[0] != "artifact-a" || record.ArtifactRefs[1] != "artifact-b" {
		t.Fatalf("artifact aggregation is not canonical: %+v", record.ArtifactRefs)
	}

	intent.CompletionCriteria[0].Value = "changed criterion"
	if record.MatchesCurrent(goal, 2, intent, plan, tasks) {
		t.Fatal("evidence matched a changed accepted completion criterion")
	}
}

func TestGoalEvidenceRejectsTamperingAndDuplicateTaskProof(t *testing.T) {
	goal, intent, plan, tasks := goalEvidenceFixture()
	record, err := NewGoalEvidence(goal, 2, intent, plan, tasks, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	record.Tasks[0].VerificationEventRef = "changed-event"
	if record.Valid() {
		t.Fatal("tampered evidence retained a valid fingerprint")
	}

	tasks[1].TaskID = tasks[0].TaskID
	if _, err := NewGoalEvidence(goal, 2, intent, plan, tasks, time.Now().UTC()); err == nil {
		t.Fatal("duplicate Task proof was accepted")
	}
}

func goalEvidenceFixture() (core.Goal, core.Intent, core.Plan, []GoalTaskEvidence) {
	intent := core.Intent{
		ID: "intent-1", AcceptedFingerprint: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		CompletionCriteria: []core.IntentValue{{Value: "deliver the reviewed result", Origin: "USER"}},
	}
	goal := core.Goal{ID: "goal-1", IntentID: intent.ID, Objective: "complete work", Status: "ACTIVE"}
	plan := core.Plan{ID: "plan-1", IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, Version: 1}
	tasks := []GoalTaskEvidence{
		{TaskID: "task-b", TaskVersion: 3, VerificationEventRef: "verify-b", CompletionEventRef: "complete-b", ArtifactRefs: []string{"artifact-b", "artifact-a"}},
		{TaskID: "task-a", TaskVersion: 2, VerificationEventRef: "verify-a", CompletionEventRef: "complete-a", ArtifactRefs: []string{"artifact-a"}},
	}
	return goal, intent, plan, tasks
}

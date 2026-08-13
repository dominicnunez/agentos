package completion

import (
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestWorkEvidenceBindsAcceptedIntentPlanAndVerifiedTasks(t *testing.T) {
	work, intent, plan, tasks := workEvidenceFixture()
	record, err := NewWorkEvidence(work, 2, intent, plan, tasks, time.Date(2026, 8, 13, 3, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if !record.Valid() || !record.MatchesCurrent(work, 2, intent, plan, tasks) {
		t.Fatalf("valid evidence was rejected: %+v", record)
	}
	if record.Tasks[0].TaskID != "task-a" || record.Tasks[1].TaskID != "task-b" {
		t.Fatalf("task evidence is not canonical: %+v", record.Tasks)
	}
	if len(record.ArtifactRefs) != 2 || record.ArtifactRefs[0] != "artifact-a" || record.ArtifactRefs[1] != "artifact-b" {
		t.Fatalf("artifact aggregation is not canonical: %+v", record.ArtifactRefs)
	}
	changedWork := work
	changedWork.GoalID = "goal-2"
	if record.MatchesCurrent(changedWork, 2, intent, plan, tasks) {
		t.Fatal("evidence matched Work rebound to a different Goal")
	}

	intent.CompletionCriteria[0].Value = "changed criterion"
	if record.MatchesCurrent(work, 2, intent, plan, tasks) {
		t.Fatal("evidence matched a changed accepted completion criterion")
	}
}

func TestWorkEvidenceRejectsTamperingAndDuplicateTaskProof(t *testing.T) {
	work, intent, plan, tasks := workEvidenceFixture()
	record, err := NewWorkEvidence(work, 2, intent, plan, tasks, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	record.Tasks[0].VerificationEventRef = "changed-event"
	if record.Valid() {
		t.Fatal("tampered evidence retained a valid fingerprint")
	}

	tasks[1].TaskID = tasks[0].TaskID
	if _, err := NewWorkEvidence(work, 2, intent, plan, tasks, time.Now().UTC()); err == nil {
		t.Fatal("duplicate Task proof was accepted")
	}
}

func workEvidenceFixture() (core.Work, core.Intent, core.Plan, []WorkTaskEvidence) {
	intent := core.Intent{
		ID: "intent-1", AcceptedFingerprint: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		CompletionCriteria: []core.IntentValue{{Value: "deliver the reviewed result", Origin: "USER"}},
	}
	work := core.Work{ID: "work-1", IntentID: intent.ID, GoalID: "goal-1", Objective: "complete work", Status: core.WorkActive}
	plan := core.Plan{ID: "plan-1", IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, Version: 1}
	tasks := []WorkTaskEvidence{
		{TaskID: "task-b", TaskVersion: 3, VerificationEventRef: "verify-b", CompletionEventRef: "complete-b", ArtifactRefs: []string{"artifact-b", "artifact-a"}},
		{TaskID: "task-a", TaskVersion: 2, VerificationEventRef: "verify-a", CompletionEventRef: "complete-a", ArtifactRefs: []string{"artifact-a"}},
	}
	return work, intent, plan, tasks
}

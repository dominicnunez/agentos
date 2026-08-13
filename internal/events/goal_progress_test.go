package events

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestGoalProgressEvaluationUsesExactCompletedWorkCriteria(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	first := core.IntentValue{Value: "first verified outcome", Origin: "USER", SourceMessageID: "message-1"}
	second := core.IntentValue{Value: "second verified outcome", Origin: "USER", SourceMessageID: "message-1"}
	goal := core.Goal{
		ID: "goal-1", OrganizationID: "org-1", MissionID: "mission-1", Objective: "measurable outcome",
		Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{first, second}, Status: core.GoalActive, CreatedAt: now,
	}
	firstEvidence := validGoalWorkEvidence(t, "work-1", "evt-work-1", "goal-1", []core.IntentValue{first}, now)

	progress, err := NewGoalProgressEvaluation(goal, 3, []GoalWorkEvidence{firstEvidence})
	if err != nil {
		t.Fatal(err)
	}
	if progress.Result != GoalProgressTargetInProgress || !progress.Criteria[0].Satisfied || progress.Criteria[1].Satisfied || !progress.Valid() {
		t.Fatalf("unexpected partial Goal evaluation: %+v", progress)
	}
	if err := ValidateGoalProgressEvaluation(goal, 3, []GoalWorkEvidence{firstEvidence}, progress); err != nil {
		t.Fatalf("valid Goal evaluation was rejected: %v", err)
	}

	secondEvidence := validGoalWorkEvidence(t, "work-2", "evt-work-2", "goal-1", []core.IntentValue{second}, now.Add(time.Second))
	achieved, err := NewGoalProgressEvaluation(goal, 3, []GoalWorkEvidence{secondEvidence, firstEvidence})
	if err != nil {
		t.Fatal(err)
	}
	if achieved.Result != GoalProgressTargetAchieved || !achieved.Criteria[0].Satisfied || !achieved.Criteria[1].Satisfied || !reflect.DeepEqual(achieved.WorkEvidenceRefs, []string{"evt-work-1", "evt-work-2"}) || !achieved.EvaluatedAt.Equal(secondEvidence.EventAt) {
		t.Fatalf("unexpected achieved Goal evaluation: %+v", achieved)
	}
	if achieved.Fingerprint == progress.Fingerprint {
		t.Fatal("changed authoritative evidence did not change the Goal evaluation fingerprint")
	}

	continuous := goal
	continuous.Mode = core.GoalContinuous
	continuousProgress, err := NewGoalProgressEvaluation(continuous, 3, []GoalWorkEvidence{firstEvidence, secondEvidence})
	if err != nil {
		t.Fatal(err)
	}
	if continuousProgress.Result != GoalProgressContinuous {
		t.Fatalf("continuous Goal became terminal: %+v", continuousProgress)
	}

	if _, err := NewGoalProgressEvaluation(goal, 3, []GoalWorkEvidence{firstEvidence, firstEvidence}); err == nil {
		t.Fatal("duplicated completed-Work evidence was accepted")
	}
	changed := achieved
	changed.GoalVersion++
	if changed.Valid() {
		t.Fatal("stale fingerprint remained valid after changing the Goal revision")
	}
}

func validGoalWorkEvidence(t *testing.T, workID core.ID, eventRef string, goalID core.ID, criteria []core.IntentValue, eventAt time.Time) GoalWorkEvidence {
	t.Helper()
	evidence := WorkCompletionEvidencePayload{
		WorkID: workID, WorkVersion: 2, GoalID: goalID, IntentID: core.ID("intent-" + string(workID)),
		IntentFingerprint: strings.Repeat("a", 64), PlanID: core.ID("plan-" + string(workID)), PlanVersion: 1,
		Criteria: criteria,
		Tasks: []WorkCompletionTaskEvidencePayload{{
			TaskID: core.ID("task-" + string(workID)), TaskVersion: 2,
			VerificationEventRef: "verify-" + string(workID), CompletionEventRef: "complete-" + string(workID), ArtifactRefs: []string{},
		}},
		ArtifactRefs: []string{}, CreatedAt: eventAt,
	}
	var err error
	evidence.Fingerprint, err = evidence.ExpectedFingerprint()
	if err != nil || !evidence.Valid() {
		t.Fatalf("construct valid Work evidence: evidence=%+v err=%v", evidence, err)
	}
	return GoalWorkEvidence{EventRef: eventRef, EventAt: eventAt, Evidence: evidence}
}

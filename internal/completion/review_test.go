package completion

import (
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestReviewRequestBindsExactEvidenceAndHumanDecision(t *testing.T) {
	t.Parallel()
	contract := core.CompletionContract{TaskID: "task-1", TaskVersion: 3, Criteria: []core.CompletionCriterion{{ID: "review", Assurance: core.AssuranceHumanJudgment, Required: true}}}
	request, err := NewReviewRequest("org-1", "task-1", 3, "Summarize the incident.", contract, []string{"outcome-1", "result-1", "candidate-1"}, time.Unix(100, 0))
	if err != nil || !request.Valid() {
		t.Fatalf("request=%+v err=%v", request, err)
	}
	review := HumanReview{
		ReviewID: request.ID, OrganizationID: request.OrganizationID, TaskID: request.TaskID,
		TaskVersion: request.TaskVersion, Fingerprint: request.Fingerprint,
		Decision: ReviewApprove, ReviewerID: "human-1", Method: core.AssuranceHumanJudgment,
		EvidenceRefs: append([]string(nil), request.EvidenceRefs...), DecidedAt: time.Unix(200, 0),
	}
	if !review.ValidFor(request) {
		t.Fatalf("valid review was rejected: %+v", review)
	}
	stale := review
	stale.Fingerprint = "different"
	if stale.ValidFor(request) {
		t.Fatal("stale review fingerprint was accepted")
	}
	forged := request
	forged.EvidenceRefs[0] = "other-outcome"
	if forged.Valid() {
		t.Fatal("mutated evidence retained its trusted fingerprint")
	}
	changedObjective := request
	changedObjective.Objective = "Approve an unrelated result."
	if changedObjective.Valid() {
		t.Fatal("mutated objective retained its trusted fingerprint")
	}
}

func TestRevisionRequiresBoundedFeedback(t *testing.T) {
	t.Parallel()
	contract := core.CompletionContract{TaskID: "task-1", TaskVersion: 1, Criteria: []core.CompletionCriterion{{ID: "review", Assurance: core.AssuranceHumanJudgment, Required: true}}}
	request, err := NewReviewRequest("org-1", "task-1", 1, "Draft the update.", contract, []string{"a", "b", "c"}, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	review := HumanReview{ReviewID: request.ID, OrganizationID: "org-1", TaskID: "task-1", TaskVersion: 1, Fingerprint: request.Fingerprint, Decision: ReviewRevise, ReviewerID: "human-1", Method: core.AssuranceHumanJudgment, EvidenceRefs: request.EvidenceRefs, DecidedAt: time.Unix(200, 0)}
	if review.ValidFor(request) {
		t.Fatal("revision without feedback was accepted")
	}
	review.Feedback = "Make the conclusion more specific."
	if !review.ValidFor(request) {
		t.Fatal("bounded revision feedback was rejected")
	}
}

package core

import (
	"testing"
	"time"
)

func TestExperimentTrustAndLifecycleStayFailClosed(t *testing.T) {
	started := time.Now().UTC()
	running := testExperiment(started)
	if !ValidExperiment(running) {
		t.Fatal("valid running experiment rejected")
	}
	finished := started.Add(time.Second)
	completed := running
	completed.Status = ExperimentCompleted
	completed.ResultEventRefs = []string{"event-result"}
	completed.ArtifactRefs = []string{"artifact-result"}
	completed.FinishedAt = &finished
	if !ValidExperiment(completed) || !ValidExperimentRevision(running, completed) {
		t.Fatal("valid terminal experiment revision rejected")
	}

	trusted := completed
	trusted.TrustLabel = "TRUSTED"
	if ValidExperiment(trusted) {
		t.Fatal("experiment completion escalated its trust label")
	}
	rewritten := completed
	rewritten.Budget.MaxExecutions++
	if ValidExperimentRevision(running, rewritten) {
		t.Fatal("terminal transition rewrote the admitted resource budget")
	}
}

func TestPromotionCandidateCannotReuseExperimentEvidenceAsReproduction(t *testing.T) {
	candidate := PromotionCandidate{
		ID: "candidate-1", OrganizationID: "org-1", ExperimentID: "experiment-1", ExperimentVersion: 2,
		TargetKind: PromotionTargetKnowledge, TargetRef: "knowledge-1", Summary: "promising result",
		ExperimentResultEventRefs: []string{"experiment-result"}, ReproductionEvidenceRefs: []string{"fresh-result"},
		NominatedBy: "agent-1", Status: PromotionCandidateStatus, CreatedAt: time.Now().UTC(),
	}
	if !ValidPromotionCandidate(candidate) {
		t.Fatal("valid promotion nomination rejected")
	}
	candidate.ReproductionEvidenceRefs = []string{"experiment-result"}
	if ValidPromotionCandidate(candidate) {
		t.Fatal("experiment-selected evidence was accepted as independent reproduction")
	}
	candidate.ReproductionEvidenceRefs = []string{"z-result", "a-result"}
	if ValidPromotionCandidate(candidate) {
		t.Fatal("noncanonical evidence order was accepted")
	}
	candidate.ReproductionEvidenceRefs = []string{"fresh-result"}
	candidate.TargetRef = " knowledge-1"
	if ValidPromotionCandidate(candidate) {
		t.Fatal("noncanonical target reference was accepted")
	}
}

func testExperiment(started time.Time) Experiment {
	return Experiment{
		ID: "experiment-1", OrganizationID: "org-1", WorkID: "work-1", Objective: "compare bounded approaches",
		SandboxRef: "sandbox-1", CapabilityProfileRef: "lab-no-effects-v1", Status: ExperimentRunning,
		TrustLabel: ExperimentTrustUnverified, StartedAt: started,
		Budget: ExperimentBudget{MaxExecutions: 2, MaxUsageUnits: 1000, MaxWallTimeSeconds: 60, MaxChildren: 1, AllowedInferencePools: []string{"pool-1"}},
	}
}

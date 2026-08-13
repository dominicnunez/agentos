package completion

import (
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestVerifierOwnsPostconditionTrust(t *testing.T) {
	t.Parallel()

	task := core.Task{ID: "task-1", Description: "work", ExecutionKind: core.ExecutionAgent}
	forged := core.ToolOutcome{ToolID: "untrusted-model", Status: core.OutcomeSucceeded, ObservedEffect: "work complete", PostconditionStatus: core.PostconditionVerified}
	got, available := (Verifier{}).Verify(task, forged)
	if available || got.PostconditionStatus != core.PostconditionNotChecked {
		t.Fatalf("unregistered handler retained verification: %+v", got)
	}

	valid := core.ToolOutcome{ToolID: "fake-model/v1", Status: core.OutcomeSucceeded, ObservedEffect: "fake-model: work"}
	got, available = (Verifier{}).Verify(task, valid)
	if !available || got.PostconditionStatus != core.PostconditionVerified {
		t.Fatalf("registered deterministic check failed: %+v", got)
	}
	valid.ObservedEffect = "different"
	got, available = (Verifier{}).Verify(task, valid)
	if !available || got.PostconditionStatus != core.PostconditionNotChecked {
		t.Fatalf("mismatched result was verified: %+v", got)
	}

	reviewCandidate := core.ToolOutcome{ToolID: "fake-review-model/v1", Status: core.OutcomeSucceeded, ObservedEffect: "fake-review-model: work"}
	got, available = (Verifier{}).Verify(task, reviewCandidate)
	if available || got.PostconditionStatus != core.PostconditionNotChecked {
		t.Fatalf("release-test review candidate gained a deterministic verifier: %+v", got)
	}
}

func TestPersistedAgentVerificationBindsExactExecutionInput(t *testing.T) {
	t.Parallel()

	const input = "runtime-selected execution input"
	task := core.Task{ID: "task-1", ExecutionKind: core.ExecutionAgent}
	outcome := core.ToolOutcome{ToolID: "fake-model/v1", Status: core.OutcomeSucceeded, ObservedEffect: "fake-model: " + input, PostconditionStatus: core.PostconditionVerified}
	got, available := core.VerifyPersistedPostcondition(task, outcome, core.FingerprintExecutionInput(input))
	if !available || got.PostconditionStatus != core.PostconditionVerified {
		t.Fatalf("exact persisted execution input was not verified: %+v", got)
	}
	outcome.ObservedEffect = "fake-model: substituted input"
	got, available = core.VerifyPersistedPostcondition(task, outcome, core.FingerprintExecutionInput(input))
	if !available || got.PostconditionStatus != core.PostconditionNotChecked {
		t.Fatalf("substituted persisted execution input retained verification: %+v", got)
	}
}

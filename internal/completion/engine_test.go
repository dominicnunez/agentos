package completion

import (
	"github.com/dominicnunez/agentos/internal/core"
	"testing"
)

func TestCompletionRequiresVerification(t *testing.T) {
	c := core.CompletionContract{TaskID: "t", TaskVersion: 1, Criteria: []core.CompletionCriterion{{ID: "c", Assurance: core.AssuranceDeterministic, Required: true}}}
	if (Engine{}).Evaluate(c, core.ToolOutcome{Status: core.OutcomeSucceeded}).Complete {
		t.Fatal("unverified outcome completed")
	}
	if !(Engine{}).Evaluate(c, core.ToolOutcome{Status: core.OutcomeSucceeded, PostconditionStatus: core.PostconditionVerified}).Complete {
		t.Fatal("verified success did not complete")
	}
}

func TestHumanJudgmentDoesNotMasqueradeAsDeterministicEvidence(t *testing.T) {
	t.Parallel()
	contract := core.CompletionContract{TaskID: "t", TaskVersion: 1, Criteria: []core.CompletionCriterion{{ID: "review", Assurance: core.AssuranceHumanJudgment, Required: true}}}
	outcome := core.ToolOutcome{Status: core.OutcomeSucceeded, PostconditionStatus: core.PostconditionNotChecked}
	if result := (Engine{}).Evaluate(contract, outcome); result.Complete || len(result.Reasons) == 0 {
		t.Fatalf("unreviewed judgment completed: %+v", result)
	}
	if result := (Engine{}).EvaluateHuman(contract, outcome, true); !result.Complete {
		t.Fatalf("approved human judgment was rejected: %+v", result)
	}
	if result := (Engine{}).EvaluateHuman(contract, outcome, false); result.Complete || len(result.Reasons) == 0 {
		t.Fatalf("rejected human judgment completed: %+v", result)
	}
	if outcome.PostconditionStatus != core.PostconditionNotChecked {
		t.Fatalf("human judgment rewrote runtime evidence: %+v", outcome)
	}
}

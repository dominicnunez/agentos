package completion

import (
	"github.com/dominicnunez/agentos/internal/core"
	"testing"
)

func TestEvaluateRequiresVerifiedSuccess(t *testing.T) {
	c := core.CompletionContract{TaskID: "t", TaskVersion: 1, Criteria: []core.CompletionCriterion{{ID: "c", Assurance: core.AssuranceDeterministic, Required: true}}}
	if (Engine{}).Evaluate(c, core.ToolOutcome{Status: core.OutcomeSucceeded}).Complete {
		t.Fatal("unverified outcome completed")
	}
	if !(Engine{}).Evaluate(c, core.ToolOutcome{Status: core.OutcomeSucceeded, PostconditionStatus: core.PostconditionVerified}).Complete {
		t.Fatal("verified success did not complete")
	}
}

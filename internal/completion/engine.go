package completion

import "github.com/dominicnunez/agentos/internal/core"

type Result struct {
	Complete bool     `json:"complete"`
	Reasons  []string `json:"reasons,omitempty"`
}
type Engine struct{}

func (Engine) Evaluate(c core.CompletionContract, o core.ToolOutcome) Result {
	var reasons []string
	if o.Status != core.OutcomeSucceeded {
		reasons = append(reasons, "tool outcome did not succeed")
	}
	if o.PostconditionStatus != core.PostconditionVerified {
		reasons = append(reasons, "postcondition is not verified")
	}
	for _, criterion := range c.Criteria {
		if criterion.Required && criterion.Assurance != core.AssuranceDeterministic {
			reasons = append(reasons, "unsupported assurance for criterion "+criterion.ID)
		}
	}
	return Result{Complete: len(reasons) == 0, Reasons: reasons}
}

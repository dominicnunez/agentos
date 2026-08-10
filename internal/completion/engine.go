package completion

import "github.com/dominicnunez/agentos/internal/core"

type Result struct {
	Complete bool     `json:"complete"`
	Reasons  []string `json:"reasons,omitempty"`
}
type Engine struct{}

func (Engine) Evaluate(c core.CompletionContract, o core.ToolOutcome) Result {
	return evaluate(c, o, nil)
}

// EvaluateHuman applies an authenticated human judgment without rewriting the
// ToolOutcome into deterministic evidence.
func (Engine) EvaluateHuman(c core.CompletionContract, o core.ToolOutcome, approved bool) Result {
	return evaluate(c, o, &approved)
}

func evaluate(c core.CompletionContract, o core.ToolOutcome, humanApproved *bool) Result {
	var reasons []string
	if o.Status != core.OutcomeSucceeded {
		reasons = append(reasons, "tool outcome did not succeed")
	}
	for _, criterion := range c.Criteria {
		if !criterion.Required {
			continue
		}
		switch criterion.Assurance {
		case core.AssuranceDeterministic:
			if o.PostconditionStatus != core.PostconditionVerified {
				reasons = append(reasons, "postcondition is not verified for criterion "+criterion.ID)
			}
		case core.AssuranceHumanJudgment:
			if humanApproved == nil {
				reasons = append(reasons, "human judgment is required for criterion "+criterion.ID)
			} else if !*humanApproved {
				reasons = append(reasons, "human judgment rejected criterion "+criterion.ID)
			}
		default:
			reasons = append(reasons, "unsupported assurance for criterion "+criterion.ID)
		}
	}
	return Result{Complete: len(reasons) == 0, Reasons: reasons}
}

package completion

import "github.com/dominicnunez/agentos/internal/core"

type Result = core.CompletionResult
type Engine struct{}

func (Engine) Evaluate(c core.CompletionContract, o core.ToolOutcome) Result {
	return core.EvaluateCompletion(c, o, nil)
}

// EvaluateHuman applies an authenticated human judgment without rewriting the
// ToolOutcome into deterministic evidence.
func (Engine) EvaluateHuman(c core.CompletionContract, o core.ToolOutcome, approved bool) Result {
	return core.EvaluateCompletion(c, o, &approved)
}

// EvaluateHumanTask validates required structured fields and artifact evidence.
// It checks contract satisfaction, not the truth of a user's statements.
func (Engine) EvaluateHumanTask(c core.CompletionContract, submission core.HumanTaskSubmission) Result {
	return core.EvaluateHumanTaskCompletion(c, submission)
}

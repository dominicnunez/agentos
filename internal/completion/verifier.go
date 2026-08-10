package completion

import (
	"strings"

	"github.com/dominicnunez/agentos/internal/core"
)

// Verifier is the runtime-owned V1 postcondition boundary. Handler claims are
// discarded; only outcomes with a registered deterministic check can become
// verified. The second return value reports whether the runtime had such a
// check, so callers can distinguish failed verification from work that needs
// independent judgment.
type Verifier struct{}

func (Verifier) Verify(task core.Task, outcome core.ToolOutcome) (core.ToolOutcome, bool) {
	outcome.PostconditionStatus = core.PostconditionNotChecked
	var verified bool
	switch outcome.ToolID {
	case "builtin.echo":
		if outcome.Status != core.OutcomeSucceeded {
			return outcome, true
		}
		value, ok := outcome.ObservedEffect.(string)
		verified = ok && task.ExecutionKind == core.ExecutionDeterministic && strings.HasPrefix(task.Description, "echo ") && value == strings.TrimPrefix(task.Description, "echo ")
	case "fake-model/v1":
		if outcome.Status != core.OutcomeSucceeded {
			return outcome, true
		}
		value, ok := outcome.ObservedEffect.(string)
		verified = ok && task.ExecutionKind == core.ExecutionAgent && value == "fake-model: "+task.Description
	default:
		return outcome, false
	}
	if verified {
		outcome.PostconditionStatus = core.PostconditionVerified
	}
	return outcome, true
}

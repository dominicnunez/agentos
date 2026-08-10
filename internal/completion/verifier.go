package completion

import (
	"strings"

	"github.com/dominicnunez/agentos/internal/core"
)

// Verifier is the runtime-owned V1 postcondition boundary. Handler claims are
// discarded; only outcomes with a registered deterministic check can become
// verified.
type Verifier struct{}

func (Verifier) Verify(task core.Task, outcome core.ToolOutcome) core.ToolOutcome {
	outcome.PostconditionStatus = core.PostconditionNotChecked
	if outcome.Status != core.OutcomeSucceeded {
		return outcome
	}
	var verified bool
	switch outcome.ToolID {
	case "builtin.echo":
		value, ok := outcome.ObservedEffect.(string)
		verified = ok && task.ExecutionKind == core.ExecutionDeterministic && strings.HasPrefix(task.Description, "echo ") && value == strings.TrimPrefix(task.Description, "echo ")
	case "fake-model/v1":
		value, ok := outcome.ObservedEffect.(string)
		verified = ok && task.ExecutionKind == core.ExecutionAgent && value == "fake-model: "+task.Description
	}
	if verified {
		outcome.PostconditionStatus = core.PostconditionVerified
	}
	return outcome
}

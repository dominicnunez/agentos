package completion

import "github.com/dominicnunez/agentos/internal/core"

// Verifier is the runtime-owned V1 postcondition boundary. Handler claims are
// discarded; only outcomes with a registered deterministic check can become
// verified. The second return value reports whether the runtime had such a
// check, so callers can distinguish failed verification from work that needs
// independent judgment.
type Verifier struct{}

func (Verifier) Verify(task core.Task, outcome core.ToolOutcome) (core.ToolOutcome, bool) {
	return core.VerifyRegisteredPostcondition(task, outcome)
}

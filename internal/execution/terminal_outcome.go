package execution

import (
	"errors"

	"github.com/dominicnunez/agentos/internal/events"
)

// TerminalOutcome is accounting evidence, never successful model output or
// permission to retry. Missing usage must retain conservative reservation charges.
type TerminalOutcome struct {
	Status string
	Usage  *events.InferenceUsageRecordedPayload
}

type terminalResponseError struct{ outcome TerminalOutcome }

func (e *terminalResponseError) Error() string { return "model returned terminal " + e.outcome.Status }

// TerminalResponseOutcome returns a copy of adapter-validated terminal evidence.
// Raw error strings and caller-supplied response text cannot create this evidence.
func TerminalResponseOutcome(err error) (TerminalOutcome, bool) {
	var terminal *terminalResponseError
	if !errors.As(err, &terminal) {
		return TerminalOutcome{}, false
	}
	outcome := terminal.outcome
	if outcome.Usage != nil {
		usage := *outcome.Usage
		outcome.Usage = &usage
	}
	return outcome, true
}

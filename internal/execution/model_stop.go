package execution

// RemoteStopStatus records only provider-backed remote stop knowledge.
type RemoteStopStatus string

const RemoteStopUncertain RemoteStopStatus = "UNCERTAIN"

// ModelStopOutcome separates local turn and owned-process termination from
// handler return and remote provider work. LocalTurnStopped means the owned
// local turn cannot continue; it does not say that its caller has returned.
// A local cancellation cannot prove that remote work stopped.
type ModelStopOutcome struct {
	LocalTurnStopped          bool             `json:"local_turn_stopped"`
	LocalProcessStopAttempted bool             `json:"local_process_stop_attempted"`
	LocalProcessStopped       bool             `json:"local_process_stopped"`
	RemoteStatus              RemoteStopStatus `json:"remote_status"`
}

type modelStopCarrier interface {
	modelStopOutcome() ModelStopOutcome
}

type modelStopError struct {
	cause   error
	outcome ModelStopOutcome
}

func (e *modelStopError) Error() string                      { return e.cause.Error() }
func (e *modelStopError) Unwrap() error                      { return e.cause }
func (e *modelStopError) modelStopOutcome() ModelStopOutcome { return e.outcome }

func (o ModelStopOutcome) valid() bool {
	if o.RemoteStatus != RemoteStopUncertain {
		return false
	}
	return !o.LocalProcessStopped || o.LocalProcessStopAttempted && o.LocalTurnStopped
}

func withModelStopOutcome(cause error, outcome ModelStopOutcome) error {
	if cause == nil || !outcome.valid() {
		return cause
	}
	return &modelStopError{cause: cause, outcome: outcome}
}

// StopOutcome returns trusted provider stop evidence attached to an error.
func StopOutcome(err error) (ModelStopOutcome, bool) {
	if carrier, ok := err.(modelStopCarrier); ok {
		outcome := carrier.modelStopOutcome()
		if outcome.valid() {
			return outcome, true
		}
	}
	if wrapped, ok := err.(interface{ Unwrap() []error }); ok {
		for _, cause := range wrapped.Unwrap() {
			if outcome, ok := StopOutcome(cause); ok {
				return outcome, true
			}
		}
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return StopOutcome(wrapped.Unwrap())
	}
	return ModelStopOutcome{}, false
}

package execution

import (
	"context"
	"errors"

	"github.com/dominicnunez/agentos/internal/core"
)

// ModelFaultCode is a runtime-owned failure category, never provider text.
type ModelFaultCode string

const (
	ModelCallFailed       ModelFaultCode = "provider_failure"
	ModelContractFailed   ModelFaultCode = "provider_contract"
	InferenceDenied       ModelFaultCode = "inference_authorization"
	InferenceRecordFailed ModelFaultCode = "inference_accounting"
)

type modelFault struct {
	code                   ModelFaultCode
	cancelled              bool
	deadline               bool
	frozen                 bool
	containmentUnavailable bool
	executionStopped       bool
	hold                   *core.SecurityHoldCause
	stop                   *ModelStopOutcome
}

func (f *modelFault) Error() string {
	switch f.code {
	case InferenceDenied:
		return "model inference was not authorized; check the configured inference policy and available budget"
	case InferenceRecordFailed:
		return "model inference accounting could not be confirmed"
	case ModelContractFailed:
		return "model response violated its runtime contract"
	case ModelCallFailed:
		return "model provider request failed"
	default:
		return "model provider request failed"
	}
}

func (f *modelFault) Is(target error) bool {
	return target == context.Canceled && f.cancelled || target == context.DeadlineExceeded && f.deadline || target == core.ErrOrganizationFrozen && f.frozen || target == core.ErrContainmentUnavailable && f.containmentUnavailable || target == core.ErrExecutionStopped && f.executionStopped
}

// As exposes only the runtime-owned hold reference, never provider diagnostics.
func (f *modelFault) As(target any) bool {
	hold, ok := target.(*core.SecurityHoldCause)
	if !ok || f.hold == nil {
		return false
	}
	*hold = *f.hold
	return true
}

func (f *modelFault) modelStopOutcome() ModelStopOutcome {
	if f.stop == nil {
		return ModelStopOutcome{}
	}
	return *f.stop
}

// SafeModelError discards diagnostic text and the original error chain before
// errors cross into work results, public responses, or durable evidence. Only
// closed failure categories, hold references, cancellation/pre-send facts and
// trusted provider stop evidence survive. In particular, cancellation is not
// evidence that a request was never sent or that remote work stopped.
func SafeModelError(code ModelFaultCode, cause error) error {
	if cause == nil {
		return nil
	}
	var prior *modelFault
	if code == ModelCallFailed && errors.As(cause, &prior) {
		code = prior.code
	}
	switch code {
	case ModelCallFailed, ModelContractFailed, InferenceDenied, InferenceRecordFailed:
	default:
		code = ModelCallFailed
	}
	fault := &modelFault{
		code: code, cancelled: errors.Is(cause, context.Canceled),
		deadline:               errors.Is(cause, context.DeadlineExceeded),
		frozen:                 errors.Is(cause, core.ErrOrganizationFrozen),
		containmentUnavailable: errors.Is(cause, core.ErrContainmentUnavailable),
		executionStopped:       errors.Is(cause, core.ErrExecutionStopped),
	}
	var hold core.SecurityHoldCause
	if errors.As(cause, &hold) && hold.OrganizationID != "" && hold.EventRef != "" && hold.Sequence > 0 {
		fault.hold = &hold
	}
	if stop, ok := StopOutcome(cause); ok {
		fault.stop = &stop
	}
	if WasRequestNotSent(cause) {
		return RequestNotSent(fault)
	}
	return fault
}

// ModelErrorClass returns only a runtime-owned category suitable for evidence.
func ModelErrorClass(err error) string {
	var fault *modelFault
	if errors.As(err, &fault) {
		return string(fault.code)
	}
	return string(ModelCallFailed)
}

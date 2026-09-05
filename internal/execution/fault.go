package execution

import (
	"context"
	"errors"
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
	code      ModelFaultCode
	cancelled bool
	deadline  bool
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
	return target == context.Canceled && f.cancelled || target == context.DeadlineExceeded && f.deadline
}

// SafeModelError discards diagnostic text and the original error chain before
// errors cross into work results, public responses, or durable evidence. Only
// closed failure categories and cancellation/pre-send facts survive. In
// particular, cancellation is not evidence that a request was never sent.
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
		deadline: errors.Is(cause, context.DeadlineExceeded),
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

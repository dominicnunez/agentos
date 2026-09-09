package execution

import (
	"errors"

	"github.com/dominicnunez/agentos/internal/events"
)

// reconciledUsageError carries accounting evidence without carrying model output.
type reconciledUsageError struct {
	cause error
	usage events.InferenceUsageRecordedPayload
}

func (e reconciledUsageError) Error() string { return e.cause.Error() }
func (e reconciledUsageError) Unwrap() error { return e.cause }

// WithReconciledUsage is used by the durable inference guard after successful
// accounting when containment prevents delivery of an otherwise valid response.
func WithReconciledUsage(cause error, usage events.InferenceUsageRecordedPayload) error {
	if cause == nil || !usage.Valid() {
		return cause
	}
	return reconciledUsageError{cause: cause, usage: cloneReconciledUsage(usage)}
}

// ReconciledUsage returns a detached copy of accounting evidence on a failed call.
func ReconciledUsage(err error) (events.InferenceUsageRecordedPayload, bool) {
	var recorded reconciledUsageError
	if !errors.As(err, &recorded) {
		return events.InferenceUsageRecordedPayload{}, false
	}
	return cloneReconciledUsage(recorded.usage), true
}

func cloneReconciledUsage(usage events.InferenceUsageRecordedPayload) events.InferenceUsageRecordedPayload {
	if usage.CostUSD != nil {
		cost := *usage.CostUSD
		usage.CostUSD = &cost
	}
	return usage
}

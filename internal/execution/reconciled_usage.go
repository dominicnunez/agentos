package execution

import "github.com/dominicnunez/agentos/internal/events"

// WithReconciledUsage attaches durable accounting evidence without model output.
func WithReconciledUsage(cause error, usage events.InferenceUsageRecordedPayload) error {
	return events.WithReconciledUsage(cause, usage)
}

// ReconciledUsage returns a detached copy of accounting evidence on a failed call.
func ReconciledUsage(err error) (events.InferenceUsageRecordedPayload, bool) {
	return events.ReconciledUsage(err)
}

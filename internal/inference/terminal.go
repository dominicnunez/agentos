package inference

import (
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
)

// terminalReconciliation checks evidence against the actual admission. It never
// grants retry permission or returns successful model output.
func terminalReconciliation(terminal execution.TerminalOutcome, reservation Reservation) (Reconciliation, *events.InferenceUsageRecordedPayload) {
	var result Reconciliation
	switch terminal.Status {
	case "failed":
		result = ReconciliationTerminalFailed
		if terminal.Usage == nil {
			return ReconciliationTerminalFailedNoUsage, nil
		}
	case "incomplete":
		result = ReconciliationTerminalIncomplete
		if terminal.Usage == nil {
			return ReconciliationTerminalIncompleteNoUsage, nil
		}
	default:
		return ReconciliationUncertain, nil
	}
	usage := *terminal.Usage
	usage.ConnectionID = reservation.Request.ConnectionID
	if !usage.Valid() || usage.Provider != reservation.Request.Descriptor.Provider || usage.Model != reservation.Request.Descriptor.Model || int64(usage.InputTokens) > reservation.ReservedInputTokens || int64(usage.OutputTokens) > reservation.ReservedOutputTokens {
		return ReconciliationViolation, &usage
	}
	return result, &usage
}

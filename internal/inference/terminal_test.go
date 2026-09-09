package inference

import (
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
)

func TestTerminalReconciliationEnforcesAdmission(t *testing.T) {
	reservation := Reservation{Request: InferenceRequest{ConnectionID: "account-a", Descriptor: execution.ModelDescriptor{Provider: "provider", Model: "model"}}, ReservedInputTokens: 100, ReservedOutputTokens: 20}
	for _, status := range []string{"failed", "incomplete"} {
		for _, tc := range []struct {
			name   string
			mutate func(*events.InferenceUsageRecordedPayload)
			want   Reconciliation
		}{
			{"valid", func(*events.InferenceUsageRecordedPayload) {}, ""},
			{"provider", func(u *events.InferenceUsageRecordedPayload) { u.Provider = "other" }, ReconciliationViolation},
			{"model", func(u *events.InferenceUsageRecordedPayload) { u.Model = "other" }, ReconciliationViolation},
			{"negative", func(u *events.InferenceUsageRecordedPayload) { u.InputTokens = -1; u.TotalTokens = 4 }, ReconciliationViolation},
			{"input limit", func(u *events.InferenceUsageRecordedPayload) { u.InputTokens = 101; u.TotalTokens = 106 }, ReconciliationViolation},
			{"output limit", func(u *events.InferenceUsageRecordedPayload) { u.OutputTokens = 21; u.TotalTokens = 31 }, ReconciliationViolation},
			{"sum", func(u *events.InferenceUsageRecordedPayload) { u.TotalTokens = 999 }, ReconciliationViolation},
		} {
			t.Run(status+"/"+tc.name, func(t *testing.T) {
				usage := events.InferenceUsageRecordedPayload{Source: "provider_api", Provider: "provider", Model: "model", ConnectionID: "provider-supplied-account", InputTokens: 10, OutputTokens: 5, TotalTokens: 15}
				tc.mutate(&usage)
				result, observed := terminalReconciliation(execution.TerminalOutcome{Status: status, Usage: &usage}, reservation)
				want := tc.want
				if want == "" {
					want = ReconciliationTerminalFailed
					if status == "incomplete" {
						want = ReconciliationTerminalIncomplete
					}
				}
				if result != want || observed == nil || observed.ConnectionID != "account-a" {
					t.Fatalf("result=%s usage=%+v", result, observed)
				}
				if usage.ConnectionID != "provider-supplied-account" {
					t.Fatal("mutated adapter evidence")
				}
			})
		}
	}
	for status, want := range map[string]Reconciliation{"failed": ReconciliationTerminalFailedNoUsage, "incomplete": ReconciliationTerminalIncompleteNoUsage, "unknown": ReconciliationUncertain} {
		result, usage := terminalReconciliation(execution.TerminalOutcome{Status: status}, reservation)
		if result != want || usage != nil {
			t.Fatalf("missing usage: %s %+v", result, usage)
		}
	}
}

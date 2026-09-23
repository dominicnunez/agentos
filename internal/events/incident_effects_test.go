package events

import (
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestIncidentEffectRequiresPending(t *testing.T) {
	pending := core.EffectObligation{ID: "effect", OrganizationID: "org", TaskID: "task", ActorID: "actor", Action: "send", Resource: "destination", Scope: "org", IdempotencyKey: "key", EffectFingerprint: "legacy", AuthorizationRefs: []string{"lease"}, Status: core.EffectPending}
	if err := ValidateIncidentEffect(pending, core.EffectObligation{}, true); err != nil {
		t.Fatal(err)
	}
	for _, status := range []core.EffectStatus{core.EffectAttempted, core.EffectConfirmed, core.EffectFailed, core.EffectCancelled} {
		t.Run(string(status), func(t *testing.T) {
			value := pending
			value.Status = status
			if status != core.EffectCancelled {
				value.AttemptCount = 1
			}
			if status == core.EffectConfirmed {
				value.ConfirmationEvidenceRefs = []string{"receipt"}
			}
			if status == core.EffectFailed {
				now := time.Now().UTC()
				value.ReconciledAt = &now
				value.ReconciliationEvidenceRefs = []string{"destination-check"}
			}
			if err := ValidateIncidentEffect(value, core.EffectObligation{}, true); err == nil {
				t.Fatal("accepted effect history without initial pending intent")
			}
		})
	}
	attempt := pending
	attempt.Status, attempt.AttemptCount = core.EffectAttempted, 1
	if err := ValidateIncidentEffect(attempt, pending, false); err != nil {
		t.Fatal(err)
	}
	repeated := attempt
	repeated.AttemptCount++
	if err := ValidateIncidentEffect(repeated, attempt, false); err == nil {
		t.Fatal("accepted repeated attempt")
	}
}

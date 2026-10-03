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
	now := time.Now().UTC()
	attempt.LastAttemptAt = &now
	if err := ValidateIncidentEffect(attempt, pending, false); err != nil {
		t.Fatal(err)
	}
	repeated := attempt
	repeated.AttemptCount++
	if err := ValidateIncidentEffect(repeated, attempt, false); err == nil {
		t.Fatal("accepted repeated attempt")
	}
}

func TestIncidentEffectAttemptTime(t *testing.T) {
	now := time.Now().UTC()
	zero := time.Time{}
	pending := core.EffectObligation{ID: "effect", OrganizationID: "org", TaskID: "task", ActorID: "actor", Action: "send", Resource: "destination", Scope: "org", IdempotencyKey: "key", EffectFingerprint: "legacy", AuthorizationRefs: []string{"lease"}, Status: core.EffectPending}
	for _, status := range []core.EffectStatus{core.EffectPending, core.EffectAttempted, core.EffectConfirmed, core.EffectFailed, core.EffectCancelled} {
		for _, timestamp := range []struct {
			name  string
			value *time.Time
		}{{"missing", nil}, {"zero", &zero}, {"present", &now}} {
			t.Run(string(status)+"/"+timestamp.name, func(t *testing.T) {
				value, previous := pending, pending
				value.Status, value.LastAttemptAt = status, timestamp.value
				attempted := status == core.EffectAttempted || status == core.EffectConfirmed || status == core.EffectFailed
				if attempted {
					value.AttemptCount = 1
				}
				if status == core.EffectConfirmed || status == core.EffectFailed {
					previous.Status, previous.AttemptCount, previous.LastAttemptAt = core.EffectAttempted, 1, timestamp.value
					if status == core.EffectConfirmed {
						value.ConfirmationEvidenceRefs = []string{"receipt"}
					} else {
						value.ReconciledAt, value.ReconciliationEvidenceRefs = &now, []string{"destination-check"}
					}
				}
				err := ValidateIncidentEffect(value, previous, status == core.EffectPending)
				valid := attempted && timestamp.name == "present" || !attempted && timestamp.name == "missing"
				if (err == nil) != valid {
					t.Fatalf("timestamp validity = %v, want %v: %v", err == nil, valid, err)
				}
			})
		}
	}
}

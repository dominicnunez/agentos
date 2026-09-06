package ledger

import (
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestInferenceConnectionPolicyLifetime(t *testing.T) {
	history := []inferencePolicyRevision{{fingerprint: "first", sequence: 10}, {fingerprint: "second", sequence: 20}}
	for _, test := range []struct {
		name, fingerprint           string
		reservation, reconciliation int64
		valid                       bool
	}{
		{"before activation", "first", 9, 0, false},
		{"same event", "first", 10, 0, false},
		{"unknown revision", "foreign", 12, 15, false},
		{"retired revision", "first", 21, 0, false},
		{"outstanding on replacement", "first", 12, 0, false},
		{"reconciled after replacement", "first", 12, 22, false},
		{"reconciled at replacement", "first", 12, 20, false},
		{"valid historical use", "first", 12, 19, true},
		{"valid current use", "second", 21, 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var reconciliations []events.Event
			if test.reconciliation != 0 {
				reconciliations = []events.Event{{Sequence: test.reconciliation}}
			}
			err := validateConnectionPolicyLifetime(history, test.fingerprint, test.reservation, reconciliations)
			if (err == nil) != test.valid {
				t.Fatalf("valid=%t err=%v", test.valid, err)
			}
		})
	}
}

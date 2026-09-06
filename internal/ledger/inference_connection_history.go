package ledger

import (
	"fmt"
	"sort"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
)

type inferencePolicyRevision struct {
	fingerprint  string
	sequence     int64
	authorizedAt time.Time
}

// History is indexed from exact policy activation events and sorted by ledger
// sequence. Wall-clock ordering cannot substitute for durable event ordering.
func validateConnectionPolicyLifetime(history []inferencePolicyRevision, fingerprint string, reservedAt int64, reconciliations []events.Event) error {
	index := sort.Search(len(history), func(i int) bool { return history[i].sequence >= reservedAt })
	if reservedAt < 1 || index == 0 || history[index-1].fingerprint != fingerprint {
		return fmt.Errorf("inference connection policy was not active at reservation")
	}
	if index < len(history) {
		if len(reconciliations) != 1 || reconciliations[0].Sequence <= reservedAt || reconciliations[0].Sequence >= history[index].sequence {
			return fmt.Errorf("inference connection policy changed while a reservation was outstanding")
		}
	}
	return nil
}

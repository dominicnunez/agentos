package ledger

import "testing"

// Limit simultaneous large SQLite fixtures independently of the host's CPU
// count. Each caller owns its database; its child cases remain sequential.
var incidentTestSlots = make(chan struct{}, 2)

// Call only at the beginning of an isolated root test, never from a child of
// another slot holder. Fixture cleanups registered afterward finish first.
func parallelIncidentTest(t *testing.T) {
	t.Helper()
	t.Parallel()
	incidentTestSlots <- struct{}{}
	t.Cleanup(func() { <-incidentTestSlots })
}

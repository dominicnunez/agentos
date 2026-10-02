package ledger

import (
	"sync"
	"testing"
	"time"
)

// Limit simultaneous large SQLite fixtures independently of the host's CPU
// count. Each caller owns its database; its child cases remain sequential.
var incidentTestSlots = struct {
	sync.Mutex
	changed                *sync.Cond
	active, waitingWorkers int
}{}

func init() {
	incidentTestSlots.changed = sync.NewCond(&incidentTestSlots.Mutex)
}

// Waiting differential workers enter before another ordinary fixture. This
// avoids putting the longest streams at the end of the parallel test phase.
// No caller waits for a worker that has not requested a slot.
func acquireIncidentTest(t *testing.T, worker bool) {
	t.Helper()
	started := time.Now()
	incidentTestSlots.Lock()
	if worker {
		incidentTestSlots.waitingWorkers++
	}
	for incidentTestSlots.active >= 3 || (!worker && incidentTestSlots.waitingWorkers > 0) {
		incidentTestSlots.changed.Wait()
	}
	if worker {
		incidentTestSlots.waitingWorkers--
	}
	incidentTestSlots.active++
	incidentTestSlots.changed.Broadcast()
	incidentTestSlots.Unlock()
	t.Logf("incident fixture slot acquired after %s", time.Since(started))
	t.Cleanup(func() {
		incidentTestSlots.Lock()
		incidentTestSlots.active--
		incidentTestSlots.changed.Broadcast()
		incidentTestSlots.Unlock()
	})
}

// Call only at the beginning of an isolated root test, never from a child of
// another slot holder. Fixture cleanups registered afterward finish first.
func parallelIncidentTest(t *testing.T) {
	t.Helper()
	t.Parallel()
	acquireIncidentTest(t, false)
}

// External ledger tests share the same fixture limit and root-only contract.
func ParallelIncidentTestForTest(t *testing.T) {
	t.Helper()
	parallelIncidentTest(t)
}

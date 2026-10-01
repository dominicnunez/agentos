package ledger

import (
	"reflect"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentNumericRecordPreflight(t *testing.T) {
	parallelIncidentTest(t)
	for _, kind := range []string{"task", "organization_freeze", "capability_lease", "effect"} {
		t.Run(kind, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			correlation := "stop-work"
			switch kind {
			case "task":
				incidentTestExecution(t, store)
			case "organization_freeze":
				modelStopManifest(t, store, false, "first")
				appendHistoricalInferenceFreeze(t, store, "org-1", 1, true)
				appendInferenceFreeze(t, store, "org-1", 2, false)
				correlation = "model-stop"
			case "capability_lease":
				appendTaskProjectionParents(t, t.Context(), store, "org-1", "setup", "work-1")
				appendFactualInferenceKnowledge(t, store, "fact", "Verified fact", "A bounded observation.")
				correlation = "knowledge-fact"
			default:
				incidentEffectFixture(t, store)
			}
			_, err = store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
			if err != nil {
				t.Fatalf("healthy writer baseline: %v", err)
			}
			full, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			leases, freezes, err := authorityAdmissionsSnapshot(t.Context(), store.db)
			if err != nil {
				t.Fatalf("healthy full authority owner: %v", err)
			}
			if _, err := events.ValidateProjectionHistory(full, nil, leases, freezes); err != nil {
				t.Fatalf("healthy full projection owner: %v", err)
			}
			// SQLite INTEGER affinity permits corrupt nonnumeric TEXT in a
			// composite record key. The full stored value must be bounded before
			// database/sql attempts numeric conversion (and embeds it in errors).
			if _, err := store.db.ExecContext(t.Context(), `UPDATE records SET version=? WHERE kind=? AND version=(SELECT MAX(version) FROM records WHERE kind=?)`, strings.Repeat("x", 33<<20), kind, kind); err != nil {
				t.Fatal(err)
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
			if err == nil {
				t.Fatal("oversized stored numeric record accepted")
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("numeric corruption returned partial snapshot")
			}
			if len(err.Error()) > 4096 || strings.Contains(err.Error(), "converting driver.Value") {
				t.Fatalf("numeric value reached Scan before bounded rejection: error bytes=%d prefix=%.160s", len(err.Error()), err.Error())
			}
			if !strings.Contains(err.Error(), "bounded") && !strings.Contains(err.Error(), "support limit") && !strings.Contains(err.Error(), "exceeds limit") {
				t.Fatalf("expected bounded preflight rejection: %v", err)
			}
		})
	}
}

package ledger_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
	ledgerrecovery "github.com/dominicnunez/agentos/internal/ledger/recovery"
)

func TestIncidentKnowledgeRecordOwner(t *testing.T) {
	ledger.ParallelIncidentTestForTest(t)
	for _, family := range []string{"agent-proposal", "deterministic-validation"} {
		for _, opaque := range []bool{false, true} {
			mode := "admitted"
			if opaque {
				mode = "opaque"
			}
			t.Run(family+"/"+mode, func(t *testing.T) {
				path := ledger.IncidentKnowledgeOwnerFixtureForTest(t, family, opaque)
				_, recoveryErr := ledgerrecovery.Verify(t.Context(), path)
				if opaque {
					if recoveryErr != nil {
						t.Fatalf("opaque generic control: %v", recoveryErr)
					}
				} else if recoveryErr == nil || !strings.Contains(recoveryErr.Error(), "carries projection authority") {
					t.Fatalf("storage owner rejection: %v", recoveryErr)
				}
				store, err := ledger.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = store.Close() }()
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-2", "selected", 256)
				if opaque {
					if err != nil {
						t.Fatalf("opaque generic incident: %v", err)
					}
				} else {
					if err == nil {
						t.Fatal("admitted generic consumer hid raw evidence")
					}
					if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
						t.Fatal("invalid evidence returned partial snapshot")
					}
				}
			})
		}
	}
}

func TestIncidentKnowledgeCreatorBacking(t *testing.T) {
	ledger.ParallelIncidentTestForTest(t)
	for _, duplicate := range []bool{false, true} {
		mode := "ordinary-runtime"
		if duplicate {
			mode = "duplicate-value"
		}
		t.Run(mode, func(t *testing.T) {
			path := ledger.IncidentKnowledgeCreatorFixtureForTest(t, duplicate)
			_, recoveryErr := ledgerrecovery.Verify(t.Context(), path)
			if (recoveryErr != nil) != duplicate {
				t.Fatalf("storage owner applicability: %v", recoveryErr)
			}
			store, err := ledger.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-2", "selected", 256)
			if (err != nil) != duplicate {
				t.Fatalf("incident applicability: %v", err)
			}
			if duplicate && !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("invalid evidence returned partial snapshot")
			}
		})
	}
}

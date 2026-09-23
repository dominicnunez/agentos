package ledger

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentEffectAdmissionMetadata(t *testing.T) {
	for _, mutation := range []string{"none", "fingerprint", "matching-event", "large-fingerprint"} {
		t.Run(mutation, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "effects.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			attempt := incidentEffectFixture(t, store)
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256); err != nil {
				t.Fatalf("valid baseline: %v", err)
			}
			var admission, fingerprint string
			switch mutation {
			case "fingerprint":
				fingerprint = "unexpected"
			case "matching-event":
				if err := store.db.QueryRowContext(t.Context(), `SELECT event_id FROM events WHERE event_type='EFFECT_OBLIGATION_TRANSITIONED' AND json_extract(payload,'$.effect_obligation_id')=? ORDER BY sequence LIMIT 1`, attempt.ID).Scan(&admission); err != nil {
					t.Fatal(err)
				}
			case "large-fingerprint":
				fingerprint = strings.Repeat("x", 2<<20)
			}
			if _, err := store.db.ExecContext(t.Context(), `UPDATE records SET admission_event_id=?,admission_fingerprint=? WHERE kind='effect' AND record_id=? AND version=1`, admission, fingerprint, attempt.ID); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256)
			if mutation == "none" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil {
				t.Fatal("accepted projection authority on generic effect record")
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("rejected effect exposed a partial snapshot")
			}
			if mutation == "large-fingerprint" && !strings.Contains(err.Error(), "exceeds limit") {
				t.Fatalf("fingerprint bypassed byte preflight: %v", err)
			}
		})
	}
}

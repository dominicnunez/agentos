package ledger

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentEffectForeignScope(t *testing.T) {
	parallelIncidentTest(t)
	for _, channel := range []string{"all", "record", "envelope", "payload", "unrelated"} {
		t.Run(channel, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "effects.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			seedIncidentEffects(t, store, 2)
			baseline, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256)
			if err != nil || len(baseline.RelatedEvents) != 4 {
				t.Fatalf("valid two-effect baseline: %d related events, %v", len(baseline.RelatedEvents), err)
			}
			selectedTask := baseline.RelatedEvents[0].TaskID
			recordTask, envelopeTask, payloadTask := "foreign-task", "foreign-task", "foreign-task"
			if channel == "all" || channel == "record" {
				recordTask = selectedTask
			}
			if channel == "all" || channel == "envelope" {
				envelopeTask = selectedTask
			}
			if channel == "all" || channel == "payload" {
				payloadTask = selectedTask
			}
			// Move every revision of the second effect away from the selected
			// organization and Work correlation. Each identity channel must
			// independently discover a claim on the globally keyed selected Task.
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=CAST(json_set(body,'$.organization_id','foreign-org','$.task_id',?) AS BLOB) WHERE kind='effect' AND record_id='effect-001'`, recordTask); err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `UPDATE events SET organization_id='foreign-org',correlation_id='foreign-work',task_id=?,payload=CAST(json_set(payload,'$.organization_id','foreign-org','$.task_id',?) AS BLOB) WHERE event_type='EFFECT_OBLIGATION_TRANSITIONED' AND json_extract(payload,'$.effect_obligation_id')='effect-001'`, envelopeTask, payloadTask); err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
					return err
				}
				return rebuildEventIntegrity(t.Context(), tx)
			}); err != nil {
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
			if channel == "unrelated" {
				if err != nil || len(snapshot.RelatedEvents) != 2 {
					t.Fatalf("unrelated foreign Task affected incident: %d related events, %v", len(snapshot.RelatedEvents), err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "incident effect crosses") {
				t.Fatalf("expected invalid effect scope, got %v", err)
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("returned partial incident evidence")
			}
		})
	}
}

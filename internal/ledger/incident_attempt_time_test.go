package ledger

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentEffectAttemptTime(t *testing.T) {
	for _, terminal := range []core.EffectStatus{core.EffectAttempted, core.EffectConfirmed, core.EffectFailed} {
		for _, mutation := range []string{"none", "missing", "zero", "terminal-changed"} {
			if terminal == core.EffectAttempted && mutation == "terminal-changed" {
				continue
			}
			t.Run(string(terminal)+"/"+mutation, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "effects.db")
				store, err := Open(path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				attempt := incidentEffectFixture(t, store)
				last := attempt
				if terminal != core.EffectAttempted {
					last.Status = terminal
					refs := []string{"receipt"}
					if terminal == core.EffectConfirmed {
						last.ConfirmationEvidenceRefs = refs
					} else {
						now := time.Now().UTC()
						last.ReconciledAt, last.ReconciliationEvidenceRefs = &now, refs
					}
					if err := store.AppendRecord(t.Context(), "org-1", "EFFECT_OBLIGATION_TRANSITIONED", "", string(last.TaskID), last.AuthorizationRefs, refs, "effect", string(last.ID), 3, last); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256); err != nil {
					t.Fatalf("valid baseline: %v", err)
				}
				if mutation != "none" {
					if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
						values := []core.EffectObligation{attempt}
						if terminal != core.EffectAttempted {
							values = append(values, last)
						}
						for index, value := range values {
							switch mutation {
							case "missing":
								value.LastAttemptAt = nil
							case "zero":
								value.LastAttemptAt = &time.Time{}
							case "terminal-changed":
								if index == 0 {
									continue
								}
								changed := value.LastAttemptAt.Add(time.Second)
								value.LastAttemptAt = &changed
							}
							body, err := json.Marshal(value)
							if err != nil {
								return err
							}
							if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=? WHERE kind='effect' AND record_id=? AND version=?`, body, string(value.ID), index+2); err != nil {
								return err
							}
							if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_type='EFFECT_OBLIGATION_TRANSITIONED' AND json_extract(payload,'$.status')=?`, body, string(value.Status)); err != nil {
								return err
							}
						}
						if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
							return err
						}
						return rebuildEventIntegrity(t.Context(), tx)
					}); err != nil {
						t.Fatal(err)
					}
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
				if err == nil || !strings.Contains(err.Error(), "incident effect") {
					t.Fatalf("accepted invalid attempt timestamp: %v", err)
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("returned partial incident evidence")
				}
			})
		}
	}
}

package ledger

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

// Match Coordinator's durable order without crossing ledger's module boundary.
func appendIncidentEffectAttempt(t *testing.T, store *SQLite, task core.Task, lease core.CapabilityLease, effectID, approvalID string) core.EffectObligation {
	t.Helper()
	pending := approvedEffect(t, task, lease, effectID, approvalID)
	pending.Status = core.EffectPending
	pending.CreatedAt = time.Now().UTC()
	if err := store.AppendRecord(t.Context(), "org-1", "EFFECT_OBLIGATION_TRANSITIONED", "", string(task.ID), pending.AuthorizationRefs, nil, "effect", effectID, 1, pending); err != nil {
		t.Fatal(err)
	}
	appendEffectApproval(t, store, pending, true)
	attempt := pending
	attempt.Status, attempt.AttemptCount = core.EffectAttempted, 1
	now := time.Now().UTC()
	attempt.LastAttemptAt = &now
	trace, err := store.AuthorizeAndAppendEffectAttempt(t.Context(), pending, 2, attempt)
	if err != nil || !trace.Allowed {
		t.Fatalf("admit effect attempt: %+v %v", trace, err)
	}
	return attempt
}

func TestIncidentMissingPending(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "missing-pending"}[missing], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "history.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			attempt := incidentEffectFixture(t, store)
			confirmed := attempt
			confirmed.Status = core.EffectConfirmed
			confirmed.ConfirmationEvidenceRefs = []string{"receipt"}
			if err := store.AppendRecord(t.Context(), "org-1", "EFFECT_OBLIGATION_TRANSITIONED", "", string(attempt.TaskID), attempt.AuthorizationRefs, confirmed.ConfirmationEvidenceRefs, "effect", string(attempt.ID), 3, confirmed); err != nil {
				t.Fatal(err)
			}
			if missing {
				if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
					for _, query := range []string{
						`UPDATE events SET event_type='AUDIT_NOTE',payload=CAST('{}' AS BLOB),authorization_refs='[]' WHERE event_type='EFFECT_OBLIGATION_TRANSITIONED' AND json_extract(payload,'$.status')='PENDING'`,
						`DELETE FROM records WHERE kind='effect' AND version=1`,
						`UPDATE records SET version=version-1 WHERE kind='effect'`,
						`DELETE FROM event_integrity`,
					} {
						if _, err := tx.ExecContext(t.Context(), query); err != nil {
							return err
						}
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
			if !missing {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "attempt history is invalid") {
				t.Fatalf("missing pending intent: %v", err)
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("returned partial incident evidence")
			}
		})
	}
}

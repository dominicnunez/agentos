package ledger

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestIncidentRequiresLeaseHistory(t *testing.T) {
	for _, mutation := range []string{"missing-revocation-record", "missing-grant-record", "missing-revocation-event", "moved-revocation", "foreign-revocation"} {
		t.Run(mutation, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "authority.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			appendTaskProjectionParents(t, t.Context(), store, "org-1", "setup", "work-1")
			appendFactualInferenceKnowledge(t, store, "fact", "Verified fact", "A bounded observation.")
			var body []byte
			if err := store.db.QueryRowContext(t.Context(), `SELECT body FROM records WHERE kind='capability_lease' AND record_id='lease-fact'`).Scan(&body); err != nil {
				t.Fatal(err)
			}
			var lease core.CapabilityLease
			if err := json.Unmarshal(body, &lease); err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			lease.RevokedAt = &now
			if err := store.AppendRecord(t.Context(), "org-1", "CAPABILITY_REVOKED", "runtime", string(lease.OriginTaskID), nil, nil, "capability_lease", string(lease.ID), 2, lease); err != nil {
				t.Fatal(err)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "knowledge-fact", 256); err != nil {
				t.Fatalf("valid history: %v", err)
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				switch mutation {
				case "missing-revocation-event":
					_, err = tx.ExecContext(t.Context(), `DELETE FROM events WHERE event_type='CAPABILITY_REVOKED'`)
				case "missing-grant-record":
					_, err = tx.ExecContext(t.Context(), `DELETE FROM records WHERE kind='capability_lease' AND version=1`)
				default:
					_, err = tx.ExecContext(t.Context(), `DELETE FROM records WHERE kind='capability_lease' AND version=2`)
					if err != nil {
						return err
					}
					if mutation == "moved-revocation" {
						_, err = tx.ExecContext(t.Context(), `UPDATE events SET correlation_id='different' WHERE event_type='CAPABILITY_REVOKED'`)
					}
					if mutation == "foreign-revocation" {
						_, err = tx.ExecContext(t.Context(), `UPDATE events SET organization_id='other' WHERE event_type='CAPABILITY_REVOKED'`)
					}
				}
				if err != nil {
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
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "knowledge-fact", 256); err == nil {
				t.Fatal("accepted incomplete selected lease lifecycle")
			}
		})
	}
}

package ledger

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentAuthorityFingerprint(t *testing.T) {
	for _, kind := range []string{"capability_lease", "organization_freeze"} {
		for _, version := range []int{1, 2} {
			for _, oversized := range []bool{false, true} {
				name := kind + "/" + string(rune('0'+version))
				if oversized {
					name += "/oversized"
				}
				t.Run(name, func(t *testing.T) {
					path := filepath.Join(t.TempDir(), "authority.db")
					store, err := Open(path)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = store.Close() })
					correlation := "model-stop"
					if kind == "capability_lease" {
						correlation = "knowledge-fact"
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
						if err := store.AppendRecord(t.Context(), "org-1", "CAPABILITY_REVOKED", "runtime", string(lease.OriginTaskID), nil, nil, kind, string(lease.ID), 2, lease); err != nil {
							t.Fatal(err)
						}
					} else {
						modelStopManifest(t, store, false, "first")
						appendHistoricalInferenceFreeze(t, store, "org-1", 1, true)
						appendInferenceFreeze(t, store, "org-1", 2, false)
					}
					if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256); err != nil {
						t.Fatalf("valid history: %v", err)
					}
					fingerprint := "unexpected"
					if oversized {
						fingerprint = strings.Repeat("x", events.MaximumIncidentEvidenceBytes+1)
					}
					if _, err := store.db.ExecContext(t.Context(), `UPDATE records SET admission_fingerprint=? WHERE kind=? AND version=?`, fingerprint, kind, version); err != nil {
						t.Fatal(err)
					}
					if err := store.Close(); err != nil {
						t.Fatal(err)
					}
					store, err = Open(path)
					if err != nil {
						t.Fatal(err)
					}
					snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
					if err == nil {
						t.Fatal("accepted authority projection fingerprint")
					}
					if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
						t.Fatal("failure published incident evidence")
					}
					if oversized && !strings.Contains(err.Error(), "limit") && !strings.Contains(err.Error(), "bounded") {
						t.Fatalf("oversized metadata bypassed preflight: %v", err)
					}
				})
			}
		}
	}
}

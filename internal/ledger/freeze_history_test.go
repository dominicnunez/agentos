package ledger

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/authority"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestOwnerFreezeBuriedControl(t *testing.T) {
	for _, defect := range []string{"actor", "kind", "prior-event", "prior-version", "release-without-hold", "time"} {
		t.Run(defect, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			states := []bool{true, false, true, false}
			if defect == "release-without-hold" {
				states = []bool{true, false, false, true, false}
			}
			var prior string
			var records []events.AuthorityRecord
			for index, frozen := range states {
				version := index + 1
				state := core.FreezeState{OrganizationID: "org-1", Frozen: frozen,
					UpdatedAt: time.Unix(int64(version), 0).UTC(),
					Control:   &core.FreezeEvidence{ActorID: "owner-1", ActorKind: core.PrincipalHuman, PriorVersion: index, PriorEventRef: prior}}
				if version == 2 {
					switch defect {
					case "actor":
						state.Control.ActorID = "different-owner"
					case "kind":
						state.Control.ActorKind = core.PrincipalAgent
					case "prior-event":
						state.Control.PriorEventRef = "wrong-event"
					case "prior-version":
						state.Control.PriorVersion = 0
					case "time":
						state.UpdatedAt = time.Unix(1, 0).UTC()
					}
				}
				body, err := json.Marshal(state)
				if err != nil {
					t.Fatal(err)
				}
				// Only the internal fixture writer can inject these unsupported
				// histories; every later record has plausible immediate evidence.
				err = store.withTx(t.Context(), func(tx *sql.Tx) error {
					return appendRecord(t.Context(), tx, events.TrustedDraft{OrganizationID: "org-1", EventType: "FREEZE_SET", SourceActorID: "owner-1", Payload: json.RawMessage(body)}, "organization_freeze", "org-1", version, body)
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := store.db.QueryRowContext(t.Context(), `SELECT admission_event_id FROM records WHERE kind='organization_freeze' AND record_id='org-1' AND version=?`, version).Scan(&prior); err != nil {
					t.Fatal(err)
				}
				records = append(records, events.AuthorityRecord{Kind: "organization_freeze", RecordID: "org-1", Version: version, AdmissionEventID: prior, Body: body})
			}
			stream, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := events.ResolveAuthorityAdmissions(stream, records); err == nil {
				t.Fatal("fixture did not reproduce invalid replay history")
			}
			if _, err := store.ReadFreeze(t.Context(), "org-1"); err == nil {
				t.Error("owner status accepted buried invalid control")
			}
			err = store.withTx(t.Context(), func(tx *sql.Tx) error {
				_, _, err := containmentSinceTx(t.Context(), tx, "org-1", stream[len(stream)-1].Sequence)
				return err
			})
			if err == nil {
				t.Error("containment accepted buried invalid control")
			}
			err = store.withTx(t.Context(), func(tx *sql.Tx) error {
				_, _, _, err := authorityAdmissionAtBoundary(t.Context(), tx, "organization_freeze", "org-1", stream[len(stream)-1].Sequence+1)
				return err
			})
			if err == nil {
				t.Error("historical selection accepted buried invalid control")
			}
			if _, err := store.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{Frozen: true, ExpectedVersion: len(states), ExpectedEventRef: prior}); err == nil {
				t.Error("owner mutation accepted buried invalid control")
			}
			var count int
			if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM records WHERE kind='organization_freeze' AND record_id='org-1'`).Scan(&count); err != nil || count != len(states) {
				t.Errorf("rejected history changed: records=%d err=%v", count, err)
			}
		})
	}
}

func TestOwnerFreezeBrokenBinding(t *testing.T) {
	for _, defect := range []string{"missing-record", "orphan-event", "missing-event", "wrong-type", "wrong-tenant", "wrong-body"} {
		t.Run(defect, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			seedFreezeHistory(t, store, 4)
			query := map[string]string{
				"missing-record": `DELETE FROM records WHERE kind='organization_freeze' AND version=1`,
				"orphan-event":   `DELETE FROM records WHERE kind='organization_freeze' AND version=4`,
				"missing-event":  `DELETE FROM events WHERE event_id=(SELECT admission_event_id FROM records WHERE kind='organization_freeze' AND version=1)`,
				"wrong-type":     `UPDATE events SET event_type='INVALID' WHERE event_id=(SELECT admission_event_id FROM records WHERE kind='organization_freeze' AND version=1)`,
				"wrong-tenant":   `UPDATE events SET organization_id='org-2' WHERE event_id=(SELECT admission_event_id FROM records WHERE kind='organization_freeze' AND version=1)`,
				"wrong-body":     `UPDATE events SET payload='{}' WHERE event_id=(SELECT admission_event_id FROM records WHERE kind='organization_freeze' AND version=1)`,
			}[defect]
			if _, err := store.db.ExecContext(t.Context(), query); err != nil {
				t.Fatal(err)
			}
			if _, err := store.ReadFreeze(t.Context(), "org-1"); err == nil {
				t.Fatal("broken historical binding accepted")
			}
			// Corrupt tenant history must not prevent another tenant's reads.
			if _, err := store.ReadFreeze(t.Context(), "org-3"); err != nil {
				t.Fatalf("unrelated tenant affected: %v", err)
			}
		})
	}
}

// Construct exact durable history in one transaction so benchmark setup does
// not measure a growing series of owner-read validations. Production writes
// still use SetFreeze; this fixture exercises the reader independently.
func seedFreezeHistory(t testing.TB, store *SQLite, count int) {
	t.Helper()
	err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		var prior string
		for version := 1; version <= count; version++ {
			state := core.FreezeState{OrganizationID: "org-1", Frozen: version%2 == 1,
				UpdatedAt: time.Unix(int64(version), 0).UTC(),
				Control:   &core.FreezeEvidence{ActorID: "owner-1", ActorKind: core.PrincipalHuman, PriorVersion: version - 1, PriorEventRef: prior}}
			body, err := json.Marshal(state)
			if err != nil {
				return err
			}
			if err := appendRecord(t.Context(), tx, events.TrustedDraft{OrganizationID: "org-1", EventType: "FREEZE_SET", SourceActorID: "owner-1", Payload: json.RawMessage(body)}, "organization_freeze", "org-1", version, body); err != nil {
				return err
			}
			if err := tx.QueryRowContext(t.Context(), `SELECT admission_event_id FROM records WHERE kind='organization_freeze' AND record_id='org-1' AND version=?`, version).Scan(&prior); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestFreezeHistoryQueryPlan(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	rows, err := store.db.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+freezeHistorySQL, "org-1", "org-1", "org-1", "org-1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var plans []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plans = append(plans, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	plan := strings.Join(plans, "\n")
	t.Log(plan)
	if strings.Contains(plan, "SCAN r") || strings.Contains(plan, "SCAN e") || !strings.Contains(plan, "records_admission_event_idx (admission_event_id=?)") {
		t.Fatalf("history query lost bounded tenant or exact-event lookup:\n%s", plan)
	}
}

func BenchmarkFreezeHistory(b *testing.B) {
	for _, count := range []int{16, 256, 4096} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			store, err := Open(":memory:")
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = store.Close() })
			seedFreezeHistory(b, store, count)
			for _, operation := range []string{"status", "interval"} {
				b.Run(operation, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						err := store.withContainmentSnapshot(b.Context(), func(tx *sql.Tx) error {
							if operation == "status" {
								state, _, _, err := readFreeze(b.Context(), tx, "org-1")
								if err == nil && (state.Version != count || state.State.Frozen) {
									b.Fatal("history status incorrect")
								}
								return err
							}
							epoch, hold, err := containmentSinceTx(b.Context(), tx, "org-1", 0)
							if err == nil && (epoch < int64(count) || hold == nil || hold.Sequence != 1) {
								b.Fatal("history interval lost first hold")
							}
							return err
						})
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

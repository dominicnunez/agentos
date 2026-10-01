package ledger

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
	"modernc.org/sqlite"
)

var incidentSelectionCalls atomic.Int64

func init() {
	// The result is deterministic; the counter only observes repeated work.
	sqlite.MustRegisterDeterministicScalarFunction("agentos_test_incident_selection", 1, func(_ *sqlite.FunctionContext, _ []driver.Value) (driver.Value, error) {
		incidentSelectionCalls.Add(1)
		return int64(1), nil
	})
}

func TestIncidentSelectionEvaluatedOnce(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	var written []events.Event
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		for range 6 {
			event, err := appendEvent(t.Context(), tx, events.TrustedDraft{
				OrganizationID: "org-1", EventType: "AUDIT_NOTE", CorrelationID: "selection",
				Payload: map[string]string{"text": "bounded evidence"},
			})
			if err != nil {
				return err
			}
			written = append(written, event)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	tx, err := store.db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	// Preserve the original predicate-based selection as an independent oracle.
	want, err := collectEvents(tx.QueryContext(t.Context(), `SELECT `+incidentEventColumns+` FROM events WHERE sequence IN (?,?) ORDER BY sequence LIMIT 3`, written[4].Sequence, written[1].Sequence))
	if err != nil {
		t.Fatal(err)
	}
	var wantBytes int64
	if err := tx.QueryRowContext(t.Context(), `SELECT SUM(`+incidentEventBytes+`) FROM events WHERE sequence IN (?,?)`, written[4].Sequence, written[1].Sequence).Scan(&wantBytes); err != nil {
		t.Fatal(err)
	}
	budget := incidentBudget{events: 2, bytes: 2 << 20}
	incidentSelectionCalls.Store(0)
	got, err := incidentEvents(t.Context(), tx, &budget,
		`sequence IN (?,?) AND agentos_test_incident_selection(sequence)=1`, written[4].Sequence, written[1].Sequence)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].EventID != written[1].EventID || got[1].EventID != written[4].EventID {
		t.Fatalf("selected events lost original identity or sequence order: %v", got)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("primary-key fetch changed the complete selected event values")
	}
	if budget.events != 0 || budget.bytes != 2<<20-wantBytes || len(budget.checkedEvents) != 2 || !budget.checkedEvents[written[1].EventID] || !budget.checkedEvents[written[4].EventID] {
		t.Fatalf("selection did not retain evidence accounting: %+v", budget)
	}
	if calls := incidentSelectionCalls.Load(); calls != 2 {
		t.Fatalf("selected predicate evaluated %d times for two candidate rows; want one evaluation per candidate", calls)
	}
}

func TestIncidentSequenceIdentityDomain(t *testing.T) {
	for _, sample := range []struct {
		name  string
		value any
		valid bool
	}{
		{"large-integer", int64(9223372036854775807), true},
		{"negative-integer", int64(-9223372036854775807), true},
		{"zero-integer", int64(0), true},
		{"numeric-text", "12", false},
		{"oversized-text", strings.Repeat("9", 64<<10), false},
		{"real", float64(12), false},
		{"blob", []byte("12"), false},
		{"null", nil, false},
	} {
		t.Run(sample.name, func(t *testing.T) {
			db, err := sql.Open("sqlite", ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			// Deliberately damaged layout: normal storage has an INTEGER primary
			// key. No affinity here may normalize the raw corruption under test.
			_, err = db.ExecContext(t.Context(), `CREATE TABLE events AS SELECT ? AS sequence,'event-1' AS event_id,'org-1' AS organization_id,'AUDIT_NOTE' AS event_type,'' AS source_actor_id,'' AS source_execution_id,'' AS recipient_scope,'' AS recipient_id,'' AS task_id,x'5b5d' AS authorization_refs,x'5b5d' AS artifact_refs,x'7b7d' AS payload,'selection' AS correlation_id,'2026-01-01T00:00:00Z' AS created_at,1 AS schema_version`, sample.value)
			if err != nil {
				t.Fatal(err)
			}
			tx, err := db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			budget := incidentBudget{events: 1, bytes: 2 << 20}
			got, err := incidentEvents(t.Context(), tx, &budget, `event_id=?`, "event-1")
			if sample.valid {
				sequence, ok := sample.value.(int64)
				if !ok {
					t.Fatal("integer control requires an int64 fixture")
				}
				if err != nil || len(got) != 1 || got[0].Sequence != sequence {
					t.Fatalf("exact integer identity lost: %v %v", got, err)
				}
				return
			}
			if err == nil || len(got) != 0 || len(err.Error()) > 4096 || strings.Contains(err.Error(), "converting driver.Value") {
				t.Fatalf("raw sequence escaped bounded rejection: events=%d error=%v", len(got), err)
			}
			if budget.events != 1 || budget.bytes != 2<<20 || budget.checkedEvents != nil {
				t.Fatal("failed sequence selection changed evidence budget or proof")
			}
		})
	}
}

func TestIncidentLargeSequenceKeys(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	var ids []string
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		for range 2 {
			event, err := appendEvent(t.Context(), tx, events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", Payload: map[string]string{"text": "large sequence"}})
			if err != nil {
				return err
			}
			ids = append(ids, event.EventID)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Exercise the helper's integer-key boundary under the canonical primary
	// key schema. The public integrity owner separately requires contiguity.
	for index, sequence := range []int64{9223372036854775806, 9223372036854775807} {
		if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET sequence=? WHERE event_id=?`, sequence, ids[index]); err != nil {
			t.Fatal(err)
		}
	}
	tx, err := store.db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	budget := incidentBudget{events: 2, bytes: 2 << 20}
	got, err := incidentEvents(t.Context(), tx, &budget, `event_id IN (?,?)`, ids[1], ids[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].EventID != ids[0] || got[1].EventID != ids[1] || got[0].Sequence != 9223372036854775806 || got[1].Sequence != 9223372036854775807 {
		t.Fatal("large adjacent primary keys were rounded or reordered")
	}
}

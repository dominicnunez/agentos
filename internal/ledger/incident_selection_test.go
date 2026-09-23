package ledger

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentReferencePlan(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	rows, err := store.db.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+incidentReferenceSQL(1), "org-1", "seed")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		t.Log(detail)
		plan = append(plan, detail)
		for _, table := range []string{"events", "records", "e", "r", "target"} {
			if detail == "SCAN "+table || strings.HasPrefix(detail, "SCAN "+table+" ") {
				t.Fatalf("unindexed history scan: %s", detail)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"CO-ROUTINE walk", "records_replaced_work_idx", "events_incident_execution_idx", "events_message_idx", "events_source_message_idx", "sqlite_autoindex_records_1"} {
		if !strings.Contains(strings.Join(plan, "\n"), want) {
			t.Fatalf("missing indexed closure operation %s", want)
		}
	}
}

func TestIncidentSelectorBounds(t *testing.T) {
	for _, distinct := range []bool{false, true} {
		refs := make([]string, events.MaximumIncidentEvidence+1)
		for i := range refs {
			refs[i] = "missing"
			if distinct {
				refs[i] = fmt.Sprintf("missing-%d", i)
			}
		}
		body, err := json.Marshal(map[string]any{"event_refs": refs})
		if err != nil {
			t.Fatal(err)
		}
		selectors, err := incidentDocumentSelectors(events.Event{EventType: "CAPABILITY_CHECKED", Payload: body})
		if distinct && err == nil {
			t.Fatal("distinct missing targets escaped reference budget")
		}
		if !distinct && (err != nil || len(selectors) != 1) {
			t.Fatalf("repeated reference: selectors=%v err=%v", selectors, err)
		}
	}
	_, err := incidentDocumentSelectors(events.Event{EventType: "CAPABILITY_CHECKED", Payload: make([]byte, events.MaximumIncidentEvidenceBytes+1)})
	if err == nil {
		t.Fatal("oversized document accepted")
	}
}

// Selection-level fixture: every other edge is an envelope relationship rather
// than a payload reference. Domain validation remains covered by public-reader
// lineage fixtures; this tests the recursive lookup mechanism itself.
func TestIncidentEnvelopeClosure(t *testing.T) {
	for _, mode := range []string{"correlation", "execution"} {
		t.Run(mode, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			previous := ""
			want := map[string]bool{}
			for i := range 16 {
				group := fmt.Sprintf("group-%d", i)
				bridge := events.TrustedDraft{OrganizationID: "org-1", EventType: "CAPABILITY_CHECKED", Payload: map[string]string{"event_ref": previous}}
				seed := events.TrustedDraft{OrganizationID: "org-1", EventType: "CAPABILITY_CHECKED", Payload: map[string]string{}}
				if mode == "correlation" {
					bridge.CorrelationID, seed.CorrelationID = group, group
				} else {
					bridge.SourceExecutionID, seed.SourceExecutionID = group, group
				}
				for index, draft := range []events.TrustedDraft{bridge, seed} {
					event, err := store.Append(t.Context(), draft)
					if err != nil {
						t.Fatal(err)
					}
					if mode == "correlation" && index == 1 {
						// This is a selector fixture, not an admitted Intent fixture.
						if _, err := store.db.ExecContext(t.Context(), "UPDATE events SET event_type='INTENT_CONFIRMED' WHERE event_id=?", event.EventID); err != nil {
							t.Fatal(err)
						}
					}
					want[event.EventID] = true
					previous = event.EventID
				}
			}
			rows, err := store.db.QueryContext(t.Context(), incidentReferenceSQL(1), "org-1", previous)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = rows.Close() }()
			for rows.Next() {
				var sequence, phase int64
				var kind, identity string
				var id, organization sql.NullString
				var size sql.NullInt64
				if err := rows.Scan(&sequence, &phase, &kind, &identity, &id, &organization, &size); err != nil {
					t.Fatal(err)
				}
				if phase < 0 {
					t.Fatal("unexpected overflow")
				}
				if phase == 0 {
					delete(want, id.String)
				}
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if len(want) != 0 {
				t.Fatalf("lost %d envelope-linked events", len(want))
			}
		})
	}
}

func TestIncidentReferenceFanout(t *testing.T) {
	for _, width := range []int{64, 256, 1024} {
		t.Run(fmt.Sprint(width), func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			refs := make([]string, width)
			for i := range refs {
				event, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", Payload: map[string]string{"note": "target"}})
				if err != nil {
					t.Fatal(err)
				}
				refs[i] = event.EventID
			}
			seed, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "CAPABILITY_CHECKED", Payload: map[string]any{"event_refs": refs}})
			if err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			rows, err := store.db.QueryContext(t.Context(), incidentReferenceSQL(1), "org-1", seed.EventID)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = rows.Close() }()
			count := 0
			for rows.Next() {
				var sequence, phase int64
				var size sql.NullInt64
				var id, organization sql.NullString
				var kind, identity string
				if err := rows.Scan(&sequence, &phase, &kind, &identity, &id, &organization, &size); err != nil {
					t.Fatal(err)
				}
				if phase < 0 {
					t.Fatal("unexpected overflow")
				}
				if phase == 0 {
					count++
				}
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			if count != width+1 {
				t.Fatalf("got%d want%d", count, width+1)
			}
			t.Logf("width=%d elapsed=%s", width, time.Since(started))
		})
	}
}

func TestIncidentReferenceGrammar(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	target, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", Payload: map[string]string{"note": "target"}})
	if err != nil {
		t.Fatal(err)
	}
	for name, payload := range map[string]any{
		"direct":          map[string]any{"event_refs": []string{target.EventID}},
		"opaque":          map[string]any{"recovery_result": map[string]string{"event_ref": target.EventID}},
		"nested opaque":   map[string]any{"nested": map[string]any{"fields": map[string]string{"event_ref": target.EventID}}},
		"suffix mismatch": map[string]string{"eventXref": target.EventID},
		"stop":            map[string]any{"observed_effect": map[string]string{"stop_request_ref": target.EventID}},
		"other effect":    map[string]any{"observed_effect": map[string]string{"event_ref": target.EventID}},
	} {
		t.Run(name, func(t *testing.T) {
			body, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			var decoded any
			if err := json.Unmarshal(body, &decoded); err != nil {
				t.Fatal(err)
			}
			d := incidentDependencies{keys: map[incidentKey]bool{}, refs: map[string]bool{}, reverse: map[incidentKey]bool{}}
			d.discover(decoded, "")
			seed, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "CAPABILITY_CHECKED", Payload: payload})
			if err != nil {
				t.Fatal(err)
			}
			rows, err := store.db.QueryContext(t.Context(), incidentReferenceSQL(1), "org-1", seed.EventID)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = rows.Close() }()
			found := false
			for rows.Next() {
				var sequence, phase int64
				var size sql.NullInt64
				var id, organization sql.NullString
				var kind, identity string
				if err := rows.Scan(&sequence, &phase, &kind, &identity, &id, &organization, &size); err != nil {
					t.Fatal(err)
				}
				if id.String == target.EventID {
					found = true
				}
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			_, want := d.refs[target.EventID]
			if found != want {
				t.Fatalf("SQL=%t Go=%t", found, want)
			}
		})
	}
}

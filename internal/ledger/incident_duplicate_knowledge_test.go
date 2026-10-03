package ledger

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentDuplicateKnowledgeClaims(t *testing.T) {
	parallelIncidentTest(t)
	for _, family := range []string{"proposal", "validation"} {
		for _, mode := range []string{"raw-reference", "consumer-gate", "consumer-reference", "consumer-value", "consumer-projection", "consumer-kind", "consumer-status", "unconsumed"} {
			if family == "proposal" && mode == "consumer-status" {
				continue
			}
			t.Run(family+"/"+mode, func(t *testing.T) {
				store, err := Open(":memory:")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				selected, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-2", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "selected", Payload: map[string]string{"note": "selected"}})
				if err != nil {
					t.Fatal(err)
				}
				var raw events.Event
				field, gate, refs := "outcome_event_ref", "validation_method", "validation_refs"
				decoy := []byte(`"UNVALIDATED"`)
				if family == "proposal" {
					raw = appendIncidentAgentCandidate(t, store, mode != "unconsumed")
					field, gate, refs = "occurrence_event_refs", "created_by_kind", "provenance_event_refs"
					decoy = []byte(`"RUNTIME"`)
				} else {
					raw = appendIncidentDeterministicKnowledge(t, store, mode != "unconsumed")
				}
				stream, err := store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := events.ValidateProjectionHistory(stream, nil, nil, nil); err != nil {
					t.Fatalf("writer history: %v", err)
				}
				target, _ := json.Marshal(selected.EventID)
				if family == "proposal" {
					target = []byte("[" + string(target) + "]")
				}
				if mode == "raw-reference" || mode == "unconsumed" {
					if family == "proposal" {
						changeKnowledgePayloadRef(t, store, raw, field, []string{})
					}
					duplicateKnowledgeDocument(t, store, raw.EventID, nil, field, target, false)
				} else {
					changeKnowledgePayloadRef(t, store, raw, field, json.RawMessage(target))
					duplicateField, duplicateValue := gate, decoy
					if mode == "consumer-reference" {
						duplicateField, duplicateValue = refs, []byte(`[]`)
					}
					path := []string{"projection", "value"}
					switch mode {
					case "consumer-value":
						path, duplicateField, duplicateValue = []string{"projection"}, "value", []byte(`{}`)
					case "consumer-projection":
						path, duplicateField, duplicateValue = nil, "projection", []byte(`{"projection_kind":"knowledge","value":{}}`)
					case "consumer-kind":
						path, duplicateField, duplicateValue = []string{"projection"}, "projection_kind", []byte(`"unrelated"`)
					case "consumer-status":
						duplicateField, duplicateValue = "status", []byte(`"CANDIDATE"`)
					}
					duplicateKnowledgeDocument(t, store, "", path, duplicateField, duplicateValue, true)
				}
				if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
					if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
						return err
					}
					return rebuildEventIntegrity(t.Context(), tx)
				}); err != nil {
					t.Fatal(err)
				}
				stream, err = store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				_, fullErr := events.ValidateProjectionHistory(stream, nil, nil, nil)
				if mode != "unconsumed" && fullErr == nil {
					t.Fatalf("full owner applicability: %v", fullErr)
				}
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-2", "selected", 256)
				if mode == "unconsumed" {
					if err != nil {
						t.Fatalf("unused raw statement poisoned incident: %v", err)
					}
					return
				}
				if err == nil {
					t.Fatal("duplicate consumed Knowledge claim omitted")
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("invalid claim returned partial snapshot")
				}
			})
		}
	}
}

func TestIncidentDuplicateJudgmentClaims(t *testing.T) {
	parallelIncidentTest(t)
	for _, kind := range []string{"HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED", "A2A_KNOWLEDGE_JUDGMENT_RECEIVED", "KNOWLEDGE_JUDGMENT_PUBLISHED"} {
		for _, mode := range []string{"raw-reference", "consumer-gates", "unconsumed"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				store, err := Open(":memory:")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				selected, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-2", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "selected", Payload: map[string]string{"note": "selected"}})
				if err != nil {
					t.Fatal(err)
				}
				correlation := appendDerivedIncidentChain(t, store, 1)
				baseline, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
				if err != nil {
					t.Fatal(err)
				}
				stream, err := store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				leases, freezes, err := events.ResolveAuthorityAdmissions(stream, baseline.AuthorityRecords)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := events.ValidateProjectionHistory(stream, nil, leases, freezes); err != nil {
					t.Fatalf("writer history: %v", err)
				}
				var judgment events.Event
				for _, event := range stream {
					if event.EventType == "HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED" {
						judgment = event
						break
					}
				}
				if judgment.EventID == "" {
					t.Fatal("writer judgment missing")
				}
				// Other labels are deliberately malformed principal bindings. This
				// tests that each conditional raw source family reaches its owner;
				// it does not claim valid A2A/Agent admission history.
				if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET event_type=? WHERE event_id=?`, kind, judgment.EventID); err != nil {
					t.Fatal(err)
				}
				target, _ := json.Marshal(selected.EventID)
				if mode == "consumer-gates" {
					changeKnowledgePayloadRef(t, store, judgment, "capability_check_event_id", json.RawMessage(target))
					duplicateKnowledgeDocument(t, store, "", []string{"projection", "value"}, "validation_method", []byte(`"UNVALIDATED"`), true)
					duplicateKnowledgeDocument(t, store, "", []string{"projection", "value"}, "validated_by_kind", []byte(`"RUNTIME"`), true)
				} else {
					duplicateKnowledgeDocument(t, store, judgment.EventID, nil, "capability_check_event_id", target, false)
				}
				if mode == "unconsumed" {
					if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET event_type='AUDIT_NOTE',payload='{}' WHERE event_id IN (SELECT admission_event_id FROM records WHERE kind='knowledge'); DELETE FROM records WHERE kind='knowledge'`); err != nil {
						t.Fatal(err)
					}
				}
				if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
					if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
						return err
					}
					return rebuildEventIntegrity(t.Context(), tx)
				}); err != nil {
					t.Fatal(err)
				}
				if mode != "unconsumed" {
					stream, err = store.Events(t.Context(), "")
					if err != nil {
						t.Fatal(err)
					}
					if _, err := events.ValidateProjectionHistory(stream, nil, leases, freezes); err == nil {
						t.Fatal("full owner accepted duplicate judgment")
					}
				}
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-2", "selected", 256)
				if mode == "unconsumed" {
					if err != nil {
						t.Fatalf("unused statement poisoned unrelated incident: %v", err)
					}
					return
				}
				if err == nil {
					t.Fatal("duplicate governed judgment claim omitted")
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("invalid judgment returned partial snapshot")
				}
			})
		}
	}
}

// Prepend a decoy to an owned consumer field or append a second raw claim.
// Both event and record consumers are changed so either surviving channel must
// independently discover the malformed consumed relationship.
func duplicateKnowledgeDocument(t *testing.T, store *SQLite, eventID string, path []string, field string, value []byte, first bool) {
	t.Helper()
	edit := func(body []byte) []byte {
		var descend func([]byte, []string) []byte
		descend = func(raw []byte, remaining []string) []byte {
			if len(remaining) == 0 {
				member := append(append([]byte(`"`+field+`":`), value...), ',')
				if first {
					return append(append([]byte{'{'}, member...), raw[1:]...)
				}
				return append(append(append([]byte(nil), raw[:len(raw)-1]...), ','), append(member[:len(member)-1], '}')...)
			}
			var object map[string]json.RawMessage
			if err := json.Unmarshal(raw, &object); err != nil {
				t.Fatal(err)
			}
			child := object[remaining[0]]
			changed := descend(child, remaining[1:])
			return bytes.Replace(raw, child, changed, 1)
		}
		return descend(body, path)
	}
	if eventID != "" {
		var body []byte
		if err := store.db.QueryRowContext(t.Context(), `SELECT payload FROM events WHERE event_id=?`, eventID).Scan(&body); err != nil {
			t.Fatal(err)
		}
		if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, edit(body), eventID); err != nil {
			t.Fatal(err)
		}
		return
	}
	type source struct {
		id   string
		body []byte
	}
	var sources []source
	rows, err := store.db.QueryContext(t.Context(), `SELECT event_id,payload FROM events WHERE event_id IN (SELECT admission_event_id FROM records WHERE kind='knowledge')`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var s source
		if err := rows.Scan(&s.id, &s.body); err != nil {
			t.Fatal(err)
		}
		sources = append(sources, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	for _, s := range sources {
		if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, edit(s.body), s.id); err != nil {
			t.Fatal(err)
		}
	}
	switch field {
	case "projection", "projection_kind":
		// Record kind is a storage column; hide its value side independently.
		path, field, value = nil, "value", []byte(`{}`)
	default:
		path = path[1:]
	}
	sources = nil
	recordRows, err := store.db.QueryContext(t.Context(), `SELECT record_id,body FROM records WHERE kind='knowledge'`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = recordRows.Close() }()
	for recordRows.Next() {
		var s source
		if err := recordRows.Scan(&s.id, &s.body); err != nil {
			t.Fatal(err)
		}
		sources = append(sources, s)
	}
	if err := recordRows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := recordRows.Close(); err != nil {
		t.Fatal(err)
	}
	// Several revisions share an identity; match original bytes as well.
	for _, s := range sources {
		if _, err := store.db.ExecContext(t.Context(), `UPDATE records SET body=? WHERE kind='knowledge' AND record_id=? AND body=?`, edit(s.body), s.id, s.body); err != nil {
			t.Fatal(err)
		}
	}
}

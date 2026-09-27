package ledger

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

// A later dependency round can have more reverse identities than SQLite's
// expression depth, while remaining inside the incident evidence bound.
func TestIncidentReverseFrontierWide(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	d := incidentDependencies{
		organization: "org-1", keys: map[incidentKey]bool{}, reverse: map[incidentKey]bool{},
	}
	for _, kind := range []string{"work", "intent", "goal", "knowledge"} {
		for index := range 300 {
			key := incidentKey{kind: kind, id: fmt.Sprintf("%s-%04d", kind, index)}
			d.keys[key] = true // The identity was read in an earlier round.
			d.reverse[key] = false
		}
	}
	want := map[string]bool{}
	var malformedID string
	err = store.withTx(t.Context(), func(tx *sql.Tx) error {
		for _, sample := range []struct {
			organization, kind  string
			payload             map[string]string
			selected, malformed bool
		}{
			{"org-1", "INTENT_CONFIRMED", map[string]string{"replaces_work_id": "work-0000"}, true, false},
			{"org-1", "INTENT_CONFIRMED", map[string]string{"intent_id": "intent-0000"}, true, false},
			{"org-1", "GOAL_PROGRESS_EVALUATED", map[string]string{"goal_id": "goal-0000"}, true, false},
			{"org-1", "KNOWLEDGE_VALIDATION_RECORDED", map[string]string{"knowledge_id": "knowledge-0000"}, true, false},
			{"org-2", "GOAL_PROGRESS_EVALUATED", map[string]string{"goal_id": "goal-0000"}, true, false},
			{"org-1", "AUDIT_NOTE", map[string]string{"goal_id": "goal-0000", "knowledge_id": "knowledge-0000"}, false, false},
			{"org-1", "INTENT_CONFIRMED", map[string]string{"goal_id": "goal-0000"}, false, true},
		} {
			event, err := appendEvent(t.Context(), tx, events.TrustedDraft{
				OrganizationID: sample.organization, EventType: sample.kind,
				SourceActorID: "runtime", CorrelationID: "selected", Payload: sample.payload,
			})
			if err != nil {
				return err
			}
			if sample.selected {
				want[event.EventID] = true
			}
			if sample.malformed {
				malformedID = event.EventID
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Selection must leave malformed retained evidence for the later validator
	// without letting json_extract abort this bounded candidate query.
	if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET payload='{' WHERE event_id=?`, malformedID); err != nil {
		t.Fatal(err)
	}
	where, args := d.frontier()
	if where == "" || len(args) != 1200 {
		t.Fatalf("wide reverse frontier: where=%q arguments=%d", where, len(args))
	}
	tx, err := store.db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	budget := incidentBudget{events: events.MaximumIncidentEvidence, bytes: events.MaximumIncidentEvidenceBytes}
	selected, err := incidentEvents(t.Context(), tx, &budget, where, args...)
	if err != nil {
		t.Fatalf("supported reverse frontier failed: %v", err)
	}
	if len(selected) != len(want) {
		t.Fatalf("selected %d reverse candidates, want %d", len(selected), len(want))
	}
	for _, event := range selected {
		if !want[event.EventID] {
			t.Fatalf("unrelated reverse event selected: %+v", event)
		}
	}
}

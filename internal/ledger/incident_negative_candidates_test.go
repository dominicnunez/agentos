package ledger

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentNegativeOrdinaryDiscovery(t *testing.T) {
	for _, sample := range []struct {
		name               string
		body               []byte
		candidate, invalid bool
	}{
		{"ascii-object", []byte(`{"text":"ordinary note"}`), false, false},
		{"ascii-nested", []byte(`{"text":{"values":[1,true,null,"safe"]}}`), false, false},
		{"ascii-escaped-string", []byte(`{"text":"escaped\\slash\"quote\nnewline"}`), false, false},
		{"unicode", []byte(`{"text":"café"}`), true, false},
		{"unicode-escape", []byte(`{"text":"caf\u00e9"}`), true, false},
		{"duplicate-ascii", []byte(`{"text":"one","text":"two"}`), true, true},
		{"duplicate-nested", []byte(`{"text":{"value":1,"value":2}}`), true, true},
		{"duplicate-escaped", []byte(`{"text":1,"te\u0078t":2}`), true, true},
		{"surrogate-duplicate", []byte(`{"\ud800":1,"\ud900":2}`), true, true},
		{"invalid-utf8", append(append([]byte(`{"text":"`), 0xff), []byte(`"}`)...), true, true},
		{"array", []byte(`[]`), true, true},
		{"null", []byte(`null`), true, true},
		{"malformed", []byte(`{"text":`), true, true},
		{"reserved-projection", []byte(`{"projection":{}}`), true, true},
		{"escaped-reserved", []byte(`{"pr\u006fjection":{}}`), true, true},
		{"depth-64", []byte(`{"value":` + strings.Repeat(`[`, 63) + `0` + strings.Repeat(`]`, 63) + `}`), false, false},
		{"depth-65", []byte(`{"value":` + strings.Repeat(`[`, 64) + `0` + strings.Repeat(`]`, 64) + `}`), true, true},
	} {
		t.Run(sample.name, func(t *testing.T) {
			store := projectionScopeFixture(t)
			var id string
			if err := store.db.QueryRowContext(t.Context(), `SELECT event_id FROM events WHERE event_type='TEAM_CREATED'`).Scan(&id); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET event_type='AUDIT_NOTE',payload=? WHERE event_id=?`, sample.body, id); err != nil {
				t.Fatal(err)
			}
			var candidate bool
			if err := store.db.QueryRowContext(t.Context(), `SELECT `+incidentNegativeJSON+` FROM events WHERE event_id=?`, id).Scan(&candidate); err != nil {
				t.Fatal(err)
			}
			if candidate != sample.candidate {
				t.Fatalf("candidate=%v want%v", candidate, sample.candidate)
			}
			stream, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range stream {
				if event.EventID == id {
					_, _, err := events.AdmittedProjection(event)
					if (err != nil) != sample.invalid {
						t.Fatalf("shared exact owner=%v want invalid%v", err, sample.invalid)
					}
				}
			}
		})
	}
}

func TestIncidentNegativeCheckedCandidateBudget(t *testing.T) {
	store := projectionScopeFixture(t)
	var event events.Event
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range stream {
		if value.EventType == "TEAM_CREATED" {
			event = value
		}
	}
	_, _, err = events.AdmittedProjection(event)
	if err != nil {
		t.Fatal(err)
	}
	// The guard's candidate reader must not charge support already proven by
	// central incidentEvents. This test also checks that the same budget map is
	// shared through nested readers rather than copied before proof registration.
	budget := incidentBudget{events: 1, bytes: 2 << 20}
	tx, err := store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	loaded, err := incidentEvents(t.Context(), tx, &budget, `event_id=?`, event.EventID)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 || !budget.checkedEvents[event.EventID] {
		t.Fatal("bounded exact candidate proof was not registered")
	}
	if budget.events != 0 {
		t.Fatal("candidate was not charged once")
	}
	encoded, err := json.Marshal([]string{event.EventID})
	if err != nil {
		t.Fatal(err)
	}
	loaded, err = incidentEvents(t.Context(), tx, &budget, `event_id NOT IN (SELECT value FROM json_each(?)) AND event_id=?`, string(encoded), event.EventID)
	if err != nil || len(loaded) != 0 || budget.events != 0 {
		t.Fatalf("known exact candidate charged twice: %v", err)
	}
}

func TestIncidentNegativeRawNUL(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "nul-window", true)
	candidate, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "foreign-org", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "foreign", Payload: map[string]string{"text": "safe"}})
	if err != nil {
		t.Fatal(err)
	}
	appendBenchmarkTaskInference(t, store, agent, config, "selected")
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256); err != nil {
		t.Fatalf("healthy writer baseline: %v", err)
	}
	_, task := latestTestProjection[core.Task](t, t.Context(), store, "task", "task-selected")
	_, work := latestTestProjection[core.Work](t, t.Context(), store, "work", "work-selected")
	_, intent := latestTestProjection[core.Intent](t, t.Context(), store, "intent", work.IntentID)
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, append([]byte(`{"text":"safe"}`), 0), candidate.EventID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	}); err != nil {
		t.Fatal(err)
	}
	full, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	var start events.Event
	for _, event := range full {
		if event.EventType == "EXECUTION_STARTED" {
			start = event
		}
	}
	if err := events.ValidateTaskExecutionStart(start, task, 2, work, intent, full); err == nil {
		t.Fatal("full selected owner accepted raw NUL temporal source")
	}
	snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256)
	if err == nil {
		t.Fatal("public temporal discovery omitted raw NUL source rejected by exact owner")
	}
	if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
		t.Fatal("returned partial evidence")
	}
}

package ledger

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

func TestIncidentIncomingInboxRefs(t *testing.T) {
	parallelIncidentTest(t)
	for _, field := range []string{"event_ids", "execution_start_event_ref"} {
		t.Run(field, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			planScopeParents(t, store, "selected-org", "selected", "selected-work")
			agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "incoming", true)
			input, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", SourceActorID: "runtime", CorrelationID: "shared-input", EventType: "AUDIT_NOTE", RecipientScope: events.RecipientAgent, RecipientID: string(agent.ID), Payload: map[string]string{"text": "shared inbox input"}})
			if err != nil {
				t.Fatal(err)
			}
			first := appendInboxTaskInference(t, store, agent, config, "first", nil)
			refs := []string{input.EventID}
			observation, err := store.ObserveInbox(t.Context(), events.TrustedDraft{OrganizationID: "org-1", SourceActorID: string(agent.ID), SourceExecutionID: first.Scope.ExecutionID, TaskID: first.Scope.TaskID, CorrelationID: "first", EventType: "INBOX_EVENTS_OBSERVED", RecipientScope: events.RecipientAgent, RecipientID: string(agent.ID), Payload: map[string]any{"event_ids": refs}}, events.RecipientAgent, string(agent.ID), refs)
			if err != nil {
				t.Fatal(err)
			}
			// A later execution sharing this Agent's inbox must reconstruct the
			// earlier observation before omitting its already-consumed input.
			second := appendInboxTaskInference(t, store, agent, config, "second", nil)
			policy := testInferencePolicy(time.Now().UTC())
			policy.OrganizationID, policy.Provider, policy.Model, policy.ExecutionProfileVersion = "org-1", "provider", "model", config.ProfileVersion
			policy.Mode, policy.Pricing = inference.Local, nil
			if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			if _, err := store.ReserveInference(t.Context(), second); err != nil {
				t.Fatal(err)
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
				t.Fatalf("healthy full shared-inbox replay: %v", err)
			}
			own, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "second", 256)
			if err != nil {
				t.Fatalf("healthy shared-inbox incident: %v", err)
			}
			seen := map[string]bool{}
			for _, event := range own.DependencyEvents {
				seen[event.EventID] = true
			}
			if !seen[input.EventID] || !seen[observation.EventID] {
				t.Fatal("valid shared-inbox incident omitted its prior input or observation")
			}
			baseline, err := store.VerifiedIncidentEvents(t.Context(), "selected-org", "selected", 256)
			if err != nil {
				t.Fatal(err)
			}
			var selected events.Event
			for _, event := range baseline.Work.Events {
				if event.EventType == "WORK_CREATED" {
					selected = event
				}
			}
			if selected.EventID == "" {
				t.Fatal("missing selected Work")
			}
			for _, event := range baseline.DependencyEvents {
				if event.EventID == observation.EventID {
					t.Fatal("unrelated observation selected before reference changed")
				}
			}
			var payload events.InboxEventsObservedPayload
			if err := json.Unmarshal(observation.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			if field == "event_ids" {
				payload.EventIDs = []string{selected.EventID}
			} else {
				payload.ExecutionStartEventRef = selected.EventID
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				body, err := json.Marshal(payload)
				if err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, observation.EventID); err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
					return err
				}
				return rebuildEventIntegrity(t.Context(), tx)
			}); err != nil {
				t.Fatal(err)
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err == nil {
				t.Fatal("full inbox replay accepted substituted observation reference")
			} else {
				t.Logf("full inbox owner rejected: %v", err)
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "selected-org", "selected", 256)
			if err == nil {
				t.Fatal("incident omitted incoming inbox observation reference")
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("failed incident returned partial evidence")
			}
		})
	}
}

func TestIncidentInboxLinkGrammar(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for _, tc := range []struct {
		kind, body string
		want       int
	}{
		{"INBOX_EVENTS_OBSERVED", `{"event_ids":["selected"]}`, 1},
		{"INBOX_EVENTS_OBSERVED", `{"execution_start_event_ref":"selected"}`, 1},
		{"INBOX_EVENTS_OBSERVED", `{"note":{"event_ids":["selected"],"execution_start_event_ref":"selected"}}`, 0},
		{"INBOX_EVENTS_OBSERVED", `{"event_ids":[{"event_id":"selected"}]}`, 0},
		{"AUDIT_NOTE", `{"event_ids":["selected"],"execution_start_event_ref":"selected"}`, 0},
	} {
		t.Run(tc.kind+"/"+tc.body, func(t *testing.T) {
			var match, legacy int
			if err := store.db.QueryRowContext(t.Context(), `SELECT agentos_incident_link_match_v2(0,?,?,'event','selected'),agentos_incident_link_match_v1(0,?,?,'event','selected')`, tc.kind, tc.body, tc.kind, tc.body).Scan(&match, &legacy); err != nil {
				t.Fatal(err)
			}
			if match != tc.want || legacy != 0 {
				t.Fatalf("match=%d legacy=%d want=%d", match, legacy, tc.want)
			}
		})
	}
}

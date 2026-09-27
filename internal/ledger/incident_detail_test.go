package ledger

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentDetailLinks(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for _, tc := range []struct {
		event, kind, execution, detail string
		want                           int
	}{
		{"WORK_COMPLETED", "work", "", `{"evidence_event_ref":"selected"}`, 1},
		{"GOAL_ACHIEVED", "goal", "", `{"evidence_event_ref":"selected"}`, 1},
		{"TASK_VERIFIED_COMPLETE", "task", "DETERMINISTIC", `{"outcome_event_ref":"selected"}`, 1},
		{"TASK_VERIFIED_COMPLETE", "task", "HUMAN", `{"submission_event_ref":"selected"}`, 1},
		{"TASK_VERIFIED_COMPLETE", "task", "HUMAN", `{"judgment_ref":"selected"}`, 1},
		{"TASK_VERIFIED_COMPLETE", "task", "AGENT", `{"judgment_ref":"selected"}`, 1},
		{"EXECUTION_STARTED", "task", "HUMAN", `{"input_event_ref":"selected"}`, 1},
		{"EXECUTION_STARTED", "task", "DETERMINISTIC", `{"strategic_event_refs":["selected"]}`, 1},
		{"EXECUTION_STARTED", "task", "AGENT", `{"dispatch_binding":{"agent_event_ref":"selected"}}`, 1},
		{"EXECUTION_STARTED", "task", "AGENT", `{"dispatch_binding":{"blueprint_event_ref":"selected"}}`, 1},
		{"EXECUTION_STARTED", "task", "AGENT", `{"dispatch_binding":{"execution_profile_event_ref":"selected"}}`, 1},
		{"WORK_CREATED", "work", "", `{"evidence_event_ref":"selected"}`, 0},
		{"AUDIT_NOTE", "work", "", `{"evidence_event_ref":"selected"}`, 0},
		{"EXECUTION_STARTED", "task", "DETERMINISTIC", `{"input_event_ref":"selected"}`, 1},
		{"TASK_VERIFIED_COMPLETE", "task", "DETERMINISTIC", `{"submission_event_ref":"selected"}`, 1},
		{"EXECUTION_STARTED", "task", "HUMAN", `{"dispatch_binding":{"agent_event_ref":"selected"}}`, 1},
		{"EXECUTION_STARTED", "task", "AGENT", `{"input_event_refs":["selected"]}`, 0},
		{"WORK_COMPLETED", "work", "", `{"untrusted":{"evidence_event_ref":"selected"}}`, 0},
		{"WORK_COMPLETED", "work", "", `{"evidence_event_ref":7}`, 0},
	} {
		t.Run(tc.event+"/"+tc.execution+"/"+tc.detail, func(t *testing.T) {
			body := fmt.Sprintf(`{"projection":{"projection_kind":%q,"record_id":"incoming","value":{"execution_kind":%q}},"detail":%s}`, tc.kind, tc.execution, tc.detail)
			var count, match, legacy int
			if err := store.db.QueryRowContext(t.Context(), `SELECT (SELECT COUNT(*) FROM json_each(agentos_incident_links_v2(0,?,?)) WHERE json_extract(value,'$.kind')='event' AND json_extract(value,'$.id')='selected'),agentos_incident_link_match_v2(0,?,?,'event','selected'),(SELECT COUNT(*) FROM json_each(agentos_incident_links_v1(0,NULL,?)) WHERE json_extract(value,'$.kind')='event')`, tc.event, body, tc.event, body, body).Scan(&count, &match, &legacy); err != nil {
				t.Fatal(err)
			}
			if count != tc.want || match != tc.want || legacy != 0 {
				t.Fatalf("links=%d match=%d legacy=%d want=%d", count, match, legacy, tc.want)
			}
		})
	}
}

// ChangeIncidentDetailForTest preserves the real writer's projection and reseals
// one changed transition detail. Storage mutations stay in the ledger package.
func ChangeIncidentDetailForTest(t *testing.T, store *SQLite, id, field string, target any) {
	t.Helper()
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		event, found, err := eventByID(t.Context(), tx, id)
		if err != nil {
			return fmt.Errorf("read transition: %w", err)
		}
		if !found {
			return fmt.Errorf("transition event is missing")
		}
		payload, _, err := events.AdmittedProjection(event)
		if err != nil {
			return err
		}
		var detail map[string]json.RawMessage
		if err := json.Unmarshal(payload.Detail, &detail); err != nil {
			return err
		}
		detail[field], err = json.Marshal(target)
		if err != nil {
			return err
		}
		encodedDetail, err := json.Marshal(detail)
		if err != nil {
			return err
		}
		sealed, err := events.SealProjectionEvent(event, payload.Projection, encodedDetail)
		if err != nil {
			return err
		}
		body, err := json.Marshal(sealed)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, id); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `UPDATE records SET admission_fingerprint=? WHERE admission_event_id=?`, sealed.Admission.Fingerprint, id); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	}); err != nil {
		t.Fatal(err)
	}
}

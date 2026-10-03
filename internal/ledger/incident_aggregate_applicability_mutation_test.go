package ledger

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

// InsertUnusedAggregateForTest preserves a writer-produced evaluation envelope
// while retaining a statement that has no consuming terminal transition.
func InsertUnusedAggregateForTest(t *testing.T, store *SQLite, source events.Event, value any, scope ...string) string {
	t.Helper()
	organization, correlation := source.OrganizationID, "unused-aggregate"
	if len(scope) == 2 {
		organization, correlation = scope[0], scope[1]
	} else if len(scope) != 0 {
		t.Fatal("unused aggregate scope requires organization and correlation")
	}
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(t.Context(), `INSERT INTO events(sequence,event_id,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version)
SELECT (SELECT MAX(sequence)+1 FROM events),'unused-aggregate',?,?,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,?,?,created_at,schema_version FROM events WHERE event_id=?`, organization, source.EventType, body, correlation, source.EventID)
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
	return "unused-aggregate"
}

func ChangeAggregateGoalClaimForTest(t *testing.T, store *SQLite, source events.Event, target string) {
	t.Helper()
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		var value any
		var fingerprint, terminal string
		if source.EventType == "WORK_COMPLETION_EVALUATED" {
			var payload events.WorkCompletionEvidencePayload
			if err := json.Unmarshal(source.Payload, &payload); err != nil {
				return err
			}
			payload.GoalID = core.ID(target)
			var err error
			payload.Fingerprint, err = payload.ExpectedFingerprint()
			if err != nil {
				return err
			}
			value, fingerprint, terminal = payload, payload.Fingerprint, "WORK_COMPLETED"
		} else {
			var payload events.GoalProgressEvaluatedPayload
			if err := json.Unmarshal(source.Payload, &payload); err != nil {
				return err
			}
			payload.GoalID = core.ID(target)
			var err error
			payload.Fingerprint, err = payload.ExpectedFingerprint()
			if err != nil {
				return err
			}
			value, fingerprint, terminal = payload, payload.Fingerprint, "GOAL_ACHIEVED"
		}
		body, err := json.Marshal(value)
		if err != nil {
			return err
		}
		body = []byte(strings.Replace(string(body), `"goal_id":`, `"goal_id":"unrelated-goal","goal_id":`, 1))
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, source.EventID); err != nil {
			return err
		}
		var transitionID string
		if err := tx.QueryRowContext(t.Context(), `SELECT event_id FROM events WHERE event_type=? AND json_extract(payload,'$.detail.evidence_event_ref')=?`, terminal, source.EventID).Scan(&transitionID); err != nil {
			return err
		}
		transition, _, err := eventByID(t.Context(), tx, transitionID)
		if err != nil {
			return err
		}
		projection, _, err := events.AdmittedProjection(transition)
		if err != nil {
			return err
		}
		detail, err := json.Marshal(events.WorkCompletionTransitionPayload{EvidenceEventRef: source.EventID, Fingerprint: fingerprint})
		if err != nil {
			return err
		}
		sealed, err := events.SealProjectionEvent(transition, projection.Projection, detail)
		if err != nil {
			return err
		}
		body, err = json.Marshal(sealed)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, transitionID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `UPDATE records SET admission_fingerprint=? WHERE admission_event_id=?`, sealed.Admission.Fingerprint, transitionID); err != nil {
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

// InsertWrongAggregateConsumerForTest is an owner-applicability fixture: its
// label resembles a terminal consumer, but its sealed Knowledge projection
// references the evaluation as provenance rather than terminal detail. Full
// projection replay must reject this malformed lifecycle label; aggregate
// completion owners independently ignore it as a consumer of that statement.
func InsertWrongAggregateConsumerForTest(t *testing.T, store *SQLite, source events.Event, target string) {
	t.Helper()
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		var sequence int64
		if err := tx.QueryRowContext(t.Context(), `SELECT MAX(sequence)+1 FROM events`).Scan(&sequence); err != nil {
			return err
		}
		event := source
		event.Sequence, event.EventID, event.CorrelationID = sequence, "wrong-aggregate-consumer", "unused-consumer"
		event.EventType = "WORK_COMPLETED"
		event.SourceActorID = "runtime"
		event.SourceExecutionID, event.RecipientScope, event.RecipientID, event.TaskID = "", "", "", ""
		event.AuthorizationRefs = nil
		if source.EventType == "GOAL_PROGRESS_EVALUATED" {
			event.EventType = "GOAL_ACHIEVED"
		}
		value, err := json.Marshal(map[string]any{"provenance_event_refs": []string{target}})
		if err != nil {
			return err
		}
		projection := events.ProjectionRecord{ProjectionKind: "knowledge", RecordID: "wrong-consumer", Version: 1, CorrelationID: event.CorrelationID, Value: value}
		sealed, err := events.SealProjectionEvent(event, projection, nil)
		if err != nil {
			return err
		}
		body, err := json.Marshal(sealed)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(t.Context(), `INSERT INTO events(sequence,event_id,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version)
SELECT ?,?,organization_id,?,'runtime','','','','','[]',artifact_refs,?,?,created_at,schema_version FROM events WHERE event_id=?`, event.Sequence, event.EventID, event.EventType, body, event.CorrelationID, source.EventID)
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
}

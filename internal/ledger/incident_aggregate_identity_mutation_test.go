package ledger

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

// ChangeAggregateIdentityForTest keeps the evidence and consuming transition
// fingerprints valid while changing one owner-checked global identity.
func ChangeAggregateIdentityForTest(t *testing.T, store *SQLite, id, field, target string) {
	t.Helper()
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		event, found, err := eventByID(t.Context(), tx, id)
		if err != nil {
			return fmt.Errorf("read aggregate identity evidence: %w", err)
		}
		if !found {
			return fmt.Errorf("aggregate identity evidence %s not found", id)
		}
		var value any
		var fingerprint, transitionKind string
		switch event.EventType {
		case "GOAL_PROGRESS_EVALUATED":
			var payload events.GoalProgressEvaluatedPayload
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return err
			}
			if field != "mission_id" {
				return fmt.Errorf("unknown Goal identity %s", field)
			}
			payload.MissionID = core.ID(target)
			payload.Fingerprint, err = payload.ExpectedFingerprint()
			value, fingerprint, transitionKind = payload, payload.Fingerprint, "GOAL_ACHIEVED"
		case "WORK_COMPLETION_EVALUATED":
			var payload events.WorkCompletionEvidencePayload
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return err
			}
			switch field {
			case "work_id":
				payload.WorkID = core.ID(target)
			case "intent_id":
				payload.IntentID = core.ID(target)
			case "tasks.task_id":
				if len(payload.Tasks) != 1 {
					return fmt.Errorf("expected one Work Task witness")
				}
				payload.Tasks[0].TaskID = core.ID(target)
			default:
				return fmt.Errorf("unknown Work identity %s", field)
			}
			payload.Fingerprint, err = payload.ExpectedFingerprint()
			value, fingerprint, transitionKind = payload, payload.Fingerprint, "WORK_COMPLETED"
		default:
			return fmt.Errorf("unknown aggregate event %s", event.EventType)
		}
		if err != nil {
			return err
		}
		body, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, id); err != nil {
			return err
		}
		var transitionID string
		if err := tx.QueryRowContext(t.Context(), `SELECT event_id FROM events WHERE event_type=? AND json_extract(payload,'$.detail.evidence_event_ref')=?`, transitionKind, id).Scan(&transitionID); err != nil {
			return err
		}
		transition, found, err := eventByID(t.Context(), tx, transitionID)
		if err != nil {
			return fmt.Errorf("read aggregate transition: %w", err)
		}
		if !found {
			return fmt.Errorf("aggregate transition %s not found", transitionID)
		}
		projection, _, err := events.AdmittedProjection(transition)
		if err != nil {
			return err
		}
		detail, err := json.Marshal(events.WorkCompletionTransitionPayload{EvidenceEventRef: id, Fingerprint: fingerprint})
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

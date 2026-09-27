package ledger

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

// ChangeAggregateEvidenceForTest changes one owned reference while preserving
// the evaluation fingerprint and its consuming transition's exact admission.
func ChangeAggregateEvidenceForTest(t *testing.T, store *SQLite, id, field, target string) {
	t.Helper()
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		event, found, err := eventByID(t.Context(), tx, id)
		if err != nil || !found {
			return fmt.Errorf("read aggregate evidence: %v", err)
		}
		var value any
		var fingerprint, transitionKind string
		switch event.EventType {
		case "GOAL_PROGRESS_EVALUATED":
			var payload events.GoalProgressEvaluatedPayload
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return err
			}
			if len(payload.Criteria) != 1 || len(payload.WorkEvidenceRefs) != 1 {
				return fmt.Errorf("expected one Goal criterion and Work witness")
			}
			switch field {
			case "work_evidence_refs":
				payload.WorkEvidenceRefs = []string{target}
			case "criteria.work_evidence_refs":
				payload.Criteria[0].WorkEvidenceRefs = []string{target}
			default:
				return fmt.Errorf("unknown Goal reference %s", field)
			}
			payload.Fingerprint, err = payload.ExpectedFingerprint()
			value, fingerprint, transitionKind = payload, payload.Fingerprint, "GOAL_ACHIEVED"
		case "WORK_COMPLETION_EVALUATED":
			var payload events.WorkCompletionEvidencePayload
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return err
			}
			if len(payload.Tasks) != 1 {
				return fmt.Errorf("expected one Work Task witness")
			}
			switch field {
			case "tasks.verification_event_ref":
				payload.Tasks[0].VerificationEventRef = target
			case "tasks.completion_event_ref":
				payload.Tasks[0].CompletionEventRef = target
			default:
				return fmt.Errorf("unknown Work reference %s", field)
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
		if err != nil || !found {
			return fmt.Errorf("read aggregate transition: %v", err)
		}
		projection, _, err := events.AdmittedProjection(transition)
		if err != nil {
			return err
		}
		detail := events.WorkCompletionTransitionPayload{EvidenceEventRef: id, Fingerprint: fingerprint}
		encodedDetail, err := json.Marshal(detail)
		if err != nil {
			return err
		}
		sealed, err := events.SealProjectionEvent(transition, projection.Projection, encodedDetail)
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

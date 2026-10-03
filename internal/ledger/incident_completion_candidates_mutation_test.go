package ledger

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

// InsertCompletionCandidateForTest inserts a distinct, unreferenced candidate
// at a chosen sequence, resealing the healthy writer history displaced by it.
func InsertCompletionCandidateForTest(t *testing.T, store *SQLite, source events.Event, insertion int64, organization, task, correlation string) {
	source.EventID = "extra-completion-candidate"
	source.OrganizationID, source.TaskID, source.CorrelationID = organization, task, correlation
	source.SourceActorID, source.SourceExecutionID = "runtime", "distinct-execution"
	source.RecipientScope, source.RecipientID = "", ""
	source.AuthorizationRefs, source.ArtifactRefs = nil, nil
	source.Payload = []byte(`{}`)
	insertCandidateForTest(t, store, source, insertion)
}

// InsertReviewCandidateForTest preserves the foreign writer's independent
// identities and evidence, changing only its candidate correlation and position.
func InsertReviewCandidateForTest(t *testing.T, store *SQLite, source events.Event, insertion int64, correlation string) {
	source.EventID = "extra-review-candidate"
	source.CorrelationID = correlation
	insertCandidateForTest(t, store, source, insertion)
}

// DamageCompletionAnchorForTest changes only ordinary canonical evidence; the
// terminal projection admissions and inserted foreign candidate remain intact.
func DamageCompletionAnchorForTest(t *testing.T, store *SQLite, id, damage string) {
	t.Helper()
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	var anchor int64
	for _, event := range stream {
		if event.EventID == id {
			anchor = event.Sequence
		}
	}
	if anchor == 0 {
		t.Fatal("canonical completion anchor missing before mutation")
	}
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		var err error
		switch damage {
		case "missing":
			_, err = tx.ExecContext(t.Context(), `DELETE FROM events WHERE event_id=?`, id)
		case "invalid":
			_, err = tx.ExecContext(t.Context(), `UPDATE events SET event_type='AUDIT_NOTE' WHERE event_id=?`, id)
		default:
			return fmt.Errorf("unknown completion anchor damage %s", damage)
		}
		if err != nil {
			return err
		}
		if damage == "missing" {
			for _, event := range stream {
				if event.Sequence <= anchor {
					continue
				}
				event.Sequence--
				if err := moveCandidateEventForTest(t, tx, event); err != nil {
					return err
				}
			}
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	}); err != nil {
		t.Fatal(err)
	}
}

func insertCandidateForTest(t *testing.T, store *SQLite, source events.Event, insertion int64) {
	t.Helper()
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	err = store.withTx(t.Context(), func(tx *sql.Tx) error {
		for i := len(stream) - 1; i >= 0; i-- {
			event := stream[i]
			if event.Sequence < insertion {
				continue
			}
			event.Sequence++
			if err := moveCandidateEventForTest(t, tx, event); err != nil {
				return err
			}
		}
		authorization, err := json.Marshal(source.AuthorizationRefs)
		if err != nil {
			return err
		}
		artifacts, err := json.Marshal(source.ArtifactRefs)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(t.Context(), `INSERT INTO events(sequence,event_id,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, insertion, source.EventID, source.OrganizationID, source.EventType, source.SourceActorID, source.SourceExecutionID, source.RecipientScope, source.RecipientID, source.TaskID, authorization, artifacts, []byte(source.Payload), source.CorrelationID, source.CreatedAt.Format("2006-01-02T15:04:05.999999999Z07:00"), source.SchemaVersion)
		if err != nil {
			return fmt.Errorf("insert completion candidate: %w", err)
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func moveCandidateEventForTest(t *testing.T, tx *sql.Tx, event events.Event) error {
	// Decode with the original sequence, then reseal at the moved sequence.
	original, found, err := eventByID(t.Context(), tx, event.EventID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("candidate fixture event disappeared")
	}
	projection, present, err := events.AdmittedProjection(original)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(t.Context(), `UPDATE events SET sequence=? WHERE event_id=?`, event.Sequence, event.EventID); err != nil {
		return err
	}
	if !present {
		return nil
	}
	sealed, err := events.SealProjectionEvent(event, projection.Projection, projection.Detail)
	if err != nil {
		return err
	}
	body, err := json.Marshal(sealed)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, event.EventID); err != nil {
		return err
	}
	_, err = tx.ExecContext(t.Context(), `UPDATE records SET admission_fingerprint=? WHERE admission_event_id=?`, sealed.Admission.Fingerprint, event.EventID)
	return err
}

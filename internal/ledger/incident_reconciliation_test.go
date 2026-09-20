package ledger

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

func TestIncidentReconciledRowBinding(t *testing.T) {
	for _, mutation := range []string{"state", "charge"} {
		t.Run(mutation, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "reconciled.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.ActivateInferencePolicy(t.Context(), testInferencePolicy(time.Now().UTC())); err != nil {
				t.Fatal(err)
			}
			reservation, err := store.ReserveInference(t.Context(), testInferenceRequest("ordinary"))
			if err != nil {
				t.Fatal(err)
			}
			usage := testInferenceUsage()
			if _, err := store.ReconcileInference(t.Context(), reservation, &usage, inference.ReconciliationCompleted); err != nil {
				t.Fatal(err)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "organization-1", "work-1", 256); err != nil {
				t.Fatalf("real reconciled history was rejected: %v", err)
			}
			// These remain internally valid row states; only the exact durable
			// reconciliation proves which state and charge actually committed.
			statement := `UPDATE inference_reservations SET charged_input_tokens=0,charged_output_tokens=0,charged_cost_nano_usd=0`
			if mutation == "state" {
				statement = `UPDATE inference_reservations SET state='NOT_SENT',charged_input_tokens=0,charged_output_tokens=0,charged_cost_nano_usd=0`
			}
			if _, err := store.db.ExecContext(t.Context(), statement); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if _, err := store.VerifiedIncidentEvents(t.Context(), "organization-1", "work-1", 256); err == nil {
				t.Fatal("mutable accounting replaced the committed reconciliation")
			}
		})
	}
}

func TestIncidentReconciliationStates(t *testing.T) {
	states := []string{inferenceStateReserved, "RECOVERED", string(inference.ReconciliationCompleted), string(inference.ReconciliationNotSent), string(inference.ReconciliationUncertain), string(inference.ReconciliationViolation), string(inference.ReconciliationTerminalFailed), string(inference.ReconciliationTerminalIncomplete), string(inference.ReconciliationTerminalFailedNoUsage), string(inference.ReconciliationTerminalIncompleteNoUsage)}
	for _, state := range states {
		t.Run(state, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if err := store.ActivateInferencePolicy(t.Context(), testInferencePolicy(time.Now().UTC())); err != nil {
				t.Fatal(err)
			}
			reservation, err := store.ReserveInference(t.Context(), testInferenceRequest("ordinary"))
			if err != nil {
				t.Fatal(err)
			}
			switch state {
			case inferenceStateReserved:
			case "RECOVERED":
				if count, err := store.RecoverInferenceReservations(t.Context(), "organization-1"); err != nil || count != 1 {
					t.Fatalf("recover inference: count=%d err=%v", count, err)
				}
			default:
				var usage *events.InferenceUsageRecordedPayload
				switch inference.Reconciliation(state) {
				case inference.ReconciliationCompleted, inference.ReconciliationTerminalFailed, inference.ReconciliationTerminalIncomplete:
					value := testInferenceUsage()
					usage = &value
				case inference.ReconciliationNotSent, inference.ReconciliationUncertain, inference.ReconciliationViolation, inference.ReconciliationTerminalFailedNoUsage, inference.ReconciliationTerminalIncompleteNoUsage:
				}
				_, err := store.ReconcileInference(t.Context(), reservation, usage, inference.Reconciliation(state))
				if state == string(inference.ReconciliationViolation) {
					if err == nil || !strings.Contains(err.Error(), "violated") {
						t.Fatalf("violation writer did not report its committed violation: %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "organization-1", "work-1", 256)
			if err != nil || len(snapshot.Admissions) != 1 {
				t.Fatalf("valid %s history rejected: admissions=%d err=%v", state, len(snapshot.Admissions), err)
			}
		})
	}
}

func TestIncidentReconciliationHistory(t *testing.T) {
	mutations := []string{"missing", "duplicate", "orphan", "before-reservation", "malformed-then-valid", "wrong-actor", "wrong-task", "wrong-execution", "wrong-organization", "wrong-correlation", "active-row", "extra-orphan", "unrelated-orphan"}
	for _, mutation := range mutations {
		t.Run(mutation, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if err := store.ActivateInferencePolicy(t.Context(), testInferencePolicy(time.Now().UTC())); err != nil {
				t.Fatal(err)
			}
			reservation, err := store.ReserveInference(t.Context(), testInferenceRequest("ordinary"))
			if err != nil {
				t.Fatal(err)
			}
			usage := testInferenceUsage()
			if _, err := store.ReconcileInference(t.Context(), reservation, &usage, inference.ReconciliationCompleted); err != nil {
				t.Fatal(err)
			}
			stream, err := store.Events(t.Context(), "work-1")
			if err != nil || len(stream) != 2 {
				t.Fatalf("read reserved/reconciled pair: count=%d err=%v", len(stream), err)
			}
			reserved, reconciled := stream[0], stream[1]
			draft := events.TrustedDraft{OrganizationID: reconciled.OrganizationID, SourceActorID: reconciled.SourceActorID, SourceExecutionID: reconciled.SourceExecutionID, TaskID: reconciled.TaskID, CorrelationID: reconciled.CorrelationID, EventType: reconciled.EventType, Payload: reconciled.Payload}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				var statement string
				switch mutation {
				case "missing":
					statement = `UPDATE events SET event_type='AUDIT_NOTE',payload=CAST('{}' AS BLOB) WHERE event_type='INFERENCE_RECONCILED'`
				case "orphan":
					statement = `UPDATE events SET event_type='AUDIT_NOTE',payload=CAST('{}' AS BLOB) WHERE event_type='INFERENCE_RESERVED'`
				case "malformed-then-valid":
					statement = `UPDATE events SET payload=CAST('{}' AS BLOB) WHERE event_type='INFERENCE_RECONCILED'`
				case "wrong-actor":
					statement = `UPDATE events SET source_actor_id='model' WHERE event_type='INFERENCE_RECONCILED'`
				case "wrong-task":
					statement = `UPDATE events SET task_id='other-task' WHERE event_type='INFERENCE_RECONCILED'`
				case "wrong-execution":
					statement = `UPDATE events SET source_execution_id='other-call' WHERE event_type='INFERENCE_RECONCILED'`
				case "wrong-organization":
					statement = `UPDATE events SET organization_id='other-org' WHERE event_type='INFERENCE_RECONCILED'`
				case "wrong-correlation":
					statement = `UPDATE events SET correlation_id='other-run' WHERE event_type='INFERENCE_RECONCILED'`
				case "active-row":
					statement = `UPDATE inference_reservations SET state='RESERVED',charged_input_tokens=reserved_input_tokens,charged_output_tokens=reserved_output_tokens,charged_cost_nano_usd=reserved_cost_nano_usd`
				case "before-reservation":
					if _, err := tx.ExecContext(t.Context(), `UPDATE events SET sequence=-sequence WHERE event_id IN (?,?)`, reserved.EventID, reconciled.EventID); err != nil {
						return err
					}
					if _, err := tx.ExecContext(t.Context(), `UPDATE events SET sequence=CASE event_id WHEN ? THEN ? ELSE ? END WHERE sequence<0`, reserved.EventID, reconciled.Sequence, reserved.Sequence); err != nil {
						return err
					}
				}
				if statement != "" {
					if _, err := tx.ExecContext(t.Context(), statement); err != nil {
						return err
					}
				}
				if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
					return err
				}
				if err := rebuildEventIntegrity(t.Context(), tx); err != nil {
					return err
				}
				switch mutation {
				case "extra-orphan", "unrelated-orphan":
					var payload events.InferenceReconciledPayload
					if err := json.Unmarshal(reconciled.Payload, &payload); err != nil {
						return err
					}
					payload.ReservationID = "missing-reservation"
					draft.Payload = payload
					if mutation == "unrelated-orphan" {
						draft.OrganizationID = "other-org"
					}
				case "duplicate", "malformed-then-valid":
				default:
					return nil
				}
				_, err := appendEvent(t.Context(), tx, draft)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			_, err = store.VerifiedIncidentEvents(t.Context(), "organization-1", "work-1", 256)
			if mutation == "unrelated-orphan" {
				if err != nil {
					t.Fatalf("unrelated reconciliation affected selected incident: %v", err)
				}
			} else if err == nil {
				t.Fatalf("accepted %s reconciliation history", mutation)
			}
		})
	}
}

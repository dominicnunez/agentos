package ledger

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

// A real admitted model lifecycle anchors each retained-history mutation.
// Every family must be selected before its owning validator can reject it.
func TestIncidentExecutionEvidenceFamilies(t *testing.T) {
	families := []string{"EXECUTION_STARTED", "EXECUTION_CONTEXT_MANIFESTED", "EXECUTION_FINISHED", "TASK_EXECUTION_SUSPENDED", "PLANNING_CONTEXT_MANIFESTED", "INTENT_NORMALIZATION_CONTEXT_MANIFESTED", "MODEL_STOP_REQUESTED", "MODEL_STOP_UNCERTAIN", "MODEL_STOP_CONFIRMED", "EXECUTION_STOP_REQUESTED", "EXECUTION_STOP_UNCERTAIN", "EXECUTION_STOP_CONFIRMED", "TOOL_OUTCOME_RECORDED", "INFERENCE_USAGE_RECORDED", "INFERENCE_RESERVED", "INFERENCE_RECONCILED", "INFERENCE_NOT_SENT", "PLAN_CREATED", "PLANNING_FAILED", "INTENT_DRAFTED", "INTENT_NORMALIZATION_FAILED", "PLANNING_CONTAINMENT_SUSPENDED", "INTENT_NORMALIZATION_SUSPENDED", "FUTURE_ORDINARY_ACTIVITY"}
	for _, family := range families {
		t.Run(family, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			manifest := modelStopManifest(t, store, false, "incident-call")
			if _, err := store.VerifiedIncidentEvents(t.Context(), manifest.OrganizationID, manifest.CorrelationID, 256); err != nil {
				t.Fatal(err)
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				event, err := appendEvent(t.Context(), tx, events.TrustedDraft{OrganizationID: manifest.OrganizationID, SourceActorID: "runtime", SourceExecutionID: manifest.SourceExecutionID, CorrelationID: "hidden-run", EventType: "AUDIT_NOTE", Payload: map[string]any{}})
				if err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `UPDATE events SET event_type=? WHERE event_id=?`, family, event.EventID); err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
					return err
				}
				return rebuildEventIntegrity(t.Context(), tx)
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), manifest.OrganizationID, manifest.CorrelationID, 256); err == nil {
				t.Fatal("linked execution evidence was omitted")
			}
		})
	}
}

func TestIncidentExecutionReferenceCompleteness(t *testing.T) {
	links := []string{"context_event_ref", "execution_start_ref", "execution_manifest_ref", "stop_request_ref", "usage_event_ref", "outcome_event_ref", "finish_event_ref", "evidence_event_ref", "observed_effect.stop_request_ref", "detail.stop_request_ref", "detail.execution_start_ref", "execution_id", "request_id", "task_id", "duplicate-key", "other-tenant", "independent"}
	for _, link := range links {
		t.Run(link, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			manifest := modelStopManifest(t, store, false, "incident-call")
			payload := map[string]any{}
			draft := events.TrustedDraft{OrganizationID: manifest.OrganizationID, SourceActorID: "runtime", CorrelationID: "hidden-run", EventType: "PLANNING_FAILED", Payload: payload}
			switch link {
			case "task_id":
				draft.TaskID = manifest.TaskID
			case "execution_id", "request_id":
				payload[link] = manifest.SourceExecutionID
			case "other-tenant":
				draft.OrganizationID, draft.SourceExecutionID, draft.TaskID = "other-tenant", manifest.SourceExecutionID, manifest.TaskID
				payload["evidence_event_ref"] = manifest.EventID
			case "independent":
				draft.SourceExecutionID, draft.TaskID = "other-call", "other-task"
				payload["evidence_event_ref"] = "unrelated-manifest"
			case "duplicate-key":
			default:
				if prefix, field, ok := strings.Cut(link, "."); ok {
					payload[prefix] = map[string]any{field: manifest.EventID}
				} else {
					payload[link] = manifest.EventID
				}
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				event, err := appendEvent(t.Context(), tx, draft)
				if err != nil || link != "duplicate-key" {
					return err
				}
				// SQLite json_extract would see only the first key. Discovery
				// must retain the second contradictory identity as well.
				body := fmt.Sprintf(`{"evidence_event_ref":"unrelated","evidence_event_ref":%q}`, manifest.EventID)
				if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, []byte(body), event.EventID); err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
					return err
				}
				return rebuildEventIntegrity(t.Context(), tx)
			}); err != nil {
				t.Fatal(err)
			}
			_, err = store.VerifiedIncidentEvents(t.Context(), manifest.OrganizationID, manifest.CorrelationID, 256)
			if link == "other-tenant" || link == "independent" {
				if err != nil {
					t.Fatalf("unrelated identity poisoned incident: %v", err)
				}
			} else if err == nil {
				t.Fatal("strict reference escaped its selected execution")
			}
		})
	}
}

func TestIncidentHiddenInferenceExecution(t *testing.T) {
	for _, purpose := range []inference.Purpose{inference.PurposePlanning, inference.PurposeIntentNormalization} {
		t.Run(string(purpose), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "inference.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			modelStopManifest(t, store, purpose == inference.PurposeIntentNormalization, "incident-call")
			policy := testInferencePolicy(time.Now().UTC())
			policy.OrganizationID, policy.Provider, policy.Model, policy.ExecutionProfileVersion = "org-1", "provider", "model", "v1"
			if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			request := testInferenceRequest("incident-call")
			request.Scope.OrganizationID, request.Scope.CorrelationID, request.Scope.TaskID, request.Scope.IntentID = "org-1", "model-stop", "task-model-stop", "intent-model-stop"
			request.Scope.Purpose = purpose
			request.Descriptor.Provider, request.Descriptor.Model, request.Descriptor.ExecutionProfileVersion = "provider", "model", "v1"
			if _, err := store.ReserveInference(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "model-stop", 256); err != nil {
				t.Fatal(err)
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				if _, err := tx.ExecContext(t.Context(), `UPDATE events SET correlation_id='other-run' WHERE event_type='INFERENCE_RESERVED'`); err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `UPDATE inference_reservations SET correlation_id='other-run'`); err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
					return err
				}
				return rebuildEventIntegrity(t.Context(), tx)
			}); err != nil {
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
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "model-stop", 256); err == nil {
				t.Fatal("selected execution lost both its reservation and accounting to another correlation")
			}
		})
	}
}

func TestIncidentHiddenLegacyHoldOutcome(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	task := incidentTestExecution(t, store)
	hold := appendHistoricalInferenceFreeze(t, store, "org-1", 1, true)
	var sequence int64
	if err := store.db.QueryRowContext(t.Context(), `SELECT sequence FROM events WHERE event_id=?`, hold.EventRef).Scan(&sequence); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	outcome := core.ToolOutcome{ToolInvocationID: "legacy-stop", ToolID: "runtime-containment", Status: core.OutcomeFailed, PostconditionStatus: core.PostconditionNotChecked, Retryability: core.NotRetryable, ErrorClass: "security_hold", StartedAt: now, FinishedAt: now, ObservedEffect: core.ExecutionInterruptionEvidence{Hold: &core.SecurityHoldCause{OrganizationID: "org-1", EventRef: hold.EventRef, Sequence: sequence}, LocalExecutionStopped: true, ExternalEffectsStatus: "REQUIRES_RECONCILIATION"}}
	event, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", SourceActorID: "runtime", TaskID: string(task.ID), SourceExecutionID: fmt.Sprintf("execution-%s-v2", task.ID), CorrelationID: "stop-work", EventType: "TOOL_OUTCOME_RECORDED", Payload: outcome})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256); err != nil {
		t.Fatalf("valid legacy hold rejected: %v", err)
	}
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET correlation_id='other-run' WHERE event_id=?`, event.EventID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	}); err != nil {
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
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256); err == nil {
		t.Fatal("legacy hold evidence escaped its selected execution")
	}
}

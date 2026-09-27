package ledger

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentForeignHoldClaims(t *testing.T) {
	for _, family := range []string{"task", "planning", "normalization", "outcome", "legacy-outcome", "ordinary-outcome"} {
		for _, mode := range []string{"foreign", "same-org", "unrelated"} {
			t.Run(family+"/"+mode, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "holds.db")
				store, err := Open(path)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = store.Close() }()
				var task core.Task
				var manifest events.Event
				if family == "planning" || family == "normalization" {
					manifest = modelStopManifest(t, store, family == "normalization", "hold-model")
				} else {
					task = incidentTestExecution(t, store)
				}
				organization := "org-2"
				if mode == "same-org" {
					organization = "org-1"
				}
				if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: organization, EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "selected", Payload: map[string]string{"message": "Selected history"}}); err != nil {
					t.Fatal(err)
				}
				var ordinary events.Event
				if family == "ordinary-outcome" {
					now := time.Now().UTC()
					outcome := core.ToolOutcome{ToolInvocationID: "ordinary", ToolID: "ordinary-tool", Status: core.OutcomeFailed, PostconditionStatus: core.PostconditionNotChecked, Retryability: core.NotRetryable, ErrorClass: "ordinary-failure", StartedAt: now, FinishedAt: now, ObservedEffect: map[string]any{"hold": map[string]string{"event_ref": "informational"}}}
					ordinary, err = store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "TOOL_OUTCOME_RECORDED", SourceActorID: "runtime", TaskID: string(task.ID), SourceExecutionID: fmt.Sprintf("execution-%s-v2", task.ID), CorrelationID: "stop-work", Payload: outcome})
					if err != nil {
						t.Fatal(err)
					}
				}
				own := appendHistoricalInferenceFreeze(t, store, "org-1", 1, true)
				var ownSequence int64
				if err := store.db.QueryRowContext(t.Context(), `SELECT sequence FROM events WHERE event_id=?`, own.EventRef).Scan(&ownSequence); err != nil {
					t.Fatal(err)
				}
				freezes := []events.OrganizationFreezeAdmission{{OrganizationID: "org-1", EventRef: own.EventRef, Sequence: ownSequence, Frozen: true, Version: 1}}
				target := own.EventRef
				if organization != "org-1" {
					selectedHold := appendHistoricalInferenceFreeze(t, store, organization, 1, true)
					target = selectedHold.EventRef
					var sequence int64
					if err := store.db.QueryRowContext(t.Context(), `SELECT sequence FROM events WHERE event_id=?`, target).Scan(&sequence); err != nil {
						t.Fatal(err)
					}
					freezes = append(freezes, events.OrganizationFreezeAdmission{OrganizationID: core.ID(organization), EventRef: target, Sequence: sequence, Frozen: true, Version: 1})
				}
				candidate := ordinary
				field := "hold.event_ref"
				if family == "ordinary-outcome" {
					field = "observed_effect.hold.event_ref"
				}
				if manifest.EventID != "" {
					candidate, _, err = store.RequestModelStop(t.Context(), manifest.EventID, "security_hold")
				} else {
					var request events.Event
					if family == "task" || family == "outcome" {
						request, err = store.RequestExecutionStop(t.Context(), "org-1", string(task.ID), "stop-work", fmt.Sprintf("execution-%s-v2", task.ID), "security_hold")
						candidate = request
					}
					if err == nil && family != "task" && family != "ordinary-outcome" {
						now := time.Now().UTC()
						evidence := core.ExecutionInterruptionEvidence{Hold: &core.SecurityHoldCause{OrganizationID: "org-1", EventRef: own.EventRef, Sequence: ownSequence}, LocalExecutionStopped: true, ExternalEffectsStatus: "REQUIRES_RECONCILIATION"}
						outcome := core.ToolOutcome{ToolInvocationID: "held-outcome", ToolID: "runtime-containment", Status: core.OutcomeFailed, PostconditionStatus: core.PostconditionNotChecked, Retryability: core.NotRetryable, ErrorClass: "security_hold", StartedAt: now, FinishedAt: now}
						if family == "outcome" {
							evidence.StopRequestRef = request.EventID
							outcome.ObservedEffect = evidence
							var confirmed events.Event
							confirmed, err = store.RecordExecutionStop(t.Context(), request.EventID, &outcome, nil)
							if err == nil {
								var result events.ExecutionStopResult
								if err = json.Unmarshal(confirmed.Payload, &result); err == nil {
									candidate, _, err = eventByID(t.Context(), store.db, result.OutcomeEventRef)
								}
							}
						} else {
							outcome.ObservedEffect = evidence
							draft := events.TrustedDraft{OrganizationID: "org-1", EventType: "TOOL_OUTCOME_RECORDED", SourceActorID: "runtime", TaskID: string(task.ID), SourceExecutionID: fmt.Sprintf("execution-%s-v2", task.ID), CorrelationID: "stop-work", Payload: outcome}
							candidate, err = store.Append(t.Context(), draft)
						}
						field = "observed_effect.hold.event_ref"
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				validateHoldClaimOwners(t, store, freezes, false)
				if mode == "foreign" || family == "ordinary-outcome" {
					changeIncidentHoldRef(t, store, candidate, field, target)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				store, err = Open(path)
				if err != nil {
					t.Fatal(err)
				}
				invalid := mode == "foreign" && family != "ordinary-outcome"
				validateHoldClaimOwners(t, store, freezes, invalid)
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), organization, "selected", 256)
				if invalid {
					if err == nil {
						t.Fatal("foreign hold claim escaped selected freeze history")
					}
					if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
						t.Fatal("invalid hold claim returned a partial snapshot")
					}
				} else {
					if err != nil {
						t.Fatalf("valid control: %v", err)
					}
					for _, event := range snapshot.DependencyEvents {
						if event.EventID == candidate.EventID {
							t.Fatal("shared or unrelated hold expanded a valid stop")
						}
					}
				}
			})
		}
	}
}

func validateHoldClaimOwners(t *testing.T, store *SQLite, freezes []events.OrganizationFreezeAdmission, invalid bool) {
	t.Helper()
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	errs := []error{events.ValidateExecutionStops(stream, freezes), events.ValidateModelStops(stream, freezes), events.ValidateSecurityHoldOutcomes(stream, freezes)}
	var rejected bool
	for _, err := range errs {
		if err != nil {
			rejected = true
			if !invalid {
				t.Fatalf("valid owner history: %v", err)
			}
		}
	}
	if invalid && !rejected {
		t.Fatal("mutated claim did not fail a full owning validator")
	}
}

func changeIncidentHoldRef(t *testing.T, store *SQLite, event events.Event, field, target string) {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(field, ".")
	object := payload
	for _, part := range parts[:len(parts)-1] {
		object = object[part].(map[string]any)
	}
	object[parts[len(parts)-1]] = target
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, event.EventID); err != nil {
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

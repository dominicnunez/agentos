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
)

func incidentStopRequest(t *testing.T, store *SQLite, family string) events.Event {
	t.Helper()
	if family != "task" {
		manifest := modelStopManifest(t, store, family == "normalization", "incident-call")
		request, _, err := store.RequestModelStop(t.Context(), manifest.EventID, "caller_cancelled")
		if err != nil {
			t.Fatal(err)
		}
		return request
	}
	task := stopTestExecution(t, store)
	request, err := store.RequestExecutionStop(t.Context(), "org-1", string(task.ID), "stop-work", fmt.Sprintf("execution-%s-v2", task.ID), "execution_cancelled")
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func incidentStopConfirmed(t *testing.T, store *SQLite, family string, request events.Event) events.Event {
	t.Helper()
	var result events.Event
	var err error
	now := time.Now().UTC()
	if family == "task" {
		outcome := core.ToolOutcome{ToolInvocationID: "incident-stop", ToolID: "runtime-containment", Status: core.OutcomeFailed, PostconditionStatus: core.PostconditionNotChecked, Retryability: core.NotRetryable, ErrorClass: "execution_cancelled", StartedAt: now, FinishedAt: now, ObservedEffect: core.ExecutionInterruptionEvidence{StopRequestRef: request.EventID, LocalExecutionStopped: true, ExternalEffectsStatus: "REQUIRES_RECONCILIATION"}}
		result, err = store.RecordExecutionStop(t.Context(), request.EventID, &outcome, nil)
	} else {
		result, err = store.RecordModelStop(t.Context(), request.EventID, &events.ModelStopReturn{LocalState: "RETURNED", ReturnedAt: now})
	}
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func incidentStopUncertain(t *testing.T, store *SQLite, family string, request events.Event) events.Event {
	t.Helper()
	var result events.Event
	var err error
	if family == "task" {
		result, err = store.RecordExecutionStop(t.Context(), request.EventID, nil, nil)
	} else {
		result, err = store.RecordModelStop(t.Context(), request.EventID, nil)
	}
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestIncidentStopCrossCorrelation(t *testing.T) {
	for _, family := range []string{"planning", "normalization", "task"} {
		t.Run(family, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "stops.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			request := incidentStopRequest(t, store, family)
			result := incidentStopUncertain(t, store, family, request)
			if _, err := store.VerifiedIncidentEvents(t.Context(), request.OrganizationID, request.CorrelationID, 256); err != nil {
				t.Fatalf("writer stop history was invalid: %v", err)
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				if _, err := tx.ExecContext(t.Context(), `UPDATE events SET correlation_id='other-run' WHERE event_id=?`, result.EventID); err != nil {
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
			full, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if family == "task" {
				err = events.ValidateExecutionStops(full, nil)
			} else {
				err = events.ValidateModelStops(full, nil)
			}
			if err == nil {
				t.Fatal("corrupt fixture did not violate the shared full stop validator")
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), request.OrganizationID, request.CorrelationID, 256); err == nil {
				t.Fatal("stop result escaped its exact request by changing correlation")
			}
		})
	}
}

func TestIncidentStopLinkedHistory(t *testing.T) {
	variants := []string{"pending", "uncertain", "confirmed", "uncertain-confirmed", "duplicate-uncertain", "duplicate-confirmed", "early-invalid-late-confirmed", "cross-task", "cross-execution", "cross-all-envelope", "cross-tenant", "wrong-request", "cross-request", "duplicate-key-link", "malformed-linked", "malformed-unrelated"}
	for _, family := range []string{"planning", "normalization", "task"} {
		for _, variant := range variants {
			t.Run(family+"/"+variant, func(t *testing.T) {
				store, err := Open(":memory:")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				request := incidentStopRequest(t, store, family)
				valid := variant == "pending" || variant == "uncertain" || variant == "confirmed" || variant == "uncertain-confirmed" || variant == "cross-tenant" || variant == "malformed-unrelated"
				var earlier events.Event
				if variant == "uncertain" || variant == "uncertain-confirmed" || variant == "duplicate-uncertain" || variant == "early-invalid-late-confirmed" {
					earlier = incidentStopUncertain(t, store, family, request)
				}
				var confirmed events.Event
				if variant == "confirmed" || variant == "uncertain-confirmed" || variant == "duplicate-confirmed" || variant == "early-invalid-late-confirmed" {
					confirmed = incidentStopConfirmed(t, store, family, request)
				}
				if !valid || variant == "cross-tenant" || variant == "malformed-unrelated" {
					draft := modelStopDraft(request)
					draft.CorrelationID = "other-run"
					draft.EventType = "MODEL_STOP_UNCERTAIN"
					if family == "task" {
						draft.EventType = "EXECUTION_STOP_UNCERTAIN"
					}
					draft.Payload = map[string]string{"stop_request_ref": request.EventID}
					switch variant {
					case "duplicate-confirmed":
						draft.EventType, draft.Payload = confirmed.EventType, confirmed.Payload
					case "cross-task":
						draft.TaskID = "other-task"
					case "cross-execution":
						draft.SourceExecutionID = "other-execution"
					case "cross-all-envelope":
						draft.TaskID, draft.SourceExecutionID = "other-task", "other-execution"
					case "cross-tenant", "malformed-unrelated":
						draft.OrganizationID = "other-org"
					case "wrong-request":
						draft.Payload = map[string]string{"stop_request_ref": "missing-request"}
					case "cross-request":
						draft.EventType, draft.Payload = request.EventType, request.Payload
						draft.SourceExecutionID, draft.TaskID = "other-execution", "other-task"
					case "duplicate-key-link":
						draft.SourceExecutionID, draft.TaskID = "other-execution", "other-task"
					}
					if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
						if variant == "early-invalid-late-confirmed" {
							if _, err := tx.ExecContext(t.Context(), `UPDATE events SET correlation_id='other-run' WHERE event_id=?`, earlier.EventID); err != nil {
								return err
							}
						} else {
							extra, err := appendEvent(t.Context(), tx, draft)
							if err != nil {
								return err
							}
							if variant == "duplicate-key-link" {
								body := []byte(fmt.Sprintf(`{"stop_request_ref":"unrelated","stop_request_ref":%q}`, request.EventID))
								if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, extra.EventID); err != nil {
									return err
								}
							}
							if strings.HasPrefix(variant, "malformed-") {
								if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=CAST('{' AS BLOB) WHERE event_id=?`, extra.EventID); err != nil {
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
				_, err = store.VerifiedIncidentEvents(t.Context(), request.OrganizationID, request.CorrelationID, 256)
				if valid && err != nil {
					t.Fatalf("valid or isolated stop evidence rejected: %v", err)
				}
				if !valid && err == nil {
					t.Fatal("invalid linked stop evidence escaped full validation")
				}
			})
		}
	}
}

func TestIncidentStopLinkedEvidenceLimits(t *testing.T) {
	for _, variant := range []string{"events", "bytes"} {
		t.Run(variant, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			request := incidentStopRequest(t, store, "planning")
			work, err := store.Events(t.Context(), request.CorrelationID)
			if err != nil {
				t.Fatal(err)
			}
			limit := len(work)
			payload := map[string]string{"stop_request_ref": request.EventID}
			if variant == "bytes" {
				limit = 256
				payload["padding"] = strings.Repeat("x", 2<<20)
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				draft := modelStopDraft(request)
				draft.CorrelationID, draft.EventType, draft.Payload = "other-run", "MODEL_STOP_UNCERTAIN", payload
				_, err := appendEvent(t.Context(), tx, draft)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), request.OrganizationID, request.CorrelationID, limit); err == nil || !strings.Contains(err.Error(), "exceeds") {
				t.Fatalf("linked evidence did not respect pre-allocation budget: %v", err)
			}
		})
	}
}

func TestIncidentStopWithoutRequest(t *testing.T) {
	for _, family := range []string{"planning", "normalization", "task"} {
		t.Run(family, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			var contextEvent events.Event
			eventType := "MODEL_STOP_UNCERTAIN"
			if family == "task" {
				task := stopTestExecution(t, store)
				contextEvent = events.Event{OrganizationID: "org-1", TaskID: string(task.ID), SourceExecutionID: fmt.Sprintf("execution-%s-v2", task.ID), CorrelationID: "stop-work"}
				eventType = "EXECUTION_STOP_UNCERTAIN"
			} else {
				contextEvent = modelStopManifest(t, store, family == "normalization", "incident-call")
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), contextEvent.OrganizationID, contextEvent.CorrelationID, 256); err != nil {
				t.Fatal(err)
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				draft := modelStopDraft(contextEvent)
				draft.CorrelationID, draft.EventType = "other-run", eventType
				draft.Payload = map[string]string{"stop_request_ref": "missing-request"}
				_, err := appendEvent(t.Context(), tx, draft)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), contextEvent.OrganizationID, contextEvent.CorrelationID, 256); err == nil {
				t.Fatal("orphan acknowledgement escaped its selected execution")
			}
		})
	}
}

func TestIncidentStopGrowth(t *testing.T) {
	for _, count := range []int{1, 32} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			for i := range count {
				manifest := modelStopManifest(t, store, false, fmt.Sprintf("attempt-%d", i))
				request, _, err := store.RequestModelStop(t.Context(), manifest.EventID, "caller_cancelled")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := store.RecordModelStop(t.Context(), request.EventID, &events.ModelStopReturn{LocalState: "NOT_STARTED"}); err != nil {
					t.Fatal(err)
				}
			}
			started := time.Now()
			for range 5 {
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "model-stop", 256)
				if err != nil || len(snapshot.Work.Events) != 3*count {
					t.Fatalf("stop snapshot history=%d err=%v", len(snapshot.Work.Events), err)
				}
			}
			t.Logf("%d model stop lifecycles; five complete incident reads=%s", count, time.Since(started))
		})
	}
}

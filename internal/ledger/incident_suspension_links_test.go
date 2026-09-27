package ledger

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentIncomingSuspensionDetail(t *testing.T) {
	for _, field := range []string{"stop_request_ref", "execution_start_ref", "request-start", "result-request", "confirmed-outcome_event_ref", "confirmed-usage_event_ref", "confirmed-finish_event_ref", "outcome-request", "ordinary-outcome", "legacy-tool-outcome", "legacy-execution_start_ref", "legacy-outcome_event_ref", "unrelated", "opaque-note"} {
		t.Run(field, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "suspension.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-2", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "selected-setup"}, ProjectionKind: "organization", RecordID: "org-2", Version: 1, Value: core.Organization{ID: "org-2", Name: "Selected", PolicyVersion: "v1", CreatedAt: time.Now().UTC()}}); err != nil {
				t.Fatal(err)
			}
			selected, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-2", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "selected", Payload: map[string]string{"message": "Selected evidence"}})
			if err != nil {
				t.Fatal(err)
			}
			legacy := field == "legacy-execution_start_ref" || field == "legacy-outcome_event_ref"
			var request, result events.Event
			if legacy {
				task := incidentTestExecution(t, store)
				appendLegacyEffectTestSuspension(t, store, task)
				request = events.Event{OrganizationID: "org-1", CorrelationID: "stop-work"}
			} else {
				request = incidentStopRequest(t, store, "task")
				if field == "result-request" {
					result = incidentStopUncertain(t, store, "task", request)
				}
				if strings.HasPrefix(field, "confirmed-") || field == "outcome-request" {
					result = incidentStopConfirmed(t, store, "task", request)
				}
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), request.OrganizationID, request.CorrelationID, 256); err != nil {
				t.Fatalf("writer stop history: %v", err)
			}
			full, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if err := events.ValidateExecutionStops(full, nil); err != nil {
				t.Fatalf("writer stop replay: %v", err)
			}
			var suspension, outcomeEvent events.Event
			for _, event := range full {
				if event.EventType == "TASK_EXECUTION_SUSPENDED" {
					suspension = event
				}
				if event.EventType == "TOOL_OUTCOME_RECORDED" {
					outcomeEvent = event
				}
			}
			if suspension.EventID == "" {
				t.Fatal("missing writer suspension")
			}
			corrupt := field == "stop_request_ref" || field == "execution_start_ref" || field == "request-start" || field == "result-request" || strings.HasPrefix(field, "confirmed-") || field == "outcome-request"
			if field == "stop_request_ref" || field == "execution_start_ref" {
				ChangeIncidentDetailForTest(t, store, suspension.EventID, field, selected.EventID)
			}
			if legacy {
				ChangeIncidentDetailForTest(t, store, suspension.EventID, strings.TrimPrefix(field, "legacy-"), selected.EventID)
			}
			if field == "request-start" {
				changeIncidentStopRef(t, store, request, "execution_start_ref", selected.EventID)
			}
			if field == "result-request" {
				changeIncidentStopRef(t, store, result, "stop_request_ref", selected.EventID)
			}
			if strings.HasPrefix(field, "confirmed-") {
				changeIncidentStopRef(t, store, result, strings.TrimPrefix(field, "confirmed-"), selected.EventID)
			}
			if field == "outcome-request" {
				changeIncidentStopRef(t, store, outcomeEvent, "observed_effect.stop_request_ref", selected.EventID)
			}
			if field == "ordinary-outcome" || field == "legacy-tool-outcome" {
				toolID, evidence := "ordinary-tool", map[string]string{"stop_request_ref": selected.EventID}
				if field == "legacy-tool-outcome" {
					toolID, evidence = "runtime-containment", map[string]string{"execution_start_ref": selected.EventID}
				}
				now := time.Now().UTC()
				outcome := core.ToolOutcome{ToolInvocationID: "unrelated-outcome", ToolID: toolID, Status: core.OutcomeFailed, PostconditionStatus: core.PostconditionNotChecked, Retryability: core.NotRetryable, ErrorClass: "execution_cancelled", StartedAt: now, FinishedAt: now, ObservedEffect: evidence}
				if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "TOOL_OUTCOME_RECORDED", SourceActorID: "runtime", CorrelationID: "unrelated-outcome", Payload: outcome}); err != nil {
					t.Fatal(err)
				}
			}
			if field == "opaque-note" {
				if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "opaque", Payload: map[string]any{"detail": map[string]string{"stop_request_ref": selected.EventID, "execution_start_ref": selected.EventID}}}); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			full, err = store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			replayErr := events.ValidateExecutionStops(full, nil)
			if corrupt && replayErr == nil {
				t.Fatalf("mutated stop contract did not fail expected owner: %v", replayErr)
			}
			if !corrupt && replayErr != nil {
				t.Fatalf("control stop replay: %v", replayErr)
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-2", "selected", 256)
			if corrupt {
				if err == nil {
					t.Fatal("foreign stop evidence naming selected event escaped incident validation")
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("invalid stop evidence returned a partial snapshot")
				}
			} else if err != nil {
				t.Fatalf("unrelated control rejected: %v", err)
			}
		})
	}
}

func changeIncidentStopRef(t *testing.T, store *SQLite, event events.Event, field, target string) {
	t.Helper()
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(target)
	if err != nil {
		t.Fatal(err)
	}
	if fields := strings.Split(field, "."); len(fields) == 2 {
		var nested map[string]json.RawMessage
		if err := json.Unmarshal(payload[fields[0]], &nested); err != nil {
			t.Fatal(err)
		}
		nested[fields[1]] = encoded
		payload[fields[0]], err = json.Marshal(nested)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		payload[field] = encoded
	}
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

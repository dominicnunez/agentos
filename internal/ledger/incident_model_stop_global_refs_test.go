package ledger

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentModelStopGlobalReferences(t *testing.T) {
	for _, variant := range []string{"context", "request", "usage"} {
		t.Run(variant, func(t *testing.T) {
			store, err := Open(filepath.Join(t.TempDir(), "model-stops.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })

			selectedContext := modelStopManifest(t, store, false, "selected-call")
			selectedRequest, _, err := store.RequestModelStop(t.Context(), selectedContext.EventID, "caller_cancelled")
			if err != nil {
				t.Fatal(err)
			}
			foreignDraft := modelStopDraft(selectedContext)
			foreignDraft.OrganizationID = "other-org"
			foreignDraft.CorrelationID = "other-run"
			foreignDraft.TaskID = "task-other-run"
			foreignDraft.SourceExecutionID = "other-call"
			foreignDraft.EventType = selectedContext.EventType
			foreignDraft.Payload = json.RawMessage(selectedContext.Payload)
			foreignContext, err := store.Append(t.Context(), foreignDraft)
			if err != nil {
				t.Fatal(err)
			}
			foreignRequest, _, err := store.RequestModelStop(t.Context(), foreignContext.EventID, "caller_cancelled")
			if err != nil {
				t.Fatal(err)
			}

			target := foreignRequest
			var changedPayload any = events.ModelStopRequest{ContextEventRef: selectedContext.EventID, ReasonClass: "caller_cancelled"}
			if variant != "context" {
				if variant == "usage" {
					usage := events.InferenceUsageRecordedPayload{Source: "provider", Provider: "provider", Model: "model", InputTokens: 1, TotalTokens: 1}
					returned := &events.ModelStopReturn{LocalState: "RETURNED", ReturnedAt: time.Now().UTC(), Usage: &usage}
					selectedResult, err := store.RecordModelStop(t.Context(), selectedRequest.EventID, returned)
					if err != nil {
						t.Fatal(err)
					}
					var selectedPayload events.ModelStopResult
					if err := json.Unmarshal(selectedResult.Payload, &selectedPayload); err != nil || selectedPayload.UsageEventRef == "" {
						t.Fatalf("selected confirmation lacks usage: payload=%+v err=%v", selectedPayload, err)
					}
					foreignResult, err := store.RecordModelStop(t.Context(), foreignRequest.EventID, returned)
					if err != nil {
						t.Fatal(err)
					}
					target = foreignResult
					result := events.ModelStopResult{}
					if err := json.Unmarshal(foreignResult.Payload, &result); err != nil {
						t.Fatal(err)
					}
					result.UsageEventRef = selectedPayload.UsageEventRef
					changedPayload = result
				} else {
					foreignResult, err := store.RecordModelStop(t.Context(), foreignRequest.EventID, nil)
					if err != nil {
						t.Fatal(err)
					}
					target = foreignResult
					changedPayload = events.ModelStopResult{StopRequestRef: selectedRequest.EventID}
				}
			}
			valid, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if err := events.ValidateModelStops(valid, nil); err != nil {
				t.Fatalf("writer history failed full model-stop validation: %v", err)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), selectedContext.OrganizationID, selectedContext.CorrelationID, 256); err != nil {
				t.Fatalf("writer history failed incident verification: %v", err)
			}
			if err := changeModelStopPayload(t, store, target.EventID, changedPayload); err != nil {
				t.Fatal(err)
			}

			full, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if err := events.ValidateModelStops(full, nil); err == nil {
				t.Fatal("corrupted global model-stop reference passed full validation")
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), selectedContext.OrganizationID, selectedContext.CorrelationID, 256); err == nil {
				t.Fatal("incident reader omitted a foreign model-stop event referencing selected global evidence")
		} else if !strings.Contains(err.Error(), "model stop") && !strings.Contains(err.Error(), "incident execution evidence crosses") && !strings.Contains(err.Error(), "incident dependency crosses organization") {
				t.Fatalf("incident rejected fixture for an unrelated reason: %v", err)
			}
		})
	}
}

func changeModelStopPayload(t *testing.T, store *SQLite, eventID string, payload any) error {
	t.Helper()
	return store.withTx(t.Context(), func(tx *sql.Tx) error {
		body, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, eventID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	})
}

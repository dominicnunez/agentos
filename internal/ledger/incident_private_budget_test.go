package ledger

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

func TestIncidentPrivateInferenceBudget(t *testing.T) {
	for _, sample := range []struct {
		name                  string
		reservations, padding int
		wantError             string
	}{
		{"dense", 300, 0, ""},
		{"aggregate-count", 1000, 0, "incident inference accounting exceeds byte limit"},
		{"aggregate-bytes", 800, 1, "incident inference accounting exceeds byte limit"},
	} {
		t.Run(sample.name, func(t *testing.T) {
			store, err := Open(filepath.Join(t.TempDir(), "budget.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			appendPrivateInferenceGoal(t, store)
			policy := testInferencePolicy(time.Now().UTC())
			policy.OrganizationID, policy.Provider, policy.Model, policy.ExecutionProfileVersion = "org-1", "provider", "model", "v1"
			policy.MaxConcurrentRequests = sample.reservations
			policy.MaxTokensPerWindow = 120*int64(sample.reservations) + 100
			policy.Pricing.MaxCostNanoUSDPerWindow = 400000 * int64(sample.reservations)
			if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < sample.reservations; i++ {
				id := fmt.Sprintf("private-%04d", i)
				if sample.padding != 0 {
					id += strings.Repeat("x", 512-len(id))
				}
				request := testInferenceRequest(id)
				request.Scope.OrganizationID, request.Scope.CorrelationID, request.Scope.TaskID, request.Scope.IntentID = "org-1", "model-stop", "task-model-stop", "intent-model-stop"
				request.Scope.Purpose = inference.PurposeIntentNormalization
				request.Descriptor.Provider, request.Descriptor.Model, request.Descriptor.ExecutionProfileVersion = "provider", "model", "v1"
				if _, err := store.ReserveInference(t.Context(), request); err != nil {
					t.Fatal(err)
				}
			}
			if sample.name == "aggregate-count" {
				for range 2080 {
					if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", CorrelationID: "model-stop", EventType: "AUDIT_NOTE", Payload: map[string]string{"text": "retained evidence"}}); err != nil {
						t.Fatal(err)
					}
				}
			}
			if sample.padding != 0 {
				var beforeEvents, beforeRows int64
				if err := store.db.QueryRowContext(t.Context(), `SELECT SUM(`+incidentEventBytes+`) FROM events WHERE correlation_id='model-stop'`).Scan(&beforeEvents); err != nil {
					t.Fatal(err)
				}
				if err := store.db.QueryRowContext(t.Context(), `SELECT SUM(`+incidentReservationBytes+`) FROM inference_reservations`).Scan(&beforeRows); err != nil {
					t.Fatal(err)
				}
				padding := int64(events.MaximumIncidentEvidenceBytes) - beforeEvents - beforeRows/2
				for _, size := range []int64{padding / 2, padding - padding/2} {
					if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", CorrelationID: "model-stop", EventType: "AUDIT_NOTE", Payload: map[string]string{"text": strings.Repeat("x", int(size))}}); err != nil {
						t.Fatal(err)
					}
				}
			}
			// Each family individually fits. Only their combined retained support
			// should exhaust the private count or byte budget.
			var eventCount, rowCount int
			var eventBytes, rowBytes int64
			if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*),SUM(`+incidentEventBytes+`) FROM events WHERE correlation_id='model-stop'`).Scan(&eventCount, &eventBytes); err != nil {
				t.Fatal(err)
			}
			if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*),SUM(`+incidentReservationBytes+`) FROM inference_reservations`).Scan(&rowCount, &rowBytes); err != nil {
				t.Fatal(err)
			}
			if eventCount >= events.MaximumIncidentEvidence || rowCount >= events.MaximumIncidentEvidence || eventBytes >= events.MaximumIncidentEvidenceBytes || rowBytes >= events.MaximumIncidentEvidenceBytes {
				t.Fatalf("family alone exhausted budget: events=%d/%d bytes=%d/%d", eventCount, rowCount, eventBytes, rowBytes)
			}
			if sample.name == "aggregate-count" {
				var allEvents, records, policies int
				if err := store.db.QueryRowContext(t.Context(), `SELECT (SELECT COUNT(*) FROM events),(SELECT COUNT(*) FROM records),(SELECT COUNT(*) FROM inference_policies)`).Scan(&allEvents, &records, &policies); err != nil {
					t.Fatal(err)
				}
				if allEvents+rowCount >= events.MaximumIncidentEvidence {
					t.Fatal("event and accounting rows alone exhausted support budget")
				}
				// Projection support includes a record and its admission; policy
				// support includes a policy and its activation. They must share
				// the remaining bound with loaded events and accounting rows.
				if allEvents+rowCount+2*records+2*policies <= events.MaximumIncidentEvidence {
					t.Fatal("combined support fixture did not exceed the count bound")
				}
			}
			if sample.name == "aggregate-bytes" && eventBytes+rowBytes <= events.MaximumIncidentEvidenceBytes {
				t.Fatal("combined evidence did not exceed the byte bound")
			}
			t.Logf("events=%d reservations=%d eventBytes=%d accountingBytes=%d", eventCount, rowCount, eventBytes, rowBytes)
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "goal", 256)
			if sample.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), sample.wantError) {
					t.Fatalf("want %q, got %v", sample.wantError, err)
				}
				if len(snapshot.DependencyEvents) != 0 || len(snapshot.Work.Events) != 0 {
					t.Fatal("overbudget operation returned partial evidence")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			for _, event := range snapshot.DependencyEvents {
				if event.EventType == "INFERENCE_RESERVED" {
					count++
				}
			}
			if count != sample.reservations || len(snapshot.Work.Events) != 1 {
				t.Fatalf("public=%d private reservations=%d", len(snapshot.Work.Events), count)
			}
			if _, err := events.ValidateIncidentHistory(snapshot); err != nil {
				t.Fatal(err)
			}
		})
	}
}

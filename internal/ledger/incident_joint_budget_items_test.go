package ledger

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

// Public references and hidden support consume one aggregate allowance.
func TestIncidentJointSupportItems(t *testing.T) {
	parallelIncidentTest(t)
	for _, reservations := range []int{500, 550} {
		t.Run(fmt.Sprint(reservations), func(t *testing.T) {
			store, err := Open(filepath.Join(t.TempDir(), "joint-budget.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			appendPrivateInferenceGoal(t, store)
			policy := testInferencePolicy(time.Now().UTC())
			policy.OrganizationID, policy.Provider, policy.Model, policy.ExecutionProfileVersion = "org-1", "provider", "model", "v1"
			policy.MaxConcurrentRequests = reservations
			policy.MaxTokensPerWindow = 120*int64(reservations) + 100
			policy.Pricing.MaxCostNanoUSDPerWindow = 400000 * int64(reservations)
			if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < reservations; i++ {
				request := testInferenceRequest(fmt.Sprintf("joint-%04d", i))
				request.Scope.OrganizationID, request.Scope.CorrelationID, request.Scope.TaskID, request.Scope.IntentID = "org-1", "model-stop", "task-model-stop", "intent-model-stop"
				request.Scope.Purpose = inference.PurposeIntentNormalization
				request.Descriptor.Provider, request.Descriptor.Model, request.Descriptor.ExecutionProfileVersion = "provider", "model", "v1"
				if _, err := store.ReserveInference(t.Context(), request); err != nil {
					t.Fatal(err)
				}
			}
			for note := 0; note < 3; note++ {
				refs := make([]string, 1000)
				for i := range refs {
					refs[i] = fmt.Sprintf("joint-artifact-%d-%04d", note, i)
				}
				if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", CorrelationID: "goal", EventType: "AUDIT_NOTE", ArtifactRefs: refs, Payload: map[string]string{"text": "public retained evidence"}}); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
				t.Fatalf("full owner rejected healthy history: %v", err)
			}
			var accountingRows int
			if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM inference_reservations`).Scan(&accountingRows); err != nil {
				t.Fatal(err)
			}
			if accountingRows != reservations {
				t.Fatalf("accounting rows=%d want%d", accountingRows, reservations)
			}
			lowerBound := 2*reservations + 3000
			if (lowerBound > events.MaximumIncidentEvidence) != (reservations == 550) {
				t.Fatal("fixture does not discriminate the joint bound")
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "goal", 256)
			if reservations == 550 {
				if err == nil {
					t.Fatalf("accepted joint support above4096: at least%d private reservation events/rows and public nested references", lowerBound)
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("overbudget read returned partial evidence")
				}
				return
			}
			if err != nil {
				t.Fatalf("below-bound healthy control rejected: %v", err)
			}
			if len(snapshot.Work.Events) != 4 {
				t.Fatalf("public events=%d want4", len(snapshot.Work.Events))
			}
			publicRefs := 0
			for _, event := range snapshot.Work.Events {
				publicRefs += len(event.ArtifactRefs)
			}
			if publicRefs != 3000 {
				t.Fatalf("public artifact references=%d want3000", publicRefs)
			}
			count := 0
			for _, event := range snapshot.DependencyEvents {
				if event.EventType == "INFERENCE_RESERVED" {
					count++
				}
			}
			if count != reservations {
				t.Fatalf("private selected reservations=%d want%d", count, reservations)
			}
			if _, err := events.ValidateIncidentHistory(snapshot); err != nil {
				t.Fatalf("public history validator rejected below-bound control: %v", err)
			}
		})
	}
}

package ledger

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/inference"
)

func TestInferenceReplayChecksHistoricalAuthorizationAndPricingTime(t *testing.T) {
	for _, minute := range []int{5, 45, 50} {
		t.Run((time.Duration(minute) * time.Minute).String(), func(t *testing.T) {
			store, err := Open(filepath.Join(t.TempDir(), "validity.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			start := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
			store.now = func() time.Time { return start.Add(30 * time.Minute) }
			policy := testInferencePolicy(store.now())
			policy.Version, policy.ConnectionID = inference.ConnectionPolicyVersion, "connection"
			policy.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, MaxCostNanoUSDPerWindow: 1000000, MaxConcurrentRequests: 2}
			policy.AuthorizedAt, policy.AuthorizationExpiresAt = start.Add(10*time.Minute), start.Add(50*time.Minute)
			policy.Pricing.ExpiresAt = start.Add(45 * time.Minute)
			if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			request := testInferenceRequest("request")
			request.ConnectionID = policy.ConnectionID
			reservation, err := store.ReserveInference(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
				t.Fatal(err)
			}
			// Keep the row and event mutually consistent and inside the same
			// accounting window; only the policy/pricing validity should fail.
			altered := start.Add(time.Duration(minute) * time.Minute).Format(time.RFC3339Nano)
			if _, err := store.db.ExecContext(t.Context(), `UPDATE inference_reservations SET created_at=? WHERE reservation_id=?`, altered, reservation.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET payload=CAST(json_set(payload,'$.admitted_at',?) AS BLOB) WHERE event_type='INFERENCE_RESERVED' AND json_extract(payload,'$.reservation_id')=?`, altered, reservation.ID); err != nil {
				t.Fatal(err)
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err == nil {
				t.Fatal("historically unauthorized admission passed replay")
			}
			if _, err := store.RecoverInferenceReservations(t.Context(), policy.OrganizationID); err == nil {
				t.Fatal("historically unauthorized admission was recovered")
			}
			var state string
			if err := store.db.QueryRowContext(t.Context(), `SELECT state FROM inference_reservations WHERE reservation_id=?`, reservation.ID).Scan(&state); err != nil || state != inferenceStateReserved {
				t.Fatalf("failed recovery changed state: %s err=%v", state, err)
			}
		})
	}
}

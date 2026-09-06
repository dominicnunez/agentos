package ledger

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/inference"
)

// Independently configured connections remain available simultaneously;
// historical v1 policy replacement retains its original singleton semantics.
func TestInferenceDistinctProviderRoutesRemainAvailable(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "routes.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.now = func() time.Time { return now }
	first := testInferencePolicy(now)
	second := testInferencePolicy(now)
	first.Version, first.ConnectionID = inference.ConnectionPolicyVersion, "first"
	second.Version, second.ConnectionID = inference.ConnectionPolicyVersion, "second"
	first.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, MaxCostNanoUSDPerWindow: 1000000, MaxConcurrentRequests: 2}
	second.OrganizationBudget = first.OrganizationBudget
	second.Provider = "provider-2"
	second.Model = "model-2"
	second.AuthorizedAt = first.AuthorizedAt.Add(time.Minute)
	for _, policy := range []inference.Policy{first, second} {
		if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
			t.Fatal(err)
		}
	}
	for i, policy := range []inference.Policy{first, second} {
		request := testInferenceRequest([]string{"route-one", "route-two"}[i])
		request.ConnectionID = policy.ConnectionID
		request.Descriptor.Provider = policy.Provider
		request.Descriptor.Model = policy.Model
		reservation, err := store.ReserveInference(t.Context(), request)
		if err != nil {
			t.Errorf("configured provider %s is unavailable: %v", policy.Provider, err)
			continue
		}
		fingerprint, err := policy.Fingerprint()
		if err != nil || reservation.PolicyFingerprint != fingerprint {
			t.Fatalf("route reservation used another policy: %v", err)
		}
		if _, err := store.ReconcileInference(t.Context(), reservation, nil, inference.ReconciliationNotSent); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestInferenceConcurrentConnectionsRecoverExactAdmissions(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "concurrent.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.now = func() time.Time { return now }
	requests := make([]inference.InferenceRequest, 2)
	for i, connection := range []string{"first", "second"} {
		policy := testInferencePolicy(now)
		policy.Version, policy.ConnectionID = inference.ConnectionPolicyVersion, connection
		policy.Provider, policy.Model = connection, connection
		policy.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, MaxCostNanoUSDPerWindow: 1000000, MaxConcurrentRequests: 2}
		if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
			t.Fatal(err)
		}
		request := testInferenceRequest(connection)
		request.ConnectionID, request.Descriptor.Provider, request.Descriptor.Model = connection, connection, connection
		requests[i] = request
	}
	type result struct {
		reservation inference.Reservation
		err         error
	}
	results := make(chan result, 2)
	for _, request := range requests {
		go func() {
			reservation, err := store.ReserveInference(t.Context(), request)
			results <- result{reservation, err}
		}()
	}
	var reservations []inference.Reservation
	for range requests {
		item := <-results
		if item.err != nil {
			t.Fatal(item.err)
		}
		reservations = append(reservations, item.reservation)
	}
	for _, original := range reservations {
		altered := original
		if altered.Request.ConnectionID == "first" {
			altered.Request.ConnectionID = "second"
		} else {
			altered.Request.ConnectionID = "first"
		}
		if _, err := store.ReconcileInference(t.Context(), altered, nil, inference.ReconciliationNotSent); err == nil {
			t.Fatal("substituted connection released a reservation")
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now.Add(time.Minute) }
	if count, err := store.RecoverInferenceReservations(t.Context(), "organization-1"); err != nil || count != 2 {
		t.Fatalf("recovered=%d want2 err=%v", count, err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, reservation := range reservations {
		var connection, state string
		var input, output, cost int64
		if err := store.db.QueryRowContext(t.Context(), `SELECT connection_id,state,charged_input_tokens,charged_output_tokens,charged_cost_nano_usd FROM inference_reservations WHERE reservation_id=?`, reservation.ID).Scan(&connection, &state, &input, &output, &cost); err != nil {
			t.Fatal(err)
		}
		if connection != reservation.Request.ConnectionID || state != inferenceStateUncertain || input != reservation.ReservedInputTokens || output != reservation.ReservedOutputTokens || cost != reservation.ReservedCostNanoUSD {
			t.Fatal("recovery changed connection identity or released uncertain usage")
		}
	}
}

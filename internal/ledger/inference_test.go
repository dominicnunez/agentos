package ledger

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/inference"
)

func testInferencePolicy(now time.Time) inference.Policy {
	return inference.Policy{
		Version: inference.PolicyVersion, OrganizationID: "organization-1", Provider: "provider-1", Model: "model-1",
		ExecutionProfileVersion: "profile-v1", Mode: inference.MeteredAPI,
		MaxInputTokensPerRequest: 100, MaxOutputTokensPerRequest: 20, MaxTokensPerWindow: 250,
		ContinuityReserveTokens: 100, WindowDurationSeconds: 3600, MaxConcurrentRequests: 1, MaxAttemptsPerRequest: 1,
		AuthorizedBy: "local-uid-1000", AuthorizedAt: now.Add(-time.Hour), AuthorizationExpiresAt: now.Add(time.Hour),
		Pricing: &inference.Pricing{
			InputNanoUSDPerMillionTokens: 2_000_000_000, OutputNanoUSDPerMillionTokens: 10_000_000_000,
			MaxCostNanoUSDPerRequest: 400_000, MaxCostNanoUSDPerWindow: 1_000_000,
			ExpiresAt: now.Add(time.Hour),
		},
	}
}

func testInferenceRequest(id string) inference.InferenceRequest {
	digest := sha256.Sum256([]byte("prompt-" + id))
	return inference.InferenceRequest{
		Scope: inference.Scope{
			OrganizationID: "organization-1", Purpose: inference.PurposeTaskExecution, RequestID: id,
			TaskID: "task-1", ExecutionID: id, CorrelationID: "work-1",
		},
		Descriptor:   execution.ModelDescriptor{Provider: "provider-1", Model: "model-1", ExecutionProfileVersion: "profile-v1"},
		PromptSHA256: hex.EncodeToString(digest[:]),
	}
}

func testInferenceUsage() events.InferenceUsageRecordedPayload {
	return events.InferenceUsageRecordedPayload{Source: "provider", Provider: "provider-1", Model: "model-1", InputTokens: 10, OutputTokens: 5, TotalTokens: 15}
}

func TestInferenceReservationPersistsAcrossRestartAndReconcilesExactCost(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }
	policy := testInferencePolicy(now)
	if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveInference(t.Context(), testInferenceRequest("request-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	restarted.now = func() time.Time { return now.Add(time.Minute) }
	usage := testInferenceUsage()
	cost, err := restarted.ReconcileInference(t.Context(), reservation, &usage, inference.ReconciliationCompleted)
	if err != nil {
		t.Fatal(err)
	}
	if cost != 70_000 {
		t.Fatalf("cost = %d, want 70000 nanoUSD", cost)
	}
	stream, err := restarted.Events(t.Context(), "work-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(stream) != 2 || stream[0].EventType != "INFERENCE_RESERVED" || stream[1].EventType != "INFERENCE_RECONCILED" {
		t.Fatalf("unexpected inference event stream: %#v", stream)
	}
	if _, err := restarted.ReserveInference(t.Context(), testInferenceRequest("request-1")); err == nil || !strings.Contains(err.Error(), "retries fail closed") {
		t.Fatalf("completed request replay was accepted: %v", err)
	}
}

func TestInferenceStartupRecoveryRetainsFullChargeWithoutConcurrencyLeak(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }
	if err := store.ActivateInferencePolicy(t.Context(), testInferencePolicy(now)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReserveInference(t.Context(), testInferenceRequest("request-1")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	restarted.now = func() time.Time { return now.Add(time.Minute) }
	recovered, err := restarted.RecoverInferenceReservations(t.Context(), "organization-1")
	if err != nil || recovered != 1 {
		t.Fatalf("recovered=%d err=%v", recovered, err)
	}
	if _, err := restarted.ReserveInference(t.Context(), testInferenceRequest("request-2")); err == nil || strings.Contains(err.Error(), "concurrency") || !strings.Contains(err.Error(), "continuity reserve") {
		t.Fatalf("recovered reservation did not retain quota and release concurrency: %v", err)
	}
}

func TestInferenceConcurrencyUncertaintyAndQuotaFailClosed(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.now = func() time.Time { return now }
	if err := store.ActivateInferencePolicy(t.Context(), testInferencePolicy(now)); err != nil {
		t.Fatal(err)
	}
	first, err := store.ReserveInference(t.Context(), testInferenceRequest("request-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReserveInference(t.Context(), testInferenceRequest("request-2")); err == nil || !strings.Contains(err.Error(), "concurrency") {
		t.Fatalf("concurrent provider call was admitted: %v", err)
	}
	if _, err := store.ReconcileInference(t.Context(), first, nil, inference.ReconciliationUncertain); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReserveInference(t.Context(), testInferenceRequest("request-2")); err == nil || !strings.Contains(err.Error(), "continuity reserve") {
		t.Fatalf("uncertain reservation did not retain its full quota: %v", err)
	}
}

func TestInferenceCompletedUsageReleasesUnusedReservation(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.now = func() time.Time { return now }
	if err := store.ActivateInferencePolicy(t.Context(), testInferencePolicy(now)); err != nil {
		t.Fatal(err)
	}
	first, err := store.ReserveInference(t.Context(), testInferenceRequest("request-1"))
	if err != nil {
		t.Fatal(err)
	}
	usage := testInferenceUsage()
	if _, err := store.ReconcileInference(t.Context(), first, &usage, inference.ReconciliationCompleted); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReserveInference(t.Context(), testInferenceRequest("request-2")); err != nil {
		t.Fatalf("released quota was not reusable: %v", err)
	}
}

func TestInferenceRejectsExpiredAuthorizationPricingAndCrossTenantRequests(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name   string
		mutate func(*inference.Policy)
		want   string
	}{
		{name: "authorization", mutate: func(policy *inference.Policy) { policy.AuthorizationExpiresAt = now }, want: "not currently valid"},
		{name: "pricing", mutate: func(policy *inference.Policy) { policy.Pricing.ExpiresAt = now }, want: "not currently valid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := Open(filepath.Join(t.TempDir(), "ledger.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			store.now = func() time.Time { return now }
			policy := testInferencePolicy(now)
			test.mutate(&policy)
			if err := store.ActivateInferencePolicy(t.Context(), policy); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("stale policy was activated: %v", err)
			}
		})
	}
	store, err := Open(filepath.Join(t.TempDir(), "tenant.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.now = func() time.Time { return now }
	if err := store.ActivateInferencePolicy(context.Background(), testInferencePolicy(now)); err != nil {
		t.Fatal(err)
	}
	request := testInferenceRequest("request-1")
	request.Scope.OrganizationID = "organization-2"
	if _, err := store.ReserveInference(t.Context(), request); err == nil || !strings.Contains(err.Error(), "no active inference policy") {
		t.Fatalf("cross-tenant inference was admitted: %v", err)
	}
}

func TestInferenceRechecksExpiryAfterAcquiringAdmissionTransaction(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.now = func() time.Time { return now }
	policy := testInferencePolicy(now)
	if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	blocker, err := store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var clockUnixNano atomic.Int64
	clockUnixNano.Store(now.UnixNano())
	var clockReads atomic.Int32
	store.now = func() time.Time {
		clockReads.Add(1)
		return time.Unix(0, clockUnixNano.Load()).UTC()
	}
	waits := store.db.Stats().WaitCount
	result := make(chan error, 1)
	go func() {
		_, reserveErr := store.ReserveInference(context.Background(), testInferenceRequest("request-expiry-race"))
		result <- reserveErr
	}()
	deadline := time.Now().Add(time.Second)
	for store.db.Stats().WaitCount == waits && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if store.db.Stats().WaitCount == waits {
		_ = blocker.Rollback()
		t.Fatal("inference admission did not wait for the held transaction")
	}
	if clockReads.Load() != 0 {
		_ = blocker.Rollback()
		t.Fatal("inference expiry was evaluated before the admission transaction")
	}
	clockUnixNano.Store(policy.AuthorizationExpiresAt.UnixNano())
	if err := blocker.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expired inference authorization crossed the transaction boundary: %v", err)
	}
}

func TestInferenceViolationIsDurableAndCannotBeRetried(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.now = func() time.Time { return now }
	if err := store.ActivateInferencePolicy(t.Context(), testInferencePolicy(now)); err != nil {
		t.Fatal(err)
	}
	request := testInferenceRequest("request-1")
	reservation, err := store.ReserveInference(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	usage := testInferenceUsage()
	usage.OutputTokens = 21
	usage.TotalTokens = 31
	if _, err := store.ReconcileInference(t.Context(), reservation, &usage, inference.ReconciliationViolation); err == nil {
		t.Fatal("provider overuse was accepted")
	}
	var state string
	if err := store.db.QueryRowContext(t.Context(), `SELECT state FROM inference_reservations WHERE reservation_id=?`, reservation.ID).Scan(&state); err != nil || state != inferenceStateViolation {
		t.Fatalf("violation was not durable: %q %v", state, err)
	}
	if _, err := store.ReserveInference(t.Context(), request); err == nil {
		t.Fatal("violating request was retried")
	}
}

func TestInferenceMalformedUsageTerminatesReservationConservatively(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.now = func() time.Time { return now }
	if err := store.ActivateInferencePolicy(t.Context(), testInferencePolicy(now)); err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveInference(t.Context(), testInferenceRequest("request-1"))
	if err != nil {
		t.Fatal(err)
	}
	usage := testInferenceUsage()
	usage.Model = "untrusted-model"
	if _, err := store.ReconcileInference(t.Context(), reservation, &usage, inference.ReconciliationViolation); err == nil {
		t.Fatal("provider identity violation was accepted")
	}
	var state string
	var chargedInput, chargedOutput, chargedCost int64
	if err := store.db.QueryRowContext(t.Context(), `SELECT state,charged_input_tokens,charged_output_tokens,charged_cost_nano_usd FROM inference_reservations WHERE reservation_id=?`, reservation.ID).Scan(&state, &chargedInput, &chargedOutput, &chargedCost); err != nil {
		t.Fatal(err)
	}
	if state != inferenceStateViolation || chargedInput != reservation.ReservedInputTokens || chargedOutput != reservation.ReservedOutputTokens || chargedCost != reservation.ReservedCostNanoUSD {
		t.Fatalf("malformed usage did not terminate with its full conservative charge: state=%s input=%d output=%d cost=%d", state, chargedInput, chargedOutput, chargedCost)
	}
	if err := ValidateInferenceAdmissions(t.Context(), store.db); err != nil {
		t.Fatalf("durable malformed-usage violation failed validation: %v", err)
	}
}

func TestInferenceAdmissionValidationRejectsAccountingTampering(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.now = func() time.Time { return now }
	if err := store.ActivateInferencePolicy(t.Context(), testInferencePolicy(now)); err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveInference(t.Context(), testInferenceRequest("request-1"))
	if err != nil {
		t.Fatal(err)
	}
	usage := testInferenceUsage()
	if _, err := store.ReconcileInference(t.Context(), reservation, &usage, inference.ReconciliationCompleted); err != nil {
		t.Fatal(err)
	}
	if err := ValidateInferenceAdmissions(t.Context(), store.db); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `UPDATE inference_reservations SET charged_cost_nano_usd=charged_cost_nano_usd+1 WHERE reservation_id=?`, reservation.ID); err != nil {
		t.Fatal(err)
	}
	if err := ValidateInferenceAdmissions(t.Context(), store.db); err == nil {
		t.Fatal("tampered inference accounting passed admission validation")
	}
}

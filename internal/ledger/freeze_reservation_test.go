package ledger

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/authority"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

func TestFreezeReservationWithManifestedLiveGeneration(t *testing.T) {
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.now = func() time.Time { return now }
	seedFreezeHistory(t, store, 16)
	policy := freezeReservationPolicy(now)
	if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
		t.Fatal(err)
	}

	success := freezeReservationRequest("manifested-success")
	appendFreezeReservationManifest(t, store, success)
	live, finish, err := store.BeginInferenceContext(t.Context(), success.Scope.OrganizationID)
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveInference(live, success)
	finish()
	if err != nil || reservation.ID == "" || reservation.Request.Scope.RequestID != success.Scope.RequestID {
		t.Fatalf("manifested reservation = %+v, %v", reservation, err)
	}

	denied := freezeReservationRequest("manifested-held")
	appendFreezeReservationManifest(t, store, denied)
	stale, finishStale, err := store.BeginInferenceContext(t.Context(), denied.Scope.OrganizationID)
	if err != nil {
		t.Fatal(err)
	}
	defer finishStale()
	current, err := store.ReadFreeze(t.Context(), core.ID(denied.Scope.OrganizationID))
	if err != nil {
		t.Fatal(err)
	}
	hold, err := store.SetFreeze(t.Context(), core.ID(denied.Scope.OrganizationID), "owner-1", core.PrincipalHuman, authority.FreezeChange{
		Frozen: true, Reason: "reservation test hold", ExpectedEventRef: current.EventRef, ExpectedVersion: current.Version,
	})
	if err != nil || !hold.State.Frozen {
		t.Fatalf("append reservation hold = %+v, %v", hold, err)
	}
	if _, err := store.ReserveInference(context.WithoutCancel(stale), denied); !errors.Is(err, core.ErrOrganizationFrozen) {
		t.Fatalf("intervening hold reservation error = %v", err)
	}
	var reservations int
	if err := store.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM inference_reservations").Scan(&reservations); err != nil || reservations != 1 {
		t.Fatalf("held reservation changed durable rows: count=%d err=%v", reservations, err)
	}
}

func BenchmarkFreezeReservationManifestHistory(b *testing.B) {
	for _, mode := range []string{"success", "held"} {
		b.Run(mode, func(b *testing.B) {
			benchmarkFreezeReservation(b, mode == "held")
		})
	}
}

func benchmarkFreezeReservation(b *testing.B, held bool) {
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	store, err := Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })
	store.now = func() time.Time { return now }
	seedFreezeHistory(b, store, 4096)
	policy := freezeReservationPolicy(now)
	if err := store.ActivateInferencePolicy(b.Context(), policy); err != nil {
		b.Fatal(err)
	}

	var stale context.Context
	var finishStale func()
	if held {
		live, finish, err := store.BeginInferenceContext(b.Context(), "org-1")
		if err != nil {
			b.Fatal(err)
		}
		finishStale = finish
		current, err := store.ReadFreeze(b.Context(), "org-1")
		if err != nil {
			b.Fatal(err)
		}
		hold, err := store.SetFreeze(b.Context(), "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{
			Frozen: true, Reason: "benchmark hold", ExpectedEventRef: current.EventRef, ExpectedVersion: current.Version,
		})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := store.SetFreeze(b.Context(), "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{
			Frozen: false, Reason: "benchmark release", ExpectedEventRef: hold.EventRef, ExpectedVersion: hold.Version,
		}); err != nil {
			b.Fatal(err)
		}
		stale = context.WithoutCancel(live)
		b.Cleanup(finishStale)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		request := freezeReservationRequest(fmt.Sprintf("reservation-%d", i))
		appendFreezeReservationManifest(b, store, request)
		ctx := stale
		var finish func()
		if !held {
			ctx, finish, err = store.BeginInferenceContext(b.Context(), request.Scope.OrganizationID)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
		reservation, reserveErr := store.ReserveInference(ctx, request)
		b.StopTimer()
		if held {
			if !errors.Is(reserveErr, core.ErrOrganizationFrozen) {
				b.Fatalf("held reservation error = %v", reserveErr)
			}
			continue
		}
		finish()
		if reserveErr != nil || reservation.ID == "" {
			b.Fatalf("reservation = %+v, %v", reservation, reserveErr)
		}
		usage := testInferenceUsage()
		if _, err := store.ReconcileInference(b.Context(), reservation, &usage, inference.ReconciliationCompleted); err != nil {
			b.Fatal(err)
		}
	}
}

func freezeReservationRequest(id string) inference.InferenceRequest {
	request := testInferenceRequest(id)
	request.Scope.OrganizationID = "org-1"
	request.Scope.CorrelationID = id
	return request
}

func freezeReservationPolicy(now time.Time) inference.Policy {
	policy := testInferencePolicy(now)
	policy.OrganizationID = "org-1"
	policy.MaxTokensPerWindow = math.MaxInt64
	policy.MaxConcurrentRequests = 1024
	policy.Pricing.MaxCostNanoUSDPerWindow = math.MaxInt64
	return policy
}

func appendFreezeReservationManifest(t testing.TB, store *SQLite, request inference.InferenceRequest) {
	t.Helper()
	_, err := store.Append(t.Context(), events.TrustedDraft{
		OrganizationID:    request.Scope.OrganizationID,
		EventType:         "PLANNING_CONTEXT_MANIFESTED",
		SourceActorID:     "runtime",
		SourceExecutionID: request.Scope.ExecutionID,
		TaskID:            request.Scope.TaskID,
		CorrelationID:     request.Scope.CorrelationID,
		Payload: events.PlanningContextPayload{
			IntentID:                request.Scope.IntentID,
			Provider:                request.Descriptor.Provider,
			Model:                   request.Descriptor.Model,
			ExecutionProfileVersion: request.Descriptor.ExecutionProfileVersion,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

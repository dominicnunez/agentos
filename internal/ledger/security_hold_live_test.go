package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/inference"
)

type holdWaitingModel struct{ started chan struct{} }

func TestFrozenNestedAdmissionRetainsEarliestHold(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx, release, err := store.BeginExecutionContext(t.Context(), "organization-1")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	appendInferenceFreeze(t, store, "organization-1", 1, true)
	var first core.SecurityHoldCause
	if !errors.As(context.Cause(ctx), &first) {
		t.Fatalf("missing initial cancellation: %v", context.Cause(ctx))
	}
	appendInferenceFreeze(t, store, "organization-1", 2, false)
	appendInferenceFreeze(t, store, "organization-1", 3, true)
	// Reconciliation may remove cancellation, but must retain the original generation.
	_, _, err = store.BeginInferenceContext(context.WithoutCancel(ctx), "organization-1")
	var actual core.SecurityHoldCause
	if !errors.As(err, &actual) || actual != first {
		t.Fatalf("nested admission replaced earliest hold: got %v, want %v", err, first)
	}
}

func TestInitialFrozenAdmissionRetainsExactHold(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	appendInferenceFreeze(t, store, "organization-1", 1, true)
	guard, err := inference.NewGuardedAdapter(store, &holdReturningModel{freeze: func() { t.Fatal("frozen admission invoked provider") }})
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := inference.WithScope(t.Context(), testInferenceRequest("initial-frozen").Scope)
	if err != nil {
		t.Fatal(err)
	}
	_, err = guard.Complete(ctx, "prompt")
	var hold core.SecurityHoldCause
	if !errors.As(err, &hold) || !errors.Is(err, core.ErrOrganizationFrozen) || hold.OrganizationID != "organization-1" || hold.EventRef == "" || hold.Sequence == 0 {
		t.Fatalf("initial frozen admission lost exact evidence: %v", err)
	}
}

func TestProlongedAuthorityContentionStopsLiveCall(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	writer, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	call, release, err := store.BeginExecutionContext(t.Context(), "organization-1")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	// Healthy observations must renew the lease, not expire it from creation.
	select {
	case <-call.Done():
		t.Fatalf("healthy observation expired: %v", context.Cause(call))
	case <-time.After(containmentObservationTimeout + 100*time.Millisecond):
	}
	conn, err := store.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	appendInferenceFreeze(t, writer, "organization-1", 1, true)
	appendInferenceFreeze(t, writer, "organization-1", 2, false)
	select {
	case <-call.Done():
		if !errors.Is(context.Cause(call), core.ErrContainmentUnavailable) {
			t.Fatalf("unexpected stop cause: %v", context.Cause(call))
		}
	case <-time.After(2 * containmentObservationTimeout):
		t.Fatal("unobserved authority left work running")
	}
}

func TestInitialContainmentReadFailurePreventsDispatch(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	conn, err := store.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	guard, err := inference.NewGuardedAdapter(store, &holdReturningModel{freeze: func() { t.Fatal("unavailable initial authority invoked provider") }})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	ctx, err = inference.WithScope(ctx, testInferenceRequest("initial-unavailable").Scope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := guard.Complete(ctx, "prompt"); !errors.Is(err, core.ErrContainmentUnavailable) || !errors.Is(err, context.DeadlineExceeded) || !execution.WasRequestNotSent(err) {
		t.Fatalf("initial authority error lost classification: %v", err)
	}
}

func TestAuthorityReadFailureIsContainmentUnavailable(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx, release, err := store.BeginInferenceContext(t.Context(), "organization-1")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	conn, err := store.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Millisecond)
	defer cancel()
	err = store.CheckInferenceContext(checkCtx, "organization-1")
	if !errors.Is(err, core.ErrContainmentUnavailable) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("authority timeout lost safety classification: %v", err)
	}
}

func TestManifestHoldPreventsFreshGuardAdmission(t *testing.T) {
	for _, kind := range []string{"PLANNING_CONTEXT_MANIFESTED", "INTENT_NORMALIZATION_CONTEXT_MANIFESTED"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "ledger.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			writer, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = writer.Close() })
			request := testInferenceRequest("manifest-held")
			scope := request.Scope
			_, err = store.Append(t.Context(), events.TrustedDraft{OrganizationID: scope.OrganizationID, EventType: kind, SourceActorID: "runtime", SourceExecutionID: scope.ExecutionID, TaskID: scope.TaskID, CorrelationID: scope.CorrelationID, Payload: map[string]string{"source_message_id": "message-1"}})
			if err != nil {
				t.Fatal(err)
			}
			appendInferenceFreeze(t, writer, scope.OrganizationID, 1, true)
			appendInferenceFreeze(t, writer, scope.OrganizationID, 2, false)
			guard, err := inference.NewGuardedAdapter(store, &holdReturningModel{freeze: func() { t.Fatal("held manifest invoked provider") }})
			if err != nil {
				t.Fatal(err)
			}
			ctx, err := inference.WithScope(t.Context(), scope)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := guard.Complete(ctx, "prompt"); !errors.Is(err, core.ErrOrganizationFrozen) {
				t.Fatalf("fresh generation bypassed manifested hold: %v", err)
			}
			var reservations int
			if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM inference_reservations`).Scan(&reservations); err != nil || reservations != 0 {
				t.Fatalf("held manifest reserved inference: %d %v", reservations, err)
			}
		})
	}
}

type cancelledReservationStore struct {
	*SQLite
	freeze func()
}

func (s cancelledReservationStore) ReserveInference(context.Context, inference.InferenceRequest) (inference.Reservation, error) {
	s.freeze()
	return inference.Reservation{}, context.Canceled
}

func TestCancelledReservationPreservesReleasedHold(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	wrapped := cancelledReservationStore{SQLite: store, freeze: func() {
		appendInferenceFreeze(t, store, "organization-1", 1, true)
		appendInferenceFreeze(t, store, "organization-1", 2, false)
	}}
	model := &holdReturningModel{freeze: func() { t.Fatal("failed reservation invoked provider") }}
	guard, err := inference.NewGuardedAdapter(wrapped, model)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := inference.WithScope(t.Context(), testInferenceRequest("cancelled-reservation").Scope)
	if err != nil {
		t.Fatal(err)
	}
	_, err = guard.Complete(ctx, "prompt")
	var hold core.SecurityHoldCause
	if !errors.Is(err, core.ErrOrganizationFrozen) || !errors.As(err, &hold) || hold.EventRef == "" || hold.Sequence == 0 {
		t.Fatalf("reservation error lost hold identity: %v", err)
	}
}

type cancelledReturningModel struct {
	holdReturningModel
	providerError error
}

func (m *cancelledReturningModel) Complete(ctx context.Context, prompt string) (execution.ModelResponse, error) {
	response, err := m.holdReturningModel.Complete(ctx, prompt)
	if m.providerError != nil {
		return execution.ModelResponse{}, m.providerError
	}
	return response, err
}

func TestCallerCancellationDoesNotHideReleasedDurableHold(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		for _, providerFails := range []bool{false, true} {
			t.Run(fmt.Sprintf("%v/provider-error-%t", cause, providerFails), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "ledger.db")
				store, err := Open(path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				writer, err := Open(path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = writer.Close() })
				now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
				store.now = func() time.Time { return now }
				if err := store.ActivateInferencePolicy(t.Context(), testInferencePolicy(now)); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancelCause(t.Context())
				defer cancel(nil)
				model := &cancelledReturningModel{holdReturningModel: holdReturningModel{freeze: func() {
					cancel(cause)
					appendInferenceFreeze(t, writer, "organization-1", 1, true)
					appendInferenceFreeze(t, writer, "organization-1", 2, false)
				}}}
				if providerFails {
					model.providerError = cause
				}
				guard, err := inference.NewGuardedAdapter(store, model)
				if err != nil {
					t.Fatal(err)
				}
				ctx, err = inference.WithScope(ctx, testInferenceRequest("cancelled-held-call").Scope)
				if err != nil {
					t.Fatal(err)
				}
				response, err := guard.Complete(ctx, "synthetic call")
				var hold core.SecurityHoldCause
				if !errors.Is(err, cause) || !errors.Is(err, core.ErrOrganizationFrozen) || !errors.As(err, &hold) || hold.OrganizationID != "organization-1" || hold.EventRef == "" || hold.Sequence == 0 || response.Text != "" {
					t.Fatalf("local cancellation masked durable hold: %v", err)
				}
				if _, found := events.ReconciledUsage(err); found == providerFails {
					t.Fatal("unexpected reconciled usage retention")
				}
			})
		}
	}

}

type failedHeldReconciliationStore struct {
	*SQLite
	freeze func()
}

func (s failedHeldReconciliationStore) ReconcileInference(context.Context, inference.Reservation, *events.InferenceUsageRecordedPayload, inference.Reconciliation) (int64, error) {
	s.freeze()
	return 0, errors.New("accounting unavailable")
}

func TestReconciliationFailurePreservesConcurrentHold(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	if err := store.ActivateInferencePolicy(t.Context(), testInferencePolicy(now)); err != nil {
		t.Fatal(err)
	}
	wrapped := failedHeldReconciliationStore{SQLite: store, freeze: func() {
		appendInferenceFreeze(t, store, "organization-1", 1, true)
	}}
	guard, err := inference.NewGuardedAdapter(wrapped, &holdReturningModel{freeze: func() {}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := inference.WithScope(t.Context(), testInferenceRequest("held-accounting").Scope)
	if err != nil {
		t.Fatal(err)
	}
	response, err := guard.Complete(ctx, "prompt")
	var hold core.SecurityHoldCause
	if !errors.Is(err, core.ErrOrganizationFrozen) || !errors.As(err, &hold) || hold.OrganizationID != "organization-1" || hold.EventRef == "" || hold.Sequence == 0 || response.Text != "" {
		t.Fatalf("hold lost on reconciliation failure: %v", err)
	}
	if _, found := events.ReconciledUsage(err); found {
		t.Fatal("failed accounting advertised reconciled usage")
	}
}

func TestLiveContainmentConnectionContentionDoesNotDestroyAuthority(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	call, release, err := store.BeginExecutionContext(t.Context(), "organization-1")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	conn, err := store.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// Occupy the single connection beyond a monitor acquisition deadline.
	timer := time.NewTimer(400 * time.Millisecond)
	select {
	case <-call.Done():
		_ = conn.Close()
		timer.Stop()
		t.Fatalf("connection contention cancelled live work: %v", context.Cause(call))
	case <-timer.C:
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	appendInferenceFreeze(t, store, "organization-1", 1, true)
	if !errors.Is(context.Cause(call), core.ErrOrganizationFrozen) {
		t.Fatalf("authority lost after contention: %v", context.Cause(call))
	}
}

type holdReturningModel struct {
	holdWaitingModel
	freeze func()
}

func (m *holdReturningModel) Complete(context.Context, string) (execution.ModelResponse, error) {
	m.freeze()
	d := m.Descriptor()
	return execution.ModelResponse{Text: "must not escape hold", Usage: events.InferenceUsageRecordedPayload{Source: "provider", Provider: d.Provider, Model: d.Model, InputTokens: 1, OutputTokens: 1, TotalTokens: 2}}, nil
}

func TestSecurityFreezeOtherHandleSuppressesResponseButRetainsUsage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	reader, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	writer, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	reader.now = func() time.Time { return now }
	if err := reader.ActivateInferencePolicy(t.Context(), testInferencePolicy(now)); err != nil {
		t.Fatal(err)
	}
	model := &holdReturningModel{freeze: func() {
		appendInferenceFreeze(t, writer, "organization-1", 1, true)
		appendInferenceFreeze(t, writer, "organization-1", 2, false)
	}}
	guard, err := inference.NewGuardedAdapter(reader, model)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := inference.WithScope(t.Context(), testInferenceRequest("held-response").Scope)
	if err != nil {
		t.Fatal(err)
	}
	executor := execution.NewAgentExecution(guard)
	descriptor := executor.Descriptor()
	result, err := executor.Execute(ctx, core.Task{ID: "task-1", Description: "offline synthetic call", ModelInferencePolicy: core.InferenceAllowed}, core.ExecutionContextManifest{Provider: descriptor.Provider, Model: descriptor.Model, ExecutionProfileVersion: descriptor.ExecutionProfileVersion})
	if err == nil || result.Outcome.Status != core.OutcomeFailed || result.Outcome.ObservedEffect != nil {
		t.Fatal("held provider response escaped agent execution")
	}
	if result.InferenceUsage == nil || result.InferenceUsage.InputTokens != 1 || result.InferenceUsage.OutputTokens != 1 || !result.InferenceUsage.Valid() {
		t.Fatal("agent execution lost reconciled usage telemetry")
	}
	var state string
	var input, output int64
	if err := reader.db.QueryRowContext(t.Context(), `SELECT state,charged_input_tokens,charged_output_tokens FROM inference_reservations WHERE request_id='held-response'`).Scan(&state, &input, &output); err != nil {
		t.Fatal(err)
	}
	if state != inferenceStateCompleted || input != 1 || output != 1 {
		t.Fatalf("actual usage lost: %s %d/%d", state, input, output)
	}
	if err := reader.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatal(err)
	}
}

type freezeAfterReservationStore struct {
	*SQLite
	freeze func()
}

func (s freezeAfterReservationStore) ReserveInference(ctx context.Context, request inference.InferenceRequest) (inference.Reservation, error) {
	reservation, err := s.SQLite.ReserveInference(ctx, request)
	if err == nil {
		s.freeze()
	}
	return reservation, err
}

func TestSecurityFreezeAfterReservationRecordsNotSentWithoutCharge(t *testing.T) {
	for _, otherHandle := range []bool{false, true} {
		t.Run(fmt.Sprint(otherHandle), func(t *testing.T) { testSecurityFreezeAfterReservation(t, otherHandle) })
	}
}

func testSecurityFreezeAfterReservation(t *testing.T, otherHandle bool) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	if err := store.ActivateInferencePolicy(t.Context(), testInferencePolicy(now)); err != nil {
		t.Fatal(err)
	}
	writer := store
	if otherHandle {
		writer, err = Open(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = writer.Close() })
	}
	wrapped := freezeAfterReservationStore{SQLite: store, freeze: func() {
		appendInferenceFreeze(t, writer, "organization-1", 1, true)
		if otherHandle {
			appendInferenceFreeze(t, writer, "organization-1", 2, false)
		}
	}}
	model := &holdWaitingModel{started: make(chan struct{})}
	guard, err := inference.NewGuardedAdapter(wrapped, model)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := inference.WithScope(t.Context(), testInferenceRequest("not-dispatched").Scope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := guard.Complete(ctx, "offline synthetic call"); err == nil {
		t.Fatal("held call succeeded")
	}
	select {
	case <-model.started:
		t.Fatal("held reservation reached provider")
	default:
	}
	var state string
	var input, output, cost int64
	if err := store.db.QueryRowContext(t.Context(), `SELECT state,charged_input_tokens,charged_output_tokens,charged_cost_nano_usd FROM inference_reservations WHERE request_id='not-dispatched'`).Scan(&state, &input, &output, &cost); err != nil {
		t.Fatal(err)
	}
	if state != inferenceStateNotSent || input != 0 || output != 0 || cost != 0 {
		t.Fatalf("unsent reservation charged: %s %d/%d/%d", state, input, output, cost)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestSecurityFreezeReleaseRejectsStaleInferencePreparation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	reader, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	writer, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	reader.now = func() time.Time { return now }
	if err := reader.ActivateInferencePolicy(t.Context(), testInferencePolicy(now)); err != nil {
		t.Fatal(err)
	}
	preparation, release, err := reader.BeginExecutionContext(t.Context(), "organization-1")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	appendInferenceFreeze(t, writer, "organization-1", 1, true)
	appendInferenceFreeze(t, writer, "organization-1", 2, false)
	stale := context.WithoutCancel(preparation)
	if _, err := reader.ReserveInference(stale, testInferenceRequest("stale-preparation")); !errors.Is(err, core.ErrOrganizationFrozen) {
		t.Fatalf("stale inference reserved: %v", err)
	}
	if _, finish, err := reader.BeginInferenceContext(stale, "organization-1"); err == nil {
		finish()
		t.Fatal("nested inference reset interrupted preparation")
	}
	var count int
	if err := reader.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM inference_reservations`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("denied inference consumed reservation: count=%d err=%v", count, err)
	}
	fresh, finish, err := reader.BeginInferenceContext(t.Context(), "organization-1")
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	if _, err := reader.ReserveInference(fresh, testInferenceRequest("fresh-preparation")); err != nil {
		t.Fatalf("released fresh inference denied: %v", err)
	}
}

func (*holdWaitingModel) Name() string { return "hold-waiting-model" }
func (*holdWaitingModel) Descriptor() execution.ModelDescriptor {
	return testInferenceRequest("descriptor").Descriptor
}
func (m *holdWaitingModel) Complete(ctx context.Context, _ string) (execution.ModelResponse, error) {
	close(m.started)
	<-ctx.Done()
	return execution.ModelResponse{}, ctx.Err()
}

// #178: denying later reservations is insufficient if the admitted call's
// context remains live after the durable organization freeze commits.
func TestSecurityFreezeCancelsAlreadyDispatchedInference(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	if err = store.ActivateInferencePolicy(t.Context(), testInferencePolicy(now)); err != nil {
		t.Fatal(err)
	}
	model := &holdWaitingModel{started: make(chan struct{})}
	guard, err := inference.NewGuardedAdapter(store, model)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	ctx, err = inference.WithScope(ctx, testInferenceRequest("held-call").Scope)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, callErr := guard.Complete(ctx, "offline synthetic call"); done <- callErr }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(6 * time.Second):
		}
	}()
	select {
	case <-model.started:
	case err := <-done:
		t.Fatalf("call did not start: %v", err)
	case <-time.After(time.Second):
		t.Fatal("provider did not start")
	}
	appendInferenceFreeze(t, store, "organization-1", 1, true)
	select {
	case err := <-done:
		// Put the result back so deferred cleanup need not wait a second time.
		done <- err
		if err == nil {
			t.Fatal("security-held inference returned success")
		}
		var hold core.SecurityHoldCause
		if !errors.As(err, &hold) || !errors.Is(err, core.ErrOrganizationFrozen) || hold.OrganizationID != "organization-1" || hold.EventRef == "" || hold.Sequence <= 0 {
			t.Fatalf("provider cancellation lost the exact committed hold: %v", err)
		}
		if err = store.ValidateInferenceAdmissions(t.Context()); err != nil {
			t.Fatal(err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("committed freeze left the admitted provider context running")
	}
}

func TestLiveContainmentIsTenantScopedAndReleaseDoesNotReviveCalls(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	first, releaseFirst, err := store.BeginInferenceContext(t.Context(), "organization-1")
	if err != nil {
		t.Fatal(err)
	}
	defer releaseFirst()
	other, releaseOther, err := store.BeginInferenceContext(t.Context(), "organization-2")
	if err != nil {
		t.Fatal(err)
	}
	defer releaseOther()
	appendInferenceFreeze(t, store, "organization-1", 1, true)
	if first.Err() == nil || other.Err() != nil {
		t.Fatal("freeze cancellation crossed or missed its tenant")
	}
	if !errors.Is(context.Cause(first), core.ErrOrganizationFrozen) {
		t.Fatal("freeze cause was lost")
	}
	var cause core.SecurityHoldCause
	if !errors.As(context.Cause(first), &cause) || cause.OrganizationID != "organization-1" || cause.EventRef == "" || cause.Sequence <= 0 {
		t.Fatal("cancellation lacks exact durable hold reference")
	}
	var eventType, organization string
	if err := store.db.QueryRowContext(t.Context(), `SELECT event_type,organization_id FROM events WHERE event_id=? AND sequence=?`, cause.EventRef, cause.Sequence).Scan(&eventType, &organization); err != nil || eventType != "FREEZE_SET" || organization != "organization-1" {
		t.Fatalf("hold reference is not committed authority: %v", err)
	}
	if _, release, err := store.BeginInferenceContext(t.Context(), "organization-1"); err == nil {
		release()
		t.Fatal("frozen tenant registered a new live call")
	}
	appendInferenceFreeze(t, store, "organization-1", 2, false)
	if first.Err() == nil {
		t.Fatal("release revived an interrupted execution")
	}
	fresh, releaseFresh, err := store.BeginInferenceContext(t.Context(), "organization-1")
	if err != nil {
		t.Fatal(err)
	}
	defer releaseFresh()
	if fresh.Err() != nil {
		t.Fatal("explicit release did not allow a fresh context")
	}
}

func TestLiveContainmentPreservesCallerCancellationCause(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	parent, cancel := context.WithCancel(t.Context())
	call, release, err := store.BeginExecutionContext(parent, "org-1")
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	defer release()
	cancel()
	if !errors.Is(context.Cause(call), context.Canceled) || errors.Is(context.Cause(call), core.ErrOrganizationFrozen) {
		t.Fatal("caller cancellation was misreported as security hold")
	}
}

func TestLiveContainmentFailsClosedWhenAuthorityReadFails(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	call, release, err := store.BeginExecutionContext(t.Context(), "org-1")
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	defer release()
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-call.Done():
	case <-time.After(time.Second):
		t.Fatal("lost authority did not stop live work")
	}
	if !errors.Is(context.Cause(call), core.ErrContainmentUnavailable) {
		t.Fatalf("authority failure cause=%v", context.Cause(call))
	}
}

func TestLiveContainmentObservesAnotherHandleAndLatchesRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	reader, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	writer, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	call, release, err := reader.BeginInferenceContext(t.Context(), "organization-1")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	appendInferenceFreeze(t, writer, "organization-1", 1, true)
	appendInferenceFreeze(t, writer, "organization-1", 2, false)
	select {
	case <-call.Done():
	case <-time.After(time.Second):
		t.Fatal("another handle's freeze/release revived live work")
	}
	var cause core.SecurityHoldCause
	if !errors.As(context.Cause(call), &cause) {
		t.Fatalf("missing exact cross-handle hold cause: %v", context.Cause(call))
	}
	var holdEvent string
	if err := reader.db.QueryRowContext(t.Context(), `SELECT admission_event_id FROM records WHERE kind='organization_freeze' AND record_id='organization-1' AND version=1`).Scan(&holdEvent); err != nil {
		t.Fatal(err)
	}
	if cause.EventRef != holdEvent || cause.OrganizationID != "organization-1" {
		t.Fatalf("cancellation attributed to wrong authority: %+v", cause)
	}
	fresh, finish, err := reader.BeginInferenceContext(t.Context(), "organization-1")
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	if fresh.Err() != nil {
		t.Fatal("released authority rejected a fresh generation")
	}
}

func TestLiveContainmentReleaseOnlyDoesNotInventHold(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	appendInferenceFreeze(t, store, "organization-1", 1, false)
	next, cause, err := store.containmentSince(t.Context(), "organization-1", 0)
	if err != nil || cause != nil || next <= 0 {
		t.Fatalf("release-only observation: next=%d cause=%v err=%v", next, cause, err)
	}
	appendInferenceFreeze(t, store, "organization-1", 2, true)
	appendInferenceFreeze(t, store, "organization-1", 3, false)
	_, cause, err = store.containmentSince(t.Context(), "organization-1", next)
	if err != nil || cause == nil || cause.Sequence <= next {
		t.Fatalf("missed intervening freeze: cause=%v err=%v", cause, err)
	}
}

func TestContainmentReaderAllowsAuthorityWriterToWait(t *testing.T) {
	path := filepath.Join(t.TempDir(), "contention.db")
	reader, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	writer, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	snapshot, err := reader.db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = snapshot.Rollback() }()
	var count int
	if err := snapshot.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM events").Scan(&count); err != nil {
		t.Fatal(err)
	}
	written := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- writer.withTx(t.Context(), func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(t.Context(), "UPDATE events SET sequence=sequence WHERE 1=0"); err != nil {
				return err
			}
			close(written)
			return nil
		})
	}()
	select {
	case err := <-done:
		t.Fatalf("writer failed before commit: %v", err)
	case <-written:
	}
	select {
	case err := <-done:
		t.Fatalf("writer did not wait for snapshot: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := snapshot.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("writer commit after reader release: %v", err)
	}
}

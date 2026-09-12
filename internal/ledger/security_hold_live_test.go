package ledger

import (
	"context"
	"database/sql"
	"database/sql/driver"
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

func TestPlanningRetryRequiresExactNonDispatchProof(t *testing.T) {
	for _, proof := range []bool{false, true} {
		t.Run(fmt.Sprintf("proof-%t", proof), func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			request := testInferenceRequest("planning-attempt-1")
			scope := request.Scope
			draft := events.TrustedDraft{OrganizationID: scope.OrganizationID, EventType: "PLANNING_CONTEXT_MANIFESTED", SourceActorID: "runtime", SourceExecutionID: scope.ExecutionID, TaskID: scope.TaskID, CorrelationID: scope.CorrelationID, Payload: map[string]string{}}
			if _, err := store.Append(t.Context(), draft); err != nil {
				t.Fatal(err)
			}
			if proof {
				if err := store.RecordInferenceNotSent(t.Context(), request); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := store.Append(t.Context(), draft); err == nil {
				t.Fatal("same planning execution was admitted twice")
			}
			appendInferenceFreeze(t, store, scope.OrganizationID, 1, true)
			draft.SourceExecutionID = "planning-attempt-2"
			if _, err := store.Append(t.Context(), draft); err == nil {
				t.Fatal("current freeze admitted planning")
			}
			appendInferenceFreeze(t, store, scope.OrganizationID, 2, false)
			err = store.CheckExecutionContainment(t.Context(), scope.OrganizationID, scope.TaskID, scope.CorrelationID, scope.ExecutionID)
			if (err == nil) != proof {
				t.Fatalf("recovery proof=%t err=%v", proof, err)
			}
			if _, err := store.Append(t.Context(), draft); (err == nil) != proof {
				t.Fatalf("retry proof=%t err=%v", proof, err)
			}
		})
	}
}

func TestNotSentEvidenceRequiresClosedReservations(t *testing.T) {
	for _, state := range []string{"no-reservation", "reserved", "NOT_SENT", "UNCERTAIN", "COMPLETED"} {
		t.Run(state, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if err := store.ActivateInferencePolicy(t.Context(), testInferencePolicy(time.Now().UTC())); err != nil {
				t.Fatal(err)
			}
			request := testInferenceRequest("not-sent-proof")
			if state != "no-reservation" {
				reservation, err := store.ReserveInference(t.Context(), request)
				if err != nil {
					t.Fatal(err)
				}
				if state != "reserved" {
					var usage *events.InferenceUsageRecordedPayload
					if state == "COMPLETED" {
						value := testInferenceUsage()
						usage = &value
					}
					if _, err := store.ReconcileInference(t.Context(), reservation, usage, inference.Reconciliation(state)); err != nil {
						t.Fatal(err)
					}
				}
			}
			err = store.RecordInferenceNotSent(t.Context(), request)
			want := state == "no-reservation" || state == "NOT_SENT"
			if (err == nil) != want {
				t.Fatalf("not-sent evidence state=%s err=%v", state, err)
			}
			if want {
				if err := store.RecordInferenceNotSent(t.Context(), request); err != nil {
					t.Fatal(err)
				}
				request.Scope.RequestID = "replacement-request"
				if _, err := store.ReserveInference(t.Context(), request); err == nil {
					t.Fatal("closed invocation admitted new reservation")
				}
			}
			if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "organization-1", EventType: "INFERENCE_NOT_SENT", SourceActorID: "runtime", TaskID: "task-1", CorrelationID: "work-1", SourceExecutionID: "forged", Payload: map[string]string{"request_id": "forged"}}); err == nil {
				t.Fatal("ordinary publication forged not-sent evidence")
			}
		})
	}
}

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
	conn, err := store.watchDB.Conn(t.Context())
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
	conn, err := store.watchDB.Conn(t.Context())
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
	conn, err := store.watchDB.Conn(t.Context())
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
	conn, err := store.watchDB.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// Occupy the observation connection beyond a monitor acquisition deadline.
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

func TestContainmentSnapshotDeadlineBoundsDatabaseLockWait(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot-lock.db")
	reader, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	appendInferenceFreeze(t, reader, "organization-1", 1, false)
	writer, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	locked, err := writer.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = locked.ExecContext(context.Background(), "ROLLBACK"); _ = locked.Close() }()
	if _, err := locked.ExecContext(t.Context(), "BEGIN EXCLUSIVE"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	if _, _, err := reader.containmentSince(ctx, "organization-1", 0); err == nil {
		t.Fatal("locked authority snapshot succeeded")
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("snapshot ignored bounded cancellation: %s", elapsed)
	}
	if _, err := locked.ExecContext(t.Context(), "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := reader.containmentSince(t.Context(), "organization-1", 0); err != nil {
		t.Fatalf("snapshot failed after lock release: %v", err)
	}
	var timeout int
	if err := reader.db.QueryRowContext(t.Context(), "PRAGMA busy_timeout").Scan(&timeout); err != nil || timeout != 5000 {
		t.Fatalf("writer busy timeout changed: %d, %v", timeout, err)
	}
}

func TestContainmentSnapshotCancellationPreservesPrivateMemoryAuthority(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	appendInferenceFreeze(t, store, "organization-1", 1, false)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	err = store.withContainmentSnapshot(ctx, func(tx *sql.Tx) error {
		cancel()
		var count int
		return tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count)
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("snapshot ignored cancellation: %v", err)
	}
	// Force disposal even if this cancellation happened to reuse the connection.
	conn, err := store.watchDB.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Raw(func(any) error { return driver.ErrBadConn }); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("discard observation connection: %v", err)
	}
	_ = conn.Close()
	epoch, frozen, err := store.containmentEpoch(t.Context(), "organization-1")
	if err != nil || epoch <= 0 || frozen {
		t.Fatalf("cancelled snapshot destroyed authority: epoch=%d frozen=%t err=%v", epoch, frozen, err)
	}
	appendInferenceFreeze(t, store, "organization-1", 2, true)
}

func TestCancelledWriterPreservesPrivateMemoryLedger(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	appendInferenceFreeze(t, store, "organization-1", 1, false)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "DELETE FROM records"); err != nil {
		t.Fatal(err)
	}
	cancel()
	// Wait for asynchronous rollback, then explicitly inject connection disposal:
	// cancellation may reuse or discard the connection depending on its timing.
	conn, err := store.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Raw(func(any) error { return driver.ErrBadConn }); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("inject connection disposal: %v", err)
	}
	_ = conn.Close()
	var count int
	err = store.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM records WHERE kind='organization_freeze'").Scan(&count)
	if err != nil || count != 1 {
		t.Fatalf("cancelled writer erased committed authority: count=%d err=%v", count, err)
	}
	other, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.Close() }()
	epoch, _, err := other.containmentEpoch(t.Context(), "organization-1")
	if err != nil || epoch != 0 {
		t.Fatalf("private memory databases shared authority: epoch=%d err=%v", epoch, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if store.memoryKeepalive.Stats().OpenConnections != 0 {
		t.Fatal("closed ledger retained its memory keeper")
	}
}

func TestLiveContainmentReleaseDoesNotWaitForLockedSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "release-lock.db")
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
	call, release, err := reader.BeginExecutionContext(t.Context(), "organization-1")
	if err != nil {
		t.Fatal(err)
	}
	locked, err := writer.db.Conn(t.Context())
	if err != nil {
		release()
		t.Fatal(err)
	}
	defer func() { _, _ = locked.ExecContext(context.Background(), "ROLLBACK"); _ = locked.Close() }()
	if _, err := locked.ExecContext(t.Context(), "BEGIN EXCLUSIVE"); err != nil {
		release()
		t.Fatal(err)
	}
	select {
	case <-call.Done():
	case <-time.After(2 * containmentObservationTimeout):
		t.Fatal("locked snapshot did not stop live execution")
	}
	if !errors.Is(context.Cause(call), core.ErrContainmentUnavailable) {
		t.Fatalf("stop cause: %v", context.Cause(call))
	}
	done := make(chan struct{})
	go func() { release(); close(done) }()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("release remained blocked on cancelled snapshot")
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

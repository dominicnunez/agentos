package ledger

import (
	"context"
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

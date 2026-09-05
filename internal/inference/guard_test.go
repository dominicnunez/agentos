package inference

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
)

type guardStore struct {
	reservation  Reservation
	result       Reconciliation
	usage        *events.InferenceUsageRecordedPayload
	cost         int64
	reserveErr   error
	reconcileErr error
}

func (*guardStore) ActivateInferencePolicy(context.Context, Policy) error { return nil }

func (s *guardStore) ReserveInference(_ context.Context, request InferenceRequest) (Reservation, error) {
	if s.reserveErr != nil {
		return Reservation{}, s.reserveErr
	}
	s.reservation = Reservation{
		ID: "reservation-1", PolicyFingerprint: "policy-1", Mode: MeteredAPI, Request: request,
		ReservedInputTokens: 100, ReservedOutputTokens: 20, ReservedCostNanoUSD: 100,
		WindowStartedAt: time.Now().UTC(), WindowExpiresAt: time.Now().UTC().Add(time.Hour),
	}
	return s.reservation, nil
}

func (s *guardStore) ReconcileInference(_ context.Context, _ Reservation, usage *events.InferenceUsageRecordedPayload, result Reconciliation) (int64, error) {
	s.result = result
	s.usage = usage
	return s.cost, s.reconcileErr
}

type guardModel struct {
	called   bool
	response execution.ModelResponse
	err      error
}

func (*guardModel) Name() string { return "provider/model" }
func (*guardModel) Descriptor() execution.ModelDescriptor {
	return execution.ModelDescriptor{Provider: "provider", Model: "model", ExecutionProfileVersion: "profile-v1"}
}
func (m *guardModel) Complete(context.Context, string) (execution.ModelResponse, error) {
	m.called = true
	return m.response, m.err
}

func guardedContext(t *testing.T) context.Context {
	t.Helper()
	ctx, err := WithScope(t.Context(), Scope{
		OrganizationID: "organization-1", Purpose: PurposeTaskExecution, RequestID: "execution-1",
		TaskID: "task-1", ExecutionID: "execution-1", CorrelationID: "work-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func TestGuardedAdapterFailsClosedWithoutDurableScopeOrReservation(t *testing.T) {
	store := &guardStore{}
	model := &guardModel{}
	adapter, err := NewGuardedAdapter(store, model)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.Complete(t.Context(), "prompt"); err == nil || model.called {
		t.Fatalf("provider was called without scope: %v", err)
	}
	store.reserveErr = errors.New("budget exhausted")
	if _, err := adapter.Complete(guardedContext(t), "prompt"); err == nil || model.called || execution.ModelErrorClass(err) != string(execution.InferenceDenied) || strings.Contains(err.Error(), "budget exhausted") {
		t.Fatalf("provider was called without a reservation: %v", err)
	}
}

func TestGuardedAdapterSanitizesFailuresWithoutChangingReconciliation(t *testing.T) {
	private := errors.New("Authorization: Bearer synthetic-private-canary")
	for _, tc := range []struct {
		name                                  string
		providerErr, reserveErr, reconcileErr error
		want                                  Reconciliation
		code                                  execution.ModelFaultCode
		called                                bool
	}{
		{name: "authorization", reserveErr: private, code: execution.InferenceDenied},
		{name: "uncertain", providerErr: private, want: ReconciliationUncertain, code: execution.ModelCallFailed, called: true},
		{name: "not sent", providerErr: execution.RequestNotSent(private), want: ReconciliationNotSent, code: execution.ModelCallFailed, called: true},
		{name: "cancelled", providerErr: errors.Join(private, context.Canceled), want: ReconciliationUncertain, code: execution.ModelCallFailed, called: true},
		{name: "accounting", providerErr: private, reconcileErr: private, want: ReconciliationUncertain, code: execution.InferenceRecordFailed, called: true},
		{name: "successful response accounting", reconcileErr: private, want: ReconciliationCompleted, code: execution.InferenceRecordFailed, called: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &guardStore{reserveErr: tc.reserveErr, reconcileErr: tc.reconcileErr}
			model := &guardModel{err: tc.providerErr, response: execution.ModelResponse{Text: "private response", Usage: events.InferenceUsageRecordedPayload{
				Source: "provider", Provider: "provider", Model: "model", InputTokens: 1, OutputTokens: 1, TotalTokens: 2,
			}}}
			adapter, _ := NewGuardedAdapter(store, model)
			response, err := adapter.Complete(guardedContext(t), "prompt")
			if err == nil || response.Text != "" || model.called != tc.called || store.result != tc.want || execution.ModelErrorClass(err) != string(tc.code) {
				t.Fatalf("failure changed accounting or response admission: result=%s class=%s", store.result, execution.ModelErrorClass(err))
			}
			if strings.Contains(err.Error(), "synthetic-private-canary") || errors.Is(err, private) {
				t.Fatal("private provider or store diagnostic escaped the shared boundary")
			}
			if errors.Is(err, context.Canceled) != errors.Is(tc.providerErr, context.Canceled) {
				t.Fatal("cancellation fact changed")
			}
		})
	}
}

func TestGuardedAdapterReconcilesUsageBeforeReturning(t *testing.T) {
	store := &guardStore{cost: 25_000_000}
	model := &guardModel{response: execution.ModelResponse{
		Text: "answer", Usage: events.InferenceUsageRecordedPayload{
			Source: "provider", Provider: "provider", Model: "model", InputTokens: 10, OutputTokens: 5, TotalTokens: 15,
		},
	}}
	adapter, err := NewGuardedAdapter(store, model)
	if err != nil {
		t.Fatal(err)
	}
	response, err := adapter.Complete(guardedContext(t), "prompt")
	if err != nil {
		t.Fatal(err)
	}
	if store.result != ReconciliationCompleted || store.usage == nil || response.Usage.CostUSD == nil || *response.Usage.CostUSD != 0.025 {
		t.Fatalf("usage was not durably reconciled: %#v %#v", store, response.Usage)
	}
}

func TestGuardedAdapterRetainsUncertainAndViolatingReservations(t *testing.T) {
	t.Run("definite pre-send rejection", func(t *testing.T) {
		store := &guardStore{}
		model := &guardModel{err: execution.RequestNotSent(errors.New("invalid local prompt"))}
		adapter, _ := NewGuardedAdapter(store, model)
		if _, err := adapter.Complete(guardedContext(t), "prompt"); err == nil || store.result != ReconciliationNotSent || store.usage != nil {
			t.Fatalf("definite pre-send rejection retained a quota charge: %v %#v", err, store)
		}
	})
	t.Run("provider error", func(t *testing.T) {
		store := &guardStore{}
		model := &guardModel{err: errors.New("connection lost")}
		adapter, _ := NewGuardedAdapter(store, model)
		if _, err := adapter.Complete(guardedContext(t), "prompt"); err == nil || store.result != ReconciliationUncertain || store.usage != nil {
			t.Fatalf("provider uncertainty was not preserved: %v %#v", err, store)
		}
	})
	t.Run("overuse", func(t *testing.T) {
		store := &guardStore{}
		model := &guardModel{response: execution.ModelResponse{Usage: events.InferenceUsageRecordedPayload{
			Source: "provider", Provider: "provider", Model: "model", InputTokens: 10, OutputTokens: 21, TotalTokens: 31,
		}}}
		adapter, _ := NewGuardedAdapter(store, model)
		if _, err := adapter.Complete(guardedContext(t), "prompt"); err == nil || store.result != ReconciliationViolation || store.usage == nil {
			t.Fatalf("provider overuse was not failed closed: %v %#v", err, store)
		}
	})
}

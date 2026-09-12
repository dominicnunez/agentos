package effects

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/ledger"
)

type effectFunc func(context.Context, core.EffectObligation) ([]string, error)

func (f effectFunc) Apply(ctx context.Context, o core.EffectObligation) ([]string, error) {
	return f(ctx, o)
}

func TestAdapterErrorNeedsReconciliation(t *testing.T) {
	transportErr := errors.New("response lost after applying effect")
	for _, tc := range []struct {
		name     string
		cause    error
		evidence []string
		terminal ReconciliationState
	}{
		{name: "lost-response", cause: transportErr, terminal: ReconciliationConfirmed},
		{name: "receipt-and-error", cause: transportErr, evidence: []string{"partial-receipt"}, terminal: ReconciliationConfirmed},
		{name: "cancelled-caller", cause: context.Canceled, terminal: ReconciliationConfirmed},
		{name: "adapter-deadline", cause: context.DeadlineExceeded, terminal: ReconciliationFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			readCtx := context.Background()
			path := filepath.Join(t.TempDir(), "effects.db")
			store, err := ledger.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			obligation := core.EffectObligation{
				ID: "effect-error", OrganizationID: "org", TaskID: "task", ActorID: "actor",
				Action: "cache", Resource: "record", Scope: "org", AuthorizationRefs: []string{"lease"},
				IdempotencyKey: "key", ReplayContext: map[string]string{"value": "ready"},
			}
			setEffectFingerprint(t, &obligation)
			persistCapability(t, store, obligation)
			applied := 0
			apply := effectFunc(func(ctx context.Context, o core.EffectObligation) ([]string, error) {
				latest, _, err := loadEffect(ctx, store, o.ID)
				if err != nil || latest.Status != core.EffectAttempted {
					t.Fatalf("effect dispatched without durable attempt: %+v err=%v", latest, err)
				}
				applied++
				switch {
				case errors.Is(tc.cause, context.Canceled):
					cancel()
					return nil, ctx.Err()
				case errors.Is(tc.cause, context.DeadlineExceeded):
					deadlineCtx, stop := context.WithDeadline(ctx, time.Unix(1, 0))
					defer stop()
					return nil, deadlineCtx.Err()
				default:
					return tc.evidence, tc.cause
				}
			})
			result, execErr := New(store, apply, nil).Execute(ctx, obligation)
			if result.Status != core.EffectAttempted || !errors.Is(execErr, ErrEffectUncertain) || !errors.Is(execErr, tc.cause) {
				t.Errorf("adapter error lost uncertainty or cause: status=%s err=%v", result.Status, execErr)
			}
			latest, version, err := loadEffect(readCtx, store, obligation.ID)
			if err != nil || version != 2 || latest.Status != core.EffectAttempted || len(latest.ConfirmationEvidenceRefs) != 0 || len(latest.ReconciliationEvidenceRefs) != 0 {
				t.Errorf("ambiguous effect no longer reconcilable: status=%s version=%d err=%v", latest.Status, version, err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := ledger.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			store = reopened
			result, err = New(store, apply, nil).Execute(readCtx, obligation)
			if result.Status != core.EffectAttempted || !errors.Is(err, ErrEffectUncertain) || applied != 1 {
				t.Errorf("retry changed uncertain attempt: status=%s calls=%d err=%v", result.Status, applied, err)
			}
			items, err := NewReconciliationService(store).Recover(readCtx, nil)
			if err != nil || len(items) != 1 || items[0].Disposition != RecoveryUncertain {
				t.Fatalf("unavailable status check lost uncertainty: items=%+v err=%v", items, err)
			}
			checker := &statusReconciler{observation: ReconciliationObservation{
				State: tc.terminal, EvidenceRefs: []string{"destination-evidence"},
			}}
			items, err = NewReconciliationService(store).Recover(readCtx, ReconcilerResolverFunc(func(o core.EffectObligation) (Reconciler, bool) {
				if o.ID != obligation.ID || o.IdempotencyKey != obligation.IdempotencyKey || o.EffectFingerprint != obligation.EffectFingerprint {
					t.Fatalf("reconciliation lost effect identity: %+v", o)
				}
				return checker, true
			}))
			wantDisposition, wantStatus := RecoveryConfirmed, core.EffectConfirmed
			if tc.terminal == ReconciliationFailed {
				wantDisposition, wantStatus = RecoveryFailed, core.EffectFailed
			}
			if err != nil || len(items) != 1 || items[0].Disposition != wantDisposition || checker.calls != 1 || applied != 1 {
				t.Fatalf("recovery missed dispatched effect: items=%+v checks=%d applies=%d err=%v", items, checker.calls, applied, err)
			}
			latest, version, err = loadEffect(readCtx, store, obligation.ID)
			if err != nil || version != 3 || latest.Status != wantStatus || latest.ReconciledAt == nil || len(latest.ReconciliationEvidenceRefs) != 1 || latest.ReconciliationEvidenceRefs[0] != "destination-evidence" {
				t.Fatalf("destination evidence not retained: %+v version=%d err=%v", latest, version, err)
			}
			if wantStatus == core.EffectConfirmed {
				if len(latest.ConfirmationEvidenceRefs) != 1 || latest.ConfirmationEvidenceRefs[0] != "destination-evidence" {
					t.Fatalf("confirmation evidence not retained: %+v", latest)
				}
			} else if len(latest.ConfirmationEvidenceRefs) != 0 {
				t.Fatalf("failure acquired confirmation evidence: %+v", latest)
			}
		})
	}
}

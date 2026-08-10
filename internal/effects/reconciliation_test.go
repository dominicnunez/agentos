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

type statusReconciler struct {
	observation ReconciliationObservation
	err         error
	calls       int
}

func (r *statusReconciler) Check(_ context.Context, _ core.EffectObligation) (ReconciliationObservation, error) {
	r.calls++
	return r.observation, r.err
}

func TestRecoveryConfirmsAttemptedEffectAfterRestartWithoutResend(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agentos.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	attempted := attemptedEffect("effect-1")
	persistAttemptedEffect(t, store, attempted)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	checker := &statusReconciler{observation: ReconciliationObservation{State: ReconciliationConfirmed, EvidenceRefs: []string{"destination-receipt-1"}}}
	report, err := NewReconciliationService(store).Recover(ctx, ReconcilerResolverFunc(func(obligation core.EffectObligation) (Reconciler, bool) {
		if obligation.ID != attempted.ID || obligation.IdempotencyKey != attempted.IdempotencyKey {
			t.Fatalf("unexpected obligation=%+v", obligation)
		}
		return checker, true
	}))
	if err != nil || len(report) != 1 || report[0].Disposition != RecoveryConfirmed || checker.calls != 1 {
		t.Fatalf("report=%+v calls=%d err=%v", report, checker.calls, err)
	}
	latest, version, err := loadEffect(ctx, store, attempted.ID)
	if err != nil || version != 3 || latest.Status != core.EffectConfirmed || latest.ReconciledAt == nil || len(latest.ConfirmationEvidenceRefs) != 1 || latest.ConfirmationEvidenceRefs[0] != "destination-receipt-1" {
		t.Fatalf("latest=%+v version=%d err=%v", latest, version, err)
	}
	events, err := store.Events(ctx, "")
	if err != nil || len(events) != 3 || len(events[2].ArtifactRefs) != 1 || events[2].ArtifactRefs[0] != "destination-receipt-1" {
		t.Fatalf("events=%+v err=%v", events, err)
	}
	report, err = NewReconciliationService(store).Recover(ctx, ReconcilerResolverFunc(func(core.EffectObligation) (Reconciler, bool) {
		t.Fatal("terminal effect was checked again")
		return nil, false
	}))
	if err != nil || len(report) != 0 {
		t.Fatalf("terminal recovery report=%+v err=%v", report, err)
	}
}

func TestRecoveryFailsClosedWhenStatusIsUnavailableOrUnsupported(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	checks := map[core.ID]*statusReconciler{
		"failed":      {observation: ReconciliationObservation{State: ReconciliationFailed, EvidenceRefs: []string{"failure-record-1"}}},
		"unknown":     {observation: ReconciliationObservation{State: ReconciliationUnknown}},
		"check-error": {err: errors.New("destination unavailable")},
		"invalid":     {observation: ReconciliationObservation{State: "SUCCEEDED", EvidenceRefs: []string{"claim"}}},
		"no-evidence": {observation: ReconciliationObservation{State: ReconciliationConfirmed}},
	}
	for _, id := range []core.ID{"failed", "unknown", "check-error", "invalid", "no-evidence", "unsupported"} {
		persistAttemptedEffect(t, store, attemptedEffect(id))
	}
	report, err := NewReconciliationService(store).Recover(ctx, ReconcilerResolverFunc(func(obligation core.EffectObligation) (Reconciler, bool) {
		checker, ok := checks[obligation.ID]
		return checker, ok
	}))
	if err != nil || len(report) != 6 {
		t.Fatalf("report=%+v err=%v", report, err)
	}
	for _, item := range report {
		latest, version, loadErr := loadEffect(ctx, store, item.EffectID)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		if item.EffectID == "failed" {
			if item.Disposition != RecoveryFailed || version != 3 || latest.Status != core.EffectFailed || latest.ReconciledAt == nil || len(latest.ReconciliationEvidenceRefs) != 1 || len(latest.ConfirmationEvidenceRefs) != 0 {
				t.Fatalf("failed item=%+v latest=%+v version=%d", item, latest, version)
			}
			continue
		}
		if item.Disposition != RecoveryUncertain || item.Reason == "" || version != 2 || latest.Status != core.EffectAttempted {
			t.Fatalf("uncertain item=%+v latest=%+v version=%d", item, latest, version)
		}
	}
}

func TestRecoveryDoesNotApplyStaleObservationToChangedAttempt(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	attempted := attemptedEffect("changed")
	persistAttemptedEffect(t, store, attempted)
	checker := ReconcilerResolverFunc(func(core.EffectObligation) (Reconciler, bool) {
		return reconcilerFunc(func(context.Context, core.EffectObligation) (ReconciliationObservation, error) {
			changed := attempted
			changed.AttemptCount++
			now := attempted.LastAttemptAt.Add(time.Second)
			changed.LastAttemptAt = &now
			if err := store.AppendRecord(ctx, "org-1", "EFFECT_OBLIGATION_TRANSITIONED", "", "task-1", changed.AuthorizationRefs, nil, "effect", string(changed.ID), 3, changed); err != nil {
				t.Fatal(err)
			}
			return ReconciliationObservation{State: ReconciliationConfirmed, EvidenceRefs: []string{"stale-receipt"}}, nil
		}), true
	})
	report, err := NewReconciliationService(store).Recover(ctx, checker)
	if err != nil || len(report) != 1 || report[0].Disposition != RecoveryUncertain || report[0].Reason != "effect attempt changed during reconciliation" {
		t.Fatalf("report=%+v err=%v", report, err)
	}
	latest, version, err := loadEffect(ctx, store, attempted.ID)
	if err != nil || version != 3 || latest.Status != core.EffectAttempted || latest.AttemptCount != 2 {
		t.Fatalf("latest=%+v version=%d err=%v", latest, version, err)
	}
}

type reconcilerFunc func(context.Context, core.EffectObligation) (ReconciliationObservation, error)

func (f reconcilerFunc) Check(ctx context.Context, obligation core.EffectObligation) (ReconciliationObservation, error) {
	return f(ctx, obligation)
}

func attemptedEffect(id core.ID) core.EffectObligation {
	now := time.Now().UTC()
	return core.EffectObligation{
		ID: id, OrganizationID: "org-1", TaskID: "task-1", ActorID: "agent-1",
		Action: "send", Resource: "destination-1", Scope: "org-1", Descriptor: "send message",
		EffectFingerprint: "fingerprint-1", AuthorizationRefs: []string{"lease-1"}, ApprovalRef: "approval-1",
		IdempotencyKey: "idempotency-" + string(id), ReplayContext: map[string]string{"body": "hello"},
		Status: core.EffectAttempted, AttemptCount: 1, LastAttemptAt: &now, CreatedAt: now.Add(-time.Minute),
	}
}

func persistAttemptedEffect(t *testing.T, store *ledger.SQLite, attempted core.EffectObligation) {
	t.Helper()
	pending := attempted
	pending.Status = core.EffectPending
	pending.AttemptCount = 0
	pending.LastAttemptAt = nil
	if err := store.AppendRecord(context.Background(), string(pending.OrganizationID), "EFFECT_OBLIGATION_TRANSITIONED", "", string(pending.TaskID), pending.AuthorizationRefs, nil, "effect", string(pending.ID), 1, pending); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendRecord(context.Background(), string(attempted.OrganizationID), "EFFECT_OBLIGATION_TRANSITIONED", "", string(attempted.TaskID), attempted.AuthorizationRefs, nil, "effect", string(attempted.ID), 2, attempted); err != nil {
		t.Fatal(err)
	}
}

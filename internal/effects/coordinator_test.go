package effects

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/approvals"
	"github.com/dominicnunez/agentos/internal/authority"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/ledger"
)

type adapter struct{ called bool }

func (a *adapter) Apply(_ context.Context, _ core.EffectObligation) ([]string, error) {
	a.called = true
	return []string{"receipt"}, nil
}

type approvalReader struct{ approval core.HumanApproval }

func (r *approvalReader) Get(context.Context, core.ID) (core.HumanApproval, error) {
	return r.approval, nil
}

func persistCapability(t *testing.T, l *ledger.SQLite, obligation core.EffectObligation) {
	t.Helper()
	lease := core.CapabilityLease{ID: core.ID(obligation.AuthorizationRefs[0]), ActorID: obligation.ActorID, OriginTaskID: obligation.TaskID, Action: obligation.Action, Resource: obligation.Resource, Scope: obligation.Scope}
	if err := l.AppendRecord(context.Background(), string(obligation.OrganizationID), "CAPABILITY_GRANTED", "human", string(obligation.TaskID), nil, nil, "capability_lease", obligation.AuthorizationRefs[0], 1, lease); err != nil {
		t.Fatal(err)
	}
}

func persistApproval(t *testing.T, l *ledger.SQLite, approval core.HumanApproval) {
	t.Helper()
	if err := l.AppendRecord(context.Background(), string(approval.OrganizationID), "APPROVAL_DECIDED", string(approval.DecidedBy), string(approval.TaskID), nil, nil, "approval", string(approval.ID), 1, approval); err != nil {
		t.Fatal(err)
	}
}

func TestPersistBeforeEffectAndFingerprintApproval(t *testing.T) {
	l, e := ledger.Open(":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	a := &adapter{}
	reader := &approvalReader{}
	c := New(l, a, reader)
	fp, _ := Fingerprint("send", "customer", map[string]string{"body": "hi"})
	o := core.EffectObligation{ID: "e", OrganizationID: "org", TaskID: "task", ActorID: "actor", Action: "send", Resource: "customer", Scope: "org", ConsequenceBoundary: core.BoundaryPublicExternal, Descriptor: "send message", EffectFingerprint: fp, AuthorizationRefs: []string{"lease"}, ApprovalRef: "approval", IdempotencyKey: "key", ReplayContext: map[string]string{"body": "hi"}}
	persistCapability(t, l, o)
	if _, e = c.Prepare(context.Background(), o); e != nil {
		t.Fatal(e)
	}
	reader.approval = core.HumanApproval{ID: "approval", OrganizationID: "org", TaskID: "task", EffectObligationID: "e", Action: "send", Resource: "customer", Boundary: core.BoundaryPublicExternal, Status: core.ApprovalApproved, EffectFingerprint: "different"}
	if _, e = c.Execute(context.Background(), o); e == nil || a.called {
		t.Fatal("mismatched approval reached adapter")
	}
	reader.approval.EffectFingerprint = fp
	persistApproval(t, l, reader.approval)
	got, e := c.Execute(context.Background(), o)
	if e != nil || got.Status != core.EffectConfirmed || !a.called {
		t.Fatalf("got=%+v err=%v", got, e)
	}
	rows, _ := l.Records(context.Background(), "effect", "e")
	if len(rows) != 3 {
		t.Fatalf("versions=%d", len(rows))
	}
	events, err := l.Events(context.Background(), "")
	if err != nil || len(events) != 6 {
		t.Fatalf("effect transitions were not ledgered: events=%d err=%v", len(events), err)
	}
}

func TestPrepareRejectsFingerprintThatDoesNotBindReplayContext(t *testing.T) {
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	coordinator := New(l, &adapter{}, &approvalReader{})
	safeFingerprint, err := Fingerprint("send", "customer", map[string]string{"body": "safe"})
	if err != nil {
		t.Fatal(err)
	}
	obligation := core.EffectObligation{ID: "effect-1", OrganizationID: "org", TaskID: "task", ActorID: "actor", Action: "send", Resource: "customer", Scope: "org", ConsequenceBoundary: core.BoundaryPublicExternal, Descriptor: "send message", EffectFingerprint: safeFingerprint, AuthorizationRefs: []string{"lease"}, ApprovalRef: "approval-1", IdempotencyKey: "key-1", ReplayContext: map[string]string{"body": "unapproved"}}
	if _, err = coordinator.Prepare(context.Background(), obligation); err == nil {
		t.Fatal("mismatched replay context was persisted under an approved fingerprint")
	}
	rows, err := l.Records(context.Background(), "effect", "effect-1")
	if err != nil || len(rows) != 0 {
		t.Fatalf("mismatched effect reached durable records: rows=%d err=%v", len(rows), err)
	}
}

func TestUnprotectedEffectsReloadDurableState(t *testing.T) {
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	adapter := &adapter{}
	coordinator := New(l, adapter, nil)
	fingerprint, err := Fingerprint("cache", "record-1", map[string]string{"value": "ready"})
	if err != nil {
		t.Fatal(err)
	}
	obligation := core.EffectObligation{ID: "effect-confirmed", OrganizationID: "org", TaskID: "task", ActorID: "actor", Action: "cache", Resource: "record-1", Scope: "org", EffectFingerprint: fingerprint, AuthorizationRefs: []string{"lease"}, IdempotencyKey: "key-confirmed", ReplayContext: map[string]string{"value": "ready"}}
	persistCapability(t, l, obligation)
	confirmed, err := coordinator.Execute(context.Background(), obligation)
	if err != nil || confirmed.Status != core.EffectConfirmed || !adapter.called {
		t.Fatalf("initial unprotected effect failed: result=%+v called=%v err=%v", confirmed, adapter.called, err)
	}
	adapter.called = false
	confirmed, err = coordinator.Execute(context.Background(), obligation)
	if err != nil || confirmed.Status != core.EffectConfirmed || adapter.called {
		t.Fatalf("confirmed redelivery was not idempotent: result=%+v called=%v err=%v", confirmed, adapter.called, err)
	}

	uncertain := obligation
	uncertain.ID = "effect-uncertain"
	uncertain.IdempotencyKey = "key-uncertain"
	uncertain.Status = core.EffectPending
	if err = l.AppendRecord(context.Background(), "org", "EFFECT_OBLIGATION_TRANSITIONED", "", "task", nil, nil, "effect", string(uncertain.ID), 1, uncertain); err != nil {
		t.Fatal(err)
	}
	uncertain.Status = core.EffectAttempted
	uncertain.AttemptCount = 1
	if err = l.AppendRecord(context.Background(), "org", "EFFECT_OBLIGATION_TRANSITIONED", "", "task", nil, nil, "effect", string(uncertain.ID), 2, uncertain); err != nil {
		t.Fatal(err)
	}
	adapter.called = false
	requested := uncertain
	requested.Status = ""
	requested.AttemptCount = 0
	if _, err = coordinator.Execute(context.Background(), requested); !errors.Is(err, ErrEffectUncertain) || adapter.called {
		t.Fatalf("uncertain unprotected effect was replayed: called=%v err=%v", adapter.called, err)
	}
}

func TestSingleUseApprovalIsConsumedBeforeAdapter(t *testing.T) {
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	a := &adapter{}
	reader := &approvalReader{}
	c := New(l, a, reader)
	fp, _ := Fingerprint("send", "customer", map[string]string{"body": "hi"})
	reader.approval = core.HumanApproval{ID: "approval-1", OrganizationID: "org", TaskID: "task", EffectObligationID: "effect-1", Action: "send", Resource: "customer", Boundary: core.BoundaryPublicExternal, Status: core.ApprovalApproved, EffectFingerprint: fp, SingleUse: true}
	o := core.EffectObligation{ID: "effect-1", OrganizationID: "org", TaskID: "task", ActorID: "actor", Action: "send", Resource: "customer", Scope: "org", ConsequenceBoundary: core.BoundaryPublicExternal, Descriptor: "send message", EffectFingerprint: fp, AuthorizationRefs: []string{"lease"}, ApprovalRef: "approval-1", IdempotencyKey: "key-1", ReplayContext: map[string]string{"body": "hi"}}
	persistCapability(t, l, o)
	if _, err = c.Prepare(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	persistApproval(t, l, reader.approval)
	if _, err = c.Execute(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	a.called = false
	confirmed, err := c.Execute(context.Background(), o)
	if err != nil || confirmed.Status != core.EffectConfirmed || a.called {
		t.Fatalf("duplicate delivery re-applied confirmed effect: result=%+v called=%v err=%v", confirmed, a.called, err)
	}
	o.ID, o.IdempotencyKey = "effect-2", "key-2"
	if _, err = c.Prepare(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Execute(context.Background(), o); err == nil || a.called {
		t.Fatal("consumed approval authorized another adapter invocation")
	}
}

func TestProtectedEffectRejectsExpiredApprovalAndUnknownBoundary(t *testing.T) {
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	adapter := &adapter{}
	reader := &approvalReader{}
	coordinator := New(l, adapter, reader)
	fingerprint, _ := Fingerprint("deploy", "agent-os", map[string]string{"version": "1"})
	obligation := core.EffectObligation{ID: "effect-1", OrganizationID: "org", TaskID: "task", ActorID: "actor", Action: "deploy", Resource: "agent-os", Scope: "org", ConsequenceBoundary: core.BoundaryDeployment, Descriptor: "deploy Agent OS", EffectFingerprint: fingerprint, AuthorizationRefs: []string{"lease"}, ApprovalRef: "approval-1", IdempotencyKey: "key-1", ReplayContext: map[string]string{"version": "1"}}
	if _, err := coordinator.Prepare(context.Background(), obligation); err != nil {
		t.Fatal(err)
	}
	expired := time.Now().Add(-time.Minute)
	reader.approval = core.HumanApproval{ID: "approval-1", OrganizationID: "org", TaskID: "task", EffectObligationID: "effect-1", Action: "deploy", Resource: "agent-os", Boundary: core.BoundaryDeployment, Status: core.ApprovalApproved, EffectFingerprint: fingerprint, ExpiresAt: &expired}
	if _, err := coordinator.Execute(context.Background(), obligation); err == nil || adapter.called {
		t.Fatal("expired approval reached adapter")
	}
	unknown := obligation
	unknown.ID = "effect-unknown"
	unknown.ConsequenceBoundary = "NEW_UNREVIEWED_BOUNDARY"
	if _, err := coordinator.Prepare(context.Background(), unknown); err == nil {
		t.Fatal("unknown consequence boundary was treated as unprotected")
	}
}

func TestApprovalExpiryIsRecheckedInsideAttemptTransaction(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	adapter := &adapter{}
	fingerprint, err := Fingerprint("deploy", "agent-os", map[string]string{"version": "1"})
	if err != nil {
		t.Fatal(err)
	}
	obligation := core.EffectObligation{ID: "effect-1", OrganizationID: "org", TaskID: "task", ActorID: "actor", Action: "deploy", Resource: "agent-os", Scope: "org", ConsequenceBoundary: core.BoundaryDeployment, Descriptor: "deploy Agent OS", EffectFingerprint: fingerprint, AuthorizationRefs: []string{"lease"}, ApprovalRef: "approval-1", IdempotencyKey: "key-1", ReplayContext: map[string]string{"version": "1"}}
	persistCapability(t, l, obligation)
	coordinator := New(l, adapter, &approvalReader{})
	if _, err := coordinator.Prepare(ctx, obligation); err != nil {
		t.Fatal(err)
	}
	future := time.Now().UTC().Add(time.Hour)
	staleApproval := core.HumanApproval{ID: "approval-1", OrganizationID: "org", TaskID: "task", EffectObligationID: "effect-1", Action: "deploy", Resource: "agent-os", Boundary: core.BoundaryDeployment, Status: core.ApprovalApproved, EffectFingerprint: fingerprint, ExpiresAt: &future}
	coordinator.approvals = &approvalReader{approval: staleApproval}
	expired := time.Now().UTC().Add(-time.Minute)
	durableApproval := staleApproval
	durableApproval.ExpiresAt = &expired
	persistApproval(t, l, durableApproval)

	if _, err := coordinator.Execute(ctx, obligation); !errors.Is(err, approvals.ErrApprovalExpired) || adapter.called {
		t.Fatalf("expired durable approval reached adapter: called=%v err=%v", adapter.called, err)
	}
	rows, err := l.Records(ctx, "effect", "effect-1")
	if err != nil || len(rows) != 1 {
		t.Fatalf("expired approval advanced effect: versions=%d err=%v", len(rows), err)
	}
}

func TestInterruptedAttemptIsNotBlindlyReplayed(t *testing.T) {
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	adapter := &adapter{}
	reader := &approvalReader{}
	coordinator := New(l, adapter, reader)
	fingerprint, _ := Fingerprint("send", "customer", map[string]string{"body": "hi"})
	obligation := core.EffectObligation{ID: "effect-1", OrganizationID: "org", TaskID: "task", ActorID: "actor", Action: "send", Resource: "customer", Scope: "org", ConsequenceBoundary: core.BoundaryPublicExternal, Descriptor: "send message", EffectFingerprint: fingerprint, AuthorizationRefs: []string{"lease"}, ApprovalRef: "approval-1", IdempotencyKey: "key-1", ReplayContext: map[string]string{"body": "hi"}}
	pending, err := coordinator.Prepare(context.Background(), obligation)
	if err != nil {
		t.Fatal(err)
	}
	pending.Status = core.EffectAttempted
	pending.AttemptCount = 1
	if err := l.AppendRecord(context.Background(), "org", "EFFECT_OBLIGATION_TRANSITIONED", "", "task", nil, nil, "effect", "effect-1", 2, pending); err != nil {
		t.Fatal(err)
	}
	reader.approval = core.HumanApproval{ID: "approval-1", OrganizationID: "org", TaskID: "task", EffectObligationID: "effect-1", Action: "send", Resource: "customer", Boundary: core.BoundaryPublicExternal, Status: core.ApprovalApproved, EffectFingerprint: fingerprint}
	if _, err := coordinator.Execute(context.Background(), obligation); !errors.Is(err, ErrEffectUncertain) || adapter.called {
		t.Fatalf("uncertain attempt was replayed: called=%v err=%v", adapter.called, err)
	}
}

func TestFreezeAndRevokePreventEffectAtTimeOfUse(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	lease := core.CapabilityLease{ID: "lease-1", ActorID: "actor-1", OriginTaskID: "task-1", Action: "send", Resource: "customer-1", Scope: "org-1"}
	if err := l.AppendRecord(ctx, "org-1", "CAPABILITY_GRANTED", "human-1", "task-1", []string{"approval-capability-1"}, nil, "capability_lease", "lease-1", 1, lease); err != nil {
		t.Fatal(err)
	}
	fingerprint, err := Fingerprint("send", "customer-1", map[string]string{"body": "hello"})
	if err != nil {
		t.Fatal(err)
	}
	reader := &approvalReader{approval: core.HumanApproval{ID: "approval-1", OrganizationID: "org-1", TaskID: "task-1", EffectObligationID: "effect-1", Action: "send", Resource: "customer-1", Boundary: core.BoundaryPublicExternal, Status: core.ApprovalApproved, EffectFingerprint: fingerprint, SingleUse: true}}
	adapter := &adapter{}
	coordinator := New(l, adapter, reader)
	obligation := core.EffectObligation{ID: "effect-1", OrganizationID: "org-1", TaskID: "task-1", ActorID: "actor-1", Action: "send", Resource: "customer-1", Scope: "org-1", ConsequenceBoundary: core.BoundaryPublicExternal, Descriptor: "send message", EffectFingerprint: fingerprint, AuthorizationRefs: []string{"lease-1"}, ApprovalRef: "approval-1", IdempotencyKey: "key-1", ReplayContext: map[string]string{"body": "hello"}}
	if _, err := coordinator.Prepare(ctx, obligation); err != nil {
		t.Fatal(err)
	}
	persistApproval(t, l, reader.approval)
	now := time.Now().UTC()
	freeze := authority.FreezeState{OrganizationID: "org-1", Frozen: true, Reason: "incident", UpdatedAt: now}
	if err := l.AppendRecord(ctx, "org-1", "FREEZE_SET", "human-1", "task-1", nil, nil, "organization_freeze", "org-1", 1, freeze); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Execute(ctx, obligation); !errors.Is(err, ErrEffectUnauthorized) || adapter.called {
		t.Fatalf("frozen organization reached effect adapter: called=%v err=%v", adapter.called, err)
	}
	freeze.Frozen = false
	freeze.UpdatedAt = now.Add(time.Second)
	if err := l.AppendRecord(ctx, "org-1", "FREEZE_SET", "human-1", "task-1", nil, nil, "organization_freeze", "org-1", 2, freeze); err != nil {
		t.Fatal(err)
	}
	revokedAt := now.Add(2 * time.Second)
	lease.RevokedAt = &revokedAt
	if err := l.AppendRecord(ctx, "org-1", "CAPABILITY_REVOKED", "human-1", "task-1", nil, nil, "capability_lease", "lease-1", 2, lease); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Execute(ctx, obligation); !errors.Is(err, ErrEffectUnauthorized) || adapter.called {
		t.Fatalf("revoked capability reached effect adapter: called=%v err=%v", adapter.called, err)
	}
	effectRows, err := l.Records(ctx, "effect", "effect-1")
	if err != nil || len(effectRows) != 1 {
		t.Fatalf("denied effect advanced from pending: versions=%d err=%v", len(effectRows), err)
	}
	traces, err := l.Records(ctx, "authorization_trace", "effect-1")
	if err != nil || len(traces) != 2 {
		t.Fatalf("time-of-use denials were not durable: traces=%d err=%v", len(traces), err)
	}
}


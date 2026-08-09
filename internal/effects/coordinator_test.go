package effects

import (
	"context"
	"errors"
	"testing"
	"time"

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

func TestPersistBeforeEffectAndFingerprintApproval(t *testing.T) {
	l, e := ledger.Open(":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	a := &adapter{}
	reader := &approvalReader{}
	c := NewWithApprovals(l, a, reader)
	fp, _ := Fingerprint("send", "customer", map[string]string{"body": "hi"})
	o := core.EffectObligation{ID: "e", OrganizationID: "org", TaskID: "task", Action: "send", Resource: "customer", ConsequenceBoundary: core.BoundaryPublicExternal, Descriptor: "send message", EffectFingerprint: fp, ApprovalRef: "approval", IdempotencyKey: "key", ReplayContext: map[string]string{"body": "hi"}}
	if _, e = c.Prepare(context.Background(), o); e != nil {
		t.Fatal(e)
	}
	reader.approval = core.HumanApproval{ID: "approval", OrganizationID: "org", TaskID: "task", EffectObligationID: "e", Action: "send", Resource: "customer", Boundary: core.BoundaryPublicExternal, Status: core.ApprovalApproved, EffectFingerprint: "different"}
	if _, e = c.Execute(context.Background(), o); e == nil || a.called {
		t.Fatal("mismatched approval reached adapter")
	}
	reader.approval.EffectFingerprint = fp
	got, e := c.Execute(context.Background(), o)
	if e != nil || got.Status != core.EffectConfirmed || !a.called {
		t.Fatalf("got=%+v err=%v", got, e)
	}
	rows, _ := l.Records(context.Background(), "effect", "e")
	if len(rows) != 3 {
		t.Fatalf("versions=%d", len(rows))
	}
	events, err := l.Events(context.Background(), "")
	if err != nil || len(events) != 3 {
		t.Fatalf("effect transitions were not ledgered: events=%d err=%v", len(events), err)
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
	c := NewWithApprovals(l, a, reader)
	fp, _ := Fingerprint("send", "customer", map[string]string{"body": "hi"})
	reader.approval = core.HumanApproval{ID: "approval-1", OrganizationID: "org", TaskID: "task", EffectObligationID: "effect-1", Action: "send", Resource: "customer", Boundary: core.BoundaryPublicExternal, Status: core.ApprovalApproved, EffectFingerprint: fp, SingleUse: true}
	o := core.EffectObligation{ID: "effect-1", OrganizationID: "org", TaskID: "task", Action: "send", Resource: "customer", ConsequenceBoundary: core.BoundaryPublicExternal, Descriptor: "send message", EffectFingerprint: fp, ApprovalRef: "approval-1", IdempotencyKey: "key-1", ReplayContext: map[string]string{"body": "hi"}}
	if _, err = c.Prepare(context.Background(), o); err != nil {
		t.Fatal(err)
	}
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
	coordinator := NewWithApprovals(l, adapter, reader)
	fingerprint, _ := Fingerprint("deploy", "agent-os", map[string]string{"version": "1"})
	obligation := core.EffectObligation{ID: "effect-1", OrganizationID: "org", TaskID: "task", Action: "deploy", Resource: "agent-os", ConsequenceBoundary: core.BoundaryDeployment, Descriptor: "deploy Agent OS", EffectFingerprint: fingerprint, ApprovalRef: "approval-1", IdempotencyKey: "key-1", ReplayContext: map[string]string{"version": "1"}}
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

func TestInterruptedAttemptIsNotBlindlyReplayed(t *testing.T) {
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	adapter := &adapter{}
	reader := &approvalReader{}
	coordinator := NewWithApprovals(l, adapter, reader)
	fingerprint, _ := Fingerprint("send", "customer", map[string]string{"body": "hi"})
	obligation := core.EffectObligation{ID: "effect-1", OrganizationID: "org", TaskID: "task", Action: "send", Resource: "customer", ConsequenceBoundary: core.BoundaryPublicExternal, Descriptor: "send message", EffectFingerprint: fingerprint, ApprovalRef: "approval-1", IdempotencyKey: "key-1", ReplayContext: map[string]string{"body": "hi"}}
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

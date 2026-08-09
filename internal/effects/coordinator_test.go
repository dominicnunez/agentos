package effects

import (
	"context"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/ledger"
)

type adapter struct{ called bool }

func (a *adapter) Apply(_ context.Context, _ core.EffectObligation) ([]string, error) {
	a.called = true
	return []string{"receipt"}, nil
}
func TestPersistBeforeEffectAndFingerprintApproval(t *testing.T) {
	l, e := ledger.Open(":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	a := &adapter{}
	c := New(l, a)
	fp, _ := Fingerprint("send", "customer", map[string]string{"body": "hi"})
	o := core.EffectObligation{ID: "e", Action: "send", Resource: "customer", EffectFingerprint: fp, IdempotencyKey: "key"}
	bad := core.HumanApproval{Status: "APPROVED", EffectFingerprint: "different"}
	if _, e = c.Execute(context.Background(), o, &bad); e == nil || a.called {
		t.Fatal("mismatched approval reached adapter")
	}
	good := core.HumanApproval{Status: "APPROVED", EffectFingerprint: fp}
	got, e := c.Execute(context.Background(), o, &good)
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
	c := New(l, a)
	fp, _ := Fingerprint("send", "customer", map[string]string{"body": "hi"})
	approval := core.HumanApproval{ID: "approval-1", Status: "APPROVED", EffectFingerprint: fp, SingleUse: true}
	o := core.EffectObligation{ID: "effect-1", EffectFingerprint: fp, IdempotencyKey: "key-1"}
	if _, err = c.Execute(context.Background(), o, &approval); err != nil {
		t.Fatal(err)
	}
	a.called = false
	o.ID, o.IdempotencyKey = "effect-2", "key-2"
	if _, err = c.Execute(context.Background(), o, &approval); err == nil || a.called {
		t.Fatal("consumed approval authorized another adapter invocation")
	}
}

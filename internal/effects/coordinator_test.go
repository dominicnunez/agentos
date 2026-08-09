package effects

import (
	"context"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/ledger"
	"testing"
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
}

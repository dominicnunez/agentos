package authority

import (
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestCheckRequiresExactUnfrozenLease(t *testing.T) {
	lease := core.CapabilityLease{ID: "l", ActorID: "a", OriginTaskID: "t", Action: "write", Resource: "invoice", Scope: "org/o"}
	if !Check(time.Now(), "a", "t", "write", "invoice", "org/o", []core.CapabilityLease{lease}, false).Allowed {
		t.Fatal("exact lease denied")
	}
	if Check(time.Now(), "a", "t", "write", "invoice/*", "org/o", []core.CapabilityLease{lease}, false).Allowed {
		t.Fatal("resource inherited")
	}
	if Check(time.Now(), "a", "t", "write", "invoice", "org/o", []core.CapabilityLease{lease}, true).Allowed {
		t.Fatal("freeze bypassed")
	}
}

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

func TestChildAssignmentDoesNotInheritParentCapability(t *testing.T) {
	now := time.Now().UTC()
	parent := core.Task{ID: "task-parent", AssigneeType: "AGENT", AssigneeID: "agent-1"}
	child := core.Task{ID: "task-child", ParentID: parent.ID, AssigneeType: parent.AssigneeType, AssigneeID: parent.AssigneeID}
	parentLease := core.CapabilityLease{ID: "lease-parent", ActorID: parent.AssigneeID, OriginTaskID: parent.ID, Action: "write", Resource: "invoice-1", Scope: "org-1"}

	if trace := Check(now, parent.AssigneeID, parent.ID, "write", "invoice-1", "org-1", []core.CapabilityLease{parentLease}, false); !trace.Allowed {
		t.Fatalf("parent lease did not authorize its exact task: %+v", trace)
	}
	if trace := Check(now, child.AssigneeID, child.ID, "write", "invoice-1", "org-1", []core.CapabilityLease{parentLease}, false); trace.Allowed || trace.LeaseID != "" {
		t.Fatalf("child assignment inherited parent capability: %+v", trace)
	}

	childLease := parentLease
	childLease.ID = "lease-child"
	childLease.OriginTaskID = child.ID
	if trace := Check(now, child.AssigneeID, child.ID, "write", "invoice-1", "org-1", []core.CapabilityLease{parentLease, childLease}, false); !trace.Allowed || trace.LeaseID != childLease.ID {
		t.Fatalf("explicit child capability was not required and selected: %+v", trace)
	}
}

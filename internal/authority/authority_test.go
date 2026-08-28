package authority

import (
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestCheckRequiresExactUnfrozenLease(t *testing.T) {
	lease := core.CapabilityLease{ID: "l", ActorID: "a", ActorKind: core.PrincipalAgent, OriginTaskID: "t", Action: "write", Resource: "invoice", Scope: "org/o"}
	if !Check(time.Now(), "a", core.PrincipalAgent, "t", "write", "invoice", "org/o", []core.CapabilityLease{lease}, false).Allowed {
		t.Fatal("exact lease denied")
	}
	if Check(time.Now(), "a", core.PrincipalAgent, "t", "write", "invoice/*", "org/o", []core.CapabilityLease{lease}, false).Allowed {
		t.Fatal("resource inherited")
	}
	if Check(time.Now(), "a", core.PrincipalAgent, "t", "write", "invoice", "org/o", []core.CapabilityLease{lease}, true).Allowed {
		t.Fatal("freeze bypassed")
	}
}

func TestChildAssignmentDoesNotInheritParentCapability(t *testing.T) {
	now := time.Now().UTC()
	parent := core.Task{ID: "task-parent", AssigneeType: "AGENT", AssigneeID: "agent-1"}
	child := core.Task{ID: "task-child", ParentID: parent.ID, AssigneeType: parent.AssigneeType, AssigneeID: parent.AssigneeID}
	parentLease := core.CapabilityLease{ID: "lease-parent", ActorID: parent.AssigneeID, ActorKind: core.PrincipalAgent, OriginTaskID: parent.ID, Action: "write", Resource: "invoice-1", Scope: "org-1"}

	if trace := Check(now, parent.AssigneeID, core.PrincipalAgent, parent.ID, "write", "invoice-1", "org-1", []core.CapabilityLease{parentLease}, false); !trace.Allowed {
		t.Fatalf("parent lease did not authorize its exact task: %+v", trace)
	}
	if trace := Check(now, child.AssigneeID, core.PrincipalAgent, child.ID, "write", "invoice-1", "org-1", []core.CapabilityLease{parentLease}, false); trace.Allowed || trace.LeaseID != "" {
		t.Fatalf("child assignment inherited parent capability: %+v", trace)
	}

	childLease := parentLease
	childLease.ID = "lease-child"
	childLease.OriginTaskID = child.ID
	if trace := Check(now, child.AssigneeID, core.PrincipalAgent, child.ID, "write", "invoice-1", "org-1", []core.CapabilityLease{parentLease, childLease}, false); !trace.Allowed || trace.LeaseID != childLease.ID {
		t.Fatalf("explicit child capability was not required and selected: %+v", trace)
	}
}

func TestCheckSeparatesPrincipalKindsSharingOneID(t *testing.T) {
	lease := core.CapabilityLease{ID: "lease-external", ActorID: "shared", ActorKind: core.PrincipalExternalAgent, OriginTaskID: "task-1", Action: "write", Resource: "record-1", Scope: "org-1"}
	if trace := Check(time.Now().UTC(), "shared", core.PrincipalAgent, "task-1", "write", "record-1", "org-1", []core.CapabilityLease{lease}, false); trace.Allowed || trace.LeaseID != "" {
		t.Fatalf("internal Agent used external Agent authority: %+v", trace)
	}
	if trace := Check(time.Now().UTC(), "shared", core.PrincipalExternalAgent, "task-1", "write", "record-1", "org-1", []core.CapabilityLease{lease}, false); !trace.Allowed || trace.LeaseID != lease.ID || trace.ActorKind != core.PrincipalExternalAgent {
		t.Fatalf("exact external Agent authority was rejected: %+v", trace)
	}
}

func TestCheckClosureRejectsConfusedDeputyCapabilities(t *testing.T) {
	now := time.Now().UTC()
	operation := core.CapabilityLease{ID: "lease-tool", ActorID: "agent-1", ActorKind: core.PrincipalAgent, OriginTaskID: "task-1", Action: "artifact.fetch", Resource: "artifact-service", Scope: "org-1"}
	requirements := []core.CapabilityRequirement{
		{Action: operation.Action, Resource: operation.Resource, Scope: operation.Scope},
		{Action: "network.fetch", Resource: "arbitrary-url", Scope: "org-1"},
	}
	trace := CheckClosure(now, operation.ActorID, operation.ActorKind, operation.OriginTaskID, operation.Action, operation.Resource, operation.Scope, requirements, []core.CapabilityLease{operation}, false)
	if trace.Allowed || len(trace.Consequential) != 1 || trace.Consequential[0].Action != "network.fetch" || trace.Consequential[0].Allowed {
		t.Fatalf("allowed broker laundered denied network authority: %+v", trace)
	}

	network := core.CapabilityLease{ID: "lease-network", ActorID: operation.ActorID, ActorKind: operation.ActorKind, OriginTaskID: operation.OriginTaskID, Action: "network.fetch", Resource: "arbitrary-url", Scope: "org-1"}
	trace = CheckClosure(now, operation.ActorID, operation.ActorKind, operation.OriginTaskID, operation.Action, operation.Resource, operation.Scope, requirements, []core.CapabilityLease{operation, network}, false)
	if !trace.Allowed || len(trace.Consequential) != 1 || trace.Consequential[0].LeaseID != network.ID {
		t.Fatalf("exact consequential capability closure was rejected: %+v", trace)
	}
}

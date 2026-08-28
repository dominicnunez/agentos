// Package authority implements fail-closed, exact capability checks.
package authority

import (
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

type FreezeState struct {
	OrganizationID core.ID   `json:"organization_id"`
	Frozen         bool      `json:"frozen"`
	Reason         string    `json:"reason,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// Check deliberately performs no positive inheritance: every protected action
// needs an unrevoked lease matching actor, task origin, action, resource and scope.
func Check(now time.Time, actorID core.ID, actorKind core.PrincipalKind, taskID core.ID, action, resource, scope string, leases []core.CapabilityLease, frozen bool) core.AuthorizationTrace {
	trace := core.AuthorizationTrace{ActorID: actorID, ActorKind: actorKind, TaskID: taskID, Action: action, Resource: resource, Scope: scope}
	if frozen || actorID == "" || !core.ValidPrincipalKind(actorKind) {
		if !frozen {
			trace.Reason = "principal identity is invalid"
			return trace
		}
		trace.Reason = "organization is frozen"
		return trace
	}
	for _, lease := range leases {
		if lease.ActorID != actorID || lease.ActorKind != actorKind || lease.OriginTaskID != taskID || lease.Action != action || lease.Resource != resource || lease.Scope != scope {
			continue
		}
		if lease.RevokedAt != nil || (lease.ExpiresAt != nil && !now.Before(*lease.ExpiresAt)) {
			continue
		}
		trace.Allowed, trace.LeaseID, trace.Reason = true, lease.ID, "exact capability lease matched"
		return trace
	}
	trace.Reason = "no exact active capability lease"
	return trace
}

// CheckClosure verifies both the requested operation and every consequential
// capability declared for the Agent-controlled tool path. The downstream
// capabilities are not granted or inherited; each needs its own exact lease.
func CheckClosure(now time.Time, actorID core.ID, actorKind core.PrincipalKind, taskID core.ID, action, resource, scope string, requirements []core.CapabilityRequirement, leases []core.CapabilityLease, frozen bool) core.AuthorizationTrace {
	trace := Check(now, actorID, actorKind, taskID, action, resource, scope, leases, frozen)
	if !trace.Allowed {
		return trace
	}
	for _, requirement := range requirements {
		if requirement.Action == action && requirement.Resource == resource && requirement.Scope == scope {
			continue
		}
		decisionTrace := Check(now, actorID, actorKind, taskID, requirement.Action, requirement.Resource, requirement.Scope, leases, frozen)
		trace.Consequential = append(trace.Consequential, core.CapabilityDecision{
			Allowed: decisionTrace.Allowed, LeaseID: decisionTrace.LeaseID, Action: requirement.Action,
			Resource: requirement.Resource, Scope: requirement.Scope, Reason: decisionTrace.Reason,
		})
		if !decisionTrace.Allowed {
			trace.Allowed = false
			trace.Reason = "consequential capability closure denied"
			return trace
		}
	}
	return trace
}

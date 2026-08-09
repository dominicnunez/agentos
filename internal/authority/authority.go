// Package authority implements fail-closed, exact capability checks.
package authority

import (
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

// Check deliberately performs no positive inheritance: every protected action
// needs an unrevoked lease matching actor, task origin, action, resource and scope.
func Check(now time.Time, actorID, taskID core.ID, action, resource, scope string, leases []core.CapabilityLease, frozen bool) core.AuthorizationTrace {
	trace := core.AuthorizationTrace{ActorID: actorID, TaskID: taskID, Action: action, Resource: resource, Scope: scope}
	if frozen {
		trace.Reason = "organization is frozen"
		return trace
	}
	for _, lease := range leases {
		if lease.ActorID != actorID || lease.OriginTaskID != taskID || lease.Action != action || lease.Resource != resource || lease.Scope != scope {
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

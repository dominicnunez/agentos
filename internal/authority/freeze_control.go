package authority

import (
	"context"
	"fmt"

	"github.com/dominicnunez/agentos/internal/core"
)

// FreezeControl is the owner-authorized boundary for durable organization
// containment. Callers cannot replace its configured organization or owner.
type FreezeControl struct {
	store        FreezeStore
	organization core.ID
	ownerID      core.ID
}

func NewFreezeControl(store FreezeStore, organization, ownerID core.ID) (*FreezeControl, error) {
	if store == nil || organization == "" || ownerID == "" {
		return nil, fmt.Errorf("%w: store, organization, and owner are required", ErrFreezeInvalid)
	}
	return &FreezeControl{store: store, organization: organization, ownerID: ownerID}, nil
}

// ConfiguredFor reports whether a transport's trusted local identity is bound
// to this exact service instance. It does not authorize a request.
func (c *FreezeControl) ConfiguredFor(organization, ownerID core.ID) bool {
	return c != nil && organization != "" && ownerID != "" && c.organization == organization && c.ownerID == ownerID
}

func (c *FreezeControl) Read(ctx context.Context, actorID core.ID, actorKind core.PrincipalKind) (FreezeSnapshot, error) {
	if !c.authorized(actorID, actorKind) {
		return FreezeSnapshot{}, ErrFreezeUnauthorized
	}
	return c.store.ReadFreeze(ctx, c.organization)
}

func (c *FreezeControl) Change(ctx context.Context, actorID core.ID, actorKind core.PrincipalKind, change FreezeChange) (FreezeSnapshot, error) {
	if !c.authorized(actorID, actorKind) {
		return FreezeSnapshot{}, ErrFreezeUnauthorized
	}
	return c.store.SetFreeze(ctx, c.organization, c.ownerID, core.PrincipalHuman, change)
}

func (c *FreezeControl) authorized(actorID core.ID, actorKind core.PrincipalKind) bool {
	return c != nil && actorID != "" && actorID == c.ownerID && actorKind == core.PrincipalHuman
}

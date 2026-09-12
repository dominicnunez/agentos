package core

import "time"

// FreezeState is durable organization containment state. Control is absent only
// on history admitted before owner control evidence was introduced.
type FreezeState struct {
	OrganizationID ID              `json:"organization_id"`
	Frozen         bool            `json:"frozen"`
	Reason         string          `json:"reason,omitempty"`
	UpdatedAt      time.Time       `json:"updated_at"`
	Control        *FreezeEvidence `json:"control,omitempty"`
}

// FreezeEvidence binds an authenticated human command to the exact state it
// replaced. The runtime supplies identity and the ledger checks the predecessor.
type FreezeEvidence struct {
	ActorID       ID            `json:"actor_id"`
	ActorKind     PrincipalKind `json:"actor_kind"`
	PriorEventRef string        `json:"prior_event_ref"`
	PriorVersion  int           `json:"prior_version"`
}

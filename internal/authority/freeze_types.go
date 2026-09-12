package authority

import (
	"context"
	"errors"

	"github.com/dominicnunez/agentos/internal/core"
)

var (
	ErrFreezeConflict     = errors.New("freeze state no longer matches the command")
	ErrFreezeInvalid      = errors.New("freeze command is invalid")
	ErrFreezeUnauthorized = errors.New("freeze control requires the configured owner")
)

type FreezeChange struct {
	Frozen           bool   `json:"frozen"`
	Reason           string `json:"reason"`
	ExpectedEventRef string `json:"expected_event_ref"`
	ExpectedVersion  int    `json:"expected_version"`
}

type FreezeSnapshot struct {
	State    FreezeState `json:"state"`
	EventRef string      `json:"event_ref"`
	Version  int         `json:"version"`
}

// FreezeStore is a trusted runtime composition boundary; model input cannot
// supply authenticated identity. FreezeControl authorizes the configured owner.
type FreezeStore interface {
	ReadFreeze(context.Context, core.ID) (FreezeSnapshot, error)
	SetFreeze(context.Context, core.ID, core.ID, core.PrincipalKind, FreezeChange) (FreezeSnapshot, error)
}

package gateway

import (
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/intake"
)

func operatorPrincipal(id string, kind core.PrincipalKind, organizationID, channel string, capabilities []string) intake.Principal {
	return intake.Principal{
		ID: id, Kind: kind, OrganizationID: organizationID,
		Channel: channel, Capabilities: capabilities,
	}
}

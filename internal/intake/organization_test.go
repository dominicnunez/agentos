package intake

import (
	"context"
	"errors"
	"testing"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
)

func TestOrganizationStateRejectsA2AAndUnprivilegedPrincipals(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := New(app.New(events.NewGateway(store)))

	principals := []Principal{
		{ID: "external-agent-1", Kind: core.PrincipalExternalAgent, OrganizationID: "org-1", Channel: ChannelA2A, Capabilities: []string{CapabilityReadStatus}, WorkScope: WorkScopeOrganization},
		{ID: "local-user-1", Kind: core.PrincipalHuman, OrganizationID: "org-1", Channel: ChannelHumanDirect, WorkScope: WorkScopeOrganization},
	}
	for _, principal := range principals {
		if _, err := service.OrganizationState(context.Background(), principal); !errors.Is(err, ErrForbidden) {
			t.Fatalf("principal %s organization state error=%v", principal.ID, err)
		}
	}
}

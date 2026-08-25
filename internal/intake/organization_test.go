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

func TestAIMSEvidenceRequiresAuthenticatedLocalExportCapability(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := New(app.New(events.NewGateway(store)))
	for _, principal := range []Principal{
		{ID: "local-user-1", Kind: core.PrincipalHuman, OrganizationID: "org-1", Channel: ChannelHumanDirect, Capabilities: []string{CapabilityReadStatus}, WorkScope: WorkScopeOrganization},
		{ID: "external-agent-1", Kind: core.PrincipalExternalAgent, OrganizationID: "org-1", Channel: ChannelA2A, Capabilities: []string{CapabilityExportAIMSEvidence}, WorkScope: WorkScopeOrganization},
	} {
		if _, err := service.AIMSEvidence(context.Background(), principal); !errors.Is(err, ErrForbidden) {
			t.Fatalf("principal %s AIMS evidence error=%v", principal.ID, err)
		}
	}
}

func TestStrategyBootstrapRequiresAuthenticatedLocalStrategyCapability(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := New(app.New(events.NewGateway(store)))
	request := StrategyBootstrap{
		RequestID: "strategy-1", MissionID: "mission-1", MissionStatement: "Durable direction",
		GoalID: "goal-1", GoalObjective: "Verified outcome", GoalMode: core.GoalTarget,
		SuccessCriteria: []string{"Evidence is durable"},
	}
	local := Principal{
		ID: "local-uid-1000", Kind: core.PrincipalHuman, OrganizationID: "org-1", Channel: ChannelHumanDirect,
		Capabilities: []string{CapabilityManageStrategy, CapabilityReadStatus}, WorkScope: WorkScopeOrganization,
	}
	view, err := service.BootstrapStrategy(context.Background(), local, request)
	if err != nil || len(view.Missions) != 1 || len(view.Goals) != 1 {
		t.Fatalf("local strategy view=%+v err=%v", view, err)
	}
	for _, principal := range []Principal{
		{ID: "local-uid-1001", Kind: core.PrincipalHuman, OrganizationID: "org-1", Channel: ChannelHumanDirect, WorkScope: WorkScopeOrganization},
		{ID: "external-agent-1", Kind: core.PrincipalExternalAgent, OrganizationID: "org-1", Channel: ChannelA2A, Capabilities: []string{CapabilityManageStrategy}, WorkScope: WorkScopeOrganization},
	} {
		if _, err := service.BootstrapStrategy(context.Background(), principal, request); !errors.Is(err, ErrForbidden) {
			t.Fatalf("principal %s strategy error=%v", principal.ID, err)
		}
	}
}

func TestStrategyCapacityIsTerminalOperatorInput(t *testing.T) {
	if err := strategyIntakeError(app.ErrStrategyCapacity); !errors.Is(err, ErrCapacity) || errors.Is(err, ErrUnavailable) {
		t.Fatalf("strategy capacity mapping=%v", err)
	}
}

package app

import (
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/projections"
)

func TestGovernanceInspectionObservesAfterVerifiedSnapshot(t *testing.T) {
	ctx := t.Context()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	gateway := events.NewGateway(store)
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1"}
	if err := projections.New(gateway).SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "setup", 1, organization, nil); err != nil {
		t.Fatal(err)
	}
	stream, err := store.Events(ctx, "setup")
	if err != nil || len(stream) != 1 {
		t.Fatalf("organization event=%+v err=%v", stream, err)
	}
	report, found, err := New(gateway).GovernanceInspection(ctx, organization.ID)
	if err != nil || !found {
		t.Fatalf("inspection found=%t err=%v", found, err)
	}
	if report.ObservedAt.Before(stream[0].CreatedAt) {
		t.Fatalf("inspection observed at %s before snapshot event %s", report.ObservedAt, stream[0].CreatedAt)
	}
}

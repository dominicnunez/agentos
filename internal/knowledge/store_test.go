package knowledge

import (
	"context"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
)

func TestStoreAdmitsValidatedKnowledgeAndRetrievesOnlyActiveTenantScope(t *testing.T) {
	ctx := context.Background()
	store, gateway := newKnowledgeTestStore(t)
	orgOneEvent := seedKnowledgeOrganization(t, ctx, gateway, "org-1")
	orgTwoEvent := seedKnowledgeOrganization(t, ctx, gateway, "org-2")
	service := New(gateway)

	candidate := knowledgeCandidate("k-1", "org-1", orgOneEvent.EventID)
	proposed, err := service.Propose(ctx, candidate)
	if err != nil || proposed.EventType != "KNOWLEDGE_PROPOSED" {
		t.Fatalf("propose knowledge: event=%+v err=%v", proposed, err)
	}
	if rows, err := service.Search(ctx, "org-1", core.KnowledgeScopeOrganization, "org-1", "rollback", 10); err != nil || len(rows) != 0 {
		t.Fatalf("candidate leaked into active retrieval: rows=%+v err=%v", rows, err)
	}

	active := candidate
	active.Version = 2
	active.Status = core.KnowledgeActive
	active.ValidationMethod = core.KnowledgeValidationHuman
	active.ValidationRefs = []string{orgOneEvent.EventID}
	active.ValidatedBy = "reviewer-1"
	active.ValidatedByKind = core.PrincipalHuman
	verifiedAt := candidate.CreatedAt.Add(time.Minute)
	active.LastVerifiedAt = &verifiedAt
	active.SupersedesVersion = integerPointer(1)
	if _, err := service.Activate(ctx, active); err != nil {
		t.Fatalf("activate knowledge: %v", err)
	}

	rows, err := service.Search(ctx, "org-1", core.KnowledgeScopeOrganization, "org-1", "rollback", 10)
	if err != nil || len(rows) != 1 || rows[0].KnowledgeID != candidate.KnowledgeID {
		t.Fatalf("active knowledge unavailable: rows=%+v err=%v", rows, err)
	}
	if rows, err := service.Search(ctx, "org-2", core.KnowledgeScopeOrganization, "org-2", "rollback", 10); err != nil || len(rows) != 0 {
		t.Fatalf("knowledge crossed tenant: rows=%+v err=%v", rows, err)
	}

	crossTenant := knowledgeCandidate("k-cross", "org-1", orgTwoEvent.EventID)
	if _, err := service.Propose(ctx, crossTenant); err == nil {
		t.Fatal("cross-tenant provenance was accepted")
	}
	future := knowledgeCandidate("k-future", "org-1", orgOneEvent.EventID)
	future.CreatedAt = time.Now().UTC().Add(time.Hour)
	if _, err := service.Propose(ctx, future); err == nil {
		t.Fatal("knowledge timestamp after its admission was accepted")
	}
	if err := store.AppendRecord(ctx, "org-1", "KNOWLEDGE_PROPOSED", "runtime", "", nil, nil, "knowledge", "legacy", 1, candidate); err == nil {
		t.Fatal("generic knowledge writer remained available")
	}
}

func TestStoreTerminalRevisionRemovesKnowledgeFromRetrieval(t *testing.T) {
	ctx := context.Background()
	_, gateway := newKnowledgeTestStore(t)
	evidence := seedKnowledgeOrganization(t, ctx, gateway, "org-1")
	service := New(gateway)
	candidate := knowledgeCandidate("k-1", "org-1", evidence.EventID)
	if _, err := service.Propose(ctx, candidate); err != nil {
		t.Fatal(err)
	}
	active := candidate
	active.Version = 2
	active.Status = core.KnowledgeActive
	active.ValidationMethod = core.KnowledgeValidationDeterministic
	active.ValidationRefs = []string{evidence.EventID}
	active.ValidatedBy = "runtime"
	active.ValidatedByKind = core.PrincipalRuntime
	verifiedAt := candidate.CreatedAt.Add(time.Minute)
	active.LastVerifiedAt = &verifiedAt
	active.SupersedesVersion = integerPointer(1)
	if _, err := service.Activate(ctx, active); err != nil {
		t.Fatal(err)
	}
	superseded := active
	superseded.Version = 3
	superseded.Status = core.KnowledgeSuperseded
	superseded.SupersedesVersion = integerPointer(2)
	if _, err := service.Supersede(ctx, superseded); err != nil {
		t.Fatal(err)
	}
	derived := knowledgeCandidate("k-derived", "org-1", evidence.EventID)
	derived.Basis = core.KnowledgeBasisDerived
	derived.DerivedKnowledgeRefs = []core.VersionedRef{{
		ID: string(active.KnowledgeID), Version: "2", MaterializationState: core.MaterializedFull,
	}}
	if _, err := service.Propose(ctx, derived); err == nil {
		t.Fatal("knowledge derived from a no-longer-current active revision was accepted")
	}
	rows, err := service.Search(ctx, "org-1", core.KnowledgeScopeOrganization, "org-1", "rollback", 10)
	if err != nil || len(rows) != 0 {
		t.Fatalf("terminal knowledge remained active: rows=%+v err=%v", rows, err)
	}
}

func TestStoreSupportsFailClosedStaleAndQuarantineTransitions(t *testing.T) {
	for _, test := range []struct {
		name       string
		fromActive bool
		status     core.KnowledgeStatus
		transition func(*Store, context.Context, core.KnowledgeRecord) (events.Event, error)
	}{
		{name: "stale", fromActive: true, status: core.KnowledgeStale, transition: (*Store).MarkStale},
		{name: "quarantine candidate", status: core.KnowledgeQuarantined, transition: (*Store).Quarantine},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			_, gateway := newKnowledgeTestStore(t)
			evidence := seedKnowledgeOrganization(t, ctx, gateway, "org-1")
			service := New(gateway)
			candidate := knowledgeCandidate("k-1", "org-1", evidence.EventID)
			if _, err := service.Propose(ctx, candidate); err != nil {
				t.Fatal(err)
			}
			prior := candidate
			if test.fromActive {
				prior.Version = 2
				prior.Status = core.KnowledgeActive
				prior.ValidationMethod = core.KnowledgeValidationHuman
				prior.ValidationRefs = []string{evidence.EventID}
				prior.ValidatedBy = "reviewer-1"
				prior.ValidatedByKind = core.PrincipalHuman
				verifiedAt := candidate.CreatedAt.Add(time.Minute)
				prior.LastVerifiedAt = &verifiedAt
				prior.SupersedesVersion = integerPointer(1)
				if _, err := service.Activate(ctx, prior); err != nil {
					t.Fatal(err)
				}
			}
			next := prior
			next.Version++
			next.Status = test.status
			next.SupersedesVersion = integerPointer(prior.Version)
			if _, err := test.transition(service, ctx, next); err != nil {
				t.Fatalf("transition knowledge: %v", err)
			}
		})
	}
}

func TestPatternCandidateRequiresDistinctConcreteEvents(t *testing.T) {
	for _, refs := range [][]string{{"1", "2"}, {"1", "1", "1"}, {"1", " 1", "1 "}, {"1", "2", ""}} {
		if PatternCandidate(refs) == nil {
			t.Fatalf("invalid occurrence set accepted: %#v", refs)
		}
	}
	if err := PatternCandidate([]string{"1", "2", "3"}); err != nil {
		t.Fatalf("three distinct occurrences rejected: %v", err)
	}
}

func newKnowledgeTestStore(t *testing.T) (*ledger.SQLite, *events.Gateway) {
	t.Helper()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close ledger: %v", err)
		}
	})
	return store, events.NewGateway(store)
}

func seedKnowledgeOrganization(t *testing.T, ctx context.Context, gateway *events.Gateway, organizationID core.ID) events.Event {
	t.Helper()
	createdAt := time.Date(2026, time.August, 26, 0, 0, 0, 0, time.UTC)
	event, err := gateway.PublishProjection(ctx, events.ProjectionDraft{
		Event: events.TrustedDraft{
			OrganizationID: string(organizationID),
			EventType:      "ORGANIZATION_CREATED",
			SourceActorID:  "runtime",
			CorrelationID:  "setup-" + string(organizationID),
		},
		ProjectionKind: "organization",
		RecordID:       string(organizationID),
		Version:        1,
		Value: core.Organization{
			ID:            organizationID,
			Name:          "Test Organization",
			PolicyVersion: "policy-1",
			CreatedAt:     createdAt,
		},
	})
	if err != nil {
		t.Fatalf("seed organization %s: %v", organizationID, err)
	}
	return event
}

func knowledgeCandidate(id, organizationID core.ID, evidenceRef string) core.KnowledgeRecord {
	return core.KnowledgeRecord{
		KnowledgeID:         id,
		OrganizationID:      organizationID,
		Version:             1,
		Type:                core.KnowledgeProcedure,
		Scope:               core.KnowledgeScopeOrganization,
		ScopeID:             organizationID,
		Tags:                []string{"recovery"},
		Status:              core.KnowledgeCandidate,
		Title:               "Rollback procedure",
		Content:             "Verify the rollback before applying it.",
		Basis:               core.KnowledgeBasisHumanInput,
		ProvenanceEventRefs: []string{evidenceRef},
		CreatedBy:           "operator-1",
		CreatedByKind:       core.PrincipalHuman,
		CreatedAt:           time.Date(2026, time.August, 26, 0, 1, 0, 0, time.UTC),
		ValidationMethod:    core.KnowledgeValidationUnvalidated,
	}
}

func integerPointer(value int) *int { return &value }

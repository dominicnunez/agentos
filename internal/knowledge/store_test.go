package knowledge

import (
	"context"
	"fmt"
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
	if _, err := service.Activate(ctx, active); err == nil {
		t.Fatal("human judgment without authenticated authority was accepted")
	}
	taskID := core.ID("task-knowledge-validation")
	lease := core.CapabilityLease{
		ID:           "lease-knowledge-validation",
		ActorID:      active.ValidatedBy,
		Action:       "knowledge.validate",
		Resource:     string(candidate.KnowledgeID),
		Scope:        string(candidate.OrganizationID),
		OriginTaskID: taskID,
	}
	if err := store.AppendRecord(ctx, "org-1", "CAPABILITY_LEASED", "runtime", string(taskID), nil, nil, "capability_lease", string(lease.ID), 1, lease); err != nil {
		t.Fatalf("seed validator lease: %v", err)
	}
	trace := authorizedKnowledgeValidationTrace(lease)
	judgment, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID:    "org-1",
		EventType:         "CAPABILITY_CHECKED",
		SourceActorID:     string(lease.ActorID),
		TaskID:            string(taskID),
		AuthorizationRefs: []string{string(lease.ID)},
		Payload:           trace,
	})
	if err != nil {
		t.Fatalf("admit validator judgment: %v", err)
	}
	active.ValidationRefs = []string{judgment.EventID}
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
	forgedArtifact := knowledgeCandidate("k-artifact", "org-1", orgOneEvent.EventID)
	forgedArtifact.EvidenceArtifactRefs = []string{"artifact-not-on-evidence-event"}
	if _, err := service.Propose(ctx, forgedArtifact); err == nil {
		t.Fatal("unbound knowledge artifact evidence was accepted")
	}
	if err := store.AppendRecord(ctx, "org-1", "KNOWLEDGE_PROPOSED", "runtime", "", nil, nil, "knowledge", "legacy", 1, candidate); err == nil {
		t.Fatal("generic knowledge writer remained available")
	}
}

func TestStoreRejectsRevokedValidatorAuthorityAfterJudgmentAdmission(t *testing.T) {
	ctx := context.Background()
	store, gateway := newKnowledgeTestStore(t)
	evidence := seedKnowledgeOrganization(t, ctx, gateway, "org-1")
	service := New(gateway)
	candidate := knowledgeCandidate("k-revoked", "org-1", evidence.EventID)
	if _, err := service.Propose(ctx, candidate); err != nil {
		t.Fatal(err)
	}
	taskID := core.ID("task-revoked-validator")
	lease := core.CapabilityLease{
		ID:           "lease-revoked-validator",
		ActorID:      "reviewer-revoked",
		Action:       "knowledge.validate",
		Resource:     string(candidate.KnowledgeID),
		Scope:        string(candidate.OrganizationID),
		OriginTaskID: taskID,
	}
	if err := store.AppendRecord(ctx, "org-1", "CAPABILITY_LEASED", "runtime", string(taskID), nil, nil, "capability_lease", string(lease.ID), 1, lease); err != nil {
		t.Fatal(err)
	}
	trace := authorizedKnowledgeValidationTrace(lease)
	judgment, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID:    "org-1",
		EventType:         "CAPABILITY_CHECKED",
		SourceActorID:     string(lease.ActorID),
		TaskID:            string(taskID),
		AuthorizationRefs: []string{string(lease.ID)},
		Payload:           trace,
	})
	if err != nil {
		t.Fatal(err)
	}
	revokedAt := time.Now().UTC()
	lease.RevokedAt = &revokedAt
	if err := store.AppendRecord(ctx, "org-1", "CAPABILITY_REVOKED", "runtime", string(taskID), nil, nil, "capability_lease", string(lease.ID), 2, lease); err != nil {
		t.Fatal(err)
	}
	active := candidate
	active.Version = 2
	active.Status = core.KnowledgeActive
	active.ValidationMethod = core.KnowledgeValidationHuman
	active.ValidationRefs = []string{judgment.EventID}
	active.ValidatedBy = lease.ActorID
	active.ValidatedByKind = core.PrincipalHuman
	verifiedAt := candidate.CreatedAt.Add(time.Minute)
	active.LastVerifiedAt = &verifiedAt
	active.SupersedesVersion = integerPointer(1)
	if _, err := service.Activate(ctx, active); err == nil {
		t.Fatal("revoked validator authority was accepted")
	}
}

func authorizedKnowledgeValidationTrace(lease core.CapabilityLease) core.AuthorizationTrace {
	return core.AuthorizationTrace{
		Allowed:  true,
		LeaseID:  lease.ID,
		ActorID:  lease.ActorID,
		TaskID:   lease.OriginTaskID,
		Action:   lease.Action,
		Resource: lease.Resource,
		Scope:    lease.Scope,
		Reason:   "exact capability lease matched",
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

func TestStoreRevisesActiveKnowledgeThroughCandidateReview(t *testing.T) {
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
	correctionEvidence, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", Payload: map[string]string{"finding": "procedure changed"},
	})
	if err != nil {
		t.Fatal(err)
	}
	corrected := active
	corrected.Version = 3
	corrected.Status = core.KnowledgeCandidate
	corrected.Title = "Corrected rollback procedure"
	corrected.Content = "Verify the corrected rollback before applying it."
	corrected.ProvenanceEventRefs = []string{correctionEvidence.EventID}
	corrected.CreatedBy = "agent-2"
	corrected.CreatedByKind = core.PrincipalExternalAgent
	corrected.CreatedAt = time.Now().UTC()
	corrected.ValidationMethod = core.KnowledgeValidationUnvalidated
	corrected.ValidationRefs = nil
	corrected.ValidatedBy = ""
	corrected.ValidatedByKind = ""
	corrected.LastVerifiedAt = nil
	corrected.SupersedesVersion = integerPointer(2)
	if _, err := service.Propose(ctx, corrected); err != nil {
		t.Fatalf("propose corrected knowledge: %v", err)
	}
	if rows, err := service.Search(ctx, "org-1", core.KnowledgeScopeOrganization, "org-1", "corrected", 10); err != nil || len(rows) != 0 {
		t.Fatalf("unvalidated correction entered active retrieval: rows=%+v err=%v", rows, err)
	}
	validation, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", Payload: map[string]string{"validation": "passed"},
	})
	if err != nil {
		t.Fatal(err)
	}
	reactivated := corrected
	reactivated.Version = 4
	reactivated.Status = core.KnowledgeActive
	reactivated.ValidationMethod = core.KnowledgeValidationDeterministic
	reactivated.ValidationRefs = []string{validation.EventID}
	reactivated.ValidatedBy = "runtime"
	reactivated.ValidatedByKind = core.PrincipalRuntime
	verifiedAt = time.Now().UTC()
	reactivated.LastVerifiedAt = &verifiedAt
	reactivated.SupersedesVersion = integerPointer(3)
	if _, err := service.Activate(ctx, reactivated); err != nil {
		t.Fatalf("activate corrected knowledge: %v", err)
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
				prior.ValidationMethod = core.KnowledgeValidationDeterministic
				prior.ValidationRefs = []string{evidence.EventID}
				prior.ValidatedBy = "runtime"
				prior.ValidatedByKind = core.PrincipalRuntime
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

func TestRepeatedPatternActivationRequiresEvidenceAfterProposal(t *testing.T) {
	ctx := context.Background()
	_, gateway := newKnowledgeTestStore(t)
	seedKnowledgeOrganization(t, ctx, gateway, "org-1")
	appendEvidence := func(label string) events.Event {
		t.Helper()
		event, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
			OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", Payload: map[string]string{"label": label},
		})
		if err != nil {
			t.Fatal(err)
		}
		return event
	}
	occurrences := []string{appendEvidence("one").EventID, appendEvidence("two").EventID, appendEvidence("three").EventID}
	olderValidation := appendEvidence("older-validation")
	candidate := knowledgeCandidate("k-pattern", "org-1", occurrences[0])
	candidate.Basis = core.KnowledgeBasisRepeatedPattern
	candidate.ProvenanceEventRefs = append([]string(nil), occurrences...)
	candidate.OccurrenceEventRefs = append([]string(nil), occurrences...)
	service := New(gateway)
	if _, err := service.Propose(ctx, candidate); err != nil {
		t.Fatal(err)
	}
	active := candidate
	active.Version = 2
	active.Status = core.KnowledgeActive
	active.ValidationMethod = core.KnowledgeValidationDeterministic
	active.ValidationRefs = []string{olderValidation.EventID}
	active.ValidatedBy = "runtime"
	active.ValidatedByKind = core.PrincipalRuntime
	verifiedAt := time.Now().UTC()
	active.LastVerifiedAt = &verifiedAt
	active.SupersedesVersion = integerPointer(1)
	if _, err := service.Activate(ctx, active); err == nil {
		t.Fatal("evidence admitted before the proposal activated repeated-pattern knowledge")
	}
	active.ValidationRefs = []string{appendEvidence("subsequent-validation").EventID}
	verifiedAt = time.Now().UTC()
	active.LastVerifiedAt = &verifiedAt
	if _, err := service.Activate(ctx, active); err != nil {
		t.Fatalf("subsequent validation evidence was rejected: %v", err)
	}
}

func TestSearchFiltersBeforeApplyingResultLimit(t *testing.T) {
	ctx := context.Background()
	_, gateway := newKnowledgeTestStore(t)
	evidence := seedKnowledgeOrganization(t, ctx, gateway, "org-1")
	service := New(gateway)
	for index := 0; index < 257; index++ {
		candidate := knowledgeCandidate(core.ID(fmt.Sprintf("k-%03d", index)), "org-1", evidence.EventID)
		if index == 256 {
			candidate.Content = "needle-only-last"
		}
		if _, err := service.Propose(ctx, candidate); err != nil {
			t.Fatalf("propose knowledge %d: %v", index, err)
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
			t.Fatalf("activate knowledge %d: %v", index, err)
		}
	}
	rows, err := service.Search(ctx, "org-1", core.KnowledgeScopeOrganization, "org-1", "needle-only-last", 1)
	if err != nil || len(rows) != 1 || rows[0].KnowledgeID != "k-256" {
		t.Fatalf("post-filter result window lost the match: rows=%+v err=%v", rows, err)
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

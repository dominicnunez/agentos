package ledger

import (
	"fmt"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

// All starts and reservation consumers are selected in one public operation.
// Canonical foreign projections fall after the pinned roster and before every
// start, so they exercise temporal discovery while remaining outside snapshots.
func incidentNegativeGrowthFixture(t testing.TB, count, foreign, knowledge int) (*SQLite, string) {
	t.Helper()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	lineageCorrelation := appendIncidentLineage(t, store, knowledge)
	agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "temporal-growth", false)
	now := time.Now().UTC()
	policy := testInferencePolicy(now)
	policy.OrganizationID, policy.Provider, policy.Model, policy.ExecutionProfileVersion = "org-1", "provider", "model", config.ProfileVersion
	policy.Mode, policy.Pricing = inference.Local, nil
	if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	if foreign != 0 {
		if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-2", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "foreign-temporal"}, ProjectionKind: "organization", RecordID: "org-2", Version: 1, Value: core.Organization{ID: "org-2", Name: "Foreign", PolicyVersion: "v1", CreatedAt: now}}); err != nil {
			t.Fatal(err)
		}
		for i := range foreign {
			id := fmt.Sprintf("foreign-temporal-%d", i)
			if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-2", EventType: "TEAM_CREATED", SourceActorID: "runtime", CorrelationID: id}, ProjectionKind: "team", RecordID: id, Version: 1, Value: core.Team{ID: core.ID(id), OrganizationID: "org-2", Name: "Foreign", Status: "ACTIVE", CreatedAt: now}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	refs := []string{}
	lineage, err := store.Events(t.Context(), lineageCorrelation)
	if err != nil || len(lineage) != 1 {
		t.Fatalf("selected lineage endpoint: %v", err)
	}
	refs = append(refs, lineage[0].EventID)
	for i := range count {
		correlation := fmt.Sprintf("selected-temporal-%d", i)
		request := appendBenchmarkTaskInference(t, store, agent, config, correlation)
		reservation, err := store.ReserveInference(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.ReconcileInference(t.Context(), reservation, nil, inference.ReconciliationNotSent); err != nil {
			t.Fatal(err)
		}
		var startID string
		if err := store.db.QueryRowContext(t.Context(), `SELECT event_id FROM events WHERE correlation_id=? AND event_type='EXECUTION_STARTED'`, correlation).Scan(&startID); err != nil {
			t.Fatal(err)
		}
		refs = append(refs, startID)
	}
	id := "selected-temporal-evidence"
	correlation := "knowledge-" + id
	if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: correlation}, ProjectionKind: "knowledge", RecordID: id, Version: 1, Value: core.KnowledgeRecord{KnowledgeID: core.ID(id), OrganizationID: "org-1", Version: 1, Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: "org-1", Status: core.KnowledgeCandidate, Title: "Execution observation", Content: "Retained selected evidence.", Basis: core.KnowledgeBasisHumanInput, ProvenanceEventRefs: refs, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated}}); err != nil {
		t.Fatal(err)
	}
	full, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := events.ValidateProjectionHistory(full, nil, nil, nil); err != nil {
		t.Fatalf("full projection/Knowledge authority owner rejected writer fixture: %v", err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatalf("full inference owner rejected selected reservations: %v", err)
	}
	snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
	if err != nil {
		t.Fatal(err)
	}
	starts, reservations, knowledgeSources := 0, 0, 0
	for _, group := range [][]events.Event{snapshot.Work.Events, snapshot.RelatedEvents, snapshot.DependencyEvents} {
		for _, event := range group {
			if event.OrganizationID == "org-2" {
				t.Fatal("healthy foreign temporal source expanded the public snapshot")
			}
			switch event.EventType {
			case "EXECUTION_STARTED":
				starts++
			case "INFERENCE_RESERVED":
				reservations++
			case "KNOWLEDGE_PROPOSED":
				knowledgeSources++
			}
		}
	}
	if starts != count || reservations != count || knowledgeSources != knowledge+1 {
		t.Fatalf("selected starts=%d reservations=%d Knowledge=%d want %d/%d/%d", starts, reservations, knowledgeSources, count, count, knowledge+1)
	}
	return store, correlation
}

func TestIncidentNegativeGrowthFixture(t *testing.T) {
	for _, count := range []int{1, 16} {
		for _, knowledge := range []int{1, 8} {
			t.Run(fmt.Sprintf("starts-%d/knowledge-%d", count, knowledge), func(t *testing.T) {
				incidentNegativeGrowthFixture(t, count, 2, knowledge)
			})
		}
	}
}

func BenchmarkIncidentNegativeGrowth(b *testing.B) {
	for _, count := range []int{1, 16} {
		for _, foreign := range []int{0, 256} {
			for _, knowledge := range []int{1, 8} {
				b.Run(fmt.Sprintf("starts-%d/foreign-%d/knowledge-%d", count, foreign, knowledge), func(b *testing.B) {
					store, correlation := incidentNegativeGrowthFixture(b, count, foreign, knowledge)
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						if _, err := store.VerifiedIncidentEvents(b.Context(), "org-1", correlation, 256); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}

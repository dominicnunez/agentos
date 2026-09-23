package ledger

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentLineageQueryGrowth(t *testing.T) {
	checkIncidentLineageGrowth(t, "provenance")
}

func TestIncidentDerivedQueryGrowth(t *testing.T) {
	checkIncidentLineageGrowth(t, "derived")
}

func checkIncidentLineageGrowth(t *testing.T, kind string) {
	t.Helper()
	var small int64
	for _, depth := range []int{8, 32, 128} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "lineage.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			var correlation string
			if kind == "derived" {
				correlation = appendDerivedIncidentChain(t, store, depth)
			} else {
				correlation = appendIncidentLineage(t, store, depth)
			}
			if err := store.db.Close(); err != nil {
				t.Fatal(err)
			}
			var count atomic.Int64
			store.db = sql.OpenDB(&incidentCountConnector{path: path, count: &count, inner: store.db.Driver()})
			store.db.SetMaxOpenConns(1)
			for read := range 2 {
				count.Store(0)
				started := time.Now()
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
				if err != nil {
					t.Fatalf("valid depth %d: %v", depth, err)
				}
				queries := count.Load()
				t.Logf("depth=%d read=%d statements=%d elapsed=%s", depth, read, queries, time.Since(started))
				// Every ancestor must survive the filter, not just the selected tip.
				knowledge := 0
				for _, stream := range [][]events.Event{snapshot.Work.Events, snapshot.DependencyEvents} {
					for _, event := range stream {
						if event.EventType == "KNOWLEDGE_PROPOSED" {
							knowledge++
						}
					}
				}
				if knowledge != depth {
					t.Fatalf("selected %d Knowledge nodes, want %d", knowledge, depth)
				}
				if depth == 8 && read == 0 {
					small = queries
				}
				// These histories fit one bounded support batch. A small allowance
				// permits fixed boundary checks, not one round trip per ancestor.
				if queries > small+12 {
					t.Errorf("lineage depth drives SQL round trips: %d statements, small history %d", queries, small)
				}
			}
		})
	}
}

// Distinct judgment and projection correlations prevent shared validation
// evidence from accidentally selecting every ancestor in one frontier.
func appendDerivedIncidentChain(t testing.TB, store *SQLite, depth int) string {
	t.Helper()
	ctx := t.Context()
	_, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"},
		ProjectionKind: "organization", RecordID: "org-1", Version: 1,
		Value: core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: time.Now().UTC()},
	})
	if err != nil {
		t.Fatal(err)
	}
	var prior, correlation string
	for n := range depth {
		id := fmt.Sprintf("derived-%d", n)
		artifact := "artifact-" + id
		evidence, err := store.Append(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: id, ArtifactRefs: []string{artifact}, Payload: map[string]string{"summary": "A recorded observation."}})
		if err != nil {
			t.Fatal(err)
		}
		candidate := core.KnowledgeRecord{
			KnowledgeID: core.ID(id), OrganizationID: "org-1", Version: 1, Type: core.KnowledgeLesson,
			Scope: core.KnowledgeScopeOrganization, ScopeID: "org-1", Status: core.KnowledgeCandidate,
			Title: "Derived observation", Content: "Preserve the supporting observation.",
			Basis: core.KnowledgeBasisExternalEvidence, ProvenanceEventRefs: []string{evidence.EventID},
			EvidenceArtifactRefs: []string{artifact}, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime,
			CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated,
		}
		if prior != "" {
			candidate.Basis = core.KnowledgeBasisDerived
			candidate.DerivedKnowledgeRefs = []core.VersionedRef{{ID: prior, Version: "2", MaterializationState: core.MaterializedFull}}
		}
		correlation = "knowledge-" + id
		if _, err := store.AppendProjection(ctx, events.ProjectionDraft{
			Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: correlation, ArtifactRefs: []string{artifact}},
			ProjectionKind: "knowledge", RecordID: id, Version: 1, Value: candidate,
		}); err != nil {
			t.Fatal(err)
		}
		lease := core.CapabilityLease{ID: core.ID("lease-" + id), ActorID: "validator", ActorKind: core.PrincipalHuman, OriginTaskID: "validation-task", Action: "knowledge.validate", Resource: id, Scope: "org-1"}
		if err := store.AppendRecord(ctx, "org-1", "CAPABILITY_GRANTED", "runtime", "validation-task", nil, nil, "capability_lease", string(lease.ID), 1, lease); err != nil {
			t.Fatal(err)
		}
		trace := core.AuthorizationTrace{Allowed: true, LeaseID: lease.ID, ActorID: lease.ActorID, ActorKind: lease.ActorKind, TaskID: lease.OriginTaskID, Action: lease.Action, Resource: lease.Resource, Scope: lease.Scope}
		capability, err := store.Append(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "CAPABILITY_CHECKED", SourceActorID: "validator", TaskID: "validation-task", AuthorizationRefs: []string{string(lease.ID)}, Payload: trace})
		if err != nil {
			t.Fatal(err)
		}
		judgment, err := store.Append(ctx, events.TrustedDraft{
			OrganizationID: "org-1", EventType: "HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED", SourceActorID: "validator", TaskID: "validation-task", CorrelationID: id, ArtifactRefs: []string{artifact},
			Payload: events.KnowledgeJudgmentPayload{KnowledgeID: candidate.KnowledgeID, CandidateVersion: 1, Decision: events.KnowledgeJudgmentValidated, ContextUse: core.KnowledgeFactualReference, Statement: "The recorded observation is a factual reference.", CapabilityCheckEventID: capability.EventID, SourcePrincipalID: "validator", SourcePrincipalKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", ArtifactRefs: []string{artifact}},
		})
		if err != nil {
			t.Fatal(err)
		}
		active := candidate
		previous, verifiedAt := 1, time.Now().UTC()
		active.Version, active.Status, active.ContextUse = 2, core.KnowledgeActive, core.KnowledgeFactualReference
		active.SupersedesVersion, active.LastVerifiedAt = &previous, &verifiedAt
		active.ValidationMethod, active.ValidatedBy, active.ValidatedByKind = core.KnowledgeValidationHuman, "validator", core.PrincipalHuman
		active.ValidationRefs = []string{capability.EventID, judgment.EventID}
		if _, err := store.AppendProjection(ctx, events.ProjectionDraft{
			Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_ACTIVATED", SourceActorID: "runtime", CorrelationID: correlation, ArtifactRefs: []string{artifact}},
			ProjectionKind: "knowledge", RecordID: id, Version: 2, Value: active,
		}); err != nil {
			t.Fatal(err)
		}
		prior = id
	}
	return correlation
}

func BenchmarkIncidentLineageRead(b *testing.B) {
	for _, depth := range []int{8, 32, 128} {
		for _, unrelated := range []int{0, 512} {
			b.Run(fmt.Sprintf("depth-%d/unrelated-%d", depth, unrelated), func(b *testing.B) {
				store, err := Open(":memory:")
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = store.Close() })
				correlation := appendIncidentLineage(b, store, depth)
				for range unrelated {
					if _, err := store.Append(b.Context(), events.TrustedDraft{
						OrganizationID: "org-1", EventType: "AUDIT_NOTE", CorrelationID: "unrelated",
						Payload: map[string]string{"text": "Not part of the selected lineage."},
					}); err != nil {
						b.Fatal(err)
					}
				}
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

func appendIncidentLineage(t testing.TB, store *SQLite, depth int) string {
	t.Helper()
	now := time.Now().UTC()
	parent, err := store.AppendProjection(t.Context(), events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"},
		ProjectionKind: "organization", RecordID: "org-1", Version: 1,
		Value: core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now},
	})
	if err != nil {
		t.Fatal(err)
	}
	for n := range depth {
		id := fmt.Sprintf("lineage-%d", n)
		parent, err = store.AppendProjection(t.Context(), events.ProjectionDraft{
			Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: "knowledge-" + id},
			ProjectionKind: "knowledge", RecordID: id, Version: 1,
			Value: core.KnowledgeRecord{
				KnowledgeID: core.ID(id), OrganizationID: "org-1", Version: 1,
				Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: "org-1",
				Status: core.KnowledgeCandidate, Title: "Observed lesson", Content: "Preserve prior evidence.",
				Basis: core.KnowledgeBasisHumanInput, ProvenanceEventRefs: []string{parent.EventID},
				CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: time.Now().UTC(),
				ValidationMethod: core.KnowledgeValidationUnvalidated,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return parent.CorrelationID
}

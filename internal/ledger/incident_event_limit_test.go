package ledger

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

// Public events have their own 256-item bound. Their incoming-link selectors
// must not consume the separate budget for private supporting evidence.
func TestIncidentPublicEvidenceLimit(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "organization", RecordID: "org-1", Version: 1, Value: core.Organization{ID: "org-1", Name: "Evidence", PolicyVersion: "v1", CreatedAt: time.Now().UTC()}}); err != nil {
		t.Fatal(err)
	}
	var previous, correlation string
	for node := range 16 {
		refs := make([]string, 0, 251)
		if previous != "" {
			refs = append(refs, previous)
		}
		for source := range 250 {
			evidence, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "observations", Payload: map[string]int{"observation": node*250 + source}})
			if err != nil {
				t.Fatal(err)
			}
			refs = append(refs, evidence.EventID)
		}
		id := fmt.Sprintf("bounded-%d", node)
		correlation = "knowledge-" + id
		value := core.KnowledgeRecord{KnowledgeID: core.ID(id), OrganizationID: "org-1", Version: 1, Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: "org-1", Status: core.KnowledgeCandidate, Title: "Recorded observations", Content: "Retain the source observations", Basis: core.KnowledgeBasisExternalEvidence, ProvenanceEventRefs: refs, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated}
		event, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: correlation}, ProjectionKind: "knowledge", RecordID: id, Version: 1, Value: value})
		if err != nil {
			t.Fatal(err)
		}
		previous = event.EventID
	}
	for range 255 {
		if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: correlation, Payload: map[string]string{"summary": "Public observation"}}); err != nil {
			t.Fatal(err)
		}
	}
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := events.ValidateProjectionHistory(stream, nil, nil, nil); err != nil {
		t.Fatalf("writer-produced full history: %v", err)
	}
	started := time.Now()
	snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
	t.Logf("events=%d read=%s", len(stream), time.Since(started))
	if err != nil {
		t.Fatalf("bounded public and private history rejected: %v", err)
	}
	if len(snapshot.Work.Events) != 256 || len(snapshot.DependencyEvents) != 4016 {
		t.Fatalf("incomplete bounded history: public=%d private=%d", len(snapshot.Work.Events), len(snapshot.DependencyEvents))
	}
	if err := events.ValidateIncidentBounds(snapshot); err != nil {
		t.Fatal(err)
	}
	// A later incoming owner makes more private evidence relevant without
	// changing the public event count. Its payload and full history remain valid.
	refs := []string{previous}
	for range 100 {
		evidence, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "overflow-observations", Payload: map[string]string{"observation": "Additional evidence"}})
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, evidence.EventID)
	}
	value := core.KnowledgeRecord{KnowledgeID: "overflow", OrganizationID: "org-1", Version: 1, Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: "org-1", Status: core.KnowledgeCandidate, Title: "More observations", Content: "Retain more source observations", Basis: core.KnowledgeBasisExternalEvidence, ProvenanceEventRefs: refs, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated}
	if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: "knowledge-overflow"}, ProjectionKind: "knowledge", RecordID: "overflow", Version: 1, Value: value}); err != nil {
		t.Fatal(err)
	}
	stream, err = store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := events.ValidateProjectionHistory(stream, nil, nil, nil); err != nil {
		t.Fatalf("writer-produced over-limit history: %v", err)
	}
	snapshot, err = store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
	if err == nil || !strings.Contains(err.Error(), "support limit") || !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
		t.Fatalf("private evidence overflow: snapshot=%+v err=%v", snapshot, err)
	}
	if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: correlation, Payload: map[string]string{"summary": "One public observation too many"}}); err != nil {
		t.Fatal(err)
	}
	snapshot, err = store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
	if err == nil || !strings.Contains(err.Error(), "limit") || !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
		t.Fatalf("public evidence overflow: snapshot=%+v err=%v", snapshot, err)
	}
}

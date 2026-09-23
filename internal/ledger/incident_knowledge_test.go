package ledger

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/replay"
)

func TestIncidentAgentKnowledgeCreator(t *testing.T) {
	path := filepath.Join(t.TempDir(), "knowledge.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	task := incidentTestExecution(t, store)
	id := core.ID("agent-knowledge")
	title, content, applicability := "Execution observation", "An observation from the admitted execution.", ""
	basis := core.KnowledgeBasisSingleExperience
	proposal, err := events.NewGateway(store).PublishAgentDraft(t.Context(), "org-1", string(task.AssigneeID), "execution-stop-task-v2", "stop-work", events.Draft{
		EventType: "KNOWLEDGE_PROPOSED", TaskID: string(task.ID),
		Payload: events.KnowledgeProposedPayload{KnowledgeID: &id, KnowledgeType: core.KnowledgeLesson, Title: &title, Content: content, BasisType: &basis, Applicability: &applicability},
	})
	if err != nil {
		t.Fatal(err)
	}
	knowledge := core.KnowledgeRecord{KnowledgeID: id, OrganizationID: "org-1", Version: 1, Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: "org-1", Status: core.KnowledgeCandidate, Title: title, Content: content, Basis: basis, ProvenanceEventRefs: []string{proposal.EventID}, CreatedBy: task.AssigneeID, CreatedByKind: core.PrincipalAgent, CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated}
	_, err = store.AppendProjection(t.Context(), events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: "knowledge-agent-knowledge"},
		ProjectionKind: "knowledge", RecordID: string(id), Version: 1, Value: knowledge,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "knowledge-agent-knowledge", 256)
	if err != nil {
		t.Fatalf("incident omitted Agent creator evidence: %v", err)
	}
	if _, err := replay.ProjectIncident(snapshot, "knowledge-agent-knowledge"); err != nil {
		t.Fatal(err)
	}
}

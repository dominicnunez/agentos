package ledger

import (
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentUnconsumedJudgment(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	selected, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-2", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "selected", Payload: map[string]string{"message": "Selected evidence"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED", SourceActorID: "human-1", TaskID: "foreign-task", CorrelationID: "unconsumed", Payload: events.KnowledgeJudgmentPayload{KnowledgeID: "unconsumed", CandidateVersion: 1, Decision: events.KnowledgeJudgmentValidated, Statement: "Unconsumed statement", CapabilityCheckEventID: selected.EventID, SourcePrincipalID: "human-1", SourcePrincipalKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", ArtifactRefs: []string{}}}); err != nil {
		t.Fatalf("writer rejected unconsumed judgment: %v", err)
	}
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := events.ValidateProjectionHistory(stream, nil, nil, nil); err != nil {
		t.Fatalf("full owner rejected unconsumed judgment: %v", err)
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-2", "selected", 256); err != nil {
		t.Fatalf("unconsumed judgment poisoned unrelated incident: %v", err)
	}
}

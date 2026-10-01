package ledger

import (
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentUnusedKnowledgeStatements(t *testing.T) {
	for _, kind := range []string{"KNOWLEDGE_VALIDATION_RECORDED", "KNOWLEDGE_JUDGMENT_PUBLISHED", "HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED", "A2A_KNOWLEDGE_JUDGMENT_RECEIVED"} {
		for _, mode := range []string{"foreign-identity", "foreign-witness", "public", "unrelated"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				store, err := Open(":memory:")
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = store.Close() }()
				appendTaskProjectionParents(t, t.Context(), store, "org-1", "setup", "work-1")
				appendFactualInferenceKnowledge(t, store, "fact", "Verified fact", "A bounded observation.")
				baseline, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "knowledge-fact", 256)
				if err != nil {
					t.Fatalf("healthy public: %v", err)
				}
				knowledge, witness := "fact", "unused-foreign-reference"
				organization, correlation := "foreign-org", "unconsumed"
				if mode == "unrelated" {
					knowledge = "unrelated"
				}
				if mode == "foreign-witness" {
					witness = baseline.Work.Events[0].EventID
				}
				if mode == "public" {
					organization, correlation = "org-1", "knowledge-fact"
				}
				// No admission references this raw statement. Its field spelling
				// supplies neither creator nor validator evidence by itself.
				draft := events.TrustedDraft{OrganizationID: organization, EventType: kind, SourceActorID: "runtime", TaskID: "unconsumed-task", CorrelationID: correlation, Payload: map[string]any{"knowledge_id": knowledge, "occurrence_event_refs": []string{witness}, "outcome_event_ref": witness, "capability_check_event_id": witness}}
				raw, err := store.Append(t.Context(), draft)
				if err != nil {
					t.Fatal(err)
				}
				full, err := store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				leases, freezes, err := events.ResolveAuthorityAdmissions(full, append(append([]events.AuthorityRecord{}, baseline.FreezeRecords...), baseline.AuthorityRecords...))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := events.ValidateProjectionHistory(full, nil, leases, freezes); err != nil {
					t.Fatalf("full owner consumed unused statement: %v", err)
				}
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "knowledge-fact", 256)
				if err != nil {
					t.Fatalf("unused statement poisoned public reader: %v", err)
				}
				for _, event := range snapshot.DependencyEvents {
					if event.EventID == raw.EventID {
						t.Fatal("unused statement selected as incoming evidence")
					}
				}
				if mode == "public" {
					visible := false
					for _, event := range snapshot.Work.Events {
						visible = visible || event.EventID == raw.EventID
					}
					if !visible {
						t.Fatal("unused public statement disappeared")
					}
				}
			})
		}
	}
}

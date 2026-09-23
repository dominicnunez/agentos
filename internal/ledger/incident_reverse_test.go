package ledger

import (
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentReverseSelectionIgnoresNotes(t *testing.T) {
	for _, field := range []string{"replaces_work_id", "goal_id", "knowledge_id"} {
		t.Run(field, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			now := time.Now().UTC()
			id := "work-1"
			correlation := "selected"
			switch field {
			case "replaces_work_id":
				appendTaskProjectionParents(t, t.Context(), store, "org-1", correlation, id)
			case "goal_id":
				id = "goal-1"
				appendTestMission(t, t.Context(), store, "org-1", "mission-1", now)
				_, err = store.AppendProjection(t.Context(), events.ProjectionDraft{
					Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "GOAL_CREATED", SourceActorID: "runtime", CorrelationID: correlation},
					ProjectionKind: "goal", RecordID: id, Version: 1,
					Value: core.Goal{ID: core.ID(id), OrganizationID: "org-1", MissionID: "mission-1", Objective: "test", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "test", Origin: "RUNTIME_DEFAULT"}}, Status: core.GoalActive, CreatedAt: now},
				})
			case "knowledge_id":
				id = "knowledge-1"
				correlation = "knowledge-knowledge-1"
				appendTestMission(t, t.Context(), store, "org-1", "mission-1", now)
				stream, readErr := store.Events(t.Context(), "")
				if readErr != nil {
					t.Fatal(readErr)
				}
				_, err = store.AppendProjection(t.Context(), events.ProjectionDraft{
					Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: correlation},
					ProjectionKind: "knowledge", RecordID: id, Version: 1,
					Value: core.KnowledgeRecord{KnowledgeID: core.ID(id), OrganizationID: "org-1", Version: 1, Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: "org-1", Status: core.KnowledgeCandidate, Title: "test", Content: "test", Basis: core.KnowledgeBasisHumanInput, ProvenanceEventRefs: []string{stream[0].EventID}, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated},
				})
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256); err != nil {
				t.Fatalf("valid baseline: %v", err)
			}
			note, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", CorrelationID: "unrelated", Payload: map[string]string{field: id}})
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range snapshot.DependencyEvents {
				if event.EventID == note.EventID {
					t.Fatal("ordinary note entered the dependency set through a contract field name")
				}
			}
			if _, err := events.ValidateIncidentHistory(snapshot); err != nil {
				t.Fatal(err)
			}
		})
	}
}

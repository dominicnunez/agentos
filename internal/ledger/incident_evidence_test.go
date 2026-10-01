package ledger

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentEvidenceTaskSelection(t *testing.T) {
	parallelIncidentTest(t)
	for _, scope := range []string{"public", "private", "foreign", "unrelated"} {
		for _, execution := range []string{"", "different-execution"} {
			t.Run(scope+"/"+execution, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "evidence.db")
				store, err := Open(path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				task := incidentTestExecution(t, store)
				evidence, err := store.AppendAgentEvidence(t.Context(), events.TrustedDraft{
					OrganizationID: "org-1", EventType: "EVIDENCE_PUBLISHED", SourceActorID: string(task.AssigneeID),
					SourceExecutionID: "execution-stop-task-v2", TaskID: string(task.ID), CorrelationID: "stop-work", ArtifactRefs: []string{"artifact-1"},
					Payload: events.EvidencePublishedPayload{Summary: "bounded evidence", ArtifactRefs: []string{"artifact-1"}},
				})
				if err != nil {
					t.Fatal(err)
				}
				correlation := "stop-work"
				if scope == "private" {
					stream, err := store.Events(t.Context(), "")
					if err != nil {
						t.Fatal(err)
					}
					var start string
					for _, event := range stream {
						if event.EventType == "EXECUTION_STARTED" {
							start = event.EventID
						}
					}
					correlation = "knowledge-private-execution"
					_, err = store.AppendProjection(t.Context(), events.ProjectionDraft{
						Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: correlation},
						ProjectionKind: "knowledge", RecordID: "private-execution", Version: 1,
						Value: core.KnowledgeRecord{KnowledgeID: "private-execution", OrganizationID: "org-1", Version: 1, Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: "org-1", Status: core.KnowledgeCandidate, Title: "Execution observation", Content: "Recorded execution", Basis: core.KnowledgeBasisHumanInput, ProvenanceEventRefs: []string{start}, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated},
					})
					if err != nil {
						t.Fatal(err)
					}
				}
				if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256); err != nil {
					t.Fatalf("valid baseline: %v", err)
				}
				organization, taskID := "org-1", string(task.ID)
				if scope == "foreign" {
					organization = "other-org"
				}
				if scope == "unrelated" {
					organization, taskID = "other-org", "other-task"
				}
				if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
					if _, err := tx.ExecContext(t.Context(), `UPDATE events SET organization_id=?,task_id=?,source_execution_id=?,correlation_id='moved-evidence' WHERE event_id=?`, organization, taskID, execution, evidence.EventID); err != nil {
						return err
					}
					if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
						return err
					}
					return rebuildEventIntegrity(t.Context(), tx)
				}); err != nil {
					t.Fatal(err)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				store, err = Open(path)
				if err != nil {
					t.Fatal(err)
				}
				stream, err := store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := events.ValidateProjectionHistory(stream, nil, nil, nil); err == nil {
					t.Fatal("full history accepted missing evidence execution binding")
				}
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
				if scope == "unrelated" {
					if err != nil {
						t.Fatalf("unrelated task poisoned incident: %v", err)
					}
					return
				}
				if err == nil {
					t.Fatal("incident omitted task-bound evidence")
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("invalid evidence returned partial snapshot")
				}
			})
		}
	}
}

func TestIncidentEvidenceActorBinding(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	task := incidentTestExecution(t, store)
	evidence, err := store.AppendAgentEvidence(t.Context(), events.TrustedDraft{
		OrganizationID: "org-1", EventType: "EVIDENCE_PUBLISHED", SourceActorID: string(task.AssigneeID),
		SourceExecutionID: "execution-stop-task-v2", TaskID: string(task.ID), CorrelationID: "stop-work", ArtifactRefs: []string{"artifact-1"},
		Payload: events.EvidencePublishedPayload{Summary: "bounded evidence", ArtifactRefs: []string{"artifact-1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256)
	if err != nil {
		t.Fatal(err)
	}
	for i := range snapshot.Work.Events {
		if snapshot.Work.Events[i].EventID == evidence.EventID {
			snapshot.Work.Events[i].SourceActorID = "wrong-agent"
		}
	}
	if _, err := events.ValidateIncidentHistory(snapshot); err == nil {
		t.Error("direct incident validation accepted evidence from the wrong actor")
	}
	err = store.withTx(t.Context(), func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET source_actor_id='wrong-agent' WHERE event_id=?`, evidence.EventID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256); err == nil {
		t.Fatal("incident reader accepted evidence from the wrong actor")
	}
}

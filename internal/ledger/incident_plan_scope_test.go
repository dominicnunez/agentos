package ledger

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentPlanScope(t *testing.T) {
	for _, private := range []bool{false, true} {
		for _, variant := range []string{"foreign-task", "foreign-empty-task", "foreign-other-task", "same-tenant", "unrelated", "opaque-note"} {
			name := variant
			if private {
				name += "/private"
			}
			t.Run(name, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "plans.db")
				store, err := Open(path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				task := incidentTestExecution(t, store)
				selected := "stop-work"
				if private {
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
					if start == "" {
						t.Fatal("fixture lacks execution start")
					}
					selected = "knowledge-private-plan"
					_, err = store.AppendProjection(t.Context(), events.ProjectionDraft{
						Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: selected},
						ProjectionKind: "knowledge", RecordID: "private-plan", Version: 1,
						Value: core.KnowledgeRecord{KnowledgeID: "private-plan", OrganizationID: "org-1", Version: 1, Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: "org-1", Status: core.KnowledgeCandidate, Title: "Execution observation", Content: "Recorded execution", Basis: core.KnowledgeBasisHumanInput, ProvenanceEventRefs: []string{start}, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated},
					})
					if err != nil {
						t.Fatal(err)
					}
				}
				if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", selected, 256); err != nil {
					t.Fatalf("valid baseline: %v", err)
				}
				organization, correlation, taskID, kind := "foreign", "stop-work", string(task.ID), "PLAN_CREATED"
				switch variant {
				case "foreign-empty-task":
					taskID = ""
				case "foreign-other-task":
					taskID = "other-task"
				case "same-tenant":
					organization = "org-1"
				case "unrelated":
					correlation, taskID = "unrelated", "other-task"
				case "opaque-note":
					kind = "AUDIT_NOTE"
				}
				if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
					_, err := tx.ExecContext(t.Context(), `INSERT INTO events(event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version)
SELECT 'extra-plan',(SELECT MAX(sequence)+1 FROM events),?,?,source_actor_id,source_execution_id,recipient_scope,recipient_id,?,authorization_refs,artifact_refs,payload,?,created_at,schema_version FROM events WHERE event_type='PLAN_CREATED' AND correlation_id='stop-work'`, organization, kind, taskID, correlation)
					if err != nil {
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
				_, fullErr := events.ValidateProjectionHistory(stream, nil, nil, nil)
				valid := variant == "unrelated" || variant == "opaque-note"
				if !valid && (fullErr == nil || !strings.Contains(fullErr.Error(), "Plan")) {
					t.Fatalf("full history must reject conflicting Plan: %v", fullErr)
				}
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", selected, 256)
				if valid {
					if fullErr != nil {
						t.Fatalf("unrelated history changed full Plan validation: %v", fullErr)
					}
					if err != nil {
						t.Fatalf("unrelated history poisoned incident: %v", err)
					}
					for _, event := range snapshot.DependencyEvents {
						if event.EventID == "extra-plan" {
							t.Fatal("unrelated Plan or opaque note selected")
						}
					}
					return
				}
				if err == nil {
					t.Fatal("incident omitted conflicting Plan")
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("failed incident returned partial evidence")
				}
			})
		}
	}
}

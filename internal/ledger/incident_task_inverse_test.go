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

func TestIncidentPrivateTaskSuspension(t *testing.T) {
	for _, mutation := range []string{"selected-task", "other-task", "other-organization"} {
		t.Run(mutation, func(t *testing.T) { checkPrivateTaskSuspension(t, mutation) })
	}
}

func checkPrivateTaskSuspension(t *testing.T, mutation string) {
	path := filepath.Join(t.TempDir(), "task-inverse.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	prior, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", TaskID: "stop-task", CorrelationID: "hidden-suspension", Payload: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	incidentTestExecution(t, store)
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
		t.Fatal("missing real execution start")
	}
	_, err = store.AppendProjection(t.Context(), events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: "knowledge-private-task"},
		ProjectionKind: "knowledge", RecordID: "private-task", Version: 1,
		Value: core.KnowledgeRecord{KnowledgeID: "private-task", OrganizationID: "org-1", Version: 1, Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: "org-1", Status: core.KnowledgeCandidate, Title: "Execution observation", Content: "Recorded execution", Basis: core.KnowledgeBasisHumanInput, ProvenanceEventRefs: []string{start}, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "knowledge-private-task", 256); err != nil {
		t.Fatalf("valid private task fixture: %v", err)
	}
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		organization, task := "org-1", "stop-task"
		if mutation == "other-task" {
			task = "unrelated-task"
		}
		if mutation == "other-organization" {
			organization = "unrelated-org"
		}
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET event_type='TASK_EXECUTION_SUSPENDED',organization_id=?,task_id=? WHERE event_id=?`, organization, task, prior.EventID); err != nil {
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
	stream, err = store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	stopErr := events.ValidateExecutionStops(stream, nil)
	if mutation == "selected-task" && (stopErr == nil || !strings.Contains(stopErr.Error(), "follows suspended revision")) {
		t.Fatalf("full stop validation must reject later task projection: %v", stopErr)
	}
	if mutation != "selected-task" && stopErr != nil {
		t.Fatalf("unrelated legacy suspension: %v", stopErr)
	}
	snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "knowledge-private-task", 256)
	if mutation != "selected-task" {
		if err != nil {
			t.Fatalf("unrelated suspension poisoned incident: %v", err)
		}
		for _, event := range snapshot.DependencyEvents {
			if event.EventID == prior.EventID {
				t.Fatal("unrelated suspension selected")
			}
		}
		return
	}
	if err == nil {
		t.Fatal("private task's retained legacy suspension was omitted")
	}
	if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
		t.Fatal("failed read returned partial evidence")
	}
}

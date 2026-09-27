package ledger

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentTaskLifecycleEnvelope(t *testing.T) {
	for _, label := range []string{"TASK_CREATED", "TASK_BLOCKED", "TASK_ASSIGNMENT_REVALIDATED", "TASK_EXECUTION_SUSPENDED", "TASK_RECOVERED", "TASK_RESUMED", "EXECUTION_STARTED", "TASK_VERIFIED_COMPLETE", "COMPLETION_REJECTED", "TASK_DEPENDENCY_FAILED", "TASK_REMEDIATION_FAILED", "TASK_WORK_FAILED"} {
		for _, scope := range []string{"selected", "foreign-selected", "malformed-selected", "unrelated"} {
			t.Run(label+"/"+scope, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "lifecycle.db")
				store, err := Open(path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				appendTaskProjectionParents(t, t.Context(), store, "org-1", "selected", "work-1")
				var retained events.Event
				for _, id := range []string{"selected-task", "retained-task"} {
					retained, err = store.AppendProjection(t.Context(), events.ProjectionDraft{
						Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: id, CorrelationID: "selected"},
						ProjectionKind: "task", RecordID: id, Version: 1,
						Value: core.Task{ID: core.ID(id), WorkID: "work-1", Description: "bounded task", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending},
					})
					if err != nil {
						t.Fatal(err)
					}
				}
				stream, err := store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := events.ValidateProjectionHistory(stream, nil, nil, nil); err != nil {
					t.Fatalf("valid full history: %v", err)
				}
				if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256); err != nil {
					t.Fatalf("valid incident: %v", err)
				}
				organization, task := "org-1", "selected-task"
				if scope == "foreign-selected" {
					organization = "org-2"
				}
				if scope == "unrelated" {
					task = "unrelated-task"
				}
				payload := "{}"
				if scope == "malformed-selected" {
					payload = "{"
				}
				if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
					if _, err := tx.ExecContext(t.Context(), `DELETE FROM records WHERE admission_event_id=?`, retained.EventID); err != nil {
						return err
					}
					if _, err := tx.ExecContext(t.Context(), `UPDATE events SET event_type=?,organization_id=?,task_id=?,correlation_id='hidden',payload=CAST(? AS BLOB) WHERE event_id=?`, label, organization, task, payload, retained.EventID); err != nil {
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
				_, replayErr := events.ValidateProjectionHistory(stream, nil, nil, nil)
				if replayErr == nil {
					t.Fatal("full history accepted unadmitted lifecycle event")
				}
				if label == "TASK_CREATED" && scope != "malformed-selected" && !strings.Contains(replayErr.Error(), "without typed admission") {
					t.Fatalf("unexpected full history rejection: %v", replayErr)
				}
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256)
				if scope == "unrelated" {
					if err != nil {
						t.Fatalf("unrelated lifecycle poisoned incident: %v", err)
					}
					for _, event := range snapshot.DependencyEvents {
						if event.EventID == retained.EventID {
							t.Fatal("unrelated lifecycle selected")
						}
					}
					return
				}
				if err == nil {
					t.Fatal("incident omitted selected task lifecycle envelope")
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("failed incident returned partial evidence")
				}
			})
		}
	}
}

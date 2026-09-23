package ledger

import (
	"fmt"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentDistinctTaskGrowth(t *testing.T) {
	for _, count := range []int{1, 64} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			appendTaskProjectionParents(t, t.Context(), store, "org-1", "many-tasks", "work-1")
			agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "many-tasks", false)
			// The supported closed-set writer admits the same Task history in one
			// transaction, without rebuilding the entire graph after each Task.
			drafts := make([]events.ProjectionDraft, 0, count)
			for i := range count {
				id := fmt.Sprintf("task-%d", i)
				task := core.Task{
					ID: core.ID(id), WorkID: "work-1", Description: "bounded Agent work", ExecutionKind: core.ExecutionAgent,
					ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: &config,
					TaskContractVersion: "1", Status: core.TaskPending,
				}
				drafts = append(drafts, events.ProjectionDraft{
					Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: id, CorrelationID: "many-tasks"},
					ProjectionKind: "task", RecordID: id, Version: 1, Value: task,
				})
			}
			if _, err := store.AppendProjections(t.Context(), drafts); err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			for range 5 {
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "many-tasks", 256)
				if err != nil {
					t.Fatal(err)
				}
				var tasks int
				for _, event := range snapshot.Work.Events {
					if event.EventType == "TASK_CREATED" {
						tasks++
					}
				}
				if tasks != count {
					t.Fatalf("selected %d Tasks, want %d", tasks, count)
				}
			}
			t.Logf("%d Tasks; five public incident reads=%s", count, time.Since(started))
			if _, err := store.db.ExecContext(t.Context(), `INSERT INTO records SELECT kind,record_id,version+1,body,'','',created_at FROM records WHERE kind='task' AND record_id='task-0'`); err != nil {
				t.Fatal(err)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "many-tasks", 256); err == nil {
				t.Fatal("accepted orphan revision in grouped Task history")
			}
		})
	}
}

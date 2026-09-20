package ledger

import (
	"fmt"
	"testing"
	"time"
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
			for i := range count {
				appendPendingAgentExecutionTask(t, t.Context(), store, "many-tasks", fmt.Sprintf("task-%d", i), agent, config)
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

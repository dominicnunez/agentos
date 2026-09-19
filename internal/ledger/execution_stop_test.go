package ledger

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func stopTestExecution(t *testing.T, store *SQLite) core.Task {
	t.Helper()
	appendTaskProjectionParents(t, t.Context(), store, "org-1", "stop-work", "work-1")
	agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "stop-work", false)
	task := appendPendingAgentExecutionTask(t, t.Context(), store, "stop-work", "stop-task", agent, config)
	if _, err := startPendingAgentExecution(t.Context(), store, "stop-work", task); err != nil {
		t.Fatal(err)
	}
	return task
}

func TestExecutionStopHistoryCost(t *testing.T) {
	// Sixteen Tasks reach the supported fifteen-peer context boundary.
	for _, size := range []int{1, 16} {
		t.Run(fmt.Sprintf("stopped-tasks-%d", size), func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			appendTaskProjectionParents(t, t.Context(), store, "org-1", "stop-cost", "work-1")
			agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "stop-cost", false)
			var task core.Task
			var executionID string
			var requestElapsed time.Duration
			for index := 0; index < size; index++ {
				task = appendPendingAgentExecutionTask(t, t.Context(), store, "stop-cost", fmt.Sprintf("cost-task-%d", index), agent, config)
				if _, err := startPendingAgentExecution(t.Context(), store, "stop-cost", task); err != nil {
					t.Fatal(err)
				}
				executionID = fmt.Sprintf("execution-%s-v2", task.ID)
				started := time.Now()
				if _, err := store.RequestExecutionStop(t.Context(), "org-1", string(task.ID), "stop-cost", executionID, "execution_cancelled"); err != nil {
					t.Fatal(err)
				}
				requestElapsed = time.Since(started)
			}
			before := stopAdmissionEventCount(t, store)
			draft := events.TrustedDraft{OrganizationID: "org-1", TaskID: string(task.ID), CorrelationID: "stop-cost", SourceExecutionID: executionID, SourceActorID: "runtime", EventType: "CUSTOM_EXECUTION_EVENT", Payload: map[string]string{"message": "late activity"}}
			started := time.Now()
			for range 50 {
				if _, err := store.Append(t.Context(), draft); !errors.Is(err, core.ErrExecutionStopped) {
					t.Fatalf("late operation was not denied: %v", err)
				}
			}
			elapsed := time.Since(started)
			if stopAdmissionEventCount(t, store) != before {
				t.Fatal("repeated denied operation published events")
			}
			t.Logf("history=%d events: last stop request=%s, 50 complete denied publications=%s (mean=%s)", before, requestElapsed, elapsed, elapsed/50)
		})
	}
}

func TestExecutionStopWritesAreAtomic(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	task := stopTestExecution(t, store)
	executionID := fmt.Sprintf("execution-%s-v2", task.ID)
	before, err := store.Events(t.Context(), "stop-work")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `CREATE TRIGGER reject_stop BEFORE INSERT ON events WHEN NEW.event_type='TASK_EXECUTION_SUSPENDED' BEGIN SELECT RAISE(ABORT,'injected stop write failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RequestExecutionStop(t.Context(), "org-1", string(task.ID), "stop-work", executionID, "execution_cancelled"); err == nil {
		t.Fatal("failed suspension admitted a stop request")
	}
	after, err := store.Events(t.Context(), "stop-work")
	if err != nil || len(after) != len(before) {
		t.Fatalf("partial stop persisted: events=%d want=%d err=%v", len(after), len(before), err)
	}
	if _, err := store.db.ExecContext(t.Context(), `DROP TRIGGER reject_stop`); err != nil {
		t.Fatal(err)
	}
	request, err := store.RequestExecutionStop(t.Context(), "org-1", string(task.ID), "stop-work", executionID, "execution_cancelled")
	if err != nil {
		t.Fatal(err)
	}
	for _, args := range [][4]string{{"org-2", string(task.ID), "stop-work", executionID}, {"org-1", string(task.ID), "other-work", executionID}, {"org-1", string(task.ID), "stop-work", "other-execution"}} {
		if _, err := store.RequestExecutionStop(t.Context(), args[0], args[1], args[2], args[3], "execution_cancelled"); err == nil {
			t.Fatalf("crossed stop identity: %v", args)
		}
	}
	retry, err := store.RequestExecutionStop(t.Context(), "org-1", string(task.ID), "stop-work", executionID, "execution_cancelled")
	if err != nil || retry.EventID != request.EventID {
		t.Fatalf("request retry duplicated evidence: %v", err)
	}
	uncertain, err := store.RecordExecutionStop(t.Context(), request.EventID, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if retry, err := store.RecordExecutionStop(t.Context(), request.EventID, nil, nil); err != nil || retry.EventID != uncertain.EventID {
		t.Fatalf("uncertain retry: %v", err)
	}
	now := time.Now().UTC()
	outcome := core.ToolOutcome{ToolInvocationID: "stop-invocation", ToolID: "runtime-containment", Status: core.OutcomeFailed, PostconditionStatus: core.PostconditionNotChecked, Retryability: core.NotRetryable, ErrorClass: "execution_cancelled", StartedAt: now, FinishedAt: now, ObservedEffect: core.ExecutionInterruptionEvidence{StopRequestRef: request.EventID, LocalExecutionStopped: true, ExternalEffectsStatus: "REQUIRES_RECONCILIATION"}}
	usage := testInferenceUsage()
	before, err = store.Events(t.Context(), "stop-work")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `CREATE TRIGGER reject_stop BEFORE INSERT ON events WHEN NEW.event_type='EXECUTION_STOP_CONFIRMED' BEGIN SELECT RAISE(ABORT,'injected confirmation failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordExecutionStop(t.Context(), request.EventID, &outcome, &usage); err == nil {
		t.Fatal("confirmation failure was ignored")
	}
	after, err = store.Events(t.Context(), "stop-work")
	if err != nil || len(after) != len(before) {
		t.Fatalf("partial confirmation persisted: %v", err)
	}
	if _, err := store.db.ExecContext(t.Context(), `DROP TRIGGER reject_stop`); err != nil {
		t.Fatal(err)
	}
	confirmed, err := store.RecordExecutionStop(t.Context(), request.EventID, &outcome, &usage)
	if err != nil {
		t.Fatal(err)
	}
	if retry, err := store.RecordExecutionStop(t.Context(), request.EventID, &outcome, &usage); err != nil || retry.EventID != confirmed.EventID {
		t.Fatalf("confirmation retry: %v", err)
	}
	if _, err := store.RecordExecutionStop(t.Context(), request.EventID, &outcome, nil); err == nil {
		t.Fatal("confirmation retry dropped original usage")
	}
	changed := usage
	changed.Model += "-different"
	if _, err := store.RecordExecutionStop(t.Context(), request.EventID, &outcome, &changed); err == nil {
		t.Fatal("confirmation retry changed original usage")
	}
	var result events.ExecutionStopResult
	if err := json.Unmarshal(confirmed.Payload, &result); err != nil || result.OutcomeEventRef == "" || result.UsageEventRef == "" || result.FinishEventRef == "" {
		t.Fatalf("missing atomic stop evidence: %+v err=%v", result, err)
	}
}

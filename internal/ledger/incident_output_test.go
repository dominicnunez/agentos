package ledger

import (
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentIgnoresRecoveryResultGraphKeys(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	task := incidentTestExecution(t, store)
	appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "unrelated", false)
	now := time.Now().UTC()
	_, err = store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", CorrelationID: "stop-work", TaskID: string(task.ID), SourceActorID: "runtime", SourceExecutionID: "execution-stop-task-v2", EventType: "TOOL_OUTCOME_RECORDED", Payload: core.ToolOutcome{ToolInvocationID: "invocation", ToolID: "test", Status: core.OutcomeSucceeded, PostconditionStatus: core.PostconditionVerified, Retryability: core.NotRetryable, RecoveryAttempted: true, RecoveryResult: map[string]any{"agent_id": "agent-unrelated", "nested": map[string]string{"goal_id": "missing-goal"}}, StartedAt: now, FinishedAt: now}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `DELETE FROM records WHERE kind='execution_profile' AND record_id='profile-unrelated'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256); err != nil {
		t.Fatalf("opaque tool recovery output expanded graph selection: %v", err)
	}
}

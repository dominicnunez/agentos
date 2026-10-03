package recovery

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
)

// Exercise real admission writers and prove full recovery covers every
// completed Task in the durable event stream.
func TestRecoveryCompletedTaskHistory(t *testing.T) {
	const count = 4
	path := recoveryCompletedTaskHistory(t, count)
	result, err := Verify(t.Context(), path)
	if err != nil {
		t.Fatalf("verify completed Task history: %v", err)
	}
	if result.EventCount != 1+10*count || result.EventChainSHA256 == "" {
		t.Fatalf("verification missed completed Task history: %+v", result)
	}
	stream := completedTaskStream(t, path)
	graph, err := events.ValidateProjectionHistory(stream, nil, nil, nil)
	if err != nil {
		t.Fatalf("replay completed Task history: %v", err)
	}
	if len(graph.Tasks) != count {
		t.Fatalf("replayed Tasks=%d, want %d", len(graph.Tasks), count)
	}
	for _, state := range graph.Tasks {
		if state.Value.Status != core.TaskCompleted {
			t.Fatalf("replayed Task %s is %s", state.Value.ID, state.Value.Status)
		}
	}
}

// Measure full offline verification after many separate works have completed
// their Tasks. Fixture creation is excluded from the measurement.
func BenchmarkRecoveryCompletedTaskHistory(b *testing.B) {
	for _, count := range []int{8, 32, 128} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			path := recoveryCompletedTaskHistory(b, count)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				result, err := Verify(b.Context(), path)
				if err != nil {
					b.Fatal(err)
				}
				if result.EventCount != int64(1+10*count) || result.EventChainSHA256 == "" {
					b.Fatalf("verification missed completed history: events=%d tasks=%d", result.EventCount, count)
				}
			}
		})
	}
}

// Isolate the replay validator from SQL and integrity costs after the complete
// recovery benchmark above has established the public operation's behavior.
func BenchmarkCompletedTaskProjectionHistory(b *testing.B) {
	for _, count := range []int{8, 32, 128} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			path := recoveryCompletedTaskHistory(b, count)
			stream := completedTaskStream(b, path)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := events.ValidateProjectionHistory(stream, nil, nil, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func completedTaskStream(t testing.TB, path string) []events.Event {
	t.Helper()
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := store.Events(t.Context(), "")
	if closeErr := store.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	return stream
}

func recoveryCompletedTaskHistory(t testing.TB, count int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "completed-tasks.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	}()
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Recovery growth", PolicyVersion: "v1", CreatedAt: now}
	appendProjection := func(kind, id, label, correlation, taskID string, version int, value any, detail any) {
		t.Helper()
		if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{
			Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: label, SourceActorID: "runtime", TaskID: taskID, CorrelationID: correlation, Payload: detail},
			ProjectionKind: kind, RecordID: id, Version: version, Value: value,
		}); err != nil {
			t.Fatalf("append %s for %s: %v", label, id, err)
		}
	}
	appendProjection("organization", string(organization.ID), "ORGANIZATION_CREATED", "setup", "", 1, organization, nil)
	for index := 0; index < count; index++ {
		correlation := fmt.Sprintf("work-%d", index)
		taskID := fmt.Sprintf("task-%d", index)
		intent := core.Intent{ID: core.ID(fmt.Sprintf("intent-%d", index)), OrganizationID: organization.ID, OriginalInstruction: "echo bounded work", NormalizedObjective: "echo bounded work", AcceptedFingerprint: "internal-recovery", CreatedAt: now}
		work := core.Work{ID: core.ID(correlation), IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
		task := core.Task{ID: core.ID(taskID), WorkID: work.ID, Description: "echo bounded work", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, RuntimeHandlerRef: "builtin.echo", TaskContractVersion: "1", Status: core.TaskPending}
		appendProjection("intent", string(intent.ID), "INTENT_CREATED", correlation, "", 1, intent, nil)
		appendProjection("work", string(work.ID), "WORK_CREATED", correlation, "", 1, work, nil)
		appendProjection("task", taskID, "TASK_CREATED", correlation, taskID, 1, task, nil)
		appendRecoveryPlan(t, store, correlation, intent, task)
		task.Status = core.TaskRunning
		if _, _, err := store.AppendExecutionStart(t.Context(), events.ProjectionDraft{
			Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_STARTED", SourceActorID: "runtime", TaskID: taskID, CorrelationID: correlation, Payload: events.ExecutionStartDetail{}},
			ProjectionKind: "task", RecordID: taskID, Version: 2, Value: task,
		}, nil, nil); err != nil {
			t.Fatalf("start Task %s: %v", taskID, err)
		}
		// A deterministic registered handler gives the completion verifier an
		// independently checkable postcondition without a model or network call.
		executionID := fmt.Sprintf("execution-%s-v2", task.ID)
		outcome := core.ToolOutcome{
			ToolInvocationID: core.ID("outcome-" + taskID), ToolID: "builtin.echo", ToolVersion: "v1",
			Status: core.OutcomeSucceeded, ObservedEffect: "bounded work",
			PostconditionStatus: core.PostconditionVerified, Retryability: core.NotRetryable,
			StartedAt: now, FinishedAt: now,
		}
		appendEvent := func(label string, payload any) events.Event {
			t.Helper()
			event, err := store.Append(t.Context(), events.TrustedDraft{
				OrganizationID: "org-1", EventType: label, SourceActorID: "runtime",
				SourceExecutionID: executionID, TaskID: taskID, CorrelationID: correlation, Payload: payload,
			})
			if err != nil {
				t.Fatalf("append %s for %s: %v", label, taskID, err)
			}
			return event
		}
		outcomeEvent := appendEvent("TOOL_OUTCOME_RECORDED", outcome)
		summary, err := core.ToolOutcomeSummary(outcome)
		if err != nil {
			t.Fatal(err)
		}
		resultEvent := appendEvent("RESULT_PUBLISHED", events.ResultPublishedPayload{Summary: summary, ArtifactRefs: outcome.ArtifactRefs})
		appendEvent("CANDIDATE_COMPLETE", events.CandidateCompletePayload{
			ToolInvocationID: string(outcome.ToolInvocationID), ResultEventID: resultEvent.EventID,
			ArtifactRefs: outcome.ArtifactRefs,
		})
		decision := events.CompletionDecisionPayload{
			Contract: core.VerifiedOutcomeCompletionContract(task.ID, 2),
			Result:   core.CompletionResult{Complete: true}, OutcomeEventRef: outcomeEvent.EventID,
		}
		appendEvent("COMPLETION_VERIFIED", decision)
		task.Status = core.TaskCompleted
		appendProjection("task", taskID, "TASK_VERIFIED_COMPLETE", correlation, taskID, 3, task, decision)
	}
	return path
}

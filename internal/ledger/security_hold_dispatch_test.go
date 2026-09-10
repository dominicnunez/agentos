package ledger

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestSecurityFreezeRejectsAllStaleExecutionOutputs(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	writer, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	appendTaskProjectionParents(t, ctx, store, "org-1", "held-outputs", "work-1")
	agent, config := appendTaskAssignmentAgent(t, ctx, store, "org-1", "held-outputs", false)
	task := appendPendingAgentExecutionTask(t, ctx, store, "held-outputs", "held-task", agent, config)
	live, release, err := store.BeginExecutionContext(ctx, "org-1")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err := startPendingAgentExecution(live, store, "held-outputs", task); err != nil {
		t.Fatal(err)
	}
	appendInferenceFreeze(t, writer, "org-1", 1, true)
	appendInferenceFreeze(t, writer, "org-1", 2, false)
	before, err := store.Events(ctx, "held-outputs")
	if err != nil {
		t.Fatal(err)
	}
	for _, callCtx := range []context.Context{context.WithoutCancel(live), ctx} {
		for _, eventType := range []string{"KNOWLEDGE_PROPOSED", "KNOWLEDGE_JUDGMENT_PUBLISHED", "SKILL_PROPOSED", "CUSTOM_EXECUTION_EVENT", "EVIDENCE_PUBLISHED", "MESSAGE"} {
			draft := events.TrustedDraft{OrganizationID: "org-1", EventType: eventType, SourceActorID: string(agent.ID), SourceExecutionID: fmt.Sprintf("execution-%s-v2", task.ID), TaskID: string(task.ID), CorrelationID: "held-outputs", Payload: map[string]string{"claim": "untrusted"}}
			if eventType == "MESSAGE" {
				draft.RecipientScope, draft.RecipientID = events.RecipientAgent, string(agent.ID)
			}
			if eventType == "EVIDENCE_PUBLISHED" {
				draft.ArtifactRefs = []string{"artifact-1"}
				draft.Payload = events.EvidencePublishedPayload{Summary: "untrusted evidence", ArtifactRefs: draft.ArtifactRefs}
				_, err = store.AppendAgentEvidence(callCtx, draft)
			} else {
				_, err = store.Append(callCtx, draft)
			}
			if !errors.Is(err, core.ErrOrganizationFrozen) {
				t.Fatalf("%s did not enforce execution hold: %v", eventType, err)
			}
		}
	}
	after, err := store.Events(ctx, "held-outputs")
	if err != nil || len(after) != len(before) {
		t.Fatalf("rejected execution outputs were persisted: %v", err)
	}
}

func TestSecurityFreezeOtherHandleInvalidatesPreparationBeforeMonitor(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "ledger.db")
	reader, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	writer, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	appendTaskProjectionParents(t, ctx, reader, "org-1", "held-work", "work-1")
	agent, config := appendTaskAssignmentAgent(t, ctx, reader, "org-1", "held-work", false)
	task := appendPendingAgentExecutionTask(t, ctx, reader, "held-work", "held-task", agent, config)
	preparation, release, err := reader.BeginExecutionContext(ctx, "org-1")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	appendInferenceFreeze(t, writer, "org-1", 1, true)
	appendInferenceFreeze(t, writer, "org-1", 2, false)
	// Suppress cancellation delivery while retaining the production context
	// generation: admission must independently detect the committed history.
	_, err = startPendingAgentExecution(context.WithoutCancel(preparation), reader, "held-work", task)
	var cause core.SecurityHoldCause
	if !errors.As(err, &cause) {
		t.Fatalf("stale preparation admitted without monitor: %v", err)
	}
	_, persisted := latestTestProjection[core.Task](t, ctx, reader, "task", task.ID)
	if persisted.Status != core.TaskPending {
		t.Fatal("stale preparation changed pending state")
	}
	fresh, finish, err := reader.BeginExecutionContext(ctx, "org-1")
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	if _, err := startPendingAgentExecution(fresh, reader, "held-work", task); err != nil {
		t.Fatalf("fresh released preparation rejected: %v", err)
	}
}

func TestSecurityFreezeReleaseCannotReviveOldOutcome(t *testing.T) {
	ctx := t.Context()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	appendTaskProjectionParents(t, ctx, store, "org-1", "held-work", "work-1")
	agent, config := appendTaskAssignmentAgent(t, ctx, store, "org-1", "held-work", false)
	task := appendPendingAgentExecutionTask(t, ctx, store, "held-work", "held-task", agent, config)
	if _, err := startPendingAgentExecution(ctx, store, "held-work", task); err != nil {
		t.Fatal(err)
	}
	appendInferenceFreeze(t, store, "org-1", 1, true)
	appendInferenceFreeze(t, store, "org-1", 2, false)
	now := time.Now().UTC()
	outcome := core.ToolOutcome{ToolInvocationID: "invocation", ToolID: "echo", Status: core.OutcomeSucceeded, PostconditionStatus: core.PostconditionVerified, Retryability: core.NotRetryable, StartedAt: now, FinishedAt: now}
	draft := events.TrustedDraft{OrganizationID: "org-1", EventType: "TOOL_OUTCOME_RECORDED", SourceActorID: "runtime", SourceExecutionID: fmt.Sprintf("execution-%s-v2", task.ID), TaskID: string(task.ID), CorrelationID: "held-work", Payload: outcome}
	_, err = store.Append(ctx, draft)
	var cause core.SecurityHoldCause
	if !errors.As(err, &cause) || cause.EventRef == "" {
		t.Fatalf("released old execution admitted success: %v", err)
	}
	// A handler failure is still ordinary execution output. A committed hold
	// must turn it into a suspension, not authorize terminal task failure.
	outcome.Status = core.OutcomeFailed
	outcome.PostconditionStatus = core.PostconditionNotChecked
	outcome.ErrorClass = "provider_failure"
	draft.Payload = outcome
	if _, err := store.Append(ctx, draft); !errors.As(err, &cause) {
		t.Fatalf("released old execution admitted ordinary failure: %v", err)
	}
	outcome.ToolID = "runtime-containment"
	outcome.Status = core.OutcomeFailed
	outcome.PostconditionStatus = core.PostconditionNotChecked
	outcome.ErrorClass = "security_hold"
	outcome.ObservedEffect = core.ExecutionInterruptionEvidence{Hold: &cause, LocalExecutionStopped: true, ExternalEffectsStatus: "REQUIRES_RECONCILIATION"}
	draft.Payload = outcome
	if _, err := store.Append(ctx, draft); err != nil {
		t.Fatalf("exact interrupted audit rejected after release: %v", err)
	}
}

func TestSecurityFreezeRejectsSuccessfulOutcomeAtAdmission(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC()
	outcome := core.ToolOutcome{ToolInvocationID: "invocation", ToolID: "echo", Status: core.OutcomeSucceeded, PostconditionStatus: core.PostconditionVerified, Retryability: core.NotRetryable, StartedAt: now, FinishedAt: now}
	draft := events.TrustedDraft{OrganizationID: "org-1", EventType: "TOOL_OUTCOME_RECORDED", SourceActorID: "runtime", SourceExecutionID: "execution-1", TaskID: "task-1", CorrelationID: "held-outcome", Payload: outcome}
	appendInferenceFreeze(t, store, "org-1", 1, true)
	if _, err := store.Append(t.Context(), draft); !errors.Is(err, core.ErrOrganizationFrozen) {
		t.Fatalf("held success admission: %v", err)
	}
	stream, err := store.Events(t.Context(), "held-outcome")
	if err != nil || len(stream) != 0 {
		t.Fatalf("denied outcome persisted: %v", err)
	}
	// A failed interruption is audit evidence, but its hold cannot be invented.
	outcome.ToolID = "runtime-containment"
	outcome.Status = core.OutcomeFailed
	outcome.PostconditionStatus = core.PostconditionNotChecked
	outcome.ErrorClass = "security_hold"
	outcome.ObservedEffect = core.ExecutionInterruptionEvidence{Hold: &core.SecurityHoldCause{OrganizationID: "org-1", EventRef: "invented", Sequence: 1}, LocalExecutionStopped: true, ExternalEffectsStatus: "REQUIRES_RECONCILIATION"}
	draft.Payload = outcome
	if _, err := store.Append(t.Context(), draft); err == nil {
		t.Fatal("forged hold evidence admitted")
	}
}

func TestSecurityFreezeRejectsExecutionBeforeInboxSelection(t *testing.T) {
	ctx := t.Context()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	appendTaskProjectionParents(t, ctx, store, "org-1", "held-work", "work-1")
	agent, config := appendTaskAssignmentAgent(t, ctx, store, "org-1", "held-work", false)
	task := appendPendingAgentExecutionTask(t, ctx, store, "held-work", "held-task", agent, config)
	before, err := store.Events(ctx, "held-work")
	if err != nil {
		t.Fatal(err)
	}
	appendInferenceFreeze(t, store, "org-1", 1, true)
	task.Status = core.TaskRunning
	selected := false
	_, _, err = store.AppendExecutionStart(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_STARTED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "held-work"},
		ProjectionKind: "task", RecordID: string(task.ID), Version: 2, Value: task,
	}, []events.InboxRoute{{Scope: events.RecipientTask, ID: string(task.ID)}, {Scope: events.RecipientAgent, ID: string(agent.ID)}}, func(selection events.ExecutionStartSelection) (core.ExecutionContextManifest, error) {
		selected = true
		return testAgentStartManifest(task, selection), nil
	})
	if err == nil || !strings.Contains(err.Error(), "frozen") || selected {
		t.Fatalf("held dispatch reached inbox selection: selected=%v err=%v", selected, err)
	}
	after, err := store.Events(ctx, "held-work")
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatal("denied dispatch published work events")
	}
	_, persisted := latestTestProjection[core.Task](t, ctx, store, "task", task.ID)
	if persisted.Status != core.TaskPending {
		t.Fatal("denied dispatch changed task state")
	}
	appendInferenceFreeze(t, store, "org-1", 2, false)
	if _, err = startPendingAgentExecution(ctx, store, "held-work", persisted); err != nil {
		t.Fatalf("released task cannot start: %v", err)
	}
}

func TestManifestExecutionContainmentAfterRelease(t *testing.T) {
	for _, kind := range []string{"INTENT_NORMALIZATION_CONTEXT_MANIFESTED", "PLANNING_CONTEXT_MANIFESTED"} {
		t.Run(kind, func(t *testing.T) {
			ctx := t.Context()
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			appendInferenceFreeze(t, store, "org-1", 1, true)
			appendInferenceFreeze(t, store, "org-1", 2, false)
			draft := events.TrustedDraft{OrganizationID: "org-1", EventType: kind, SourceActorID: "runtime", SourceExecutionID: "model-attempt-1", TaskID: "task-1", CorrelationID: "work-1", Payload: map[string]string{"source_message_id": "message-1"}}
			if _, err := store.Append(ctx, draft); err != nil {
				t.Fatalf("new manifest after release: %v", err)
			}
			draft.EventType = "INTENT_DRAFTED"
			if _, err := store.Append(ctx, draft); err != nil {
				t.Fatalf("new output after release: %v", err)
			}
			appendInferenceFreeze(t, store, "org-1", 3, true)
			for _, released := range []bool{false, true} {
				if released {
					appendInferenceFreeze(t, store, "org-1", 4, false)
				}
				if _, err := store.Append(ctx, draft); !errors.Is(err, core.ErrOrganizationFrozen) {
					t.Fatalf("stale output released=%v: %v", released, err)
				}
			}
			draft.SourceExecutionID = "unknown-execution"
			if _, err := store.Append(ctx, draft); err == nil {
				t.Fatal("unmanifested execution accepted")
			}
		})
	}
}

package ledger

import (
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

// incidentTestExecution supplies the complete admission history inspected by an
// incident reader. The smaller stop fixture intentionally tests a different
// boundary and remains unchanged.
func incidentTestExecution(t *testing.T, store *SQLite) core.Task {
	t.Helper()
	ctx := t.Context()
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	intent := core.Intent{ID: "intent-work-1", OrganizationID: organization.ID, OriginalInstruction: "test task", NormalizedObjective: "test task", AcceptedFingerprint: core.FingerprintExecutionInput("internal incident test task"), CreatedAt: now}
	work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup-stop-work"}, ProjectionKind: "organization", RecordID: "org-1", Version: 1, Value: organization},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: "stop-work"}, ProjectionKind: "intent", RecordID: string(intent.ID), Version: 1, Value: intent},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: "stop-work"}, ProjectionKind: "work", RecordID: string(work.ID), Version: 1, Value: work},
	} {
		if _, err := store.AppendProjection(ctx, draft); err != nil {
			t.Fatal(err)
		}
	}
	agent, config := appendTaskAssignmentAgent(t, ctx, store, "org-1", "stop-work", false)
	task := appendPendingAgentExecutionTask(t, ctx, store, "stop-work", "stop-task", agent, config)
	plan := core.Plan{ID: "plan-stop-work", IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, Version: 1, Tasks: []core.PlanTask{{Key: "agent-work", Description: task.Description, ExecutionKind: task.ExecutionKind, ModelInferencePolicy: task.ModelInferencePolicy}}, CreatedAt: now}
	var err error
	plan.Fingerprint, err = core.FingerprintPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "PLAN_CREATED", SourceActorID: "runtime", TaskID: "task-stop-work", CorrelationID: "stop-work", Payload: plan}); err != nil {
		t.Fatal(err)
	}
	started, err := startPendingAgentExecution(ctx, store, "stop-work", task)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := store.Events(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	running := task
	running.Status = core.TaskRunning
	if err := events.ValidateTaskExecutionStart(started, running, 2, work, intent, stream); err != nil {
		t.Fatalf("incident fixture start admission: %v", err)
	}
	return task
}

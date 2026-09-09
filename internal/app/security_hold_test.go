package app

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/projections"
)

func TestSchedulerLeavesHeldTenantPendingAndRunsOtherTenant(t *testing.T) {
	for _, timing := range []string{"during-handler", "before-admission", "freeze-release-before-admission", "freeze-release-before-start"} {
		t.Run(timing, func(t *testing.T) { testSchedulerSecurityHold(t, timing) })
	}
}

func testSchedulerSecurityHold(t *testing.T, timing string) {
	ctx := t.Context()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	interceptor := &holdBeforeOutcomeLedger{SQLite: store}
	gateway := events.NewGateway(interceptor)
	repository := projections.New(gateway)
	for _, suffix := range []string{"a", "b"} {
		org := core.Organization{ID: core.ID("org-" + suffix), Name: "Organization " + suffix, PolicyVersion: "v1"}
		correlation := "request-" + suffix
		if err = repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", correlation, 1, org, nil); err != nil {
			t.Fatal(err)
		}
		agent := seedTestAgents(t, ctx, repository, correlation, org.ID, execution.FakeModel{}.Descriptor(), core.ID("agent-"+suffix))[0]
		intent := acceptedTestIntent(core.ID("intent-"+suffix), org.ID, "echo bounded work")
		work := core.Work{ID: core.ID("work-" + suffix), IntentID: intent.ID, Objective: "echo bounded work", Status: "ACTIVE"}
		task := core.Task{ID: core.ID("task-" + correlation), WorkID: work.ID, Description: "echo bounded work", AcceptanceCriteria: intent.CompletionCriteria, ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: testAgentConfig(agent), TaskContractVersion: "1", Status: core.TaskPending}
		if err = saveTestTaskGraph(ctx, repository, org.ID, correlation, intent, work, task); err != nil {
			t.Fatal(err)
		}
		if err = saveTestPlan(ctx, gateway, correlation, intent, task); err != nil {
			t.Fatal(err)
		}
	}
	freeze := struct {
		OrganizationID core.ID   `json:"organization_id"`
		Frozen         bool      `json:"frozen"`
		Reason         string    `json:"reason,omitempty"`
		UpdatedAt      time.Time `json:"updated_at"`
	}{OrganizationID: "org-a", Frozen: true, Reason: "security hold", UpdatedAt: time.Now().UTC()}
	if err = store.AppendRecord(ctx, "org-a", "FREEZE_SET", "user-1", "task-request-a", nil, nil, "organization_freeze", "org-a", 1, freeze); err != nil {
		t.Fatal(err)
	}
	service := New(gateway)
	runs, err := service.runReady(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := runs["task-request-a"]; ok {
		t.Fatal("held task ran")
	}
	if _, ok := runs["task-request-b"]; !ok {
		t.Fatal("held tenant blocked unrelated tenant")
	}
	snapshot, err := repository.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Tasks["task-request-a"].Value.Status != core.TaskPending || snapshot.Tasks["task-request-b"].Value.Status != core.TaskCompleted {
		t.Fatal("scheduler changed held task or failed unrelated task")
	}
	freeze.Frozen = false
	freeze.UpdatedAt = time.Now().UTC()
	if err = store.AppendRecord(ctx, "org-a", "FREEZE_SET", "user-1", "task-request-a", nil, nil, "organization_freeze", "org-a", 2, freeze); err != nil {
		t.Fatal(err)
	}
	commitHold := func() {
		freeze.Frozen = true
		freeze.UpdatedAt = time.Now().UTC()
		if err := store.AppendRecord(ctx, "org-a", "FREEZE_SET", "user-1", "task-request-a", nil, nil, "organization_freeze", "org-a", 3, freeze); err != nil {
			t.Fatal(err)
		}
	}
	switch timing {
	case "freeze-release-before-start":
		interceptor.beforeStart = func() {
			commitHold()
			freeze.Frozen = false
			freeze.UpdatedAt = time.Now().UTC()
			if err := store.AppendRecord(ctx, "org-a", "FREEZE_SET", "user-1", "task-request-a", nil, nil, "organization_freeze", "org-a", 4, freeze); err != nil {
				t.Fatal(err)
			}
		}
		service.deterministic = holdDuringHandler{freeze: func() { t.Fatal("cancelled preparation dispatched handler") }}
	case "during-handler":
		service.deterministic = holdDuringHandler{freeze: commitHold}
	default:
		interceptor.beforeOutcome = func() {
			commitHold()
			if timing == "freeze-release-before-admission" {
				freeze.Frozen = false
				freeze.UpdatedAt = time.Now().UTC()
				if err := store.AppendRecord(ctx, "org-a", "FREEZE_SET", "user-1", "task-request-a", nil, nil, "organization_freeze", "org-a", 4, freeze); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	runs, err = service.runReady(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if timing == "freeze-release-before-start" {
		if _, ran := runs["task-request-a"]; ran {
			t.Fatal("cancelled preparation produced an execution")
		}
		snapshot, err = repository.Load(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.Tasks["task-request-a"].Value.Status != core.TaskPending {
			t.Fatal("cancelled preparation changed pending task")
		}
		return
	}
	if runs["task-request-a"].Outcome.Status == core.OutcomeSucceeded {
		t.Fatal("handler success escaped containment")
	}
	snapshot, err = repository.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Tasks["task-request-a"].Value.Status == core.TaskCompleted {
		t.Fatal("held handler completed task")
	}
	stream, err := gateway.Events(ctx, "request-a")
	if err != nil {
		t.Fatal(err)
	}
	var recorded bool
	for _, event := range stream {
		if event.EventType == "RESULT_PUBLISHED" || event.EventType == "CANDIDATE_COMPLETE" {
			t.Fatalf("interrupted execution published ordinary output: %s", event.EventType)
		}
		if event.EventType != "TOOL_OUTCOME_RECORDED" {
			continue
		}
		var outcome core.ToolOutcome
		if err := json.Unmarshal(event.Payload, &outcome); err != nil {
			t.Fatal(err)
		}
		if outcome.ErrorClass != "security_hold" {
			continue
		}
		encoded, err := json.Marshal(outcome.ObservedEffect)
		if err != nil {
			t.Fatal(err)
		}
		var evidence core.ExecutionInterruptionEvidence
		if err := json.Unmarshal(encoded, &evidence); err != nil {
			t.Fatal(err)
		}
		if evidence.Hold == nil || evidence.Hold.OrganizationID != "org-a" || evidence.Hold.EventRef == "" || evidence.Hold.Sequence >= event.Sequence {
			t.Fatalf("missing prior hold binding: %+v", evidence)
		}
		if !evidence.LocalExecutionStopped || evidence.ExternalEffectsStatus != "REQUIRES_RECONCILIATION" {
			t.Fatalf("incorrect interruption certainty: %+v", evidence)
		}
		if evidence.ReportedOutcome.ToolInvocationID == "" || evidence.ReportedOutcome.StartedAt.Before(outcome.StartedAt) || evidence.ReportedOutcome.FinishedAt.After(outcome.FinishedAt) {
			t.Fatal("handler evidence or runtime timing was lost")
		}
		recorded = true
	}
	if !recorded {
		t.Fatal("interruption evidence was not persisted")
	}
	_, freezes, err := gateway.KnowledgeAuthorityAdmissions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []string{"event-ref", "sequence", "organization", "successful-outcome"} {
		corrupted := append([]events.Event(nil), stream...)
		for index, event := range corrupted {
			if event.EventType != "TOOL_OUTCOME_RECORDED" {
				continue
			}
			var outcome core.ToolOutcome
			if err := json.Unmarshal(event.Payload, &outcome); err != nil {
				t.Fatal(err)
			}
			if outcome.ErrorClass != "security_hold" {
				continue
			}
			encoded, err := json.Marshal(outcome.ObservedEffect)
			if err != nil {
				t.Fatal(err)
			}
			var evidence core.ExecutionInterruptionEvidence
			if err := json.Unmarshal(encoded, &evidence); err != nil {
				t.Fatal(err)
			}
			switch mutation {
			case "event-ref":
				evidence.Hold.EventRef = "invented"
			case "sequence":
				evidence.Hold.Sequence = event.Sequence + 1
			case "organization":
				evidence.Hold.OrganizationID = "org-b"
			case "successful-outcome":
				outcome.Status = core.OutcomeSucceeded
			}
			outcome.ObservedEffect = evidence
			corrupted[index].Payload, err = json.Marshal(outcome)
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := events.ValidateSecurityHoldOutcomes(corrupted, freezes); err == nil {
			t.Fatalf("replay accepted %s corruption", mutation)
		}
	}
}

type holdBeforeOutcomeLedger struct {
	*ledger.SQLite
	beforeOutcome func()
	beforeStart   func()
}

func (l *holdBeforeOutcomeLedger) AppendExecutionStart(ctx context.Context, draft events.ProjectionDraft, routes []events.InboxRoute, validate events.ExecutionStartValidator) (events.Event, []events.InboxSelection, error) {
	if draft.Event.OrganizationID == "org-a" && l.beforeStart != nil {
		before := l.beforeStart
		l.beforeStart = nil
		before()
	}
	return l.SQLite.AppendExecutionStart(ctx, draft, routes, validate)
}

func (l *holdBeforeOutcomeLedger) Append(ctx context.Context, draft events.TrustedDraft) (events.Event, error) {
	if draft.EventType == "TOOL_OUTCOME_RECORDED" && draft.OrganizationID == "org-a" && l.beforeOutcome != nil {
		before := l.beforeOutcome
		l.beforeOutcome = nil
		before()
	}
	return l.SQLite.Append(ctx, draft)
}

type holdDuringHandler struct{ freeze func() }

func (h holdDuringHandler) Execute(ctx context.Context, task core.Task, manifest core.ExecutionContextManifest) (execution.Result, error) {
	h.freeze()
	// Return a superficially valid success despite cancellation. The runtime
	// must independently reject it as task completion evidence.
	return (execution.Deterministic{}).Execute(ctx, task, manifest)
}

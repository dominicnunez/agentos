package app

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/projections"
)

func TestReleasedHoldAllowsHumanContinuations(t *testing.T) {
	testHumanContinuations(t, false)
}

func TestHeldHumanContinuationsRemainSuspended(t *testing.T) {
	testHumanContinuations(t, true)
}

func testHumanContinuations(t *testing.T, interrupt bool) {
	for _, structured := range []bool{false, true} {
		name := "external-input"
		if structured {
			name = "human-completion"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			store, err := ledger.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			interceptor := &humanHoldLedger{SQLite: store}
			gateway := events.NewGateway(interceptor)
			repository := projections.New(gateway)
			seedTestGoal(t, ctx, repository, "org-1", "mission-1", "goal-1", core.GoalActive)
			service := New(gateway)
			in := confirmedGoalSubmit(t, ctx, gateway, "released-human", "org-1", "goal-1", "provide a governed decision", core.ExecutionHuman)
			result, err := service.Submit(ctx, in)
			if err != nil || result.Task.Status != core.TaskBlocked {
				t.Fatalf("prepare user task: %v", err)
			}
			if !structured {
				intent := acceptedTestIntent("legacy-intent", "org-1", "provide input")
				work := core.Work{ID: "legacy-work", IntentID: intent.ID, Objective: "provide input", Status: core.WorkActive}
				task := result.Task
				task.ID, task.WorkID, task.Status, task.CompletionContract = "task-legacy-input", work.ID, core.TaskPending, nil
				if err := saveTestTaskGraph(ctx, repository, "org-1", "legacy-input", intent, work, task); err != nil {
					t.Fatal(err)
				}
				if err := saveTestPlan(ctx, gateway, "legacy-input", intent, task); err != nil {
					t.Fatal(err)
				}
				result.Task = task
			}
			for index, frozen := range []bool{true, false} {
				state := struct {
					OrganizationID core.ID   `json:"organization_id"`
					Frozen         bool      `json:"frozen"`
					UpdatedAt      time.Time `json:"updated_at"`
				}{"org-1", frozen, time.Now().UTC()}
				if err := store.AppendRecord(ctx, "org-1", "FREEZE_SET", "user-1", "released-human", nil, nil, "organization_freeze", "org-1", index+1, state); err != nil {
					t.Fatal(err)
				}
			}
			if interrupt {
				interceptor.beforeOutcome = func() {
					for index, frozen := range []bool{true, false} {
						state := struct {
							OrganizationID core.ID   `json:"organization_id"`
							Frozen         bool      `json:"frozen"`
							UpdatedAt      time.Time `json:"updated_at"`
						}{"org-1", frozen, time.Now().UTC()}
						if err := store.AppendRecord(ctx, "org-1", "FREEZE_SET", "user-1", "released-human", nil, nil, "organization_freeze", "org-1", index+3, state); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			if structured {
				err = service.ProvideHumanCompletion(ctx, HumanCompletionInput{OrganizationID: "org-1", PrincipalID: "user-1", SourceChannel: "HUMAN_DIRECT", RequestID: "released-human", TaskID: string(result.Task.ID), Submission: core.HumanTaskSubmission{MessageID: "completion-1", Fields: map[string]string{"response": "completed input"}}})
			} else {
				input, publishErr := gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "A2A_INPUT_RECEIVED", SourceActorID: "agent-1", TaskID: string(result.Task.ID), CorrelationID: "legacy-input", Payload: events.OperatorInputReceivedPayload{MessageID: "input-1", Text: "completed input", SourcePrincipalID: "agent-1", SourcePrincipalKind: string(core.PrincipalExternalAgent), SourceChannel: "A2A"}})
				if publishErr != nil {
					t.Fatal(publishErr)
				}
				err = service.continueExternalInputTask(ctx, "org-1", result.Task.ID, "legacy-input", input)
				if err == nil {
					// Replay the durable legacy continuation itself. Current Work
					// plan synthesis requires structured human completion contracts.
					err = New(gateway).continueExternalInputTask(ctx, "org-1", result.Task.ID, "legacy-input", input)
				}
			}
			if err != nil {
				t.Fatalf("released continuation rejected: %v", err)
			}
			if structured || interrupt {
				if _, err := New(gateway).Recover(ctx); err != nil {
					t.Fatalf("recover completed continuation: %v", err)
				}
			}
			snapshot, err := repository.Load(ctx)
			wantStatus := core.TaskCompleted
			if interrupt {
				wantStatus = core.TaskBlocked
			}
			if err != nil || snapshot.Tasks[result.Task.ID].Value.Status != wantStatus {
				t.Fatalf("continuation did not remain completed: %v", err)
			}
		})
	}
}

type humanHoldLedger struct {
	*ledger.SQLite
	beforeOutcome func()
}

func (l *humanHoldLedger) Append(ctx context.Context, draft events.TrustedDraft) (events.Event, error) {
	if draft.EventType == "TOOL_OUTCOME_RECORDED" && l.beforeOutcome != nil {
		before := l.beforeOutcome
		l.beforeOutcome = nil
		before()
	}
	return l.SQLite.Append(ctx, draft)
}

func TestSchedulerLeavesHeldTenantPendingAndRunsOtherTenant(t *testing.T) {
	for _, timing := range []string{"during-handler", "before-admission", "freeze-release-before-admission", "freeze-release-before-start", "after-outcome", "freeze-release-after-outcome", "before-candidate", "before-completion", "freeze-release-before-completion", "crash-before-suspension", "hold-during-recovery"} {
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
	case "crash-before-suspension":
		service.deterministic = holdDuringHandler{freeze: commitHold}
		interceptor.crashOnSuspension = true
	case "hold-during-recovery":
		interceptor.crashBeforeOutcome = true
		interceptor.publicationEventType = "TASK_RECOVERED"
		interceptor.beforePublication = func() {
			commitHold()
			freeze.Frozen = false
			freeze.UpdatedAt = time.Now().UTC()
			if err := store.AppendRecord(ctx, "org-a", "FREEZE_SET", "user-1", "task-request-a", nil, nil, "organization_freeze", "org-a", 4, freeze); err != nil {
				t.Fatal(err)
			}
		}
	case "after-outcome", "freeze-release-after-outcome", "before-candidate", "before-completion", "freeze-release-before-completion":
		interceptor.publicationEventType = "RESULT_PUBLISHED"
		if timing == "before-candidate" {
			interceptor.publicationEventType = "CANDIDATE_COMPLETE"
		}
		if strings.HasSuffix(timing, "before-completion") {
			interceptor.publicationEventType = "TASK_VERIFIED_COMPLETE"
		}
		interceptor.beforePublication = func() {
			commitHold()
			if strings.HasPrefix(timing, "freeze-release-") {
				freeze.Frozen = false
				freeze.UpdatedAt = time.Now().UTC()
				if err := store.AppendRecord(ctx, "org-a", "FREEZE_SET", "user-1", "task-request-a", nil, nil, "organization_freeze", "org-a", 4, freeze); err != nil {
					t.Fatal(err)
				}
			}
		}
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
	if timing == "crash-before-suspension" || timing == "hold-during-recovery" {
		if !errors.Is(err, errSuspensionCrash) {
			t.Fatalf("expected injected crash: %v", err)
		}
		if timing == "crash-before-suspension" {
			freeze.Frozen = false
			freeze.UpdatedAt = time.Now().UTC()
			if err := store.AppendRecord(ctx, "org-a", "FREEZE_SET", "user-1", "task-request-a", nil, nil, "organization_freeze", "org-a", 4, freeze); err != nil {
				t.Fatal(err)
			}
		}
		_, err = New(gateway).Recover(ctx)
	}
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
	if snapshot.Tasks["task-request-a"].Value.Status != core.TaskBlocked || snapshot.Works["work-a"].Value.Status != core.WorkActive {
		t.Fatal("interrupted task was not suspended with its work still active")
	}
	stream, err := gateway.Events(ctx, "request-a")
	if err != nil {
		t.Fatal(err)
	}
	var recorded bool
	for _, event := range stream {
		allowResult := timing == "before-candidate" || strings.HasSuffix(timing, "before-completion")
		allowCandidate := strings.HasSuffix(timing, "before-completion")
		if event.EventType == "RESULT_PUBLISHED" && !allowResult || event.EventType == "CANDIDATE_COMPLETE" && !allowCandidate || event.EventType == "TASK_VERIFIED_COMPLETE" || event.EventType == "COMPLETION_REJECTED" {
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
	if !recorded && timing != "hold-during-recovery" {
		t.Fatal("interruption evidence was not persisted")
	}
	if err := service.reconcileWorks(ctx); err != nil {
		t.Fatal(err)
	}
	restarted := New(gateway)
	recovered, err := restarted.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.TasksExecuted != 0 {
		t.Fatal("restart replayed a suspended execution without reconciliation")
	}
	snapshot, err = repository.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Tasks["task-request-a"].Value.Status != core.TaskBlocked || snapshot.Works["work-a"].Value.Status != core.WorkActive {
		t.Fatal("restart terminalized suspended work")
	}
	if timing == "hold-during-recovery" {
		return // This crash intentionally left no handler outcome to validate.
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
	beforeOutcome        func()
	beforeStart          func()
	beforePublication    func()
	publicationEventType string
	crashOnSuspension    bool
	crashBeforeOutcome   bool
}

var errSuspensionCrash = errors.New("injected crash before suspension persistence")

func (l *holdBeforeOutcomeLedger) AppendExecutionStart(ctx context.Context, draft events.ProjectionDraft, routes []events.InboxRoute, validate events.ExecutionStartValidator) (events.Event, []events.InboxSelection, error) {
	if draft.Event.OrganizationID == "org-a" && l.beforeStart != nil {
		before := l.beforeStart
		l.beforeStart = nil
		before()
	}
	return l.SQLite.AppendExecutionStart(ctx, draft, routes, validate)
}

func (l *holdBeforeOutcomeLedger) Append(ctx context.Context, draft events.TrustedDraft) (events.Event, error) {
	if draft.EventType == "TOOL_OUTCOME_RECORDED" && draft.OrganizationID == "org-a" && l.crashBeforeOutcome {
		l.crashBeforeOutcome = false
		return events.Event{}, errSuspensionCrash
	}
	if draft.EventType == l.publicationEventType && draft.OrganizationID == "org-a" && l.beforePublication != nil {
		before := l.beforePublication
		l.beforePublication = nil
		before()
	}
	if draft.EventType == "TOOL_OUTCOME_RECORDED" && draft.OrganizationID == "org-a" && l.beforeOutcome != nil {
		before := l.beforeOutcome
		l.beforeOutcome = nil
		before()
	}
	return l.SQLite.Append(ctx, draft)
}

func (l *holdBeforeOutcomeLedger) AppendProjection(ctx context.Context, draft events.ProjectionDraft) (events.Event, error) {
	if draft.Event.EventType == "TASK_EXECUTION_SUSPENDED" && l.crashOnSuspension {
		l.crashOnSuspension = false
		return events.Event{}, errSuspensionCrash
	}
	if draft.Event.EventType == l.publicationEventType && draft.Event.OrganizationID == "org-a" && l.beforePublication != nil {
		before := l.beforePublication
		l.beforePublication = nil
		before()
		// Model a recovery caller without the live context generation: the
		// final writer must independently bind the durable start and hold.
		ctx = context.Background()
	}
	return l.SQLite.AppendProjection(ctx, draft)
}

type holdDuringHandler struct{ freeze func() }

func (h holdDuringHandler) Execute(ctx context.Context, task core.Task, manifest core.ExecutionContextManifest) (execution.Result, error) {
	h.freeze()
	// Return a superficially valid success despite cancellation. The runtime
	// must independently reject it as task completion evidence.
	return (execution.Deterministic{}).Execute(ctx, task, manifest)
}

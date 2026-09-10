package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/completion"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/modelinput"
	"github.com/dominicnunez/agentos/internal/planning"
	"github.com/dominicnunez/agentos/internal/projections"
)

func TestReleasedHoldAllowsHumanContinuations(t *testing.T) {
	testHumanContinuations(t, false, false, false)
}

func TestHeldHumanContinuationsRemainSuspended(t *testing.T) {
	testHumanContinuations(t, true, false, false)
}

func TestFrozenHumanContinuationsRemainBlocked(t *testing.T) {
	testHumanContinuations(t, false, true, false)
}

func TestReleasedHoldBetweenResumeAndStartRemainsLatched(t *testing.T) {
	testHumanContinuations(t, false, false, true)
}

func testHumanContinuations(t *testing.T, interrupt, beforeStart, resumeGap bool) {
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
				if beforeStart || resumeGap {
					task.Status = core.TaskBlocked
					if err := repository.SaveTask(ctx, "org-1", "TASK_BLOCKED", "runtime", "legacy-input", 2, task, nil); err != nil {
						t.Fatal(err)
					}
				}
			}
			for index, frozen := range []bool{true, false} {
				if beforeStart && !frozen {
					break
				}
				state := struct {
					OrganizationID core.ID   `json:"organization_id"`
					Frozen         bool      `json:"frozen"`
					UpdatedAt      time.Time `json:"updated_at"`
				}{"org-1", frozen, time.Now().UTC()}
				if err := store.AppendRecord(ctx, "org-1", "FREEZE_SET", "user-1", "released-human", nil, nil, "organization_freeze", "org-1", index+1, state); err != nil {
					t.Fatal(err)
				}
			}
			if interrupt || resumeGap {
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
				if resumeGap {
					interceptor.afterResume = interceptor.beforeOutcome
					interceptor.beforeOutcome = nil
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
			if beforeStart || resumeGap {
				if !errors.Is(err, core.ErrOrganizationFrozen) {
					t.Fatalf("held continuation was admitted: %v", err)
				}
				for range 2 {
					if _, err := New(gateway).Recover(ctx); err != nil {
						t.Fatalf("held continuation blocked recovery: %v", err)
					}
				}
				if resumeGap {
					stream, readErr := store.Events(ctx, "")
					if readErr != nil {
						t.Fatal(readErr)
					}
					var resumeSequence int64
					for _, event := range stream {
						if event.TaskID != string(result.Task.ID) {
							continue
						}
						if event.EventType == "TASK_RESUMED" {
							resumeSequence = event.Sequence
						}
						if resumeSequence != 0 && event.EventType == "EXECUTION_STARTED" {
							t.Fatal("held resume admitted execution after recovery")
						}
						if !structured && event.EventType == "A2A_INPUT_RECEIVED" {
							if retryErr := New(gateway).continueExternalInputTask(ctx, "org-1", result.Task.ID, "legacy-input", event); !errors.Is(retryErr, core.ErrOrganizationFrozen) {
								t.Fatalf("restarted continuation bypassed hold: %v", retryErr)
							}
						}
					}
					if resumeSequence == 0 {
						t.Fatal("test did not reach durable resume")
					}
				}
				snapshot, err := repository.Load(ctx)
				want := core.TaskBlocked
				if resumeGap {
					want = core.TaskPending
				}
				if err != nil || snapshot.Tasks[result.Task.ID].Value.Status != want {
					t.Fatalf("held continuation left blocked state: %v", err)
				}
				other, err := New(gateway).Submit(ctx, Submit{RequestID: "other-human-hold", OrganizationID: "org-2", Statement: "echo independent", Kind: core.ExecutionDeterministic})
				if err != nil || other.Task.Status != core.TaskCompleted {
					t.Fatalf("held human input blocked unrelated tenant: %v", err)
				}
				return
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
	afterResume   func()
}

func (l *humanHoldLedger) AppendProjection(ctx context.Context, draft events.ProjectionDraft) (events.Event, error) {
	event, err := l.SQLite.AppendProjection(ctx, draft)
	if err == nil && draft.Event.EventType == "TASK_RESUMED" && l.afterResume != nil {
		after := l.afterResume
		l.afterResume = nil
		after()
	}
	return event, err
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
			interceptor.publicationEventType = "COMPLETION_VERIFIED"
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

// Preserve the registered generation but strip cancellation, exercising the
// authoritative writer check even if the live monitor has already signalled.
type preparationHoldLedger struct {
	*ledger.SQLite
	beforeWrite func()
}

func (l *preparationHoldLedger) AppendProjection(ctx context.Context, draft events.ProjectionDraft) (events.Event, error) {
	if l.beforeWrite != nil {
		before := l.beforeWrite
		l.beforeWrite = nil
		before()
		ctx = context.WithoutCancel(ctx)
	}
	return l.SQLite.AppendProjection(ctx, draft)
}

func TestPreStartExitsRetainContainmentGeneration(t *testing.T) {
	for _, name := range []string{"HUMAN", "TOOL", "TEAM", "MIXED", "DETERMINISTIC", "AGENT", "stale-strategy"} {
		t.Run(name, func(t *testing.T) {
			kind := core.ExecutionKind(name)
			if name == "stale-strategy" {
				kind = core.ExecutionHuman
			}
			for _, held := range []bool{false, true} {
				t.Run(fmt.Sprintf("held=%t", held), func(t *testing.T) {
					ctx := t.Context()
					store, err := ledger.Open(":memory:")
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = store.Close() })
					writer := &preparationHoldLedger{SQLite: store}
					gateway := events.NewGateway(writer)
					repository := projections.New(gateway)
					if name == "stale-strategy" {
						seedTestGoal(t, ctx, repository, "org-a", "mission-preparation", "goal-preparation", core.GoalActive)
					} else if err := repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "preparation", 1, core.Organization{ID: "org-a", Name: "Preparation", PolicyVersion: "v1"}, nil); err != nil {
						t.Fatal(err)
					}
					intent := acceptedTestIntent("intent-preparation", "org-a", "provide bounded work")
					work := core.Work{ID: "work-preparation", IntentID: intent.ID, Objective: "provide bounded work", Status: core.WorkActive}
					task := core.Task{ID: "task-preparation", WorkID: work.ID, Description: "provide bounded work", AcceptanceCriteria: intent.CompletionCriteria, ExecutionKind: kind, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending}
					if name == "stale-strategy" {
						in := confirmedGoalSubmit(t, ctx, gateway, "preparation", "org-a", "goal-preparation", "provide bounded work", core.ExecutionHuman)
						result, err := New(gateway).Submit(ctx, in)
						if err != nil {
							t.Fatal(err)
						}
						task = result.Task
						prepared, err := repository.Load(ctx)
						if err != nil {
							t.Fatal(err)
						}
						goalState := prepared.Goals["goal-preparation"]
						goal := goalState.Value
						goal.Status = core.GoalPaused
						if err := repository.SaveGoal(ctx, "GOAL_PAUSED", "runtime", goalState.CorrelationID, goalState.Version+1, goal, nil); err != nil {
							t.Fatal(err)
						}
					} else if err := saveTestTaskGraph(ctx, repository, "org-a", "preparation", intent, work, task); err != nil {
						t.Fatal(err)
					}
					snapshot, err := repository.Load(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if held {
						writer.beforeWrite = func() {
							for index, frozen := range []bool{true, false} {
								state := struct {
									OrganizationID core.ID   `json:"organization_id"`
									Frozen         bool      `json:"frozen"`
									UpdatedAt      time.Time `json:"updated_at"`
								}{"org-a", frozen, time.Now().UTC()}
								if err := store.AppendRecord(ctx, "org-a", "FREEZE_SET", "user-1", "preparation", nil, nil, "organization_freeze", "org-a", index+1, state); err != nil {
									t.Fatal(err)
								}
							}
						}
					}
					_, err = New(gateway).executeTask(ctx, snapshot, snapshot.Tasks[task.ID], false)
					if held && !errors.Is(err, core.ErrOrganizationFrozen) {
						t.Fatalf("released hold admitted pre-start mutation: %v", err)
					}
					if !held && err != nil {
						t.Fatalf("ordinary preparation exit: %v", err)
					}
					after, err := repository.Load(ctx)
					if err != nil {
						t.Fatal(err)
					}
					want := core.TaskBlocked
					if name == "stale-strategy" {
						want = core.TaskFailed
					}
					if held {
						want = task.Status
					}
					if after.Tasks[task.ID].Value.Status != want {
						t.Fatalf("status = %s, want %s", after.Tasks[task.ID].Value.Status, want)
					}
					if writer.beforeWrite != nil {
						t.Fatal("preparation mutation was not reached")
					}
				})
			}
		})
	}
}

func TestVerifiedCompletionSurvivesLaterHoldAndRecovery(t *testing.T) {
	for _, crash := range []bool{false, true} {
		t.Run(fmt.Sprintf("crash-%t", crash), func(t *testing.T) {
			store, err := ledger.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			writer := &holdBeforeOutcomeLedger{SQLite: store, publicationEventType: "TASK_VERIFIED_COMPLETE", beforePublication: func() {
				for version, frozen := range []bool{true, false} {
					state := struct {
						OrganizationID core.ID   `json:"organization_id"`
						Frozen         bool      `json:"frozen"`
						UpdatedAt      time.Time `json:"updated_at"`
					}{"org-a", frozen, time.Now().UTC()}
					if err := store.AppendRecord(t.Context(), "org-a", "FREEZE_SET", "user-1", "verified-hold", nil, nil, "organization_freeze", "org-a", version+1, state); err != nil {
						t.Fatal(err)
					}
				}
			}}
			gateway := events.NewGateway(writer)
			if crash {
				gateway = events.NewGateway(&failOnceProjectionEvent{SQLite: store, eventType: "TASK_VERIFIED_COMPLETE"})
			}
			service := New(gateway)
			_, submitErr := service.Submit(t.Context(), Submit{RequestID: "verified-hold", OrganizationID: "org-a", Statement: "echo verified", Kind: core.ExecutionDeterministic})
			if crash {
				if submitErr == nil {
					t.Fatal("missing crash")
				}
				writer.beforePublication()
			} else if submitErr != nil {
				t.Fatal(submitErr)
			}
			service = New(events.NewGateway(store))
			service.deterministic = holdDuringHandler{freeze: func() { t.Fatal("verified execution dispatched again during recovery") }}
			for range 2 {
				if _, err := service.Recover(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			snapshot, err := service.state.Load(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			for _, task := range snapshot.Tasks {
				if task.Value.Status != core.TaskCompleted {
					t.Fatalf("verified execution lost completion: %s", task.Value.Status)
				}
			}
			stream, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if countEventType(stream, "COMPLETION_VERIFIED") != 1 || countEventType(stream, "TASK_EXECUTION_SUSPENDED") != 0 {
				t.Fatal("completed execution was repeated or suspended")
			}
		})
	}
}

func (h holdDuringHandler) Execute(ctx context.Context, task core.Task, manifest core.ExecutionContextManifest) (execution.Result, error) {
	h.freeze()
	// Return a superficially valid success despite cancellation. The runtime
	// must independently reject it as task completion evidence.
	return (execution.Deterministic{}).Execute(ctx, task, manifest)
}

func TestIndependentReviewSurvivesLaterReleasedHold(t *testing.T) {
	for _, decision := range []completion.ReviewDecision{completion.ReviewApprove, completion.ReviewReject} {
		t.Run(string(decision), func(t *testing.T) {
			ctx := t.Context()
			store, err := ledger.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			service := NewWithModel(events.NewGateway(store), describedModel{})
			submitted, err := service.Submit(ctx, Submit{RequestID: "review-hold", OrganizationID: "org-1", Statement: "summarize", Kind: core.ExecutionAgent})
			if err != nil {
				t.Fatal(err)
			}
			view, found, err := service.CompletionReview(ctx, "org-1", string(submitted.Task.ID))
			if err != nil || !found {
				t.Fatalf("review: %v %v", found, err)
			}
			for index, frozen := range []bool{true, false} {
				state := struct {
					OrganizationID core.ID   `json:"organization_id"`
					Frozen         bool      `json:"frozen"`
					UpdatedAt      time.Time `json:"updated_at"`
				}{OrganizationID: "org-1", Frozen: frozen, UpdatedAt: time.Now().UTC()}
				if err := store.AppendRecord(ctx, "org-1", "FREEZE_SET", "user-1", "review-hold", nil, nil, "organization_freeze", "org-1", index+1, state); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := service.ReviewCompletion(ctx, reviewInput(view, decision, "Reviewed candidate")); err != nil {
				t.Fatal(err)
			}
			if _, err := service.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			snapshot, err := projections.New(events.NewGateway(store)).Load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			want := core.TaskCompleted
			if decision == completion.ReviewReject {
				want = core.TaskFailed
			}
			if got := snapshot.Tasks[submitted.Task.ID].Value.Status; got != want {
				t.Fatalf("status=%s want=%s", got, want)
			}
		})
	}
}

func TestFrozenFailurePropagationDoesNotBlockOtherTenants(t *testing.T) {
	for _, eventType := range []string{"TASK_DEPENDENCY_FAILED", "TASK_WORK_FAILED"} {
		t.Run(eventType, func(t *testing.T) {
			ctx := t.Context()
			store, err := ledger.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			intercepted := &failOnceProjectionEvent{SQLite: store, eventType: eventType}
			model := &organizationLoopModel{plan: `{"tasks":[{"key":"a-fail","description":"first work","execution_kind":"AGENT","model_inference_policy":"REQUIRED","depends_on":[]},{"key":"z-unused","description":"unnecessary work","execution_kind":"AGENT","model_inference_policy":"REQUIRED","depends_on":[]}]}`}
			service := NewWithModelAndPlanner(events.NewGateway(intercepted), &failingExecutionModel{}, newOrganizationPlanner(t, model))
			if _, err := service.Submit(ctx, Submit{RequestID: "held-failure", OrganizationID: "org-1", Statement: "prepare a briefing", Kind: core.ExecutionAgent}); err == nil || !intercepted.failed {
				t.Fatalf("missing injected propagation failure: %v", err)
			}
			state := struct {
				OrganizationID core.ID   `json:"organization_id"`
				Frozen         bool      `json:"frozen"`
				UpdatedAt      time.Time `json:"updated_at"`
			}{"org-1", true, time.Now().UTC()}
			if err := store.AppendRecord(ctx, "org-1", "FREEZE_SET", "user-1", "held-failure", nil, nil, "organization_freeze", "org-1", 1, state); err != nil {
				t.Fatal(err)
			}
			other := New(events.NewGateway(store))
			result, err := other.Submit(ctx, Submit{RequestID: "unrelated", OrganizationID: "org-2", Statement: "echo independent", Kind: core.ExecutionDeterministic})
			if err != nil || result.Task.Status != core.TaskCompleted {
				t.Fatalf("unrelated task=%+v err=%v", result.Task, err)
			}
			if _, err := other.Recover(ctx); err != nil {
				t.Fatalf("frozen propagation blocked recovery: %v", err)
			}
		})
	}
}

type heldPlanningPlanner struct {
	failingPlanningPlanner
	freeze func()
}

func (p *heldPlanningPlanner) Build(ctx context.Context, input planning.Input, kind core.ExecutionKind) (planning.Result, error) {
	result, _ := p.failingPlanningPlanner.Build(ctx, input, kind)
	p.freeze()
	return result, core.ErrOrganizationFrozen
}
func TestHeldPlanningRemainsActiveThroughReleasedRecovery(t *testing.T) {
	ctx := t.Context()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	freeze := struct {
		OrganizationID core.ID   `json:"organization_id"`
		Frozen         bool      `json:"frozen"`
		UpdatedAt      time.Time `json:"updated_at"`
	}{"org-1", true, time.Now().UTC()}
	planner := &heldPlanningPlanner{freeze: func() {
		if err := store.AppendRecord(ctx, "org-1", "FREEZE_SET", "user-1", "held-planning", nil, nil, "organization_freeze", "org-1", 1, freeze); err != nil {
			t.Fatal(err)
		}
	}}
	service := NewWithModelAndPlanner(events.NewGateway(store), execution.FakeModel{}, planner)
	submission := Submit{RequestID: "held-planning", OrganizationID: "org-1", Statement: "perform adaptive work", Kind: core.ExecutionAgent}
	if _, err := service.Submit(ctx, submission); !errors.Is(err, core.ErrOrganizationFrozen) {
		t.Fatalf("planning error=%v", err)
	}
	freeze.Frozen = false
	freeze.UpdatedAt = time.Now().UTC()
	if err := store.AppendRecord(ctx, "org-1", "FREEZE_SET", "user-1", "held-planning", nil, nil, "organization_freeze", "org-1", 2, freeze); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := service.Recover(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := service.Submit(ctx, submission); !errors.Is(err, core.ErrOrganizationFrozen) {
			t.Fatalf("planning retry error=%v", err)
		}
	}
	snapshot, err := service.state.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, work := range snapshot.Works {
		if work.Value.Status != core.WorkActive {
			t.Fatalf("held planning became terminal: %+v", work)
		}
	}
	stream, err := service.ExternalEvents(ctx, "org-1", "held-planning")
	if err != nil {
		t.Fatal(err)
	}
	if planner.calls != 1 || countEventType(stream, "INFERENCE_USAGE_RECORDED") != 1 || countEventType(stream, "WORK_PLANNING_FAILED") != 0 || len(snapshot.Tasks) != 0 {
		t.Fatalf("held planning replayed, lost usage or materialized: calls=%d", planner.calls)
	}
}

func TestCompletionReviewCannotResumeSuspendedExecution(t *testing.T) {
	ctx := t.Context()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	freeze := struct {
		OrganizationID core.ID   `json:"organization_id"`
		Frozen         bool      `json:"frozen"`
		UpdatedAt      time.Time `json:"updated_at"`
	}{"org-a", true, time.Now().UTC()}
	intercepted := &holdBeforeOutcomeLedger{SQLite: store, publicationEventType: "TASK_BLOCKED", beforePublication: func() {
		if err := store.AppendRecord(ctx, "org-a", "FREEZE_SET", "user-1", "held-review", nil, nil, "organization_freeze", "org-a", 1, freeze); err != nil {
			t.Fatal(err)
		}
	}}
	service := NewWithModel(events.NewGateway(intercepted), describedModel{})
	submitted, err := service.Submit(ctx, Submit{RequestID: "held-review", OrganizationID: "org-a", Statement: "prepare a note", Kind: core.ExecutionAgent})
	if !errors.Is(err, core.ErrOrganizationFrozen) {
		t.Fatalf("expected interrupted execution: %v", err)
	}
	if submitted.Task.Status != core.TaskBlocked {
		t.Fatalf("task=%+v", submitted.Task)
	}
	requests, _, err := completionReviewRecords(submitted.Events)
	if err != nil || len(requests) != 1 {
		t.Fatalf("pending request missing: %v", err)
	}
	var request completion.ReviewRequest
	for _, r := range requests {
		request = r
	}
	for _, released := range []bool{false, true} {
		if released {
			freeze.Frozen = false
			freeze.UpdatedAt = time.Now().UTC()
			if err := store.AppendRecord(ctx, "org-a", "FREEZE_SET", "user-1", "held-review", nil, nil, "organization_freeze", "org-a", 2, freeze); err != nil {
				t.Fatal(err)
			}
		}
		if page, err := service.PendingCompletionReviews(ctx, "org-a", "", 10); err != nil || len(page.Reviews) != 0 {
			t.Fatalf("suspension exposed ordinary review: page=%+v err=%v", page, err)
		}
		if _, err := service.ReviewCompletion(ctx, reviewInput(CompletionReviewView{Request: request}, completion.ReviewRevise, "try again")); err == nil {
			t.Fatal("ordinary review resumed suspended task")
		}
		snapshot, err := service.state.Load(ctx)
		if err != nil {
			t.Fatal(err)
		}
		state := snapshot.Tasks[submitted.Task.ID]
		task := state.Value
		task.Status = core.TaskPending
		if err := service.state.SaveTask(ctx, "org-a", "TASK_RESUMED", "runtime", state.CorrelationID, state.Version+1, task, nil); !errors.Is(err, core.ErrOrganizationFrozen) {
			t.Fatalf("direct resumption bypassed suspension: %v", err)
		}
		if _, err := service.Recover(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReviewRequestSurvivesCrashAndLaterReleasedHold(t *testing.T) {
	ctx := t.Context()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	intercepted := &failOnceProjectionEvent{SQLite: store, eventType: "TASK_BLOCKED"}
	service := NewWithModel(events.NewGateway(intercepted), describedModel{})
	if _, err := service.Submit(ctx, Submit{RequestID: "review-crash", OrganizationID: "org-1", Statement: "prepare a note", Kind: core.ExecutionAgent}); !errors.Is(err, errProjectionWrite) {
		t.Fatalf("crash missing: %v", err)
	}
	for index, frozen := range []bool{true, false} {
		state := struct {
			OrganizationID core.ID   `json:"organization_id"`
			Frozen         bool      `json:"frozen"`
			UpdatedAt      time.Time `json:"updated_at"`
		}{"org-1", frozen, time.Now().UTC()}
		if err := store.AppendRecord(ctx, "org-1", "FREEZE_SET", "user-1", "review-crash", nil, nil, "organization_freeze", "org-1", index+1, state); err != nil {
			t.Fatal(err)
		}
	}
	recovered := NewWithModel(events.NewGateway(store), describedModel{})
	if _, err := recovered.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	page, err := recovered.PendingCompletionReviews(ctx, "org-1", "", 10)
	if err != nil || len(page.Reviews) != 1 {
		t.Fatalf("finished candidate lost its review: %+v %v", page, err)
	}
	if _, err := recovered.ReviewCompletion(ctx, reviewInput(page.Reviews[0], completion.ReviewApprove, "")); err != nil {
		t.Fatal(err)
	}
}

func TestPreManifestPlanningHoldSurvivesRecovery(t *testing.T) {
	ctx := t.Context()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	intercepted := &holdBeforeOutcomeLedger{SQLite: store, publicationEventType: "PLANNING_CONTEXT_MANIFESTED", beforePublication: func() {
		state := struct {
			OrganizationID core.ID   `json:"organization_id"`
			Frozen         bool      `json:"frozen"`
			UpdatedAt      time.Time `json:"updated_at"`
		}{"org-a", true, time.Now().UTC()}
		if err := store.AppendRecord(ctx, "org-a", "FREEZE_SET", "user-1", "pre-manifest", nil, nil, "organization_freeze", "org-a", 1, state); err != nil {
			t.Fatal(err)
		}
	}}
	planner := &failingPlanningPlanner{}
	service := NewWithModelAndPlanner(events.NewGateway(intercepted), execution.FakeModel{}, planner)
	if _, err := service.Submit(ctx, Submit{RequestID: "pre-manifest", OrganizationID: "org-a", Statement: "prepare a note", Kind: core.ExecutionAgent}); !errors.Is(err, core.ErrOrganizationFrozen) {
		t.Fatalf("missing hold: %v", err)
	}
	for range 2 {
		if _, err := service.Recover(ctx); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := service.state.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range snapshot.Works {
		if state.Value.Status != core.WorkActive {
			t.Fatal("pre-manifest held Work became terminal")
		}
	}
	if planner.calls != 0 {
		t.Fatal("held planner was invoked")
	}
	other := New(events.NewGateway(store))
	if result, err := other.Submit(ctx, Submit{RequestID: "other-work", OrganizationID: "org-b", Statement: "echo independent", Kind: core.ExecutionDeterministic}); err != nil || result.Task.Status != core.TaskCompleted {
		t.Fatalf("other tenant blocked: %v", err)
	}
}

func TestFinishedPlanningFailureSurvivesLaterReleasedHold(t *testing.T) {
	ctx := t.Context()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	intercepted := &failOnceProjectionEvent{SQLite: store, eventType: "WORK_PLANNING_FAILED"}
	planner := &failingPlanningPlanner{}
	service := NewWithModelAndPlanner(events.NewGateway(intercepted), execution.FakeModel{}, planner)
	if _, err := service.Submit(ctx, Submit{RequestID: "finished-planning", OrganizationID: "org-1", Statement: "prepare a note", Kind: core.ExecutionAgent}); err == nil || !intercepted.failed {
		t.Fatalf("missing failure projection crash: %v", err)
	}
	for index, frozen := range []bool{true, false} {
		state := struct {
			OrganizationID core.ID   `json:"organization_id"`
			Frozen         bool      `json:"frozen"`
			UpdatedAt      time.Time `json:"updated_at"`
		}{"org-1", frozen, time.Now().UTC()}
		if err := store.AppendRecord(ctx, "org-1", "FREEZE_SET", "user-1", "finished-planning", nil, nil, "organization_freeze", "org-1", index+1, state); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if _, err := service.Recover(ctx); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := service.state.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Works) != 1 {
		t.Fatalf("unexpected work count: %d", len(snapshot.Works))
	}
	for _, state := range snapshot.Works {
		if state.Value.Status != core.WorkFailed {
			t.Fatalf("finished planning failure remained active: %+v", state)
		}
	}
	if planner.calls != 1 {
		t.Fatal("finished planner was replayed")
	}
}

type unavailablePlanningPlanner struct{ failingPlanningPlanner }

type holdBeforePlanningFailureLedger struct {
	*ledger.SQLite
	before func()
}

func (l *holdBeforePlanningFailureLedger) Append(ctx context.Context, draft events.TrustedDraft) (events.Event, error) {
	if draft.EventType == "PLANNING_FAILED" && l.before != nil {
		before := l.before
		l.before = nil
		before()
		ctx = context.Background() // The durable writer must not depend on a live generation.
	}
	return l.SQLite.Append(ctx, draft)
}

func TestHoldBeforePlanningFailurePreventsOrdinaryFinish(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	writer := &holdBeforePlanningFailureLedger{SQLite: store, before: func() {
		for index, frozen := range []bool{true, false} {
			state := struct {
				OrganizationID core.ID   `json:"organization_id"`
				Frozen         bool      `json:"frozen"`
				UpdatedAt      time.Time `json:"updated_at"`
			}{"org-1", frozen, time.Now().UTC()}
			if err := store.AppendRecord(t.Context(), "org-1", "FREEZE_SET", "user-1", "planning-finish-race", nil, nil, "organization_freeze", "org-1", index+1, state); err != nil {
				t.Fatal(err)
			}
		}
	}}
	planner := &failingPlanningPlanner{}
	service := NewWithModelAndPlanner(events.NewGateway(writer), execution.FakeModel{}, planner)
	in := Submit{RequestID: "planning-finish-race", OrganizationID: "org-1", Statement: "prepare a note", Kind: core.ExecutionAgent}
	if _, err := service.Submit(t.Context(), in); !errors.Is(err, core.ErrOrganizationFrozen) {
		t.Fatalf("held ordinary failure admitted: %v", err)
	}
	service = NewWithModelAndPlanner(events.NewGateway(store), execution.FakeModel{}, planner)
	for range 2 {
		if _, err := service.Recover(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := service.state.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, work := range snapshot.Works {
		if work.Value.Status != core.WorkActive {
			t.Fatal("held failure terminalized Work")
		}
	}
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if planner.calls != 1 || countEventType(stream, "PLANNING_FAILED") != 0 || countEventType(stream, "WORK_PLANNING_FAILED") != 0 {
		t.Fatal("held planning replayed or published ordinary failure")
	}
}

func (p *unavailablePlanningPlanner) Build(context.Context, planning.Input, core.ExecutionKind) (planning.Result, error) {
	p.calls++
	return planning.Result{}, execution.SafeModelError(execution.ModelCallFailed, core.ErrContainmentUnavailable)
}

type unavailableUsagePlanner struct{ failingPlanningPlanner }

func (p *unavailableUsagePlanner) Build(ctx context.Context, in planning.Input, kind core.ExecutionKind) (planning.Result, error) {
	result, _ := p.failingPlanningPlanner.Build(ctx, in, kind)
	return result, core.ErrContainmentUnavailable
}

type failUsageLedger struct{ *ledger.SQLite }

func (l failUsageLedger) Append(ctx context.Context, draft events.TrustedDraft) (events.Event, error) {
	if draft.EventType == "INFERENCE_USAGE_RECORDED" {
		return events.Event{}, errors.New("injected usage persistence failure")
	}
	return l.SQLite.Append(ctx, draft)
}

func TestPlanningUsageFailurePreservesSafetyInterruption(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	planner := &unavailableUsagePlanner{}
	service := NewWithModelAndPlanner(events.NewGateway(failUsageLedger{store}), execution.FakeModel{}, planner)
	if _, err := service.Submit(t.Context(), Submit{RequestID: "usage-unavailable", OrganizationID: "org-1", Statement: "prepare a note", Kind: core.ExecutionAgent}); !errors.Is(err, core.ErrContainmentUnavailable) {
		t.Fatalf("usage error masked safety interruption: %v", err)
	}
	for range 2 {
		if _, err := service.Recover(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := service.state.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range snapshot.Works {
		if state.Value.Status != core.WorkActive {
			t.Fatal("usage error terminalized interrupted planning")
		}
	}
	if planner.calls != 1 {
		t.Fatal("interrupted planning replayed")
	}
}

type unavailableOrganizationLedger struct{ *ledger.SQLite }

func TestIntentBindingRejectionDistinguishesUnavailableState(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := New(events.NewGateway(store))
	check := func(wantRejected bool) {
		t.Helper()
		goalErr := service.ValidateSelectedGoal(t.Context(), "org-1", "goal-missing")
		_, replacementErr := service.ResolveReplacementGoal(t.Context(), "org-1", "work-missing")
		for _, err := range []error{goalErr, replacementErr} {
			if err == nil || errors.Is(err, ErrIntentBindingRejected) != wantRejected {
				t.Fatalf("incorrect binding failure classification: %v", err)
			}
		}
	}
	check(true)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	check(false)
}

func (l unavailableOrganizationLedger) BeginExecutionContext(ctx context.Context, organization string) (context.Context, func(), error) {
	if organization == "org-a" {
		return nil, nil, core.ErrContainmentUnavailable
	}
	return l.SQLite.BeginExecutionContext(ctx, organization)
}

func TestInitialContainmentFailureDefersOnlyAffectedOrganization(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := New(events.NewGateway(unavailableOrganizationLedger{store}))
	held, err := service.Submit(t.Context(), Submit{RequestID: "deferred", OrganizationID: "org-a", Statement: "echo pending", Kind: core.ExecutionDeterministic})
	if err != nil || held.Task.Status != core.TaskPending {
		t.Fatalf("unavailable authority did not preserve pending work: %+v %v", held.Task, err)
	}
	other, err := service.Submit(t.Context(), Submit{RequestID: "independent", OrganizationID: "org-b", Statement: "echo ready", Kind: core.ExecutionDeterministic})
	if err != nil || other.Task.Status != core.TaskCompleted {
		t.Fatalf("unavailable authority blocked other organization: %v", err)
	}
	for range 2 {
		if _, err := service.Recover(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
}

type lostPlanningSuspensionLedger struct{ *ledger.SQLite }

func (l lostPlanningSuspensionLedger) Append(ctx context.Context, draft events.TrustedDraft) (events.Event, error) {
	if draft.EventType == "PLANNING_CONTAINMENT_SUSPENDED" {
		return events.Event{}, errSuspensionCrash
	}
	return l.SQLite.Append(ctx, draft)
}

func TestUnavailablePlanningContainmentRemainsActive(t *testing.T) {
	for _, lostMarker := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost-marker-%t", lostMarker), func(t *testing.T) { testUnavailablePlanningContainmentRemainsActive(t, lostMarker) })
	}
}

func testUnavailablePlanningContainmentRemainsActive(t *testing.T, lostMarker bool) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	planner := &unavailablePlanningPlanner{}
	gateway := events.NewGateway(store)
	if lostMarker {
		gateway = events.NewGateway(lostPlanningSuspensionLedger{store})
	}
	service := NewWithModelAndPlanner(gateway, execution.FakeModel{}, planner)
	if _, err := service.Submit(t.Context(), Submit{RequestID: "unavailable-planning", OrganizationID: "org-1", Statement: "prepare a note", Kind: core.ExecutionAgent}); !errors.Is(err, core.ErrContainmentUnavailable) {
		t.Fatalf("containment interruption lost: %v", err)
	}
	// Reconstruct the service without the failing writer or original call context.
	service = NewWithModelAndPlanner(events.NewGateway(store), execution.FakeModel{}, planner)
	for range 2 {
		if _, err := service.Recover(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := service.state.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Works) != 1 {
		t.Fatalf("unexpected work count: %d", len(snapshot.Works))
	}
	for _, state := range snapshot.Works {
		if state.Value.Status != core.WorkActive {
			t.Fatal("unavailable containment terminalized planning work")
		}
	}
	if planner.calls != 1 {
		t.Fatal("suspended planner was replayed")
	}
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range stream {
		if event.EventType == "PLANNING_FAILED" || event.EventType == "WORK_PLANNING_FAILED" {
			t.Fatal("safety interruption published planning failure")
		}
	}
	if lostMarker && countEventType(stream, "PLANNING_CONTAINMENT_SUSPENDED") != 0 {
		t.Fatal("lost-marker scenario unexpectedly retained a suspension marker")
	}
	if _, err := service.Submit(t.Context(), Submit{RequestID: "unavailable-planning", OrganizationID: "org-1", Statement: "prepare a note", Kind: core.ExecutionAgent}); !errors.Is(err, core.ErrContainmentUnavailable) || planner.calls != 1 {
		t.Fatalf("retry failed to retain unresolved admission: calls=%d err=%v", planner.calls, err)
	}
}

type unavailableTaskModel struct{}

func (unavailableTaskModel) Name() string { return describedModel{}.Name() }
func (unavailableTaskModel) Descriptor() execution.ModelDescriptor {
	return describedModel{}.Descriptor()
}

func (unavailableTaskModel) Complete(context.Context, string) (execution.ModelResponse, error) {
	return execution.ModelResponse{}, execution.SafeModelError(execution.ModelCallFailed, core.ErrContainmentUnavailable)
}

func (m unavailableTaskModel) CompleteRequest(ctx context.Context, _ modelinput.Request) (execution.ModelResponse, error) {
	return m.Complete(ctx, "")
}

func TestInnerInferenceContainmentFailureSuspendsTask(t *testing.T) {
	for _, crash := range []bool{false, true} {
		t.Run(fmt.Sprintf("crash-%t", crash), func(t *testing.T) { testInnerInferenceContainmentFailureSuspendsTask(t, crash) })
	}
}

func testInnerInferenceContainmentFailureSuspendsTask(t *testing.T, crash bool) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	intercepted := &failOnceProjectionEvent{SQLite: store}
	if crash {
		intercepted.eventType = "TASK_EXECUTION_SUSPENDED"
	}
	service := NewWithModel(events.NewGateway(intercepted), unavailableTaskModel{})
	result, err := service.Submit(t.Context(), Submit{RequestID: "inner-unavailable", OrganizationID: "org-1", Statement: "prepare a note", Kind: core.ExecutionAgent})
	if crash && (err == nil || !intercepted.failed) {
		t.Fatalf("missing suspension crash: %v", err)
	}
	if !crash && !errors.Is(err, core.ErrContainmentUnavailable) {
		t.Fatalf("missing containment interruption: %v", err)
	}
	if !crash && result.Task.Status != core.TaskBlocked {
		t.Fatalf("inner containment failure terminalized task: %s", result.Task.Status)
	}
	for range 2 {
		if _, err := service.Recover(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if countEventType(stream, "TASK_EXECUTION_SUSPENDED") != 1 || countEventType(stream, "COMPLETION_REJECTED") != 0 || countEventType(stream, "TASK_RESULT_RECORDED") != 0 {
		t.Fatal("inner containment failure published an ordinary result")
	}
	snapshot, err := service.state.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range snapshot.Works {
		if state.Value.Status != core.WorkActive {
			t.Fatal("inner containment failure failed Work")
		}
	}
}

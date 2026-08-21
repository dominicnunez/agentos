package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/completion"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/lab"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/planning"
	"github.com/dominicnunez/agentos/internal/projections"
	"github.com/dominicnunez/agentos/internal/telemetry"
)

func seedTestAgents(t *testing.T, ctx context.Context, repository *projections.Repository, correlationID string, organizationID core.ID, descriptor execution.ModelDescriptor, agentIDs ...core.ID) []core.Agent {
	t.Helper()
	blueprint := core.AgentBlueprint{
		ID: core.ID("blueprint-test-" + string(organizationID)), OrganizationID: organizationID, Version: "blueprint-v1",
		Role: "test worker", OperatingInstructions: "perform bounded test work", RequiredCapabilityClasses: []string{}, Status: "ACTIVE",
	}
	profile := core.ExecutionProfile{
		ID: core.ID("profile-test-" + string(organizationID)), OrganizationID: organizationID, Version: descriptor.ExecutionProfileVersion,
		ModelProvider: descriptor.Provider, Model: descriptor.Model, PromptVersion: "v1", ToolRefs: []string{}, Status: "ACTIVE",
	}
	if err := repository.SaveAgentBlueprint(ctx, "AGENT_BLUEPRINT_CREATED", "runtime", correlationID, 1, blueprint, nil); err != nil {
		t.Fatal(err)
	}
	if err := repository.SaveExecutionProfile(ctx, "EXECUTION_PROFILE_CREATED", "runtime", correlationID, 1, profile, nil); err != nil {
		t.Fatal(err)
	}
	agents := make([]core.Agent, 0, len(agentIDs))
	for _, agentID := range agentIDs {
		agent := core.Agent{
			ID: agentID, OrganizationID: organizationID, BlueprintID: blueprint.ID, BlueprintVersion: blueprint.Version,
			ExecutionProfileID: profile.ID, ExecutionProfileVersion: profile.Version, RuntimeAdapter: "local", Status: "ACTIVE",
		}
		if err := repository.SaveAgent(ctx, "AGENT_CREATED", "runtime", correlationID, 1, agent, nil); err != nil {
			t.Fatal(err)
		}
		agents = append(agents, agent)
	}
	return agents
}

func testAgentConfig(agent core.Agent) *core.AgentConfig {
	return &core.AgentConfig{
		BlueprintID: agent.BlueprintID, BlueprintVersion: agent.BlueprintVersion,
		ProfileID: agent.ExecutionProfileID, ProfileVersion: agent.ExecutionProfileVersion,
		RuntimeAdapter: agent.RuntimeAdapter,
	}
}

func acceptedTestIntent(id, organizationID core.ID, objective string) core.Intent {
	return core.Intent{
		ID: id, OrganizationID: organizationID, OriginalInstruction: objective, NormalizedObjective: objective,
		AcceptedFingerprint: strings.Repeat("a", 64),
		CompletionCriteria:  []core.IntentValue{{Value: "the requested outcome is independently verified", Origin: "RUNTIME_TEST"}},
	}
}

func TestRecoveryRestoresDurableExperimentContainment(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	gateway := events.NewGateway(store)
	repository := projections.New(gateway)
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	if err := repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "org-1", 1, organization, nil); err != nil {
		t.Fatal(err)
	}
	const correlationID = "experiment-crash"
	const taskID = "task-" + correlationID
	const sourceMessageID = "message-1"
	if _, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "INTAKE_MESSAGE_RECORDED", SourceActorID: "user-1", TaskID: taskID, CorrelationID: correlationID,
		Payload: events.IntakeMessageRecordedPayload{MessageID: sourceMessageID, Text: "echo recovered", SourcePrincipalID: "user-1", SourcePrincipalKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", RequestedExecutionKind: core.ExecutionDeterministic},
	}); err != nil {
		t.Fatal(err)
	}
	value := core.IntentValue{Value: "recovered", Origin: "EXPLICIT", SourceMessageID: sourceMessageID}
	draft := core.IntentDraft{
		ID: "intent-" + correlationID, OrganizationID: organization.ID, Version: 1, Status: core.IntentStatusReadyForReview, Mode: core.IntentModeExperiment,
		RequestedExecutionKind: core.ExecutionDeterministic, Objective: "echo recovered", Context: []core.IntentValue{}, Deliverables: []core.IntentValue{value}, CompletionCriteria: []core.IntentValue{value}, Constraints: []core.IntentValue{}, ResolvedDecisions: []core.IntentDecision{}, ConsequenceCandidates: []string{}, MissingUserInputs: []core.IntentValue{}, CreatedAt: now,
	}
	draft.Fingerprint, err = core.FingerprintIntentDraft(draft)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_DRAFTED", SourceActorID: "runtime", TaskID: taskID, CorrelationID: correlationID, Payload: events.IntentDraftedPayload{SourceMessageID: sourceMessageID, Draft: draft, Reply: "Review this experiment."}}); err != nil {
		t.Fatal(err)
	}
	confirmation := events.IntentConfirmedPayload{IntentID: string(draft.ID), Version: 1, Fingerprint: draft.Fingerprint, ConfirmingActorID: "user-1", ConfirmingActorKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", MessageID: "confirmation-1"}
	if _, err := gateway.PublishIntentConfirmation(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CONFIRMED", SourceActorID: "user-1", TaskID: taskID, CorrelationID: correlationID, Payload: confirmation}, "", ""); err != nil {
		t.Fatal(err)
	}
	intent := core.Intent{
		ID: draft.ID, OrganizationID: organization.ID, OriginalInstruction: "echo recovered", NormalizedObjective: draft.Objective,
		SourcePrincipalID: "user-1", SourcePrincipalKind: core.PrincipalHuman, SourceChannel: "HUMAN_DIRECT", ExternalRequestID: correlationID, SourceMessageID: sourceMessageID,
		Deliverables: draft.Deliverables, CompletionCriteria: draft.CompletionCriteria, AcceptedFingerprint: draft.Fingerprint, CreatedAt: now,
	}
	work := core.Work{ID: "work-" + correlationID, IntentID: intent.ID, Objective: draft.Objective, Status: core.WorkActive, CreatedAt: now}
	if _, err := lab.New(gateway).StartSubmission(ctx, correlationID, intent, work, lab.DefaultSpec()); err != nil {
		t.Fatal(err)
	}
	result, err := New(gateway).Recover(ctx)
	if err != nil {
		t.Fatalf("recover durable experiment: %v", err)
	}
	if result.PlansMaterialized != 1 || result.TasksExecuted != 1 {
		t.Fatalf("recovery=%+v", result)
	}
	snapshot, err := repository.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	experiment := snapshot.Experiments[core.ID("experiment-"+string(work.ID))]
	if experiment.Value.Status != core.ExperimentCompleted || experiment.Value.TrustLabel != core.ExperimentTrustUnverified {
		t.Fatalf("recovered experiment=%+v", experiment.Value)
	}
	stream, err := gateway.Events(ctx, correlationID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range stream {
		if event.EventType == "WORK_PLANNING_FAILED" {
			t.Fatal("recovery discarded durable experimental containment")
		}
	}
}

func buildTestPlan(correlationID string, intent core.Intent, tasks ...core.Task) (core.Plan, error) {
	keys := make(map[core.ID]string, len(tasks))
	prefix := core.ID("task-" + correlationID)
	for _, task := range tasks {
		switch {
		case task.ID == prefix:
			keys[task.ID] = "root"
		case strings.HasPrefix(string(task.ID), string(prefix)+"-"):
			keys[task.ID] = strings.TrimPrefix(string(task.ID), string(prefix)+"-")
		default:
			return core.Plan{}, errors.New("test Task ID does not follow the durable Plan mapping")
		}
	}
	planned := make([]core.PlanTask, 0, len(tasks))
	for _, task := range tasks {
		dependencies := make([]string, 0, len(task.DependsOn))
		for _, dependencyID := range task.DependsOn {
			key, ok := keys[dependencyID]
			if !ok {
				return core.Plan{}, errors.New("test Task dependency is outside the Plan")
			}
			dependencies = append(dependencies, key)
		}
		planned = append(planned, core.PlanTask{
			Key: keys[task.ID], Description: task.Description, ExecutionKind: task.ExecutionKind,
			ModelInferencePolicy: task.ModelInferencePolicy, DependsOn: dependencies,
		})
	}
	plan := core.Plan{
		ID: core.ID("plan-" + correlationID), IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint,
		Version: 1, Tasks: planned, CreatedAt: time.Unix(1, 0).UTC(),
	}
	fingerprint, err := core.FingerprintPlan(plan)
	if err != nil {
		return core.Plan{}, err
	}
	plan.Fingerprint = fingerprint
	return plan, nil
}

func saveTestPlan(ctx context.Context, gateway *events.Gateway, correlationID string, intent core.Intent, tasks ...core.Task) error {
	plan, err := buildTestPlan(correlationID, intent, tasks...)
	if err != nil {
		return err
	}
	_, err = gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: string(intent.OrganizationID), EventType: "PLAN_CREATED", SourceActorID: "runtime",
		TaskID: "task-" + correlationID, Payload: plan, CorrelationID: correlationID,
	})
	return err
}

func saveTestTaskGraph(ctx context.Context, repository *projections.Repository, organizationID core.ID, correlationID string, intent core.Intent, work core.Work, tasks ...core.Task) error {
	if err := repository.SaveIntent(ctx, "INTENT_CREATED", "runtime", correlationID, 1, intent, nil); err != nil {
		return err
	}
	if err := repository.SaveWork(ctx, organizationID, "WORK_CREATED", "runtime", correlationID, 1, work, nil); err != nil {
		return err
	}
	return repository.SaveNewTasks(ctx, organizationID, "runtime", correlationID, tasks)
}

func bindTestAgentExecutionBriefs(t *testing.T, correlationID string, intent core.Intent, tasks ...*core.Task) {
	t.Helper()
	values := make([]core.Task, 0, len(tasks))
	for _, task := range tasks {
		values = append(values, *task)
	}
	plan, err := buildTestPlan(correlationID, intent, values...)
	if err != nil {
		t.Fatal(err)
	}
	for index, task := range tasks {
		if task.ExecutionKind != core.ExecutionAgent {
			continue
		}
		task.ExecutionBrief, err = core.AgentTaskExecutionBrief(intent, plan.Tasks[index], plan.Fingerprint)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func saveTestVerifiedTask(ctx context.Context, gateway *events.Gateway, repository *projections.Repository, organizationID core.ID, correlationID string, state projections.Versioned[core.Task]) error {
	task := state.Value
	executionInput := ""
	var snapshot projections.Snapshot
	if task.ExecutionKind == core.ExecutionAgent {
		var err error
		snapshot, err = repository.Load(ctx)
		if err != nil {
			return err
		}
		blueprint, ok := snapshot.AgentBlueprints[task.AgentConfig.BlueprintID]
		if !ok {
			return fmt.Errorf("test Agent blueprint is unavailable")
		}
		_, executionInput, err = core.MaterializeAgentExecutionInput(core.AgentExecutionInputContext{Blueprint: blueprint.Value, Task: task})
		if err != nil {
			return err
		}
	}
	task.Status = core.TaskRunning
	startVersion := state.Version + 1
	if task.ExecutionKind == core.ExecutionAgent {
		_, selections, err := repository.StartAgentExecution(ctx, organizationID, correlationID, startVersion, task, "", nil, nil, actionBoundaryRoutes(snapshot, task), func([]events.InboxSelection) error { return nil })
		if err != nil {
			return err
		}
		for _, selection := range selections {
			if len(selection.Events) != 0 {
				return fmt.Errorf("test verified Agent unexpectedly selected inbox input")
			}
		}
	} else if _, err := repository.StartTaskExecution(ctx, organizationID, correlationID, startVersion, task, "", "", nil, nil); err != nil {
		return err
	}
	now := time.Now().UTC()
	toolID, observed := "builtin.echo", any(strings.TrimPrefix(task.Description, "echo "))
	if task.ExecutionKind == core.ExecutionAgent {
		toolID = "fake-model/v1"
		observed = "fake-model: " + executionInput
	}
	outcome := core.ToolOutcome{ToolInvocationID: core.ID("test-outcome-" + string(task.ID)), ToolID: toolID, ToolVersion: "v1", Status: core.OutcomeSucceeded, ObservedEffect: observed, PostconditionStatus: core.PostconditionVerified, Retryability: core.NotRetryable, StartedAt: now, FinishedAt: now}
	executionID := fmt.Sprintf("execution-%s-v%d", task.ID, startVersion)
	if task.ExecutionKind == core.ExecutionAgent {
		manifest := core.ExecutionContextManifest{
			ExecutionID: core.ID(executionID), AgentID: task.AssigneeID,
			AgentBlueprintVersion: task.AgentConfig.BlueprintVersion, ExecutionProfileVersion: task.AgentConfig.ProfileVersion,
			RuntimeAdapter: task.AgentConfig.RuntimeAdapter, Provider: "fake", Model: toolID,
			TaskID: task.ID, TaskContractVersion: task.TaskContractVersion, PromptVersion: "v1",
			PolicyVersion: "v1", ContextBuilderVersion: "v1",
			ExecutionInputSHA256: core.FingerprintExecutionInput(executionInput), CreatedAt: now,
		}
		if _, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
			OrganizationID: string(organizationID), EventType: "EXECUTION_CONTEXT_MANIFESTED",
			SourceExecutionID: executionID, TaskID: string(task.ID), Payload: manifest, CorrelationID: correlationID,
		}); err != nil {
			return err
		}
	}
	outcomeEvent, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: string(organizationID), EventType: "TOOL_OUTCOME_RECORDED", SourceActorID: "runtime",
		SourceExecutionID: executionID, TaskID: string(task.ID), Payload: outcome, CorrelationID: correlationID,
	})
	if err != nil {
		return err
	}
	summary, err := core.ToolOutcomeSummary(outcome)
	if err != nil {
		return err
	}
	resultDraft := events.Draft{
		EventType: "RESULT_PUBLISHED", TaskID: string(task.ID),
		Payload: events.ResultPublishedPayload{Summary: summary, ArtifactRefs: outcome.ArtifactRefs},
	}
	var resultEvent events.Event
	if task.ExecutionKind == core.ExecutionAgent {
		resultEvent, err = gateway.PublishAgentDraft(ctx, string(organizationID), string(task.AssigneeID), executionID, correlationID, resultDraft)
	} else {
		resultEvent, err = gateway.PublishTrusted(ctx, events.TrustedDraft{
			OrganizationID: string(organizationID), EventType: resultDraft.EventType, SourceActorID: "runtime",
			SourceExecutionID: executionID, TaskID: resultDraft.TaskID, Payload: resultDraft.Payload, CorrelationID: correlationID,
		})
	}
	if err != nil {
		return err
	}
	candidateDraft := events.Draft{
		EventType: "CANDIDATE_COMPLETE", TaskID: string(task.ID),
		Payload: events.CandidateCompletePayload{ToolInvocationID: string(outcome.ToolInvocationID), ResultEventID: resultEvent.EventID, ArtifactRefs: outcome.ArtifactRefs},
	}
	if task.ExecutionKind == core.ExecutionAgent {
		_, err = gateway.PublishAgentDraft(ctx, string(organizationID), string(task.AssigneeID), executionID, correlationID, candidateDraft)
	} else {
		_, err = gateway.PublishTrusted(ctx, events.TrustedDraft{
			OrganizationID: string(organizationID), EventType: candidateDraft.EventType, SourceActorID: "runtime",
			SourceExecutionID: executionID, TaskID: candidateDraft.TaskID, Payload: candidateDraft.Payload, CorrelationID: correlationID,
		})
	}
	if err != nil {
		return err
	}
	contract := core.VerifiedOutcomeCompletionContract(task.ID, startVersion)
	detail := completionDetail{Contract: contract, Result: completion.Result{Complete: true}, OutcomeEventRef: outcomeEvent.EventID}
	if _, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: string(organizationID), EventType: "COMPLETION_VERIFIED", SourceActorID: "runtime",
		SourceExecutionID: executionID, TaskID: string(task.ID), Payload: detail, CorrelationID: correlationID,
	}); err != nil {
		return err
	}
	task.Status = core.TaskCompleted
	return repository.SaveTask(ctx, organizationID, "TASK_VERIFIED_COMPLETE", "runtime", correlationID, state.Version+2, task, detail)
}

func seedTestGoal(t *testing.T, ctx context.Context, repository *projections.Repository, organizationID, missionID, goalID core.ID, goalStatus core.GoalStatus) {
	t.Helper()
	now := time.Now().UTC()
	organization := core.Organization{ID: organizationID, Name: string(organizationID), PolicyVersion: "v1", CreatedAt: now}
	if err := repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "seed-"+string(organizationID), 1, organization, nil); err != nil {
		t.Fatal(err)
	}
	mission := core.Mission{ID: missionID, OrganizationID: organizationID, Statement: "durable test direction", Status: core.MissionActive, CreatedAt: now}
	if err := repository.SaveMission(ctx, "MISSION_CREATED", "runtime", "seed-"+string(missionID), 1, mission, nil); err != nil {
		t.Fatal(err)
	}
	goal := core.Goal{
		ID: goalID, OrganizationID: organizationID, MissionID: missionID, Objective: "measurable test outcome",
		Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "verified result", Origin: "RUNTIME_TEST"}}, Status: goalStatus, CreatedAt: now,
	}
	if err := repository.SaveGoal(ctx, "GOAL_CREATED", "runtime", "seed-"+string(goalID), 1, goal, nil); err != nil {
		t.Fatal(err)
	}
}

func confirmedGoalSubmit(t *testing.T, ctx context.Context, gateway *events.Gateway, requestID, organizationID string, goalID core.ID, statement string, kind core.ExecutionKind) Submit {
	t.Helper()
	correlationID, err := gateway.ReserveExternalWork(ctx, organizationID, requestID)
	if err != nil {
		t.Fatal(err)
	}
	sourceMessageID := "message-" + requestID
	originalInstruction := statement + " under " + string(goalID)
	in := Submit{
		RequestID: requestID, OrganizationID: organizationID, GoalID: goalID, Statement: originalInstruction, Kind: kind,
		MessageID: sourceMessageID, SourcePrincipalID: "user-1", SourcePrincipalKind: core.PrincipalHuman, SourceChannel: "HUMAN_DIRECT",
	}
	draft, err := acceptedDraftForSubmission(in, correlationID)
	if err != nil {
		t.Fatal(err)
	}
	draft.ID = core.ID("intent-" + correlationID)
	draft.Goal = &core.IntentValue{Value: string(goalID), Origin: "EXPLICIT", SourceMessageID: sourceMessageID}
	draft.Objective = statement
	draft.Fingerprint, err = core.FingerprintIntentDraft(draft)
	if err != nil {
		t.Fatal(err)
	}
	in.NormalizedIntent = &draft
	if _, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: organizationID, EventType: "INTAKE_MESSAGE_RECORDED", SourceActorID: "user-1", TaskID: "task-" + correlationID, CorrelationID: correlationID,
		Payload: events.IntakeMessageRecordedPayload{MessageID: sourceMessageID, Text: originalInstruction, SourcePrincipalID: "user-1", SourcePrincipalKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", RequestedExecutionKind: kind},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: organizationID, EventType: "INTENT_DRAFTED", SourceActorID: "runtime", TaskID: "task-" + correlationID, CorrelationID: correlationID,
		Payload: events.IntentDraftedPayload{SourceMessageID: sourceMessageID, Draft: draft, Reply: "Review the proposed intent before work begins."},
	}); err != nil {
		t.Fatal(err)
	}
	payload := events.IntentConfirmedPayload{
		IntentID: string(draft.ID), GoalID: string(goalID), Version: draft.Version, Fingerprint: draft.Fingerprint,
		ConfirmingActorID: "user-1", ConfirmingActorKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", MessageID: "confirmation-" + requestID,
	}
	if _, err := gateway.PublishIntentConfirmation(ctx, events.TrustedDraft{
		OrganizationID: organizationID, EventType: "INTENT_CONFIRMED", SourceActorID: "user-1", TaskID: "task-" + correlationID, CorrelationID: correlationID, Payload: payload,
	}, goalID, ""); err != nil {
		t.Fatal(err)
	}
	return in
}

type strategicDriftPlanner struct{ revise func() }

func (strategicDriftPlanner) Descriptor() (planning.Descriptor, bool) {
	return planning.Descriptor{}, false
}

func (p strategicDriftPlanner) Build(_ context.Context, input planning.Input, kind core.ExecutionKind) (planning.Result, error) {
	if p.revise != nil {
		p.revise()
	}
	return planning.Result{Tasks: []core.PlanTask{{
		Key: "root", Description: input.Intent.Objective, ExecutionKind: kind,
		ModelInferencePolicy: core.InferenceAllowed, DependsOn: []string{},
	}}}, nil
}

func TestVerticalSlice(t *testing.T) {
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := l.Close(); err != nil {
			t.Errorf("close ledger: %v", err)
		}
	})
	s := New(events.NewGateway(l))
	r, err := s.Submit(context.Background(), Submit{RequestID: "r1", OrganizationID: "o1", Statement: "echo hello", Kind: core.ExecutionDeterministic})
	if err != nil {
		t.Fatal(err)
	}
	if !r.Completion.Complete || r.Task.Status != core.TaskCompleted || r.Work.Status != "COMPLETED" {
		t.Fatalf("unexpected result: %#v", r)
	}
	assertEventOrder(t, r.Events, "TASK_CREATED", "EXECUTION_STARTED", "TOOL_OUTCOME_RECORDED", "RESULT_PUBLISHED", "COMPLETION_VERIFIED", "TASK_VERIFIED_COMPLETE", "WORK_COMPLETION_EVALUATED", "RUN_TELEMETRY_RECORDED", "WORK_COMPLETED")
	var run telemetry.Run
	var workEvidence completion.WorkEvidence
	var workEvidenceEventID string
	var workDetail workCompletionDetail
	telemetryEvents := 0
	for _, event := range r.Events {
		switch event.EventType {
		case "WORK_COMPLETION_EVALUATED":
			workEvidenceEventID = event.EventID
			if err := json.Unmarshal(event.Payload, &workEvidence); err != nil {
				t.Fatal(err)
			}
		case "WORK_COMPLETED":
			var payload events.ProjectionEventPayload
			if err := json.Unmarshal(event.Payload, &payload); err != nil || json.Unmarshal(payload.Detail, &workDetail) != nil {
				t.Fatalf("decode Work completion transition: %v", err)
			}
		case "RUN_TELEMETRY_RECORDED":
			telemetryEvents++
			if err := json.Unmarshal(event.Payload, &run); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !workEvidence.Valid() || len(workEvidence.Tasks) != 1 || workEvidence.Tasks[0].TaskID != r.Task.ID || workDetail.EvidenceEventRef != workEvidenceEventID || workDetail.Fingerprint != workEvidence.Fingerprint {
		t.Fatalf("Work completion is not bound to exact verified evidence: evidence=%+v detail=%+v", workEvidence, workDetail)
	}
	if telemetryEvents != 1 || run.Outcome != "VERIFIED_COMPLETE" || len(run.ExecutionMechanisms) != 1 || run.ExecutionMechanisms[0].Kind != core.ExecutionDeterministic || run.ToolCalls != 1 || !run.CostComplete {
		t.Fatalf("run telemetry=%+v events=%d", run, telemetryEvents)
	}
	replayed, err := s.Submit(context.Background(), Submit{RequestID: "r1", OrganizationID: "o1", Statement: "echo hello", Kind: core.ExecutionDeterministic})
	if err != nil {
		t.Fatal(err)
	}
	telemetryEvents = 0
	for _, event := range replayed.Events {
		if event.EventType == "RUN_TELEMETRY_RECORDED" {
			telemetryEvents++
		}
	}
	if telemetryEvents != 1 || len(replayed.Events) != len(r.Events) {
		t.Fatalf("replay duplicated terminal telemetry: before=%d after=%d telemetry=%d", len(r.Events), len(replayed.Events), telemetryEvents)
	}
}

func TestSubmitRejectsMismatchedOperatorIdentity(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	gateway := events.NewGateway(store)
	service := New(gateway)

	tests := []struct {
		name          string
		channel       string
		principalKind core.PrincipalKind
	}{
		{name: "A2A caller labeled as user", channel: "A2A", principalKind: core.PrincipalHuman},
		{name: "direct caller labeled as external Agent", channel: "HUMAN_DIRECT", principalKind: core.PrincipalExternalAgent},
		{name: "A2A caller labeled as runtime", channel: "A2A", principalKind: core.PrincipalRuntime},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requestID := fmt.Sprintf("mismatched-operator-%d", index)
			_, err := service.Submit(ctx, Submit{
				RequestID: requestID, OrganizationID: "org-1", Statement: "echo rejected",
				Kind: core.ExecutionDeterministic, MessageID: "message-" + requestID,
				SourcePrincipalID: "operator-1", SourcePrincipalKind: test.principalKind,
				SourceChannel: test.channel,
			})
			if err == nil {
				t.Fatal("mismatched operator identity was accepted")
			}
			if _, found, err := gateway.ResolveExternalWork(ctx, "org-1", requestID); err != nil {
				t.Fatal(err)
			} else if found {
				t.Fatal("rejected operator identity reserved durable external work")
			}
		})
	}
}

type eventReadCountingLedger struct {
	*ledger.SQLite
	eventReads int
}

func (l *eventReadCountingLedger) Events(ctx context.Context, correlationID string) ([]events.Event, error) {
	l.eventReads++
	return l.SQLite.Events(ctx, correlationID)
}

func TestRoutineReconciliationDoesNotReplayCompletedWorkHistory(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	seed := New(events.NewGateway(store))
	const completedWorks = 8
	for index := range completedWorks {
		requestID := fmt.Sprintf("completed-history-%02d", index)
		result, err := seed.Submit(ctx, Submit{
			RequestID: requestID, OrganizationID: "org-1", Statement: "echo " + requestID,
			Kind: core.ExecutionDeterministic,
		})
		if err != nil {
			t.Fatal(err)
		}
		if result.Work.Status != core.WorkCompleted {
			t.Fatalf("seeded Work %s status=%s", result.Work.ID, result.Work.Status)
		}
	}

	counted := &eventReadCountingLedger{SQLite: store}
	routine := New(events.NewGateway(counted))
	if err := routine.reconcileWorks(ctx); err != nil {
		t.Fatal(err)
	}
	if counted.eventReads != 0 {
		t.Fatalf("routine reconciliation replayed %d completed Work streams", counted.eventReads)
	}

	if _, err := routine.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if counted.eventReads == 0 {
		t.Fatal("startup recovery skipped authoritative completion-chain validation")
	}
}

func TestWorkCompletionAdmissionBindsPlanAndRuntimeContract(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	service := New(events.NewGateway(l))
	result, err := service.Submit(ctx, Submit{RequestID: "completion-boundary", OrganizationID: "org-1", Statement: "echo verified", Kind: core.ExecutionDeterministic})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.state.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	workState := snapshot.Works[result.Work.ID]
	intentState := snapshot.Intents[result.Work.IntentID]
	taskState := snapshot.Tasks[result.Task.ID]
	stream, err := service.gateway.Events(ctx, taskState.CorrelationID)
	if err != nil {
		t.Fatal(err)
	}
	var evidenceEvent events.Event
	for _, event := range stream {
		if event.EventType == "WORK_COMPLETION_EVALUATED" {
			evidenceEvent = event
		}
	}
	binding := events.WorkCompletionBinding{
		OrganizationID: "org-1", CorrelationID: taskState.CorrelationID,
		Work: workState.Value, WorkVersion: workState.Version, Intent: intentState.Value,
		Tasks: []events.WorkCompletionTaskBinding{{Task: taskState.Value, Version: taskState.Version, CorrelationID: taskState.CorrelationID}},
	}
	if _, err := events.ValidateWorkCompletionEvidenceChain(binding, evidenceEvent, stream); err != nil {
		t.Fatalf("valid completion chain was rejected: %v", err)
	}

	mutations := map[string]func(*core.Task){
		"description":      func(task *core.Task) { task.Description = "substituted work" },
		"execution kind":   func(task *core.Task) { task.ExecutionKind = core.ExecutionAgent },
		"inference policy": func(task *core.Task) { task.ModelInferencePolicy = core.InferenceRequired },
		"dependencies":     func(task *core.Task) { task.DependsOn = []core.ID{"task-outside-plan"} },
	}
	for name, mutate := range mutations {
		t.Run("reject substituted "+name, func(t *testing.T) {
			changed := binding
			changed.Tasks = append([]events.WorkCompletionTaskBinding(nil), binding.Tasks...)
			mutate(&changed.Tasks[0].Task)
			if _, err := events.ValidateWorkCompletionEvidenceChain(changed, evidenceEvent, stream); err == nil {
				t.Fatalf("substituted %s matched immutable Plan", name)
			}
		})
	}

	forgedStream := append([]events.Event(nil), stream...)
	for index := range forgedStream {
		event := &forgedStream[index]
		switch event.EventType {
		case "COMPLETION_VERIFIED":
			var detail completionDetail
			if err := json.Unmarshal(event.Payload, &detail); err != nil {
				t.Fatal(err)
			}
			detail.Contract = core.CompletionContract{TaskID: result.Task.ID, TaskVersion: detail.Contract.TaskVersion}
			detail.Result = completion.Result{Complete: true}
			event.Payload, err = json.Marshal(detail)
			if err != nil {
				t.Fatal(err)
			}
		case "TASK_VERIFIED_COMPLETE":
			var payload events.ProjectionEventPayload
			var detail completionDetail
			if err := json.Unmarshal(event.Payload, &payload); err != nil || json.Unmarshal(payload.Detail, &detail) != nil {
				t.Fatal("decode completed Task transition")
			}
			detail.Contract = core.CompletionContract{TaskID: result.Task.ID, TaskVersion: detail.Contract.TaskVersion}
			detail.Result = completion.Result{Complete: true}
			payload.Detail, err = json.Marshal(detail)
			if err != nil {
				t.Fatal(err)
			}
			event.Payload, err = json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := events.ValidateWorkCompletionEvidenceChain(binding, evidenceEvent, forgedStream); err == nil {
		t.Fatal("caller-selected empty completion contract authorized Work completion")
	}
	forgedOutcomeStream := append([]events.Event(nil), stream...)
	for index := range forgedOutcomeStream {
		if forgedOutcomeStream[index].EventType != "TOOL_OUTCOME_RECORDED" {
			continue
		}
		var outcome core.ToolOutcome
		if err := json.Unmarshal(forgedOutcomeStream[index].Payload, &outcome); err != nil {
			t.Fatal(err)
		}
		outcome.ObservedEffect = "different"
		outcome.PostconditionStatus = core.PostconditionVerified
		forgedOutcomeStream[index].Payload, err = json.Marshal(outcome)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := events.ValidateWorkCompletionEvidenceChain(binding, evidenceEvent, forgedOutcomeStream); err == nil {
		t.Fatal("forged deterministic postcondition authorized Work completion")
	}
}

func TestWorkCompletionAdmissionRecomputesAgentPostcondition(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	service := New(events.NewGateway(l))
	result, err := service.Submit(ctx, Submit{RequestID: "agent-postcondition", OrganizationID: "org-1", Statement: "summarize", Kind: core.ExecutionAgent})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.state.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	workState := snapshot.Works[result.Work.ID]
	intentState := snapshot.Intents[result.Work.IntentID]
	taskState := snapshot.Tasks[result.Task.ID]
	stream, err := service.gateway.Events(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	var evidenceEvent events.Event
	for _, event := range stream {
		if event.EventType == "WORK_COMPLETION_EVALUATED" {
			evidenceEvent = event
		}
	}
	binding := events.WorkCompletionBinding{
		OrganizationID: "org-1", CorrelationID: taskState.CorrelationID,
		Work: workState.Value, WorkVersion: workState.Version, Intent: intentState.Value,
		Tasks: []events.WorkCompletionTaskBinding{{Task: taskState.Value, Version: taskState.Version, CorrelationID: taskState.CorrelationID}},
		AgentBlueprints: map[core.ID]core.AgentBlueprint{
			taskState.Value.AgentConfig.BlueprintID: snapshot.AgentBlueprints[taskState.Value.AgentConfig.BlueprintID].Value,
		},
		ExecutionProfiles: map[core.ID]core.ExecutionProfile{
			taskState.Value.AgentConfig.ProfileID: snapshot.ExecutionProfiles[taskState.Value.AgentConfig.ProfileID].Value,
		},
	}
	if _, err := events.ValidateWorkCompletionEvidenceChain(binding, evidenceEvent, stream); err != nil {
		t.Fatalf("valid Agent completion chain was rejected: %v", err)
	}
	profileMismatch := binding
	profile := snapshot.ExecutionProfiles[taskState.Value.AgentConfig.ProfileID].Value
	profile.Model = "different-model"
	profileMismatch.ExecutionProfiles = map[core.ID]core.ExecutionProfile{profile.ID: profile}
	if _, err := events.ValidateWorkCompletionEvidenceChain(profileMismatch, evidenceEvent, stream); err == nil {
		t.Fatal("Agent manifest model differed from its pinned execution profile")
	}
	missingBlueprint := binding
	missingBlueprint.AgentBlueprints = nil
	if _, err := events.ValidateWorkCompletionEvidenceChain(missingBlueprint, evidenceEvent, stream); err == nil {
		t.Fatal("Agent completion resolved its blueprint outside admitted state")
	}

	forgedBlueprint := snapshot.AgentBlueprints[taskState.Value.AgentConfig.BlueprintID].Value
	forgedBlueprint.OperatingInstructions = "substituted operating instructions"
	_, substitutedInput, err := core.MaterializeAgentExecutionInput(core.AgentExecutionInputContext{
		Blueprint: forgedBlueprint,
		Task:      taskState.Value,
	})
	if err != nil {
		t.Fatal(err)
	}
	var startEvent events.Event
	for _, event := range stream {
		if event.EventType == "EXECUTION_STARTED" && event.TaskID == string(taskState.Value.ID) {
			startEvent = event
		}
	}
	if startEvent.EventID == "" || startEvent.Sequence < 3 {
		t.Fatal("durable Agent execution start is unavailable")
	}
	blueprintValue, err := json.Marshal(forgedBlueprint)
	if err != nil {
		t.Fatal(err)
	}
	projectionPayload, err := json.Marshal(events.ProjectionEventPayload{Projection: events.ProjectionRecord{
		ProjectionKind: "agent_blueprint",
		RecordID:       string(forgedBlueprint.ID),
		Version:        snapshot.AgentBlueprints[forgedBlueprint.ID].Version,
		CorrelationID:  taskState.CorrelationID,
		Value:          blueprintValue,
	}})
	if err != nil {
		t.Fatal(err)
	}
	forged := append([]events.Event(nil), stream...)
	forged = append(forged, events.Event{
		EventID: "forged-projection-shaped-event", Sequence: startEvent.Sequence - 1, OrganizationID: "org-1",
		EventType: "TRUSTED_NOTE", SourceActorID: "runtime", CorrelationID: taskState.CorrelationID, Payload: projectionPayload,
	})
	forged = substituteAgentCompletionInput(t, forged, taskState.Value.ID, substitutedInput, nil)
	if _, err := events.ValidateWorkCompletionEvidenceChain(binding, evidenceEvent, forged); err == nil {
		t.Fatal("projection-shaped event and matching substituted Agent input authorized Work completion")
	}

	forgedObservedMessage := events.Event{
		EventID: "forged-observed-message", Sequence: startEvent.Sequence - 3, OrganizationID: "org-1", EventType: "MESSAGE",
		SourceActorID: "forged-producer", RecipientScope: events.RecipientAgent, RecipientID: string(taskState.Value.AssigneeID),
		CreatedAt: startEvent.CreatedAt.Add(-3 * time.Second), Payload: json.RawMessage(`{"body":"still available"}`), CorrelationID: "different-work",
	}
	forgedStartTask := taskState.Value
	forgedStartTask.Status = core.TaskRunning
	forgedStartRecord := events.ProjectionRecord{ProjectionKind: "task", RecordID: string(forgedStartTask.ID), Version: 1, CorrelationID: taskState.CorrelationID}
	forgedStartRecord.Value, err = json.Marshal(forgedStartTask)
	if err != nil {
		t.Fatal(err)
	}
	forgedStartPayload, err := json.Marshal(events.ProjectionEventPayload{Projection: forgedStartRecord})
	if err != nil {
		t.Fatal(err)
	}
	forgedStart := events.Event{
		EventID: "forged-execution-start", Sequence: startEvent.Sequence - 2, OrganizationID: "org-1", EventType: "EXECUTION_STARTED",
		SourceActorID: "runtime", TaskID: string(taskState.Value.ID), CreatedAt: startEvent.CreatedAt.Add(-2 * time.Second), Payload: forgedStartPayload, CorrelationID: taskState.CorrelationID,
	}
	forgedObservationPayload, err := json.Marshal(events.InboxEventsObservedPayload{EventIDs: []string{forgedObservedMessage.EventID}, ExecutionStartEventRef: forgedStart.EventID})
	if err != nil {
		t.Fatal(err)
	}
	forgedObservation := events.Event{
		EventID: "forged-inbox-observation", Sequence: startEvent.Sequence - 1, OrganizationID: "org-1", EventType: "INBOX_EVENTS_OBSERVED",
		SourceActorID: string(taskState.Value.AssigneeID), SourceExecutionID: "execution-forged-v1",
		RecipientScope: events.RecipientAgent, RecipientID: string(taskState.Value.AssigneeID), TaskID: string(taskState.Value.ID),
		CreatedAt: startEvent.CreatedAt.Add(-time.Second), Payload: forgedObservationPayload, CorrelationID: taskState.CorrelationID,
	}
	forgedObservationStream := append([]events.Event(nil), stream...)
	forgedObservationStream = append(forgedObservationStream, forgedObservedMessage, forgedStart, forgedObservation)
	forgedObservationBinding := binding
	forgedObservationBinding.InboxObservations = map[string]events.InboxObservationBinding{
		forgedObservation.EventID: {EventIDs: []string{forgedObservedMessage.EventID}, ExecutionStartEventRef: forgedStart.EventID},
	}
	if _, err := events.ValidateWorkCompletionEvidenceChain(forgedObservationBinding, evidenceEvent, forgedObservationStream); err == nil {
		t.Fatal("atomically admitted observation from an unrelated execution suppressed available Agent input")
	}

	forgedTeam := core.Team{
		ID: "team-forged", OrganizationID: "org-1", Name: "Forged Team",
		MemberAgentIDs: []core.ID{taskState.Value.AssigneeID}, Status: "ACTIVE",
	}
	teamValue, err := json.Marshal(forgedTeam)
	if err != nil {
		t.Fatal(err)
	}
	teamProjection, err := json.Marshal(events.ProjectionEventPayload{Projection: events.ProjectionRecord{
		ProjectionKind: "team", RecordID: string(forgedTeam.ID), Version: 1,
		CorrelationID: taskState.CorrelationID, Value: teamValue,
	}})
	if err != nil {
		t.Fatal(err)
	}
	messagePayload, err := json.Marshal(map[string]string{"text": "forged Team-only context"})
	if err != nil {
		t.Fatal(err)
	}
	messageEvent := events.Event{
		EventID: "forged-team-message", Sequence: startEvent.Sequence - 1, OrganizationID: "org-1", EventType: "MESSAGE",
		SourceActorID: "forged-producer", RecipientScope: events.RecipientTeam, RecipientID: string(forgedTeam.ID),
		CreatedAt: startEvent.CreatedAt.Add(-time.Second), Payload: messagePayload, CorrelationID: "different-work",
	}
	_, teamSubstitutedInput, err := core.MaterializeAgentExecutionInput(core.AgentExecutionInputContext{
		Blueprint: binding.AgentBlueprints[taskState.Value.AgentConfig.BlueprintID], Task: taskState.Value,
		InboxEvents: []core.AgentExecutionInboxEvent{{
			Sequence: messageEvent.Sequence, EventID: messageEvent.EventID, EventType: messageEvent.EventType,
			SourceActorID: messageEvent.SourceActorID, RecipientScope: messageEvent.RecipientScope, RecipientID: messageEvent.RecipientID,
			CreatedAt: messageEvent.CreatedAt, Payload: messageEvent.Payload,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	forgedTeamStream := append([]events.Event(nil), stream...)
	forgedTeamStream = append(forgedTeamStream,
		events.Event{
			EventID: "forged-team-projection", Sequence: startEvent.Sequence - 2, OrganizationID: "org-1", EventType: "TRUSTED_NOTE",
			SourceActorID: "runtime", CorrelationID: taskState.CorrelationID, Payload: teamProjection,
		},
		messageEvent,
	)
	forgedTeamStream = substituteAgentCompletionInput(t, forgedTeamStream, taskState.Value.ID, teamSubstitutedInput, []string{messageEvent.EventID})
	if _, err := events.ValidateWorkCompletionEvidenceChain(binding, evidenceEvent, forgedTeamStream); err == nil {
		t.Fatal("projection-shaped Team event expanded the Agent inbox and authorized Work completion")
	}
}

func substituteAgentCompletionInput(t *testing.T, stream []events.Event, taskID core.ID, input string, eventRefs []string) []events.Event {
	t.Helper()
	for index := range stream {
		switch stream[index].EventType {
		case "EXECUTION_CONTEXT_MANIFESTED":
			if stream[index].TaskID != string(taskID) {
				continue
			}
			var manifest core.ExecutionContextManifest
			if err := json.Unmarshal(stream[index].Payload, &manifest); err != nil {
				t.Fatal(err)
			}
			manifest.EventRefs = append([]string(nil), eventRefs...)
			manifest.ExecutionInputSHA256 = core.FingerprintExecutionInput(input)
			body, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			stream[index].Payload = body
		case "TOOL_OUTCOME_RECORDED":
			if stream[index].TaskID != string(taskID) {
				continue
			}
			var outcome core.ToolOutcome
			if err := json.Unmarshal(stream[index].Payload, &outcome); err != nil {
				t.Fatal(err)
			}
			outcome.ObservedEffect = "fake-model: " + input
			outcome.PostconditionStatus = core.PostconditionVerified
			body, err := json.Marshal(outcome)
			if err != nil {
				t.Fatal(err)
			}
			stream[index].Payload = body
		}
	}
	return stream
}

func TestSubmitBindsOnlyActiveGoalFromAcceptedIntent(t *testing.T) {
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := l.Close(); err != nil {
			t.Errorf("close ledger: %v", err)
		}
	})
	ctx := context.Background()
	gateway := events.NewGateway(l)
	repository := projections.New(gateway)
	seedTestGoal(t, ctx, repository, "org-1", "mission-1", "goal-1", core.GoalActive)
	seedTestGoal(t, ctx, repository, "org-2", "mission-2", "goal-2", core.GoalActive)
	service := New(gateway)

	goalBound := confirmedGoalSubmit(t, ctx, gateway, "goal-bound", "org-1", "goal-1", "echo goal result", core.ExecutionDeterministic)
	result, err := service.Submit(ctx, goalBound)
	if err != nil {
		t.Fatal(err)
	}
	if result.Intent.GoalID != "goal-1" || result.Work.GoalID != "goal-1" {
		t.Fatalf("accepted Goal was not durably bound: intent=%+v work=%+v", result.Intent, result.Work)
	}
	var plan core.Plan
	var evidence completion.WorkEvidence
	for _, event := range result.Events {
		switch event.EventType {
		case "PLAN_CREATED":
			if err := json.Unmarshal(event.Payload, &plan); err != nil {
				t.Fatal(err)
			}
		case "WORK_COMPLETION_EVALUATED":
			if err := json.Unmarshal(event.Payload, &evidence); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(plan.StrategicEventRefs) != 2 || len(plan.StrategicContextRefs) != 2 || plan.StrategicContextRefs[0].ID != "mission/mission-1" || plan.StrategicContextRefs[1].ID != "goal/goal-1" {
		t.Fatalf("durable Plan omitted exact Mission/Goal context: %+v", plan)
	}
	if evidence.GoalID != "goal-1" || !evidence.Valid() {
		t.Fatalf("Work evidence does not bind the accepted Goal: %+v", evidence)
	}
	if _, err := service.Submit(ctx, Submit{RequestID: "unconfirmed-goal", OrganizationID: "org-1", GoalID: "goal-1", Statement: "echo unreviewed", Kind: core.ExecutionDeterministic}); err == nil {
		t.Fatal("active Goal bypassed explicit Intent confirmation")
	}
	snapshot, err := repository.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	goalState := snapshot.Goals["goal-1"]
	paused := goalState.Value
	paused.Status = core.GoalPaused
	if err := repository.SaveGoal(ctx, "GOAL_PAUSED", "runtime", "pause-goal-1", goalState.Version+1, paused, nil); err != nil {
		t.Fatal(err)
	}
	if replayed, err := service.Submit(ctx, goalBound); err != nil || replayed.Work.GoalID != "goal-1" {
		t.Fatalf("valid Work retry failed after its Goal was paused: work=%+v err=%v", replayed.Work, err)
	}
	if _, err := service.Submit(ctx, Submit{RequestID: "goal-bound", OrganizationID: "org-1", Statement: "echo goal result", Kind: core.ExecutionDeterministic}); err == nil {
		t.Fatal("retry removed the immutable Goal binding")
	}
	if _, err := service.Submit(ctx, Submit{RequestID: "cross-goal", OrganizationID: "org-1", GoalID: "goal-2", Statement: "echo cross tenant", Kind: core.ExecutionDeterministic}); err == nil {
		t.Fatal("submission bound Work to another organization's Goal")
	}
	if _, err := service.Submit(ctx, Submit{RequestID: "missing-goal", OrganizationID: "org-1", GoalID: "goal-missing", Statement: "echo missing", Kind: core.ExecutionDeterministic}); err == nil {
		t.Fatal("submission bound Work to a missing Goal")
	}
}

func TestGoalRevisionAfterPlanningTerminalizesStaleWork(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gateway := events.NewGateway(l)
	repository := projections.New(gateway)
	seedTestGoal(t, ctx, repository, "org-1", "mission-1", "goal-1", core.GoalActive)
	planner := strategicDriftPlanner{revise: func() {
		snapshot, loadErr := repository.Load(ctx)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		state := snapshot.Goals["goal-1"]
		goal := state.Value
		goal.Objective = "revised after planning started"
		if saveErr := repository.SaveGoal(ctx, "GOAL_REFINED", "runtime", state.CorrelationID, state.Version+1, goal, nil); saveErr != nil {
			t.Fatal(saveErr)
		}
	}}
	service := NewWithModelAndPlanner(gateway, execution.FakeModel{}, planner)
	in := confirmedGoalSubmit(t, ctx, gateway, "strategic-drift", "org-1", "goal-1", "prepare a governed result", core.ExecutionAgent)
	result, err := service.Submit(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if result.Task.Status != core.TaskFailed || result.Work.Status != core.WorkFailed {
		t.Fatalf("strategic drift did not terminalize stale Work: task=%+v work=%+v", result.Task, result.Work)
	}
	found := false
	for _, event := range result.Events {
		if event.EventType != "TASK_WORK_FAILED" {
			continue
		}
		var payload events.ProjectionEventPayload
		var detail strategicTaskFailureDetail
		if json.Unmarshal(event.Payload, &payload) != nil || json.Unmarshal(payload.Detail, &detail) != nil {
			t.Fatal("decode strategic-context block")
		}
		found = detail.Code == "STRATEGIC_CONTEXT_CHANGED"
	}
	if !found {
		t.Fatal("strategic-context drift lacked a durable fail-closed terminal transition")
	}
}

func TestOversizedStrategicContextFailsBeforeExecutionStart(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gateway := events.NewGateway(l)
	repository := projections.New(gateway)
	seedTestGoal(t, ctx, repository, "org-1", "mission-1", "goal-1", core.GoalActive)
	snapshot, err := repository.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	missionState := snapshot.Missions["mission-1"]
	mission := missionState.Value
	mission.Statement = strings.Repeat("x", 256<<10)
	if err := repository.SaveMission(ctx, "MISSION_REVISED", "runtime", missionState.CorrelationID, missionState.Version+1, mission, nil); err != nil {
		t.Fatal(err)
	}
	model := &organizationLoopModel{}
	service := NewWithModelAndPlanner(gateway, model, newOrganizationPlanner(t, model))
	in := confirmedGoalSubmit(t, ctx, gateway, "oversized-strategy", "org-1", "goal-1", "prepare a governed result", core.ExecutionAgent)
	if _, err := service.Submit(ctx, in); err == nil || !strings.Contains(err.Error(), "validate bounded planning input") {
		t.Fatalf("oversized strategic context was not rejected by planning preflight: %v", err)
	}
	snapshot, err = repository.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Works) != 1 {
		t.Fatalf("preflight rejection produced %d Work records", len(snapshot.Works))
	}
	for _, workState := range snapshot.Works {
		if workState.Value.Status != core.WorkFailed {
			t.Fatalf("preflight rejection did not fail closed: %+v", workState)
		}
	}
	stream, err := service.ExternalEvents(ctx, "org-1", "oversized-strategy")
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range stream {
		if event.EventType == "PLANNING_CONTEXT_MANIFESTED" || event.EventType == "EXECUTION_STARTED" {
			t.Fatalf("oversized strategic context crossed the planning preflight: %s", event.EventType)
		}
	}
	if len(model.prompts) != 0 {
		t.Fatalf("oversized strategic context reached a provider: prompts=%d", len(model.prompts))
	}
}

func TestStrategicDriftDuringUserTaskContinuationReconcilesWork(t *testing.T) {
	tests := []struct {
		name         string
		continueTask func(context.Context, *Service, Result) error
	}{
		{
			name: "structured completion",
			continueTask: func(ctx context.Context, service *Service, result Result) error {
				return service.ProvideHumanCompletion(ctx, HumanCompletionInput{
					OrganizationID: "org-1", PrincipalID: "user-1", SourceChannel: "HUMAN_DIRECT",
					RequestID: "strategic-user-continuation", TaskID: string(result.Task.ID),
					Submission: core.HumanTaskSubmission{MessageID: "completion-1", Fields: map[string]string{"response": "completed input"}},
				})
			},
		},
		{
			name: "external input",
			continueTask: func(ctx context.Context, service *Service, result Result) error {
				return service.ProvideOperatorInput(ctx, OperatorInput{
					OrganizationID: "org-1", PrincipalID: "agent-1", PrincipalKind: core.PrincipalExternalAgent,
					SourceChannel: "A2A", RequestID: "strategic-user-continuation", TaskID: string(result.Task.ID),
					MessageID: "input-1", Text: "completed input",
				})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			l, err := ledger.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = l.Close() })
			gateway := events.NewGateway(l)
			repository := projections.New(gateway)
			seedTestGoal(t, ctx, repository, "org-1", "mission-1", "goal-1", core.GoalActive)
			service := New(gateway)
			in := confirmedGoalSubmit(t, ctx, gateway, "strategic-user-continuation", "org-1", "goal-1", "provide a governed decision", core.ExecutionHuman)
			result, err := service.Submit(ctx, in)
			if err != nil || result.Task.Status != core.TaskBlocked {
				t.Fatalf("blocked user task=%+v err=%v", result.Task, err)
			}
			snapshot, err := repository.Load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			goalState := snapshot.Goals["goal-1"]
			goal := goalState.Value
			goal.Objective = "revised before user continuation"
			if err := repository.SaveGoal(ctx, "GOAL_REFINED", "runtime", goalState.CorrelationID, goalState.Version+1, goal, nil); err != nil {
				t.Fatal(err)
			}
			if err := test.continueTask(ctx, service, result); err != nil {
				t.Fatal(err)
			}
			snapshot, err = repository.Load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.Tasks[result.Task.ID].Value.Status != core.TaskFailed || snapshot.Works[result.Work.ID].Value.Status != core.WorkFailed {
				t.Fatalf("strategic drift was not fully reconciled: task=%+v work=%+v", snapshot.Tasks[result.Task.ID], snapshot.Works[result.Work.ID])
			}
		})
	}
}

func TestCompletedWorkDrivesEvidenceBackedGoalProgress(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "goal-progress.db")
	l, err := ledger.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gateway := events.NewGateway(l)
	repository := projections.New(gateway)
	seedTestGoal(t, ctx, repository, "org-1", "mission-1", "goal-1", core.GoalActive)

	snapshot, err := repository.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	goalState := snapshot.Goals["goal-1"]
	bare := goalState.Value
	bare.Status = core.GoalAchieved
	if _, err := gateway.PublishProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "GOAL_ACHIEVED", SourceActorID: "runtime", CorrelationID: goalState.CorrelationID},
		ProjectionKind: projections.KindGoal, RecordID: "goal-1", Version: goalState.Version + 1, Value: bare,
	}); err == nil {
		t.Fatal("generic projection path admitted Goal achievement")
	}
	if _, err := gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "GOAL_PROGRESS_EVALUATED", SourceActorID: "runtime", CorrelationID: goalState.CorrelationID, Payload: map[string]string{"result": "forged"}}); err == nil {
		t.Fatal("generic event path admitted Goal progress")
	}

	goal := goalState.Value
	goal.SuccessCriteria = []core.IntentValue{{Value: "The requested outcome is produced and independently evaluated.", Origin: "RUNTIME_DEFAULT"}}
	if err := repository.SaveGoal(ctx, "GOAL_REFINED", "runtime", goalState.CorrelationID, goalState.Version+1, goal, nil); err != nil {
		t.Fatal(err)
	}
	service := New(gateway)
	submission := confirmedGoalSubmit(t, ctx, gateway, "goal-progress", "org-1", "goal-1", "echo verified Goal result", core.ExecutionDeterministic)
	result, err := service.Submit(ctx, submission)
	if err != nil {
		t.Fatal(err)
	}
	if result.Work.Status != core.WorkCompleted {
		t.Fatalf("Goal Work did not complete: %+v", result.Work)
	}
	snapshot, err = repository.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	achieved := snapshot.Goals["goal-1"]
	if achieved.Value.Status != core.GoalAchieved || achieved.Version != 3 {
		t.Fatalf("target Goal was not atomically achieved: %+v", achieved)
	}
	stream, err := gateway.Events(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	progressCount, achievementCount := 0, 0
	var progressEvent events.Event
	var progress events.GoalProgressEvaluatedPayload
	var achievement events.GoalAchievementTransitionPayload
	for _, event := range stream {
		switch event.EventType {
		case "GOAL_PROGRESS_EVALUATED":
			progressCount++
			progressEvent = event
			if err := json.Unmarshal(event.Payload, &progress); err != nil {
				t.Fatal(err)
			}
		case "GOAL_ACHIEVED":
			achievementCount++
			var payload events.ProjectionEventPayload
			if err := json.Unmarshal(event.Payload, &payload); err != nil || json.Unmarshal(payload.Detail, &achievement) != nil {
				t.Fatalf("decode Goal achievement: %v", err)
			}
		}
	}
	if progressCount != 1 || achievementCount != 1 || progress.Result != events.GoalProgressTargetAchieved || progress.GoalVersion != 2 || !progress.Valid() || achievement.EvidenceEventRef != progressEvent.EventID || achievement.Fingerprint != progress.Fingerprint {
		t.Fatalf("Goal terminal evidence is incomplete: progress=%+v achievement=%+v counts=%d/%d", progress, achievement, progressCount, achievementCount)
	}
	if err := gateway.ValidateGoalAchievement(ctx, "org-1", "goal-1"); err != nil {
		t.Fatalf("valid Goal achievement was rejected: %v", err)
	}
	replayLedger := &eventOnlyGoalReplayLedger{stream: stream}
	rebuilt, err := projections.New(events.NewGateway(replayLedger)).Rebuild(ctx)
	if err != nil {
		t.Fatalf("event-only replay rejected valid Goal achievement: %v", err)
	}
	if replayLedger.validationCalls != 0 {
		t.Fatal("event replay consulted the records-backed Goal validator")
	}
	if replayed := rebuilt.Goals["goal-1"]; replayed.Value.Status != core.GoalAchieved || replayed.Version != 3 {
		t.Fatalf("event-only replay lost Goal achievement: %+v", replayed)
	}
	tampered := append([]events.Event(nil), stream...)
	for index := range tampered {
		tampered[index].Payload = append([]byte(nil), tampered[index].Payload...)
		if tampered[index].EventID != progressEvent.EventID {
			continue
		}
		var forged events.GoalProgressEvaluatedPayload
		if err := json.Unmarshal(tampered[index].Payload, &forged); err != nil {
			t.Fatal(err)
		}
		forged.Fingerprint = strings.Repeat("0", 64)
		tampered[index].Payload, err = json.Marshal(forged)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := projections.New(events.NewGateway(&eventOnlyGoalReplayLedger{stream: tampered})).Rebuild(ctx); err == nil {
		t.Fatal("event-only replay admitted tampered Goal progress evidence")
	}
	reordered := append([]events.Event(nil), stream...)
	for index := range reordered {
		reordered[index].Payload = append([]byte(nil), reordered[index].Payload...)
		if reordered[index].EventType != "WORK_COMPLETED" {
			continue
		}
		var payload events.ProjectionEventPayload
		var detail events.WorkCompletionTransitionPayload
		if json.Unmarshal(reordered[index].Payload, &payload) != nil || json.Unmarshal(payload.Detail, &detail) != nil || !slices.Contains(progress.WorkEvidenceRefs, detail.EvidenceEventRef) {
			continue
		}
		reordered[index].Sequence = progressEvent.Sequence
	}
	if _, err := projections.New(events.NewGateway(&eventOnlyGoalReplayLedger{stream: reordered})).Rebuild(ctx); err == nil {
		t.Fatal("event-only replay admitted Work completed after its Goal evaluation")
	}
	reorderedGoal := append([]events.Event(nil), stream...)
	for index := range reorderedGoal {
		reorderedGoal[index].Payload = append([]byte(nil), reorderedGoal[index].Payload...)
		if reorderedGoal[index].EventType == "GOAL_REFINED" {
			reorderedGoal[index].Sequence = progressEvent.Sequence
		}
	}
	if _, err := projections.New(events.NewGateway(&eventOnlyGoalReplayLedger{stream: reorderedGoal})).Rebuild(ctx); err == nil {
		t.Fatal("event-only replay admitted a Goal evaluation that preceded its active revision")
	}
	if _, err := gateway.EvaluateGoalProgress(ctx, "org-2", "goal-1"); err == nil {
		t.Fatal("cross-tenant Goal evaluation was accepted")
	}
	if _, err := repository.EvaluateGoalProgress(ctx, "org-1", "goal-1"); err != nil {
		t.Fatalf("Goal achievement retry was not idempotent: %v", err)
	}
	afterRetry, err := gateway.Events(ctx, "")
	if err != nil || len(afterRetry) != len(stream) {
		t.Fatalf("Goal retry duplicated durable state: before=%d after=%d err=%v", len(stream), len(afterRetry), err)
	}
	if _, err := service.Recover(ctx); err != nil {
		t.Fatalf("restart rejected durable Goal achievement: %v", err)
	}

	continuous := goal
	continuous.ID = "goal-continuous"
	continuous.Mode = core.GoalContinuous
	continuous.Status = core.GoalActive
	if err := repository.SaveGoal(ctx, "GOAL_CREATED", "runtime", "seed-goal-continuous", 1, continuous, nil); err != nil {
		t.Fatal(err)
	}
	continuousSubmission := confirmedGoalSubmit(t, ctx, gateway, "continuous-progress", "org-1", continuous.ID, "echo continuous result", core.ExecutionDeterministic)
	if _, err := service.Submit(ctx, continuousSubmission); err != nil {
		t.Fatal(err)
	}
	snapshot, err = repository.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state := snapshot.Goals[continuous.ID]; state.Value.Status != core.GoalActive || state.Version != 1 {
		t.Fatalf("continuous Goal became terminal: %+v", state)
	}
	continuousStream, err := gateway.Events(ctx, "seed-goal-continuous")
	if err != nil {
		t.Fatal(err)
	}
	continuousCount := 0
	for _, event := range continuousStream {
		if event.EventType != "GOAL_PROGRESS_EVALUATED" {
			continue
		}
		var recorded events.GoalProgressEvaluatedPayload
		if json.Unmarshal(event.Payload, &recorded) == nil && recorded.Result == events.GoalProgressContinuous {
			continuousCount++
		}
	}
	if continuousCount != 1 {
		t.Fatal("continuous Goal progress was not recorded")
	}
	secondContinuous := confirmedGoalSubmit(t, ctx, gateway, "continuous-progress-2", "org-1", continuous.ID, "echo another continuous result", core.ExecutionDeterministic)
	if _, err := service.Submit(ctx, secondContinuous); err != nil {
		t.Fatal(err)
	}
	continuousStream, err = gateway.Events(ctx, "seed-goal-continuous")
	if err != nil {
		t.Fatal(err)
	}
	continuousCount = 0
	for _, event := range continuousStream {
		if event.EventType == "GOAL_PROGRESS_EVALUATED" {
			continuousCount++
		}
	}
	if continuousCount != 1 {
		t.Fatalf("unchanged continuous Goal criteria created %d progress events, want one stable witness evaluation", continuousCount)
	}
	snapshot, err = repository.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	missionState := snapshot.Missions["mission-1"]
	retiredMission := missionState.Value
	retiredMission.Status = core.MissionRetired
	if err := repository.SaveMission(ctx, "MISSION_RETIRED", "runtime", "retire-mission-1", missionState.Version+1, retiredMission, nil); err != nil {
		t.Fatal(err)
	}
	if err := gateway.ValidateGoalAchievement(ctx, "org-1", "goal-1"); err != nil {
		t.Fatalf("legitimate Mission retirement after Goal achievement was rejected: %v", err)
	}
	retiredStream, err := gateway.Events(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := projections.New(events.NewGateway(&eventOnlyGoalReplayLedger{stream: retiredStream})).Rebuild(ctx); err != nil {
		t.Fatalf("event-only replay rejected legitimate post-achievement Mission retirement: %v", err)
	}
	for name, sequence := range map[string]int64{"before": progressEvent.Sequence - 1, "at": progressEvent.Sequence} {
		t.Run("Mission retirement "+name+" evaluation", func(t *testing.T) {
			reorderedMission := append([]events.Event(nil), retiredStream...)
			for index := range reorderedMission {
				if reorderedMission[index].EventType == "MISSION_RETIRED" {
					reorderedMission[index].Sequence = sequence
				}
			}
			if _, err := projections.New(events.NewGateway(&eventOnlyGoalReplayLedger{stream: reorderedMission})).Rebuild(ctx); err == nil {
				t.Fatal("event-only replay admitted Goal achievement after Mission retirement")
			} else if !strings.Contains(strings.ToLower(err.Error()), "mission") {
				t.Fatalf("event-only replay failed outside the Mission evaluation boundary: %v", err)
			}
		})
	}
}

type eventOnlyGoalReplayLedger struct {
	stream          []events.Event
	validationCalls int
}

func (l *eventOnlyGoalReplayLedger) Append(context.Context, events.TrustedDraft) (events.Event, error) {
	return events.Event{}, errors.New("event-only replay ledger is read-only")
}

func (l *eventOnlyGoalReplayLedger) Events(_ context.Context, correlationID string) ([]events.Event, error) {
	if correlationID == "" {
		return append([]events.Event(nil), l.stream...), nil
	}
	filtered := make([]events.Event, 0)
	for _, event := range l.stream {
		if event.CorrelationID == correlationID {
			filtered = append(filtered, event)
		}
	}
	return filtered, nil
}

func (*eventOnlyGoalReplayLedger) InboxObservations(context.Context) (map[string]events.InboxObservationBinding, error) {
	return map[string]events.InboxObservationBinding{}, nil
}

func (l *eventOnlyGoalReplayLedger) ValidateGoalAchievement(context.Context, string, core.ID) error {
	l.validationCalls++
	return errors.New("records-backed Goal validation is unavailable")
}

func TestSubmitDeadlineIncludesServiceQueueWait(t *testing.T) {
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := l.Close(); err != nil {
			t.Errorf("close ledger: %v", err)
		}
	})
	s := New(events.NewGateway(l))
	s.permit <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = s.Submit(ctx, Submit{RequestID: "queued", OrganizationID: "o1", Statement: "echo queued"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("queued submission error=%v", err)
	}
	<-s.permit
}

func TestAgentExecutionUsesFakeAdapter(t *testing.T) {
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := l.Close(); err != nil {
			t.Errorf("close ledger: %v", err)
		}
	})
	r, err := New(events.NewGateway(l)).Submit(context.Background(), Submit{RequestID: "r2", OrganizationID: "o1", Statement: "summarize", Kind: core.ExecutionAgent})
	if err != nil {
		t.Fatal(err)
	}
	observed, ok := r.Outcome.ObservedEffect.(string)
	if !ok || !strings.HasPrefix(observed, "fake-model: Operate only as this runtime-selected durable Agent blueprint.") || !strings.Contains(observed, `"objective":"summarize"`) {
		t.Fatalf("effect=%q", r.Outcome.ObservedEffect)
	}
	if !r.Completion.Complete || r.Task.Status != core.TaskCompleted {
		t.Fatalf("fake model result was not deterministically verified: %+v", r)
	}
	assertEventOrder(t, r.Events, "EXECUTION_CONTEXT_MANIFESTED", "TOOL_OUTCOME_RECORDED", "INFERENCE_USAGE_RECORDED", "EXECUTION_FINISHED", "RUN_TELEMETRY_RECORDED")
}

type organizationLoopModel struct {
	prompts []string
	plan    string
}

func (*organizationLoopModel) Name() string { return "fake-model/v1" }
func (*organizationLoopModel) Descriptor() execution.ModelDescriptor {
	return execution.ModelDescriptor{Provider: "fake", Model: "fake-model/v1", ExecutionProfileVersion: "v1-fake"}
}
func (m *organizationLoopModel) Complete(_ context.Context, prompt string) (execution.ModelResponse, error) {
	m.prompts = append(m.prompts, prompt)
	text := "fake-model: " + prompt
	if strings.HasPrefix(prompt, "You are the bounded Agent OS Task-DAG planner.") {
		text = m.plan
		if text == "" {
			text = `{"tasks":[{"key":"research","description":"research the accepted objective","execution_kind":"AGENT","model_inference_policy":"REQUIRED","depends_on":[]}]}`
		}
	}
	return execution.ModelResponse{Text: text, Usage: events.InferenceUsageRecordedPayload{Source: "fake", Provider: "fake", Model: "fake-model/v1"}}, nil
}

type organizationPlanningModel struct{ model *organizationLoopModel }

func (m organizationPlanningModel) Descriptor() planning.Descriptor {
	descriptor := m.model.Descriptor()
	return planning.Descriptor{Provider: descriptor.Provider, Model: descriptor.Model, ExecutionProfileVersion: descriptor.ExecutionProfileVersion}
}
func (m organizationPlanningModel) CompleteText(ctx context.Context, prompt string) (planning.TextCompletion, error) {
	response, err := m.model.Complete(ctx, prompt)
	return planning.TextCompletion{Text: response.Text, Usage: response.Usage}, err
}

func newOrganizationPlanner(t *testing.T, model *organizationLoopModel) planning.Planner {
	t.Helper()
	planner, err := planning.NewModelPlanner(organizationPlanningModel{model: model})
	if err != nil {
		t.Fatal(err)
	}
	return planner
}

func TestGoalBoundPlanningAndExecutionUseExactStrategicContext(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gateway := events.NewGateway(l)
	repository := projections.New(gateway)
	seedTestGoal(t, ctx, repository, "org-1", "mission-1", "goal-1", core.GoalActive)
	model := &organizationLoopModel{plan: `{"tasks":[]}`}
	service := NewWithModelAndPlanner(gateway, model, newOrganizationPlanner(t, model))
	in := confirmedGoalSubmit(t, ctx, gateway, "strategic-agent", "org-1", "goal-1", "prepare a governed result", core.ExecutionAgent)
	result, err := service.Submit(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	observed, ok := result.Outcome.ObservedEffect.(string)
	if !ok || !strings.Contains(observed, "mission-1") || !strings.Contains(observed, "durable test direction") || !strings.Contains(observed, "goal-1") || !strings.Contains(observed, "measurable test outcome") {
		t.Fatalf("Agent execution omitted strategic context: %q", result.Outcome.ObservedEffect)
	}
	manifestFound := false
	for _, event := range result.Events {
		if event.EventType != "EXECUTION_CONTEXT_MANIFESTED" {
			continue
		}
		var manifest core.ExecutionContextManifest
		if json.Unmarshal(event.Payload, &manifest) != nil || len(manifest.AdditionalContextRefs) != 2 || len(manifest.EventRefs) < 2 {
			t.Fatalf("execution manifest omitted strategic provenance: %+v", manifest)
		}
		manifestFound = true
	}
	if !manifestFound {
		t.Fatal("Goal-bound Agent execution lacked a context manifest")
	}
}

type delayedOrganizationModel struct{}

func (delayedOrganizationModel) Name() string { return "fake-model/v1" }
func (delayedOrganizationModel) Descriptor() execution.ModelDescriptor {
	return execution.ModelDescriptor{Provider: "fake", Model: "fake-model/v1", ExecutionProfileVersion: "v1-fake"}
}
func (delayedOrganizationModel) Complete(ctx context.Context, prompt string) (execution.ModelResponse, error) {
	select {
	case <-time.After(20 * time.Millisecond):
	case <-ctx.Done():
		return execution.ModelResponse{}, ctx.Err()
	}
	text := "fake-model: " + prompt
	if strings.HasPrefix(prompt, "You are the bounded Agent OS Task-DAG planner.") {
		text = `{"tasks":[]}`
	}
	return execution.ModelResponse{Text: text, Usage: events.InferenceUsageRecordedPayload{
		Source: "test", Provider: "fake", Model: "fake-model/v1",
	}}, nil
}

type delayedPlanningModel struct{ model delayedOrganizationModel }

func (m delayedPlanningModel) Descriptor() planning.Descriptor {
	descriptor := m.model.Descriptor()
	return planning.Descriptor{Provider: descriptor.Provider, Model: descriptor.Model, ExecutionProfileVersion: descriptor.ExecutionProfileVersion}
}
func (m delayedPlanningModel) CompleteText(ctx context.Context, prompt string) (planning.TextCompletion, error) {
	response, err := m.model.Complete(ctx, prompt)
	return planning.TextCompletion{Text: response.Text, Usage: response.Usage}, err
}

func TestAcceptedWorkOutlivesRequestDeadlineWithBoundedTurns(t *testing.T) {
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	model := delayedOrganizationModel{}
	planner, err := planning.NewModelPlanner(delayedPlanningModel{model: model})
	if err != nil {
		t.Fatal(err)
	}
	service := NewWithModelAndPlanner(events.NewGateway(l), model, planner)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	result, err := service.Submit(ctx, Submit{RequestID: "deadline-isolation", OrganizationID: "org-1", Statement: "prepare bounded work", Kind: core.ExecutionAgent})
	if err != nil || result.Task.Status != core.TaskCompleted || result.Work.Status != "COMPLETED" {
		t.Fatalf("result=%+v work=%+v err=%v", result.Task, result.Work, err)
	}
}

type timeoutExecutionModel struct{}

func (timeoutExecutionModel) Name() string { return "timeout-model/v1" }
func (timeoutExecutionModel) Descriptor() execution.ModelDescriptor {
	return execution.ModelDescriptor{Provider: "timeout", Model: "timeout-model/v1", ExecutionProfileVersion: "v1-timeout"}
}
func (timeoutExecutionModel) Complete(ctx context.Context, _ string) (execution.ModelResponse, error) {
	<-ctx.Done()
	return execution.ModelResponse{}, ctx.Err()
}

func TestTimedOutProviderTurnPersistsTerminalFailure(t *testing.T) {
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	service := NewWithModel(events.NewGateway(l), timeoutExecutionModel{})
	service.modelTurnTimeout = 5 * time.Millisecond
	result, err := service.Submit(context.Background(), Submit{RequestID: "turn-timeout", OrganizationID: "org-1", Statement: "bounded work", Kind: core.ExecutionAgent})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("execution error=%v", err)
	}
	if result.Task.Status != core.TaskFailed || result.Work.Status != "FAILED" {
		t.Fatalf("task=%+v work=%+v", result.Task, result.Work)
	}
	if !hasEventType(result.Events, "EXECUTION_FINISHED") || !hasEventType(result.Events, "COMPLETION_REJECTED") {
		t.Fatalf("timed-out turn lacked durable terminal events: %+v", result.Events)
	}
}

func hasEventType(stream []events.Event, eventType string) bool {
	for _, event := range stream {
		if event.EventType == eventType {
			return true
		}
	}
	return false
}

func TestModelCapablePlannerSkipsInferenceForExactWork(t *testing.T) {
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	model := &organizationLoopModel{}
	service := NewWithModelAndPlanner(events.NewGateway(l), model, newOrganizationPlanner(t, model))
	result, err := service.Submit(context.Background(), Submit{RequestID: "exact-no-planning-model", OrganizationID: "org-1", Statement: "echo exact", Kind: core.ExecutionDeterministic})
	if err != nil || result.Task.Status != core.TaskCompleted {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if len(model.prompts) != 0 {
		t.Fatalf("exact work used model prompts=%+v", model.prompts)
	}
	for _, event := range result.Events {
		if event.EventType == "PLANNING_CONTEXT_MANIFESTED" || event.EventType == "INFERENCE_USAGE_RECORDED" {
			t.Fatalf("exact work recorded model use: %+v", event)
		}
	}
}

func TestAcceptedIntentBecomesDurableTaskDAGWithDependencyEvidence(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	model := &organizationLoopModel{}
	planner := newOrganizationPlanner(t, model)
	service := NewWithModelAndPlanner(events.NewGateway(l), model, planner)
	submission := Submit{RequestID: "organization-loop", OrganizationID: "org-1", Statement: "prepare a verified briefing", Kind: core.ExecutionAgent}
	result, err := service.Submit(ctx, submission)
	if err != nil {
		t.Fatal(err)
	}
	if result.Task.ID == "" || result.Task.ParentID != "" || result.Task.Status != core.TaskCompleted || result.Work.Status != "COMPLETED" {
		t.Fatalf("root result=%+v work=%+v", result.Task, result.Work)
	}
	if len(model.prompts) != 3 {
		t.Fatalf("model calls=%d prompts=%+v", len(model.prompts), model.prompts)
	}
	assertEventOrder(t, result.Events, "INTENT_CREATED", "PLAN_CREATED", "TASK_CREATED", "EXECUTION_STARTED", "RESULT_PUBLISHED", "TASK_VERIFIED_COMPLETE", "EXECUTION_STARTED", "RESULT_PUBLISHED", "TASK_VERIFIED_COMPLETE", "WORK_COMPLETED")

	snapshot, err := projections.New(events.NewGateway(l)).Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var child core.Task
	for _, state := range snapshot.Tasks {
		if state.Value.ParentID == result.Task.ID {
			child = state.Value
		}
	}
	if child.ID == "" || len(result.Task.DependsOn) != 1 || result.Task.DependsOn[0] != child.ID || child.Status != core.TaskCompleted {
		t.Fatalf("root=%+v child=%+v", result.Task, child)
	}
	var childResultEvent string
	var rootManifest core.ExecutionContextManifest
	var plan core.Plan
	for _, event := range result.Events {
		switch {
		case event.EventType == "RESULT_PUBLISHED" && core.ID(event.TaskID) == child.ID:
			childResultEvent = event.EventID
		case event.EventType == "EXECUTION_CONTEXT_MANIFESTED" && core.ID(event.TaskID) == result.Task.ID:
			if err := json.Unmarshal(event.Payload, &rootManifest); err != nil {
				t.Fatal(err)
			}
		case event.EventType == "PLAN_CREATED":
			if err := json.Unmarshal(event.Payload, &plan); err != nil {
				t.Fatal(err)
			}
		}
	}
	if childResultEvent == "" || !slices.Contains(rootManifest.EventRefs, childResultEvent) {
		t.Fatalf("child result=%q root manifest=%+v", childResultEvent, rootManifest)
	}
	observed, ok := result.Outcome.ObservedEffect.(string)
	if !ok || !strings.Contains(observed, childResultEvent) || !strings.Contains(observed, "Runtime-selected dependency evidence") {
		t.Fatalf("root execution omitted bounded dependency evidence: %q", result.Outcome.ObservedEffect)
	}
	if plan.IntentFingerprint == "" || plan.IntentFingerprint != result.Intent.AcceptedFingerprint || plan.Fingerprint == "" {
		t.Fatalf("plan=%+v intent=%+v", plan, result.Intent)
	}

	calls := len(model.prompts)
	restarted := NewWithModelAndPlanner(events.NewGateway(l), model, planner)
	replayed, err := restarted.Submit(ctx, submission)
	if err != nil || replayed.Task.ID != result.Task.ID || len(model.prompts) != calls || len(replayed.Events) != len(result.Events) {
		t.Fatalf("replay=%+v calls=%d want=%d err=%v", replayed, len(model.prompts), calls, err)
	}
}

func TestChildCompletionReviewStaysInternalAndWakesRoot(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	planningModel := &organizationLoopModel{}
	service := NewWithModelAndPlanner(events.NewGateway(l), describedModel{}, newOrganizationPlanner(t, planningModel))
	submission := Submit{RequestID: "child-review", OrganizationID: "org-1", Statement: "prepare a reviewed briefing", Kind: core.ExecutionAgent}
	submitted, err := service.Submit(ctx, submission)
	if err != nil || submitted.Task.Status != core.TaskPending || submitted.Work.Status != "ACTIVE" {
		t.Fatalf("submitted=%+v err=%v", submitted, err)
	}
	snapshot, err := projections.New(events.NewGateway(l)).Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var child core.Task
	for _, state := range snapshot.Tasks {
		if state.Value.ParentID == submitted.Task.ID {
			child = state.Value
		}
	}
	if child.ID == "" || child.Status != core.TaskBlocked {
		t.Fatalf("child=%+v", child)
	}
	page, err := service.PendingCompletionReviews(ctx, "org-1", "", 10)
	if err != nil || len(page.Reviews) != 1 || page.Reviews[0].Request.TaskID != child.ID || page.NextAfter != "" {
		t.Fatalf("pending reviews=%+v err=%v", page, err)
	}
	otherPage, err := service.PendingCompletionReviews(ctx, "other-org", "", 10)
	if err != nil || len(otherPage.Reviews) != 0 {
		t.Fatalf("cross-organization reviews=%+v err=%v", otherPage, err)
	}
	if _, stream, err := service.ExternalTaskEvents(ctx, "org-1", string(child.ID)); err != nil || len(stream) != 0 {
		t.Fatalf("child crossed external lookup boundary: events=%d err=%v", len(stream), err)
	}
	if _, found, err := service.CompletionReview(ctx, "other-org", string(child.ID)); err != nil || found {
		t.Fatalf("cross-organization child review found=%t err=%v", found, err)
	}
	childReview, found, err := service.CompletionReview(ctx, "org-1", string(child.ID))
	if err != nil || !found || childReview.Request.TaskID != child.ID {
		t.Fatalf("child review=%+v found=%t err=%v", childReview, found, err)
	}
	if _, err := service.ReviewCompletion(ctx, reviewInput(childReview, completion.ReviewApprove, "")); err != nil {
		t.Fatal(err)
	}
	rootReview, found, err := service.CompletionReview(ctx, "org-1", string(submitted.Task.ID))
	if err != nil || !found || rootReview.Request.TaskID != submitted.Task.ID {
		t.Fatalf("root review=%+v found=%t err=%v", rootReview, found, err)
	}
	if _, err := service.ReviewCompletion(ctx, reviewInput(rootReview, completion.ReviewApprove, "")); err != nil {
		t.Fatal(err)
	}
	replayed, err := service.Submit(ctx, submission)
	if err != nil || replayed.Task.Status != core.TaskCompleted || replayed.Work.Status != "COMPLETED" {
		t.Fatalf("replayed=%+v err=%v", replayed, err)
	}
}

type failingExecutionModel struct{ calls int }

func (*failingExecutionModel) Name() string { return "failing-model/v1" }
func (*failingExecutionModel) Descriptor() execution.ModelDescriptor {
	return execution.ModelDescriptor{Provider: "failing", Model: "failing-model/v1", ExecutionProfileVersion: "v1-failing"}
}
func (m *failingExecutionModel) Complete(context.Context, string) (execution.ModelResponse, error) {
	m.calls++
	return execution.ModelResponse{}, errors.New("provider failed")
}

func TestFailedChildTerminalizesRootAndWork(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	planningModel := &organizationLoopModel{}
	executionModel := &failingExecutionModel{}
	service := NewWithModelAndPlanner(events.NewGateway(l), executionModel, newOrganizationPlanner(t, planningModel))
	result, err := service.Submit(ctx, Submit{RequestID: "failed-child", OrganizationID: "org-1", Statement: "prepare a briefing", Kind: core.ExecutionAgent})
	if err != nil {
		t.Fatal(err)
	}
	if result.Task.Status != core.TaskFailed || result.Work.Status != "FAILED" {
		t.Fatalf("root=%+v work=%+v", result.Task, result.Work)
	}
	foundDependencyFailure := false
	for _, event := range result.Events {
		if event.EventType == "TASK_DEPENDENCY_FAILED" && event.TaskID == string(result.Task.ID) {
			foundDependencyFailure = true
		}
	}
	if !foundDependencyFailure {
		t.Fatal("root failure did not record its failed dependency contract")
	}
}

func TestFailedRootStopsIndependentSiblingBeforeExecution(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	planningModel := &organizationLoopModel{plan: `{"tasks":[{"key":"a-fail","description":"first work","execution_kind":"AGENT","model_inference_policy":"REQUIRED","depends_on":[]},{"key":"z-unused","description":"unnecessary work","execution_kind":"AGENT","model_inference_policy":"REQUIRED","depends_on":[]}]}`}
	executionModel := &failingExecutionModel{}
	service := NewWithModelAndPlanner(events.NewGateway(l), executionModel, newOrganizationPlanner(t, planningModel))
	result, err := service.Submit(ctx, Submit{RequestID: "stop-sibling", OrganizationID: "org-1", Statement: "prepare a briefing", Kind: core.ExecutionAgent})
	if err != nil {
		t.Fatal(err)
	}
	if executionModel.calls != 1 || result.Task.Status != core.TaskFailed || result.Work.Status != "FAILED" {
		t.Fatalf("model calls=%d root=%+v work=%+v", executionModel.calls, result.Task, result.Work)
	}
	snapshot, err := projections.New(events.NewGateway(l)).Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	workFailedSibling := false
	for _, state := range snapshot.Tasks {
		if state.Value.ParentID == result.Task.ID && state.Value.Status != core.TaskFailed {
			t.Fatalf("nonterminal sibling survived failed root: %+v", state.Value)
		}
	}
	for _, event := range result.Events {
		if event.EventType == "TASK_WORK_FAILED" {
			workFailedSibling = true
		}
	}
	if !workFailedSibling {
		t.Fatal("failed root did not record sibling terminalization")
	}
}

type describedModel struct{}

func (describedModel) Name() string { return "codex-subscription/test-model" }
func (describedModel) Descriptor() execution.ModelDescriptor {
	return execution.ModelDescriptor{Provider: "codex-subscription", Model: "test-model", ExecutionProfileVersion: "v1-codex-subscription"}
}
func (describedModel) Complete(_ context.Context, prompt string) (execution.ModelResponse, error) {
	return execution.ModelResponse{Text: "configured-model: " + prompt, Usage: events.InferenceUsageRecordedPayload{Source: "provider_cli", Provider: "codex-subscription", Model: "test-model", InputTokens: 1, OutputTokens: 1, TotalTokens: 2}}, nil
}

type changedDescriptorModel struct{}

func (changedDescriptorModel) Name() string { return "openai/changed-model" }
func (changedDescriptorModel) Descriptor() execution.ModelDescriptor {
	return execution.ModelDescriptor{Provider: "openai", Model: "changed-model", ExecutionProfileVersion: "v2-openai"}
}
func (changedDescriptorModel) Complete(_ context.Context, _ string) (execution.ModelResponse, error) {
	return execution.ModelResponse{}, errors.New("changed model must not run during replay")
}

type replacementModel struct{}

func (replacementModel) Name() string { return "openai/replacement-model" }
func (replacementModel) Descriptor() execution.ModelDescriptor {
	return execution.ModelDescriptor{Provider: "openai", Model: "replacement-model", ExecutionProfileVersion: "v2-openai"}
}
func (replacementModel) Complete(_ context.Context, prompt string) (execution.ModelResponse, error) {
	return execution.ModelResponse{Text: "replacement-model: " + prompt, Usage: events.InferenceUsageRecordedPayload{Source: "provider_api", Provider: "openai", Model: "replacement-model", InputTokens: 1, OutputTokens: 1, TotalTokens: 2}}, nil
}

func TestAgentExecutionManifestUsesConfiguredModelDescriptor(t *testing.T) {
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := l.Close(); err != nil {
			t.Errorf("close ledger: %v", err)
		}
	})
	service := NewWithModel(events.NewGateway(l), describedModel{})
	submission := Submit{RequestID: "configured-model", OrganizationID: "o1", Statement: "summarize", Kind: core.ExecutionAgent}
	r, err := service.Submit(context.Background(), submission)
	if err != nil {
		t.Fatal(err)
	}
	if r.Task.Status != core.TaskBlocked || r.Work.Status != "ACTIVE" || r.Completion.Complete {
		t.Fatalf("unverified model result did not remain blocked: %+v", r)
	}
	snapshot, err := service.state.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.AgentBlueprints) != 1 || len(snapshot.ExecutionProfiles) != 1 || len(snapshot.Agents) != 1 || r.Task.AssigneeType != "AGENT" || r.Task.AssigneeID == "" {
		t.Fatalf("durable roster or assignment is incomplete: task=%+v roster=%+v", r.Task, snapshot)
	}
	assigned := snapshot.Agents[r.Task.AssigneeID].Value
	profile := snapshot.ExecutionProfiles[assigned.ExecutionProfileID].Value
	if profile.ModelProvider != "codex-subscription" || profile.Model != "test-model" || profile.Version != "v1-codex-subscription" {
		t.Fatalf("durable execution profile does not bind the configured provider: %+v", profile)
	}
	observed, ok := r.Outcome.ObservedEffect.(string)
	if !ok || !strings.HasPrefix(observed, "configured-model: Operate only as this runtime-selected durable Agent blueprint.") || !strings.Contains(observed, `"objective":"summarize"`) {
		t.Fatalf("provider result was not preserved: %+v", r.Outcome)
	}
	assertEventOrder(t, r.Events, "RESULT_PUBLISHED", "CANDIDATE_COMPLETE", "COMPLETION_REVIEW_REQUESTED", "TASK_BLOCKED")
	foundManifest := false
	for _, event := range r.Events {
		if event.EventType != "EXECUTION_CONTEXT_MANIFESTED" {
			continue
		}
		var manifest core.ExecutionContextManifest
		if err := json.Unmarshal(event.Payload, &manifest); err != nil {
			t.Fatal(err)
		}
		if manifest.Provider != "codex-subscription" || manifest.Model != "test-model" || manifest.ExecutionProfileVersion != "v1-codex-subscription" || manifest.AgentBlueprintVersion == "" || manifest.RuntimeAdapter != "local" {
			t.Fatalf("manifest=%+v", manifest)
		}
		foundManifest = true
	}
	if !foundManifest {
		t.Fatal("execution context manifest was not recorded")
	}
	replayed, err := service.Submit(context.Background(), submission)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Task.Status != core.TaskBlocked || replayed.Completion.Complete || len(replayed.Completion.Reasons) == 0 || len(replayed.Events) != len(r.Events) {
		t.Fatalf("blocked review did not replay idempotently: before=%+v after=%+v", r, replayed)
	}
}

func TestReplayRetainsDurableAssignmentWhenConfiguredModelChanges(t *testing.T) {
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gateway := events.NewGateway(l)
	submission := Submit{RequestID: "stable-assignment", OrganizationID: "org-1", Statement: "summarize stable", Kind: core.ExecutionAgent}

	first, err := NewWithModel(gateway, describedModel{}).Submit(context.Background(), submission)
	if err != nil {
		t.Fatal(err)
	}
	if first.Task.AssigneeType != "AGENT" || first.Task.AssigneeID == "" {
		t.Fatalf("initial assignment=%+v", first.Task)
	}

	replayed, err := NewWithModel(gateway, changedDescriptorModel{}).Submit(context.Background(), submission)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Task.AssigneeID != first.Task.AssigneeID || replayed.Task.Status != first.Task.Status || !reflect.DeepEqual(replayed.Task.AgentConfig, first.Task.AgentConfig) {
		t.Fatalf("durable assignment changed during replay: before=%+v after=%+v", first.Task, replayed.Task)
	}
	snapshot, err := projections.New(gateway).Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Agents) != 1 || len(snapshot.ExecutionProfiles) != 1 {
		t.Fatalf("materialized replay bootstrapped unrelated roster state: agents=%d profiles=%d", len(snapshot.Agents), len(snapshot.ExecutionProfiles))
	}
}

func TestAgentIdentitySurvivesExecutionProfileUpdate(t *testing.T) {
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gateway := events.NewGateway(l)
	first, err := NewWithModel(gateway, describedModel{}).Submit(context.Background(), Submit{RequestID: "identity-before-profile", OrganizationID: "org-1", Statement: "echo before", Kind: core.ExecutionDeterministic})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewWithModel(gateway, replacementModel{}).Submit(context.Background(), Submit{RequestID: "identity-after-profile", OrganizationID: "org-1", Statement: "summarize after", Kind: core.ExecutionAgent})
	if err != nil {
		t.Fatal(err)
	}
	if first.Task.AssigneeID == "" || second.Task.AssigneeID != first.Task.AssigneeID {
		t.Fatalf("execution-profile update split durable Agent identity: before=%+v after=%+v", first.Task, second.Task)
	}
	if first.Task.AgentConfig == nil || second.Task.AgentConfig == nil || first.Task.AgentConfig.ProfileID == second.Task.AgentConfig.ProfileID {
		t.Fatalf("Tasks did not pin distinct execution-profile revisions: before=%+v after=%+v", first.Task.AgentConfig, second.Task.AgentConfig)
	}
	snapshot, err := projections.New(gateway).Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Agents) != 1 || len(snapshot.ExecutionProfiles) != 2 || snapshot.Agents[first.Task.AssigneeID].Value.ExecutionProfileID != second.Task.AgentConfig.ProfileID {
		t.Fatalf("Agent history or current profile binding is inconsistent: %+v", snapshot)
	}
	if snapshot.Tasks[first.Task.ID].Value.AgentConfig.ProfileID != first.Task.AgentConfig.ProfileID {
		t.Fatal("earlier Task lost its pinned execution-profile revision")
	}
}

func TestExecutionManifestUsesTaskPinnedRuntimeAfterAgentRebind(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gateway := events.NewGateway(l)
	repository := projections.New(gateway)
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1"}
	const correlationID = "pinned-runtime"
	if err := repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", correlationID, 1, organization, nil); err != nil {
		t.Fatal(err)
	}
	agent := seedTestAgents(t, ctx, repository, correlationID, organization.ID, execution.FakeModel{}.Descriptor(), "agent-1")[0]
	intent := acceptedTestIntent("intent-1", organization.ID, "summarize")
	work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: "ACTIVE"}
	task := core.Task{ID: "task-pinned-runtime", WorkID: work.ID, Description: "summarize", AcceptanceCriteria: intent.CompletionCriteria, ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: testAgentConfig(agent), TaskContractVersion: "1", Status: core.TaskPending}
	bindTestAgentExecutionBriefs(t, correlationID, intent, &task)
	for _, save := range []func() error{
		func() error {
			return repository.SaveIntent(ctx, "INTENT_CREATED", "runtime", correlationID, 1, intent, nil)
		},
		func() error {
			return repository.SaveWork(ctx, organization.ID, "WORK_CREATED", "runtime", correlationID, 1, work, nil)
		},
		func() error {
			return repository.SaveTask(ctx, organization.ID, "TASK_CREATED", "runtime", correlationID, 1, task, nil)
		},
	} {
		if err := save(); err != nil {
			t.Fatal(err)
		}
	}
	if err := saveTestPlan(ctx, gateway, correlationID, intent, task); err != nil {
		t.Fatal(err)
	}
	agent.RuntimeAdapter = "new-runtime"
	if err := repository.SaveAgent(ctx, "AGENT_CONFIGURATION_UPDATED", "runtime", correlationID, 2, agent, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := New(gateway).Recover(ctx); err != nil {
		t.Fatal(err)
	}
	stream, err := gateway.Events(ctx, correlationID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range stream {
		if event.EventType != "EXECUTION_CONTEXT_MANIFESTED" {
			continue
		}
		var manifest core.ExecutionContextManifest
		if err := json.Unmarshal(event.Payload, &manifest); err != nil {
			t.Fatal(err)
		}
		if manifest.RuntimeAdapter != task.AgentConfig.RuntimeAdapter || manifest.RuntimeAdapter == agent.RuntimeAdapter {
			t.Fatalf("manifest did not preserve the Task-pinned runtime: %+v", manifest)
		}
		return
	}
	t.Fatal("execution manifest was not recorded")
}

func TestHumanReviewerFinalizesExactModelCandidate(t *testing.T) {
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	service := NewWithModel(events.NewGateway(l), describedModel{})
	submission := Submit{RequestID: "review-approve", OrganizationID: "org-1", Statement: "summarize", Kind: core.ExecutionAgent}
	submitted, err := service.Submit(context.Background(), submission)
	if err != nil || submitted.Task.Status != core.TaskBlocked {
		t.Fatalf("submitted=%+v err=%v", submitted, err)
	}
	view, found, err := service.CompletionReview(context.Background(), "org-1", string(submitted.Task.ID))
	if err != nil || !found || !strings.HasPrefix(view.Result, "configured-model: Operate only as this runtime-selected durable Agent blueprint.") || !strings.Contains(view.Result, `"objective":"summarize"`) || len(view.Request.EvidenceRefs) != 3 {
		t.Fatalf("review=%+v found=%t err=%v", view, found, err)
	}
	stream, err := service.Events(context.Background(), submitted.Events[0].CorrelationID)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("rejects substituted result artifacts", func(t *testing.T) {
		tampered := append([]events.Event(nil), stream...)
		for index := range tampered {
			if tampered[index].EventID != view.Request.EvidenceRefs[1] {
				continue
			}
			tampered[index].ArtifactRefs = []string{"artifact-substituted"}
			tampered[index].Payload, err = json.Marshal(events.ResultPublishedPayload{Summary: view.Result, ArtifactRefs: tampered[index].ArtifactRefs})
			if err != nil {
				t.Fatal(err)
			}
		}
		if _, _, err := reviewEvidence(tampered, view.Request); err == nil {
			t.Fatal("completion review accepted result artifacts that differ from the tool outcome")
		}
	})
	t.Run("rejects reordered evidence", func(t *testing.T) {
		tampered := append([]events.Event(nil), stream...)
		var resultIndex, candidateIndex int
		for index := range tampered {
			switch tampered[index].EventID {
			case view.Request.EvidenceRefs[1]:
				resultIndex = index
			case view.Request.EvidenceRefs[2]:
				candidateIndex = index
			}
		}
		tampered[resultIndex].Sequence, tampered[candidateIndex].Sequence = tampered[candidateIndex].Sequence, tampered[resultIndex].Sequence
		if _, _, err := reviewEvidence(tampered, view.Request); err == nil {
			t.Fatal("completion review accepted reordered evidence")
		}
	})
	decided, err := service.ReviewCompletion(context.Background(), reviewInput(view, completion.ReviewApprove, ""))
	if err != nil || decided.Decision != completion.ReviewApprove {
		t.Fatalf("decided=%+v err=%v", decided, err)
	}
	replayed, err := service.Submit(context.Background(), submission)
	if err != nil || replayed.Task.Status != core.TaskCompleted || replayed.Work.Status != "COMPLETED" || !replayed.Completion.Complete {
		t.Fatalf("reviewed replay=%+v err=%v", replayed, err)
	}
	if replayed.Outcome.PostconditionStatus != core.PostconditionNotChecked {
		t.Fatalf("human judgment was rewritten as deterministic evidence: %+v", replayed.Outcome)
	}
	assertEventOrder(t, replayed.Events, "COMPLETION_REVIEW_REQUESTED", "COMPLETION_REVIEW_DECIDED", "COMPLETION_VERIFIED", "TASK_VERIFIED_COMPLETE")
	for _, event := range replayed.Events {
		if event.EventType != "COMPLETION_REVIEW_DECIDED" {
			continue
		}
		if event.SourceActorID != "reviewer-1" || event.SourceExecutionID != "" {
			t.Fatalf("review authority envelope=%+v", event)
		}
	}

	snapshot, err := service.state.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	allEvents, err := service.Events(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	workState := snapshot.Works[replayed.Work.ID]
	intentState := snapshot.Intents[replayed.Intent.ID]
	binding := events.WorkCompletionBinding{
		OrganizationID: "org-1", CorrelationID: submitted.Events[0].CorrelationID,
		Work: workState.Value, WorkVersion: workState.Version, Intent: intentState.Value,
		AgentBlueprints: map[core.ID]core.AgentBlueprint{
			replayed.Task.AgentConfig.BlueprintID: snapshot.AgentBlueprints[replayed.Task.AgentConfig.BlueprintID].Value,
		},
		ExecutionProfiles: map[core.ID]core.ExecutionProfile{
			replayed.Task.AgentConfig.ProfileID: snapshot.ExecutionProfiles[replayed.Task.AgentConfig.ProfileID].Value,
		},
	}
	for _, state := range snapshot.Tasks {
		if state.Value.WorkID == replayed.Work.ID {
			binding.Tasks = append(binding.Tasks, events.WorkCompletionTaskBinding{Task: state.Value, Version: state.Version, CorrelationID: state.CorrelationID})
		}
	}
	var evidenceEvent events.Event
	for _, event := range allEvents {
		if event.EventType == "WORK_COMPLETION_EVALUATED" && event.CorrelationID == binding.CorrelationID {
			evidenceEvent = event
		}
	}
	forgedRequest, err := completion.NewReviewRequest(
		view.Request.OrganizationID, view.Request.TaskID, view.Request.TaskVersion, "different work",
		view.Request.Contract, view.Request.EvidenceRefs, view.Request.CreatedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	forged := append([]events.Event(nil), allEvents...)
	for index := range forged {
		switch forged[index].EventType {
		case "COMPLETION_REVIEW_REQUESTED":
			if forged[index].TaskID == string(view.Request.TaskID) {
				forged[index].Payload, err = json.Marshal(forgedRequest)
			}
		case "COMPLETION_REVIEW_DECIDED":
			if forged[index].TaskID == string(view.Request.TaskID) {
				var review completion.HumanReview
				if err = json.Unmarshal(forged[index].Payload, &review); err == nil {
					review.Fingerprint = forgedRequest.Fingerprint
					forged[index].Payload, err = json.Marshal(review)
				}
			}
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := events.ValidateWorkCompletionEvidenceChain(binding, evidenceEvent, forged); err == nil {
		t.Fatal("review judgment over a substituted Task objective authorized Work completion")
	}
}

func TestCompletionReviewRejectsExternalAgentAndStaleEvidence(t *testing.T) {
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	service := NewWithModel(events.NewGateway(l), describedModel{})
	submitted, err := service.Submit(context.Background(), Submit{RequestID: "review-stale", OrganizationID: "org-1", Statement: "summarize", Kind: core.ExecutionAgent})
	if err != nil {
		t.Fatal(err)
	}
	view, found, err := service.CompletionReview(context.Background(), "org-1", string(submitted.Task.ID))
	if err != nil || !found {
		t.Fatalf("review found=%t err=%v", found, err)
	}
	external := reviewInput(view, completion.ReviewApprove, "")
	external.ReviewerKind = core.PrincipalExternalAgent
	external.SourceChannel = "A2A"
	if _, err := service.ReviewCompletion(context.Background(), external); err == nil {
		t.Fatal("external Agent finalized model completion")
	}
	stale := reviewInput(view, completion.ReviewApprove, "")
	stale.Fingerprint = strings.Repeat("0", 64)
	if _, err := service.ReviewCompletion(context.Background(), stale); err == nil {
		t.Fatal("stale completion evidence was accepted")
	}
	stream, err := service.Events(context.Background(), submitted.Events[0].CorrelationID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range stream {
		if event.EventType == "COMPLETION_REVIEW_DECIDED" {
			t.Fatal("rejected decision reached the ledger")
		}
	}
}

func TestCompletionReviewSelectsExactTaskInSharedDAGStream(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gateway := events.NewGateway(l)
	correlationID, err := gateway.ReserveExternalWork(ctx, "org-1", "two-reviews")
	if err != nil {
		t.Fatal(err)
	}
	repository := projections.New(gateway)
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1"}
	if err := repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", correlationID, 1, organization, nil); err != nil {
		t.Fatal(err)
	}
	agent := seedTestAgents(t, ctx, repository, correlationID, organization.ID, describedModel{}.Descriptor(), "agent-1")[0]
	intent := core.Intent{ID: "intent-1", OrganizationID: organization.ID, OriginalInstruction: "draft two independent updates", NormalizedObjective: "draft two independent updates"}
	work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: "ACTIVE"}
	first := core.Task{ID: "task-1", WorkID: work.ID, Description: "Draft the security update.", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: testAgentConfig(agent), TaskContractVersion: "1", Status: core.TaskPending}
	second := core.Task{ID: "task-2", WorkID: work.ID, Description: "Draft the release update.", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: testAgentConfig(agent), TaskContractVersion: "1", Status: core.TaskPending}
	for _, save := range []func() error{
		func() error {
			return repository.SaveIntent(ctx, "INTENT_CREATED", "runtime", correlationID, 1, intent, nil)
		},
		func() error {
			return repository.SaveWork(ctx, organization.ID, "WORK_CREATED", "runtime", correlationID, 1, work, nil)
		},
		func() error {
			return repository.SaveTask(ctx, organization.ID, "TASK_CREATED", "runtime", correlationID, 1, first, nil)
		},
		func() error {
			return repository.SaveTask(ctx, organization.ID, "TASK_CREATED", "runtime", correlationID, 1, second, nil)
		},
	} {
		if err := save(); err != nil {
			t.Fatal(err)
		}
	}
	service := NewWithModel(gateway, describedModel{})
	if recovered, err := service.Recover(ctx); err != nil || recovered.TasksExecuted != 2 {
		t.Fatalf("recovery=%+v err=%v", recovered, err)
	}
	firstReview, found, err := service.CompletionReview(ctx, "org-1", string(first.ID))
	if err != nil || !found || firstReview.Request.TaskID != first.ID || firstReview.Request.Objective != first.Description {
		t.Fatalf("first review=%+v found=%t err=%v", firstReview, found, err)
	}
	secondReview, found, err := service.CompletionReview(ctx, "org-1", string(second.ID))
	if err != nil || !found || secondReview.Request.TaskID != second.ID || secondReview.Request.Objective != second.Description {
		t.Fatalf("second review=%+v found=%t err=%v", secondReview, found, err)
	}
}

func TestRecoveryPreservesPreReviewContractAsBlocked(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gateway := events.NewGateway(l)
	repository := projections.New(gateway)
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1"}
	intent := core.Intent{ID: "intent-1", OrganizationID: organization.ID, OriginalInstruction: "legacy model work", NormalizedObjective: "legacy model work"}
	work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: "ACTIVE"}
	task := core.Task{ID: "task-1", WorkID: work.ID, Description: "legacy model work", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, TaskContractVersion: "1", Status: core.TaskBlocked}
	for _, save := range []func() error{
		func() error {
			return repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "legacy", 1, organization, nil)
		},
		func() error { return repository.SaveIntent(ctx, "INTENT_CREATED", "runtime", "legacy", 1, intent, nil) },
		func() error {
			return repository.SaveWork(ctx, organization.ID, "WORK_CREATED", "runtime", "legacy", 1, work, nil)
		},
		func() error {
			return repository.SaveTask(ctx, organization.ID, "TASK_BLOCKED", "runtime", "legacy", 1, task, blockedDetail("legacy review event", "manual reconciliation", "the old payload is not completion authority"))
		},
	} {
		if err := save(); err != nil {
			t.Fatal(err)
		}
	}
	legacy := completionDetail{Contract: core.CompletionContract{TaskID: task.ID, TaskVersion: 1}, Result: completion.Result{Complete: false, Reasons: []string{"independent review required"}}}
	if _, err := gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "COMPLETION_REVIEW_REQUIRED", SourceActorID: "runtime", TaskID: string(task.ID), Payload: legacy, CorrelationID: "legacy"}); err != nil {
		t.Fatal(err)
	}
	recovered, err := NewWithModel(gateway, describedModel{}).Recover(ctx)
	if err != nil || recovered.BlockedPreserved != 1 || recovered.TasksExecuted != 0 {
		t.Fatalf("legacy recovery=%+v err=%v", recovered, err)
	}
}

func TestCompletionReviewRevisionIsUntrustedExecutionContext(t *testing.T) {
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	service := NewWithModel(events.NewGateway(l), describedModel{})
	submission := Submit{RequestID: "review-revise", OrganizationID: "org-1", Statement: "summarize", Kind: core.ExecutionAgent}
	submitted, err := service.Submit(context.Background(), submission)
	if err != nil {
		t.Fatal(err)
	}
	first, found, err := service.CompletionReview(context.Background(), "org-1", string(submitted.Task.ID))
	if err != nil || !found {
		t.Fatalf("review found=%t err=%v", found, err)
	}
	if _, err := service.ReviewCompletion(context.Background(), reviewInput(first, completion.ReviewRevise, "Make the conclusion specific.")); err != nil {
		t.Fatal(err)
	}
	second, found, err := service.CompletionReview(context.Background(), "org-1", string(submitted.Task.ID))
	if err != nil || !found || second.Request.ID == first.Request.ID || !strings.Contains(second.Result, "Make the conclusion specific.") {
		t.Fatalf("revised review=%+v found=%t err=%v", second, found, err)
	}
	stream, err := service.Events(context.Background(), submitted.Events[0].CorrelationID)
	if err != nil {
		t.Fatal(err)
	}
	var decisionRef string
	for _, event := range stream {
		if event.EventType == "COMPLETION_REVIEW_DECIDED" {
			decisionRef = event.EventID
		}
	}
	manifested := false
	for _, event := range stream {
		if event.EventType != "EXECUTION_CONTEXT_MANIFESTED" {
			continue
		}
		var manifest core.ExecutionContextManifest
		if json.Unmarshal(event.Payload, &manifest) == nil {
			for _, ref := range manifest.EventRefs {
				if ref == decisionRef {
					manifested = true
				}
			}
		}
	}
	if !manifested {
		t.Fatal("revision decision was not referenced by the next execution manifest")
	}
}

func TestRecoveryFinishesDurableCompletionReviewDecision(t *testing.T) {
	path := filepath.Join(t.TempDir(), "review-recovery.db")
	l, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	gateway := events.NewGateway(l)
	service := NewWithModel(gateway, describedModel{})
	submitted, err := service.Submit(context.Background(), Submit{RequestID: "review-recovery", OrganizationID: "org-1", Statement: "summarize", Kind: core.ExecutionAgent})
	if err != nil {
		t.Fatal(err)
	}
	view, found, err := service.CompletionReview(context.Background(), "org-1", string(submitted.Task.ID))
	if err != nil || !found {
		t.Fatalf("review found=%t err=%v", found, err)
	}
	review := completion.HumanReview{
		ReviewID: view.Request.ID, OrganizationID: view.Request.OrganizationID, TaskID: view.Request.TaskID,
		TaskVersion: view.Request.TaskVersion, Fingerprint: view.Request.Fingerprint,
		Decision: completion.ReviewApprove, ReviewerID: "reviewer-1", Method: core.AssuranceHumanJudgment,
		EvidenceRefs: append([]string(nil), view.Request.EvidenceRefs...), DecidedAt: time.Now().UTC(),
	}
	if _, err := gateway.PublishTrusted(context.Background(), events.TrustedDraft{
		OrganizationID: "org-1", EventType: "COMPLETION_REVIEW_DECIDED", SourceActorID: "reviewer-1",
		TaskID: string(submitted.Task.ID), Payload: review, CorrelationID: submitted.Events[0].CorrelationID,
	}); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	recovered := NewWithModel(events.NewGateway(reopened), describedModel{})
	if _, err := recovered.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	replayed, err := recovered.Submit(context.Background(), Submit{RequestID: "review-recovery", OrganizationID: "org-1", Statement: "summarize", Kind: core.ExecutionAgent})
	if err != nil || replayed.Task.Status != core.TaskCompleted || !replayed.Completion.Complete {
		t.Fatalf("recovered=%+v err=%v", replayed, err)
	}
}

func reviewInput(view CompletionReviewView, decision completion.ReviewDecision, feedback string) CompletionReviewInput {
	return CompletionReviewInput{
		OrganizationID: string(view.Request.OrganizationID), TaskID: string(view.Request.TaskID),
		ReviewID: string(view.Request.ID), Fingerprint: view.Request.Fingerprint,
		Decision: decision, ReviewerID: "reviewer-1", ReviewerKind: core.PrincipalHuman,
		SourceChannel: "HUMAN_DIRECT", Feedback: feedback,
	}
}

func TestAcceptedIntentCriteriaBecomeIndependentReviewCriteria(t *testing.T) {
	accepted := []core.IntentValue{
		{Value: "Binary starts on supported Linux", Origin: "EXPLICIT", SourceMessageID: "message-1"},
		{Value: "Checksums match the release archive", Origin: "CONFIRMED", SourceMessageID: "message-2"},
	}
	criteria := core.ReviewedOutcomeCompletionContract("task", 1, accepted).Criteria
	if len(criteria) != len(accepted) {
		t.Fatalf("criteria=%+v", criteria)
	}
	for index, criterion := range criteria {
		if criterion.Description != accepted[index].Value || criterion.Assurance != core.AssuranceHumanJudgment || !criterion.Required {
			t.Fatalf("criterion %d=%+v", index, criterion)
		}
	}
}

type indexedTaskLedger struct{}

func (indexedTaskLedger) Append(_ context.Context, draft events.TrustedDraft) (events.Event, error) {
	return events.Event{EventID: "event-1", OrganizationID: draft.OrganizationID, EventType: draft.EventType, TaskID: draft.TaskID, CorrelationID: draft.CorrelationID}, nil
}

func (indexedTaskLedger) Events(_ context.Context, correlationID string) ([]events.Event, error) {
	return []events.Event{{EventID: "event-1", OrganizationID: "org-1", EventType: "TASK_CREATED", TaskID: "task-1", CorrelationID: correlationID}}, nil
}

func (indexedTaskLedger) ResolveExternalWork(context.Context, string, string) (string, bool, error) {
	return "correlation-1", true, nil
}

func (indexedTaskLedger) ResolveExternalRequest(context.Context, string, string) (string, bool, error) {
	return "request-1", true, nil
}

func (indexedTaskLedger) ResolveExternalTask(context.Context, string, string) (string, string, bool, error) {
	return "request-1", "correlation-1", true, nil
}

func (indexedTaskLedger) ReserveExternalWork(context.Context, string, string) (string, error) {
	return "correlation-1", nil
}

func TestExternalTaskLookupUsesDurableIndex(t *testing.T) {
	service := New(events.NewGateway(indexedTaskLedger{}))
	requestID, stream, err := service.ExternalTaskEvents(context.Background(), "org-1", "task-1")
	if err != nil || requestID != "request-1" || len(stream) != 1 || stream[0].TaskID != "task-1" {
		t.Fatalf("request=%q stream=%+v err=%v", requestID, stream, err)
	}
}

func TestUnavailableDeterministicWorkIsRejectedBeforeExecution(t *testing.T) {
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := l.Close(); err != nil {
			t.Errorf("close ledger: %v", err)
		}
	})
	_, err = New(events.NewGateway(l)).Submit(context.Background(), Submit{RequestID: "rejected", OrganizationID: "org-1", Statement: "unsupported", Kind: core.ExecutionDeterministic})
	if err == nil || !strings.Contains(err.Error(), "no registered handler") {
		t.Fatalf("submit error=%v", err)
	}
	stream, err := l.Events(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range stream {
		if event.EventType == "TASK_CREATED" || event.EventType == "EXECUTION_STARTED" || event.EventType == "TOOL_OUTCOME_RECORDED" {
			t.Fatalf("unavailable deterministic operation crossed execution admission: %+v", event)
		}
	}
}

func TestRunTelemetryCoversDAG(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := l.Close(); err != nil {
			t.Errorf("close ledger: %v", err)
		}
	})
	gateway := events.NewGateway(l)
	repository := projections.New(gateway)
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1"}
	if err := repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "request-1", 1, organization, nil); err != nil {
		t.Fatal(err)
	}
	agent := seedTestAgents(t, ctx, repository, "request-1", organization.ID, execution.FakeModel{}.Descriptor(), "agent-1")[0]
	intent := acceptedTestIntent("intent-1", organization.ID, "two steps")
	work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: "two steps", Status: "ACTIVE"}
	first := core.Task{ID: "task-request-1-first", WorkID: work.ID, ParentID: "task-request-1", Description: "echo first", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: testAgentConfig(agent), TaskContractVersion: "1", Status: core.TaskPending}
	second := core.Task{ID: "task-request-1", WorkID: work.ID, Description: "echo second", AcceptanceCriteria: intent.CompletionCriteria, DependsOn: []core.ID{first.ID}, ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: testAgentConfig(agent), TaskContractVersion: "1", Status: core.TaskPending}
	if err := saveTestTaskGraph(ctx, repository, organization.ID, "request-1", intent, work, first, second); err != nil {
		t.Fatal(err)
	}
	if err := saveTestPlan(ctx, gateway, "request-1", intent, first, second); err != nil {
		t.Fatal(err)
	}
	recovery, err := New(gateway).Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.TasksExecuted != 2 {
		t.Fatalf("recovery=%+v", recovery)
	}
	stream, err := l.Events(ctx, "request-1")
	if err != nil {
		t.Fatal(err)
	}
	completedTasks := 0
	telemetryEvents := 0
	var run telemetry.Run
	for _, event := range stream {
		switch event.EventType {
		case "TASK_VERIFIED_COMPLETE":
			completedTasks++
		case "RUN_TELEMETRY_RECORDED":
			telemetryEvents++
			if err := json.Unmarshal(event.Payload, &run); err != nil {
				t.Fatal(err)
			}
		}
	}
	if completedTasks != 2 || telemetryEvents != 1 || run.Outcome != "VERIFIED_COMPLETE" || len(run.ExecutionMechanisms) != 1 || run.ExecutionMechanisms[0].Count != 2 {
		t.Fatalf("completed=%d telemetry=%d run=%+v", completedTasks, telemetryEvents, run)
	}
}

func TestRecoverExecutesPersistedPendingWorkAndPreservesIdentity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agentos.db")
	l, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	g := events.NewGateway(l)
	repository := projections.New(g)
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1"}
	if err := repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "request-1", 1, organization, nil); err != nil {
		t.Fatal(err)
	}
	agent := seedTestAgents(t, ctx, repository, "request-1", organization.ID, execution.FakeModel{}.Descriptor(), "agent-1")[0]
	intent := acceptedTestIntent("intent-1", organization.ID, "echo after restart")
	work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: "echo after restart", Status: "ACTIVE"}
	first := core.Task{ID: "task-request-1-first", WorkID: work.ID, ParentID: "task-request-1", Description: "echo already done", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: testAgentConfig(agent), TaskContractVersion: "1", Status: core.TaskPending}
	second := core.Task{ID: "task-request-1", WorkID: work.ID, Description: "echo after restart", AcceptanceCriteria: intent.CompletionCriteria, ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, DependsOn: []core.ID{first.ID}, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: testAgentConfig(agent), TaskContractVersion: "1", Status: core.TaskPending}
	if err := saveTestTaskGraph(ctx, repository, organization.ID, "request-1", intent, work, first, second); err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	if err := saveTestPlan(ctx, g, "request-1", intent, first, second); err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	if err := saveTestVerifiedTask(ctx, g, repository, organization.ID, "request-1", projections.Versioned[core.Task]{Version: 1, CorrelationID: "request-1", Value: first}); err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l, err = ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	service := New(events.NewGateway(l))
	recovery, err := service.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.PendingFound != 1 || recovery.TasksExecuted != 1 {
		t.Fatalf("recovery=%+v", recovery)
	}
	snapshot, err := projections.New(events.NewGateway(l)).Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Agents[agent.ID].Value != agent {
		t.Fatalf("agent identity changed across restart: %+v", snapshot.Agents[agent.ID].Value)
	}
	if snapshot.Tasks[second.ID].Value.Status != core.TaskCompleted || snapshot.Works[work.ID].Value.Status != "COMPLETED" {
		t.Fatalf("pending work not recovered: task=%+v work=%+v", snapshot.Tasks[second.ID].Value, snapshot.Works[work.ID].Value)
	}
	stream, err := l.Events(ctx, "request-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range stream {
		if event.EventType == "EXECUTION_CONTEXT_MANIFESTED" {
			t.Fatal("deterministic work owned by a durable agent created model execution context")
		}
	}
}

func TestDispatchFailsClosedWhenDurableRosterEligibilityChanges(t *testing.T) {
	tests := map[string]func(*testing.T, context.Context, *projections.Repository, projections.Snapshot){
		"inactive Agent": func(t *testing.T, ctx context.Context, repository *projections.Repository, snapshot projections.Snapshot) {
			state := snapshot.Agents["agent-1"]
			agent := state.Value
			agent.Status = "INACTIVE"
			if err := repository.SaveAgent(ctx, "AGENT_DEACTIVATED", "runtime", "request-1", state.Version+1, agent, nil); err != nil {
				t.Fatal(err)
			}
		},
		"inactive blueprint": func(t *testing.T, ctx context.Context, repository *projections.Repository, snapshot projections.Snapshot) {
			state := snapshot.AgentBlueprints["blueprint-test-org-1"]
			blueprint := state.Value
			blueprint.Status = "INACTIVE"
			if err := repository.SaveAgentBlueprint(ctx, "AGENT_BLUEPRINT_UPDATED", "runtime", "request-1", state.Version+1, blueprint, nil); err != nil {
				t.Fatal(err)
			}
		},
		"inactive execution profile": func(t *testing.T, ctx context.Context, repository *projections.Repository, snapshot projections.Snapshot) {
			state := snapshot.ExecutionProfiles["profile-test-org-1"]
			profile := state.Value
			profile.Status = "INACTIVE"
			if err := repository.SaveExecutionProfile(ctx, "EXECUTION_PROFILE_UPDATED", "runtime", "request-1", state.Version+1, profile, nil); err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			l, err := ledger.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = l.Close() })
			gateway := events.NewGateway(l)
			repository := projections.New(gateway)
			organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1"}
			if err := repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "request-1", 1, organization, nil); err != nil {
				t.Fatal(err)
			}
			agent := seedTestAgents(t, ctx, repository, "request-1", organization.ID, execution.FakeModel{}.Descriptor(), "agent-1")[0]
			intent := core.Intent{ID: "intent-1", OrganizationID: organization.ID, OriginalInstruction: "bounded work", NormalizedObjective: "bounded work"}
			work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: "ACTIVE"}
			task := core.Task{ID: "task-request-1", WorkID: work.ID, Description: "bounded work", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: testAgentConfig(agent), TaskContractVersion: "1", Status: core.TaskPending}
			for _, save := range []func() error{
				func() error {
					return repository.SaveIntent(ctx, "INTENT_CREATED", "runtime", "request-1", 1, intent, nil)
				},
				func() error {
					return repository.SaveWork(ctx, organization.ID, "WORK_CREATED", "runtime", "request-1", 1, work, nil)
				},
				func() error {
					return repository.SaveTask(ctx, organization.ID, "TASK_CREATED", "runtime", "request-1", 1, task, nil)
				},
			} {
				if err := save(); err != nil {
					t.Fatal(err)
				}
			}
			snapshot, err := repository.Load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			mutate(t, ctx, repository, snapshot)

			if _, err := New(gateway).Recover(ctx); err != nil {
				t.Fatal(err)
			}
			snapshot, err = repository.Load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.Tasks[task.ID].Value.Status != core.TaskBlocked {
				t.Fatalf("ineligible assignment executed: %+v", snapshot.Tasks[task.ID].Value)
			}
			stream, err := gateway.Events(ctx, "request-1")
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range stream {
				if event.EventType == "EXECUTION_STARTED" || event.EventType == "EXECUTION_CONTEXT_MANIFESTED" || event.EventType == "TOOL_OUTCOME_RECORDED" {
					t.Fatalf("ineligible assignment crossed the execution boundary: %+v", event)
				}
			}
		})
	}
}

func TestRecoverResumesOnlyRevalidatedAssignmentBlock(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gateway := events.NewGateway(l)
	repository := projections.New(gateway)
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1"}
	if err := repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "assignment-resume", 1, organization, nil); err != nil {
		t.Fatal(err)
	}
	agent := seedTestAgents(t, ctx, repository, "assignment-resume", organization.ID, execution.FakeModel{}.Descriptor(), "agent-1")[0]
	intent := acceptedTestIntent("intent-1", organization.ID, "echo resumed")
	work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: "ACTIVE"}
	task := core.Task{ID: "task-assignment-resume", WorkID: work.ID, Description: "echo resumed", AcceptanceCriteria: intent.CompletionCriteria, ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: testAgentConfig(agent), TaskContractVersion: "1", Status: core.TaskPending}
	for _, save := range []func() error{
		func() error {
			return repository.SaveIntent(ctx, "INTENT_CREATED", "runtime", "assignment-resume", 1, intent, nil)
		},
		func() error {
			return repository.SaveWork(ctx, organization.ID, "WORK_CREATED", "runtime", "assignment-resume", 1, work, nil)
		},
		func() error {
			return repository.SaveTask(ctx, organization.ID, "TASK_CREATED", "runtime", "assignment-resume", 1, task, nil)
		},
	} {
		if err := save(); err != nil {
			t.Fatal(err)
		}
	}
	if err := saveTestPlan(ctx, gateway, "assignment-resume", intent, task); err != nil {
		t.Fatal(err)
	}
	agent.Status = "INACTIVE"
	if err := repository.SaveAgent(ctx, "AGENT_DEACTIVATED", "runtime", "assignment-resume", 2, agent, nil); err != nil {
		t.Fatal(err)
	}
	service := New(gateway)
	if _, err := service.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := repository.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Tasks[task.ID].Value.Status != core.TaskBlocked {
		t.Fatalf("ineligible assignment was not blocked: %+v", snapshot.Tasks[task.ID].Value)
	}
	agent.Status = "ACTIVE"
	if err := repository.SaveAgent(ctx, "AGENT_REACTIVATED", "runtime", "assignment-resume", 3, agent, nil); err != nil {
		t.Fatal(err)
	}
	recovered, err := service.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.TasksExecuted != 1 {
		t.Fatalf("revalidated assignment did not resume exactly once: %+v", recovered)
	}
	snapshot, err = repository.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Tasks[task.ID].Value.Status != core.TaskCompleted {
		t.Fatalf("revalidated task did not complete: %+v", snapshot.Tasks[task.ID].Value)
	}
	stream, err := gateway.Events(ctx, "assignment-resume")
	if err != nil {
		t.Fatal(err)
	}
	assertEventOrder(t, stream, "TASK_BLOCKED", "TASK_ASSIGNMENT_REVALIDATED", "EXECUTION_STARTED", "TASK_VERIFIED_COMPLETE")
}

func TestRecoverTerminalizesAssignmentBlockAfterStrategicDrift(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gateway := events.NewGateway(l)
	repository := projections.New(gateway)
	seedTestGoal(t, ctx, repository, "org-1", "mission-1", "goal-1", core.GoalActive)
	agent := seedTestAgents(t, ctx, repository, "assignment-strategic-drift", "org-1", execution.FakeModel{}.Descriptor(), "agent-1")[0]
	service := New(gateway)
	in := confirmedGoalSubmit(t, ctx, gateway, "assignment-strategic-drift", "org-1", "goal-1", "echo governed", core.ExecutionDeterministic)
	in.correlationID, err = gateway.ReserveExternalWork(ctx, "org-1", "assignment-strategic-drift")
	if err != nil {
		t.Fatal(err)
	}
	_, work, task, err := service.ensureSubmission(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	agent.Status = "INACTIVE"
	if err := repository.SaveAgent(ctx, "AGENT_DEACTIVATED", "runtime", "assignment-strategic-drift", 2, agent, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := repository.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Tasks[task.ID].Value.Status != core.TaskBlocked {
		t.Fatalf("ineligible strategic Task was not assignment-blocked: %+v", snapshot.Tasks[task.ID].Value)
	}
	goalState := snapshot.Goals["goal-1"]
	goal := goalState.Value
	goal.Status = core.GoalPaused
	if err := repository.SaveGoal(ctx, "GOAL_PAUSED", "runtime", goalState.CorrelationID, goalState.Version+1, goal, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err = repository.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Tasks[task.ID].Value.Status != core.TaskFailed || snapshot.Works[work.ID].Value.Status != core.WorkFailed {
		t.Fatalf("strategic drift left assignment-blocked Work active: task=%+v work=%+v", snapshot.Tasks[task.ID].Value, snapshot.Works[work.ID].Value)
	}
}

func TestAssignmentBlockedDependencyWaitsForRevalidation(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gateway := events.NewGateway(l)
	repository := projections.New(gateway)
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1"}
	const correlationID = "assignment-dag-resume"
	if err := repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", correlationID, 1, organization, nil); err != nil {
		t.Fatal(err)
	}
	agent := seedTestAgents(t, ctx, repository, correlationID, organization.ID, execution.FakeModel{}.Descriptor(), "agent-1")[0]
	intent := acceptedTestIntent("intent-1", organization.ID, "echo resumed DAG")
	work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: "ACTIVE"}
	root := core.Task{ID: "task-" + correlationID, WorkID: work.ID, Description: "echo aggregate", AcceptanceCriteria: intent.CompletionCriteria, ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: testAgentConfig(agent), DependsOn: []core.ID{"task-child"}, TaskContractVersion: "1", Status: core.TaskPending}
	child := core.Task{ID: "task-" + correlationID + "-child", WorkID: work.ID, ParentID: root.ID, Description: "echo child", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: testAgentConfig(agent), TaskContractVersion: "1", Status: core.TaskPending}
	root.DependsOn = []core.ID{child.ID}
	if err := saveTestTaskGraph(ctx, repository, organization.ID, correlationID, intent, work, root, child); err != nil {
		t.Fatal(err)
	}
	if err := saveTestPlan(ctx, gateway, correlationID, intent, root, child); err != nil {
		t.Fatal(err)
	}
	agent.Status = "INACTIVE"
	if err := repository.SaveAgent(ctx, "AGENT_DEACTIVATED", "runtime", correlationID, 2, agent, nil); err != nil {
		t.Fatal(err)
	}
	service := New(gateway)
	if _, err := service.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := repository.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Tasks[child.ID].Value.Status != core.TaskBlocked || snapshot.Tasks[root.ID].Value.Status != core.TaskPending {
		t.Fatalf("assignment block escaped its recoverable boundary: child=%+v root=%+v", snapshot.Tasks[child.ID].Value, snapshot.Tasks[root.ID].Value)
	}

	agent.Status = "ACTIVE"
	if err := repository.SaveAgent(ctx, "AGENT_REACTIVATED", "runtime", correlationID, 3, agent, nil); err != nil {
		t.Fatal(err)
	}
	recovered, err := service.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.TasksExecuted != 2 {
		t.Fatalf("revalidated DAG did not resume exactly once per task: %+v", recovered)
	}
	snapshot, err = repository.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Tasks[child.ID].Value.Status != core.TaskCompleted || snapshot.Tasks[root.ID].Value.Status != core.TaskCompleted {
		t.Fatalf("revalidated DAG did not complete: child=%+v root=%+v", snapshot.Tasks[child.ID].Value, snapshot.Tasks[root.ID].Value)
	}
	stream, err := gateway.Events(ctx, correlationID)
	if err != nil {
		t.Fatal(err)
	}
	assertEventOrder(t, stream, "TASK_BLOCKED", "TASK_ASSIGNMENT_REVALIDATED", "TASK_VERIFIED_COMPLETE")
}

func TestUserWorkDoesNotBootstrapOrDependOnDefaultAgent(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gateway := events.NewGateway(l)
	service := New(gateway)

	first, err := service.Submit(ctx, Submit{RequestID: "seed-default", OrganizationID: "org-1", Statement: "echo seed", Kind: core.ExecutionDeterministic})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.state.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defaultState := snapshot.Agents[first.Task.AssigneeID]
	defaultAgent := defaultState.Value
	defaultAgent.Status = "INACTIVE"
	if err := service.state.SaveAgent(ctx, "AGENT_DEACTIVATED", "runtime", "deactivate-default", defaultState.Version+1, defaultAgent, nil); err != nil {
		t.Fatal(err)
	}

	userWork, err := service.Submit(ctx, Submit{RequestID: "user-only", OrganizationID: "org-1", Statement: "provide the launch date", Kind: core.ExecutionHuman})
	if err != nil {
		t.Fatal(err)
	}
	if userWork.Task.Status != core.TaskBlocked || userWork.Task.AssigneeType != "" || userWork.Task.AssigneeID != "" {
		t.Fatalf("user work depended on an Agent assignment: %+v", userWork.Task)
	}
	snapshot, err = service.state.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Agents) != 1 || snapshot.Agents[first.Task.AssigneeID].Value.Status != "INACTIVE" {
		t.Fatalf("user work changed the deliberate roster state: %+v", snapshot.Agents)
	}

	alternative := defaultState.Value
	alternative.ID = "agent-alternative"
	if err := service.state.SaveAgent(ctx, "AGENT_CREATED", "runtime", "alternative", 1, alternative, nil); err != nil {
		t.Fatal(err)
	}
	agentWork, err := service.Submit(ctx, Submit{RequestID: "use-alternative", OrganizationID: "org-1", Statement: "summarize the launch", Kind: core.ExecutionAgent})
	if err != nil {
		t.Fatal(err)
	}
	if agentWork.Task.AssigneeID != alternative.ID {
		t.Fatalf("eligible alternative Agent was not selected: %+v", agentWork.Task)
	}
}

func TestTaskPersistenceFailurePreventsExecutionVisibility(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	store := failTaskProjection{SQLite: l}
	_, err = New(events.NewGateway(store)).Submit(ctx, Submit{RequestID: "request-1", OrganizationID: "org-1", Statement: "echo must not run", Kind: core.ExecutionDeterministic})
	if err == nil || !errors.Is(err, errProjectionWrite) {
		t.Fatalf("submit error=%v", err)
	}
	stream, readErr := l.Events(ctx, "request-1")
	if readErr != nil {
		t.Fatal(readErr)
	}
	for _, event := range stream {
		if event.EventType == "EXECUTION_STARTED" || event.EventType == "TOOL_OUTCOME_RECORDED" {
			t.Fatalf("unpersisted task became executable: %+v", event)
		}
	}
	records, err := l.Records(ctx, projections.KindTask, "")
	if err != nil || len(records) != 0 {
		t.Fatalf("task projection became visible: records=%d err=%v", len(records), err)
	}
	recovery, err := New(events.NewGateway(l)).Recover(ctx)
	if err != nil || recovery.PlansMaterialized != 1 || recovery.TasksExecuted != 1 {
		t.Fatalf("durable Plan recovery=%+v err=%v", recovery, err)
	}
	records, err = l.Records(ctx, projections.KindTask, "")
	if err != nil || len(records) != 3 {
		t.Fatalf("recovered task versions=%d err=%v", len(records), err)
	}
}

type recoveryPlanner struct{ calls int }

func (*recoveryPlanner) Descriptor() (planning.Descriptor, bool) {
	return planning.Descriptor{PromptVersion: "recovery-test-v1", Provider: "fake", Model: "fake-model/v1", ExecutionProfileVersion: "v1-fake"}, true
}

func (p *recoveryPlanner) Build(_ context.Context, input planning.Input, kind core.ExecutionKind) (planning.Result, error) {
	p.calls++
	usage := events.InferenceUsageRecordedPayload{Source: "test", Provider: "fake", Model: "fake-model/v1"}
	return planning.Result{Tasks: []core.PlanTask{{
		Key: "root", Description: input.Intent.Objective, ExecutionKind: kind,
		ModelInferencePolicy: core.InferenceAllowed, DependsOn: []string{},
	}}, Usage: &usage}, nil
}

type failingPlanningPlanner struct{ calls int }

func (*failingPlanningPlanner) Descriptor() (planning.Descriptor, bool) {
	return planning.Descriptor{PromptVersion: "failure-test-v1", Provider: "fake", Model: "fake-model/v1", ExecutionProfileVersion: "v1-fake"}, true
}

func (p *failingPlanningPlanner) Build(context.Context, planning.Input, core.ExecutionKind) (planning.Result, error) {
	p.calls++
	cost := 0.01
	usage := events.InferenceUsageRecordedPayload{
		Source: "test", Provider: "fake", Model: "fake-model/v1",
		InputTokens: 4, OutputTokens: 1, TotalTokens: 5, CostUSD: &cost,
	}
	return planning.Result{Usage: &usage}, errors.New("planner returned unusable output")
}

func TestPlanningFailureDoesNotReplayAndRecordsTelemetryBeforeWorkFailure(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	planner := &failingPlanningPlanner{}
	service := NewWithModelAndPlanner(events.NewGateway(l), execution.FakeModel{}, planner)
	submission := Submit{RequestID: "planning-failure", OrganizationID: "org-1", Statement: "perform adaptive work", Kind: core.ExecutionAgent}
	if _, err := service.Submit(ctx, submission); err == nil {
		t.Fatal("failed planning attempt was accepted")
	}
	if _, err := service.Submit(ctx, submission); err == nil {
		t.Fatal("failed planning attempt retry was accepted")
	}
	if planner.calls != 1 {
		t.Fatalf("failed planning attempt was invoked %d times", planner.calls)
	}
	stream, err := service.ExternalEvents(ctx, submission.OrganizationID, submission.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	assertEventOrder(t, stream, "PLANNING_CONTEXT_MANIFESTED", "INFERENCE_USAGE_RECORDED", "PLANNING_FAILED", "RUN_TELEMETRY_RECORDED", "WORK_PLANNING_FAILED")
	counts := make(map[string]int)
	var run telemetry.Run
	var failureEventID string
	for _, event := range stream {
		counts[event.EventType]++
		switch event.EventType {
		case "PLANNING_FAILED":
			failureEventID = event.EventID
		case "RUN_TELEMETRY_RECORDED":
			if err := json.Unmarshal(event.Payload, &run); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, eventType := range []string{"PLANNING_CONTEXT_MANIFESTED", "PLANNING_FAILED", "RUN_TELEMETRY_RECORDED", "WORK_PLANNING_FAILED"} {
		if counts[eventType] != 1 {
			t.Fatalf("%s count=%d stream=%+v", eventType, counts[eventType], stream)
		}
	}
	if run.Outcome != "PLANNING_FAILED" || len(run.ModelUses) != 1 || run.ModelUses[0].TotalTokens != 5 || !slices.Contains(run.CompletionEvidenceEventRefs, failureEventID) {
		t.Fatalf("planning-failure telemetry=%+v failure_event=%s", run, failureEventID)
	}
	snapshot, err := service.state.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Works) != 1 || len(snapshot.Tasks) != 0 {
		t.Fatalf("failed planning works=%+v tasks=%+v", snapshot.Works, snapshot.Tasks)
	}
	for _, state := range snapshot.Works {
		if state.Value.Status != "FAILED" {
			t.Fatalf("failed planning state=%+v", state)
		}
	}
}

func TestDeterministicPlanningRejectionTerminalizesImmediately(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	service := New(events.NewGateway(l))
	submission := Submit{RequestID: "deterministic-rejection", OrganizationID: "org-1", Statement: "unsupported exact operation", Kind: core.ExecutionDeterministic}
	if _, err := service.Submit(ctx, submission); err == nil {
		t.Fatal("unsupported deterministic work was accepted")
	}
	if _, err := service.Submit(ctx, submission); err == nil {
		t.Fatal("terminal planning rejection was accepted on retry")
	}
	stream, err := service.ExternalEvents(ctx, submission.OrganizationID, submission.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	assertEventOrder(t, stream, "PLANNING_FAILED", "RUN_TELEMETRY_RECORDED", "WORK_PLANNING_FAILED")
	counts := make(map[string]int)
	var detail planningFailureDetail
	for _, event := range stream {
		counts[event.EventType]++
		if event.EventType == "WORK_PLANNING_FAILED" {
			var projection events.ProjectionEventPayload
			if json.Unmarshal(event.Payload, &projection) != nil || json.Unmarshal(projection.Detail, &detail) != nil {
				t.Fatal("invalid deterministic planning-failure contract")
			}
		}
	}
	if counts["PLANNING_FAILED"] != 1 || counts["RUN_TELEMETRY_RECORDED"] != 1 || counts["WORK_PLANNING_FAILED"] != 1 || detail.Code != "PLANNING_REJECTED" {
		t.Fatalf("planning events=%+v detail=%+v", counts, detail)
	}
}

func TestRecoveryFinishesRecordedPlanningFailureWithoutRewritingItsDecision(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	planner := &failingPlanningPlanner{}
	store := &failPlanningWorkProjection{SQLite: l}
	service := NewWithModelAndPlanner(events.NewGateway(store), execution.FakeModel{}, planner)
	submission := Submit{RequestID: "planning-failure-recovery", OrganizationID: "org-1", Statement: "perform adaptive work", Kind: core.ExecutionAgent}
	if _, err := service.Submit(ctx, submission); !errors.Is(err, errPlanningWorkProjection) {
		t.Fatalf("injected terminal projection error=%v", err)
	}
	stream, err := service.ExternalEvents(ctx, submission.OrganizationID, submission.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if !hasEventType(stream, "PLANNING_FAILED") || !hasEventType(stream, "RUN_TELEMETRY_RECORDED") || hasEventType(stream, "WORK_PLANNING_FAILED") {
		t.Fatalf("unexpected pre-recovery failure state=%+v", stream)
	}
	recoveryPlanner := &recoveryPlanner{}
	recovered := NewWithModelAndPlanner(events.NewGateway(l), execution.FakeModel{}, recoveryPlanner)
	if _, err := recovered.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if recoveryPlanner.calls != 0 {
		t.Fatalf("recovery replayed planning %d times", recoveryPlanner.calls)
	}
	stream, err = recovered.ExternalEvents(ctx, submission.OrganizationID, submission.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	counts := make(map[string]int)
	for _, event := range stream {
		counts[event.EventType]++
	}
	for _, eventType := range []string{"PLANNING_FAILED", "RUN_TELEMETRY_RECORDED", "WORK_PLANNING_FAILED"} {
		if counts[eventType] != 1 {
			t.Fatalf("%s count=%d stream=%+v", eventType, counts[eventType], stream)
		}
	}
}

func TestRecoveryDoesNotReplayInterruptedAdaptivePlanning(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gateway := events.NewGateway(l)
	work, draft := seedAcceptedWorkWithoutPlan(t, gateway, "planning-interrupted")
	contextEvent, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "PLANNING_CONTEXT_MANIFESTED", SourceActorID: "runtime",
		SourceExecutionID: "planning-plan-planning-interrupted-attempt-1", TaskID: "task-planning-interrupted", CorrelationID: "planning-interrupted",
		Payload: events.PlanningContextPayload{
			PlanID: "plan-planning-interrupted", IntentID: "intent-planning-interrupted", IntentFingerprint: draft.Fingerprint,
			PromptVersion: "recovery-test-v1", Provider: "fake", Model: "fake-model/v1", ExecutionProfileVersion: "v1-fake",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	planner := &recoveryPlanner{}
	service := NewWithModelAndPlanner(gateway, execution.FakeModel{}, planner)
	if _, err := service.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if planner.calls != 0 {
		t.Fatalf("interrupted adaptive planning was replayed %d times", planner.calls)
	}
	snapshot, err := service.state.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Works[work.ID].Value.Status != "FAILED" || len(snapshot.Tasks) != 0 {
		t.Fatalf("interrupted planning remained executable: work=%+v tasks=%+v", snapshot.Works[work.ID], snapshot.Tasks)
	}
	stream, err := gateway.Events(ctx, "planning-interrupted")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range stream {
		if event.EventType != "WORK_PLANNING_FAILED" {
			continue
		}
		var projection events.ProjectionEventPayload
		var detail planningFailureDetail
		if json.Unmarshal(event.Payload, &projection) != nil || json.Unmarshal(projection.Detail, &detail) != nil || detail.Code != "PLANNING_INTERRUPTED" || detail.EvidenceEventRef != contextEvent.EventID {
			t.Fatalf("planning failure evidence=%+v", detail)
		}
		found = true
	}
	if !found {
		t.Fatal("interrupted planning was not durably terminalized")
	}
}

func TestRecoveryResumesPlanningBeforeAnyAdaptiveTurn(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gateway := events.NewGateway(l)
	work, _ := seedAcceptedWorkWithoutPlan(t, gateway, "planning-safe-resume")
	planner := &recoveryPlanner{}
	service := NewWithModelAndPlanner(gateway, execution.FakeModel{}, planner)
	recovery, err := service.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if planner.calls != 1 || recovery.PlansMaterialized != 1 {
		t.Fatalf("safe planning recovery=%+v calls=%d", recovery, planner.calls)
	}
	snapshot, err := service.state.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Works[work.ID].Value.Status != "COMPLETED" || len(snapshot.Tasks) != 1 {
		t.Fatalf("safe planning recovery did not complete: work=%+v tasks=%+v", snapshot.Works[work.ID], snapshot.Tasks)
	}
}

func seedAcceptedWorkWithoutPlan(t *testing.T, gateway *events.Gateway, correlationID string) (core.Work, core.IntentDraft) {
	t.Helper()
	ctx := context.Background()
	draft := core.IntentDraft{
		ID: core.ID("intent-" + correlationID), OrganizationID: "org-1", Version: 1,
		Status: core.IntentStatusReadyForReview, Mode: core.IntentModeStandard, RequestedExecutionKind: core.ExecutionAgent,
		Objective: "complete accepted work", Context: []core.IntentValue{},
		Deliverables:       []core.IntentValue{{Value: "completed work", Origin: "USER"}},
		CompletionCriteria: []core.IntentValue{{Value: "the work is complete", Origin: "USER"}},
		Constraints:        []core.IntentValue{}, ResolvedDecisions: []core.IntentDecision{}, ConsequenceCandidates: []string{}, MissingUserInputs: []core.IntentValue{}, CreatedAt: time.Unix(1, 0).UTC(),
	}
	fingerprint, err := core.FingerprintIntentDraft(draft)
	if err != nil {
		t.Fatal(err)
	}
	draft.Fingerprint = fingerprint
	repository := projections.New(gateway)
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1"}
	intent := core.Intent{
		ID: core.ID("intent-" + correlationID), OrganizationID: organization.ID,
		OriginalInstruction: "complete accepted work", NormalizedObjective: draft.Objective,
		SourcePrincipalID: "user-1", SourcePrincipalKind: core.PrincipalHuman, SourceHumanID: "user-1",
		SourceChannel: "HUMAN_DIRECT", ExternalRequestID: correlationID, SourceMessageID: "message-1",
		Context: draft.Context, Deliverables: draft.Deliverables, CompletionCriteria: draft.CompletionCriteria,
		ResolvedDecisions: draft.ResolvedDecisions, AcceptedFingerprint: draft.Fingerprint,
	}
	work := core.Work{ID: core.ID("work-" + correlationID), IntentID: intent.ID, Objective: draft.Objective, Status: "ACTIVE"}
	for _, save := range []func() error{
		func() error {
			return repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", correlationID, 1, organization, nil)
		},
	} {
		if err := save(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "INTAKE_MESSAGE_RECORDED", SourceActorID: "user-1", TaskID: "task-" + correlationID, CorrelationID: correlationID,
		Payload: events.IntakeMessageRecordedPayload{MessageID: "message-1", Text: intent.OriginalInstruction, SourcePrincipalID: "user-1", SourcePrincipalKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", RequestedExecutionKind: core.ExecutionAgent},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "INTENT_DRAFTED", SourceActorID: "runtime", TaskID: "task-" + correlationID, CorrelationID: correlationID,
		Payload: events.IntentDraftedPayload{SourceMessageID: "message-1", Draft: draft, Reply: "Review intent."},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.PublishIntentConfirmation(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "INTENT_CONFIRMED", SourceActorID: "user-1", TaskID: "task-" + correlationID, CorrelationID: correlationID,
		Payload: events.IntentConfirmedPayload{IntentID: string(draft.ID), Version: draft.Version, Fingerprint: draft.Fingerprint, ConfirmingActorID: "user-1", ConfirmingActorKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", MessageID: "confirmation-1"},
	}, "", ""); err != nil {
		t.Fatal(err)
	}
	for _, save := range []func() error{
		func() error {
			return repository.SaveIntent(ctx, "INTENT_CREATED", "runtime", correlationID, 1, intent, nil)
		},
		func() error {
			return repository.SaveWork(ctx, organization.ID, "WORK_CREATED", "runtime", correlationID, 1, work, nil)
		},
	} {
		if err := save(); err != nil {
			t.Fatal(err)
		}
	}
	return work, draft
}

func TestRecoveryIsDeterministicFirst(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agentos.db")
	l, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	repository := projections.New(events.NewGateway(l))
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1"}
	if err := repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "bootstrap", 1, organization, nil); err != nil {
		t.Fatal(err)
	}
	agent := seedTestAgents(t, ctx, repository, "bootstrap", organization.ID, execution.FakeModel{}.Descriptor(), "agent-1")[0]
	seedRunning := func(requestID string, kind core.ExecutionKind, statement string) core.Task {
		t.Helper()
		intent := acceptedTestIntent(core.ID("intent-"+requestID), organization.ID, statement)
		work := core.Work{ID: core.ID("work-" + requestID), IntentID: intent.ID, Objective: statement, Status: "ACTIVE"}
		policy := core.InferenceForbidden
		if kind == core.ExecutionAgent {
			policy = core.InferenceAllowed
		}
		task := core.Task{ID: core.ID("task-" + requestID), WorkID: work.ID, Description: statement, AcceptanceCriteria: intent.CompletionCriteria, ExecutionKind: kind, ModelInferencePolicy: policy, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: testAgentConfig(agent), TaskContractVersion: "1", Status: core.TaskPending}
		for _, save := range []func() error{
			func() error {
				return repository.SaveIntent(ctx, "INTENT_CREATED", "runtime", requestID, 1, intent, nil)
			},
			func() error {
				return repository.SaveWork(ctx, organization.ID, "WORK_CREATED", "runtime", requestID, 1, work, nil)
			},
			func() error {
				return repository.SaveTask(ctx, organization.ID, "TASK_CREATED", "runtime", requestID, 1, task, nil)
			},
			func() error {
				return saveTestPlan(ctx, events.NewGateway(l), requestID, intent, task)
			},
			func() error {
				task.Status = core.TaskRunning
				if kind == core.ExecutionAgent {
					snapshot, err := repository.Load(ctx)
					if err != nil {
						return err
					}
					_, _, err = repository.StartAgentExecution(ctx, organization.ID, requestID, 2, task, "", nil, nil, actionBoundaryRoutes(snapshot, task), func([]events.InboxSelection) error { return nil })
					return err
				}
				_, err := repository.StartTaskExecution(ctx, organization.ID, requestID, 2, task, "", "", nil, nil)
				return err
			},
		} {
			if err := save(); err != nil {
				t.Fatal(err)
			}
		}
		return task
	}
	deterministicTask := seedRunning("deterministic", core.ExecutionDeterministic, "echo recovered")
	agentTask := seedRunning("agent", core.ExecutionAgent, "summarize")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l, err = ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	recovery, err := New(events.NewGateway(l)).Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.RunningRecovered != 2 || recovery.TasksExecuted != 1 || recovery.BlockedPreserved != 1 {
		t.Fatalf("recovery=%+v", recovery)
	}
	snapshot, err := projections.New(events.NewGateway(l)).Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Tasks[deterministicTask.ID].Value.Status != core.TaskCompleted {
		t.Fatalf("deterministic task was not safely retried: %+v", snapshot.Tasks[deterministicTask.ID])
	}
	if snapshot.Tasks[agentTask.ID].Value.Status != core.TaskBlocked {
		t.Fatalf("uncertain adaptive task was replayed: %+v", snapshot.Tasks[agentTask.ID])
	}
	agentEvents, err := l.Events(ctx, "agent")
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range agentEvents {
		if event.EventType == "EXECUTION_CONTEXT_MANIFESTED" || event.EventType == "TOOL_OUTCOME_RECORDED" {
			t.Fatalf("uncertain adaptive execution was blindly replayed: %+v", event)
		}
	}
}

func TestRecoverCompletesDurableExternalInputExactlyOnce(t *testing.T) {
	tests := []struct {
		name, stage string
	}{
		{name: "input_durable", stage: "input_durable"},
		{name: "task_resumed", stage: "task_resumed"},
		{name: "completion_verified", stage: "completion_verified"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			l, err := ledger.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = l.Close() })
			gateway := events.NewGateway(l)
			service := New(gateway)
			result, err := service.Submit(ctx, Submit{RequestID: "request-1", OrganizationID: "org-1", Statement: "human response", Kind: core.ExecutionHuman})
			if err != nil || result.Task.Status != core.TaskBlocked || result.Task.AssigneeType != "" || result.Task.AssigneeID != "" {
				t.Fatalf("submit=%+v err=%v", result, err)
			}
			correlationID := result.Events[0].CorrelationID
			inputPayload := events.OperatorInputReceivedPayload{
				MessageID: "message-1", Text: "approved task input", SourcePrincipalID: "external-agent",
				SourcePrincipalKind: string(core.PrincipalExternalAgent), SourceChannel: "A2A",
			}
			inputEvent, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
				OrganizationID: "org-1",
				EventType:      "A2A_INPUT_RECEIVED",
				SourceActorID:  "external-agent",
				TaskID:         string(result.Task.ID),
				CorrelationID:  correlationID,
				Payload:        inputPayload,
			})
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := service.state.Load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			state := snapshot.Tasks[result.Task.ID]
			if test.stage != "input_durable" {
				task := state.Value
				task.Status = core.TaskPending
				if err := service.state.SaveTask(ctx, "org-1", "TASK_RESUMED", "runtime", correlationID, state.Version+1, task, map[string]string{"input_event_ref": inputEvent.EventID}); err != nil {
					t.Fatal(err)
				}
				state = projections.Versioned[core.Task]{Version: state.Version + 1, CorrelationID: correlationID, Value: task}
			}
			if test.stage == "completion_verified" {
				task := state.Value
				task.Status = core.TaskRunning
				if _, err := service.state.StartTaskExecution(ctx, "org-1", correlationID, state.Version+1, task, "OPERATOR_HUMAN_INPUT", inputEvent.EventID, nil, nil); err != nil {
					t.Fatal(err)
				}
				state = projections.Versioned[core.Task]{Version: state.Version + 1, CorrelationID: correlationID, Value: task}
				executionID := "external-input-" + inputEvent.EventID
				now := time.Now().UTC()
				outcome := core.ToolOutcome{ToolInvocationID: core.ID("a2a-input-" + inputEvent.EventID), ToolID: "a2a.external-input", ToolVersion: "v1", Status: core.OutcomeSucceeded, ObservedEffect: map[string]string{"status": "authorized external input persisted", "input_event_ref": inputEvent.EventID}, PostconditionStatus: core.PostconditionVerified, Retryability: core.NotRetryable, StartedAt: now, FinishedAt: now}
				outcomeEvent, err := gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "TOOL_OUTCOME_RECORDED", SourceActorID: "runtime", SourceExecutionID: executionID, TaskID: string(task.ID), Payload: outcome, CorrelationID: correlationID})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_FINISHED", SourceActorID: "runtime", SourceExecutionID: executionID, TaskID: string(task.ID), Payload: map[string]any{"status": outcome.Status}, CorrelationID: correlationID}); err != nil {
					t.Fatal(err)
				}
				summary, err := core.ToolOutcomeSummary(outcome)
				if err != nil {
					t.Fatal(err)
				}
				resultEvent, err := gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "RESULT_PUBLISHED", SourceActorID: "runtime", SourceExecutionID: executionID, TaskID: string(task.ID), Payload: events.ResultPublishedPayload{Summary: summary, ArtifactRefs: outcome.ArtifactRefs}, CorrelationID: correlationID})
				if err != nil {
					t.Fatal(err)
				}
				candidate := events.CandidateCompletePayload{ToolInvocationID: string(outcome.ToolInvocationID), ResultEventID: resultEvent.EventID, ArtifactRefs: outcome.ArtifactRefs}
				if _, err := gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "CANDIDATE_COMPLETE", SourceActorID: "runtime", SourceExecutionID: executionID, TaskID: string(task.ID), Payload: candidate, CorrelationID: correlationID}); err != nil {
					t.Fatal(err)
				}
				contract := core.ExternalInputCompletionContract(task.ID, state.Version)
				detail := completionDetail{Contract: contract, Result: service.completion.Evaluate(contract, outcome), OutcomeEventRef: outcomeEvent.EventID}
				if _, err := gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "COMPLETION_VERIFIED", SourceActorID: "runtime", SourceExecutionID: executionID, TaskID: string(task.ID), Payload: detail, CorrelationID: correlationID}); err != nil {
					t.Fatal(err)
				}
			}

			recovered := New(gateway)
			recovery, err := recovered.Recover(ctx)
			if err != nil || recovery.TasksExecuted != 1 {
				t.Fatalf("recovery=%+v err=%v", recovery, err)
			}
			snapshot, err = recovered.state.Load(ctx)
			if err != nil || snapshot.Tasks[result.Task.ID].Value.Status != core.TaskCompleted {
				t.Fatalf("recovered task=%+v err=%v", snapshot.Tasks[result.Task.ID], err)
			}
			stream, err := gateway.Events(ctx, correlationID)
			if err != nil {
				t.Fatal(err)
			}
			for _, eventType := range []string{"A2A_INPUT_RECEIVED", "TASK_RESUMED", "EXECUTION_STARTED", "TOOL_OUTCOME_RECORDED", "EXECUTION_FINISHED", "RESULT_PUBLISHED", "CANDIDATE_COMPLETE", "COMPLETION_VERIFIED", "TASK_VERIFIED_COMPLETE"} {
				count := 0
				for _, event := range stream {
					if event.EventType == eventType && event.TaskID == string(result.Task.ID) {
						count++
					}
				}
				if count != 1 {
					t.Fatalf("%s count=%d stream=%+v", eventType, count, stream)
				}
			}
			eventCount := len(stream)
			if _, err := recovered.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			stream, err = gateway.Events(ctx, correlationID)
			if err != nil || len(stream) != eventCount {
				t.Fatalf("second recovery appended events: count=%d want=%d err=%v", len(stream), eventCount, err)
			}
		})
	}
}

func TestContinuationReplayRequiresExactRuntimeEvent(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	service := New(events.NewGateway(l))
	payload := events.ResultPublishedPayload{Summary: "verified result", ArtifactRefs: []string{"artifact-1"}}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	existing := events.Event{
		EventID: "result-1", Sequence: 1, OrganizationID: "org-1", EventType: "RESULT_PUBLISHED", SourceActorID: "runtime",
		SourceExecutionID: "execution-1", TaskID: "task-1", ArtifactRefs: payload.ArtifactRefs, CorrelationID: "run-1", Payload: body,
		CreatedAt: time.Unix(1, 0).UTC(), SchemaVersion: events.SchemaVersion,
	}
	replayed, err := service.publishContinuationEventIfMissing(ctx, []events.Event{existing}, "org-1", "task-1", "run-1", "execution-1", "RESULT_PUBLISHED", payload, payload.ArtifactRefs)
	if err != nil || replayed.EventID != existing.EventID {
		t.Fatalf("exact continuation replay failed: event=%+v err=%v", replayed, err)
	}
	for name, mutate := range map[string]func(*events.Event){
		"organization": func(event *events.Event) { event.OrganizationID = "org-2" },
		"actor":        func(event *events.Event) { event.SourceActorID = "agent-1" },
		"correlation":  func(event *events.Event) { event.CorrelationID = "run-2" },
		"recipient":    func(event *events.Event) { event.RecipientScope, event.RecipientID = events.RecipientAgent, "agent-1" },
		"authorization": func(event *events.Event) {
			event.AuthorizationRefs = []string{"approval-1"}
		},
		"artifacts": func(event *events.Event) { event.ArtifactRefs = []string{"artifact-2"} },
		"schema":    func(event *events.Event) { event.SchemaVersion++ },
		"timestamp": func(event *events.Event) { event.CreatedAt = time.Time{} },
		"payload": func(event *events.Event) {
			event.Payload, _ = json.Marshal(events.ResultPublishedPayload{Summary: "substituted", ArtifactRefs: payload.ArtifactRefs})
		},
	} {
		t.Run(name, func(t *testing.T) {
			forged := existing
			mutate(&forged)
			if _, err := service.publishContinuationEventIfMissing(ctx, []events.Event{forged}, "org-1", "task-1", "run-1", "execution-1", "RESULT_PUBLISHED", payload, payload.ArtifactRefs); err == nil {
				t.Fatal("substituted continuation event was accepted")
			}
		})
	}
	stream, err := l.Events(ctx, "run-1")
	if err != nil || len(stream) != 0 {
		t.Fatalf("continuation replay test unexpectedly appended events: events=%+v err=%v", stream, err)
	}
}

func TestDecodeOperatorInputRejectsLegacyA2APayload(t *testing.T) {
	body, err := json.Marshal(map[string]string{"text": "approved task input", "source_external_actor": "external-agent"})
	if err != nil {
		t.Fatal(err)
	}
	event := events.Event{
		EventID: "input-1", Sequence: 1, EventType: "A2A_INPUT_RECEIVED", OrganizationID: "org-1", SourceActorID: "external-agent",
		TaskID: "task-1", CorrelationID: "run-1", Payload: body, CreatedAt: time.Unix(1, 0).UTC(), SchemaVersion: events.SchemaVersion,
	}
	if _, err := decodeOperatorInput(event); err == nil {
		t.Fatal("legacy A2A input compatibility payload was accepted")
	}
}

func TestAdvanceInputContinuationRejectsInvalidStateWithoutEvents(t *testing.T) {
	ctx := t.Context()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	service := New(events.NewGateway(l))
	valid := projections.Versioned[core.Task]{
		Version:       1,
		CorrelationID: "request-1",
		Value: core.Task{
			ID: "task-1", ExecutionKind: core.ExecutionHuman, Status: core.TaskBlocked,
		},
	}
	tests := []struct {
		name          string
		organization  core.ID
		correlationID string
		state         projections.Versioned[core.Task]
	}{
		{name: "missing organization", correlationID: "request-1", state: valid},
		{name: "mismatched correlation", organization: "org-1", correlationID: "different", state: valid},
		{name: "non-user task", organization: "org-1", correlationID: "request-1", state: func() projections.Versioned[core.Task] {
			state := valid
			state.Value.ExecutionKind = core.ExecutionAgent
			return state
		}()},
		{name: "running task", organization: "org-1", correlationID: "request-1", state: func() projections.Versioned[core.Task] {
			state := valid
			state.Value.Status = core.TaskRunning
			return state
		}()},
		{name: "completed task", organization: "org-1", correlationID: "request-1", state: func() projections.Versioned[core.Task] {
			state := valid
			state.Value.Status = core.TaskCompleted
			return state
		}()},
		{name: "failed task", organization: "org-1", correlationID: "request-1", state: func() projections.Versioned[core.Task] {
			state := valid
			state.Value.Status = core.TaskFailed
			return state
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := service.advanceInputContinuation(ctx, test.organization, test.correlationID, test.state, map[string]string{"input_event_ref": "event-1"}); err == nil {
				t.Fatal("invalid input continuation state was accepted")
			}
		})
	}
	stream, err := l.Events(ctx, "request-1")
	if err != nil || len(stream) != 0 {
		t.Fatalf("rejected transition appended events: count=%d err=%v", len(stream), err)
	}
}

func TestBlockedChildReturnsToParent(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gateway := events.NewGateway(l)
	repository := projections.New(gateway)
	service := New(gateway)
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1"}
	if err := repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "request-1", 1, organization, nil); err != nil {
		t.Fatal(err)
	}
	agent := seedTestAgents(t, ctx, repository, "request-1", organization.ID, execution.FakeModel{}.Descriptor(), "agent-1")[0]
	intent := core.Intent{ID: "intent-1", OrganizationID: organization.ID, OriginalInstruction: "complete governed work", NormalizedObjective: "complete governed work"}
	work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: "complete governed work", Status: "ACTIVE"}
	child := core.Task{ID: "task-child", WorkID: work.ID, ParentID: "task-request-1", Description: "use unavailable tool", ExecutionKind: core.ExecutionTool, ModelInferencePolicy: core.InferenceForbidden, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: testAgentConfig(agent), TaskContractVersion: "1", Status: core.TaskPending}
	parent := core.Task{ID: "task-request-1", WorkID: work.ID, Description: "govern child remediation", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, DependsOn: []core.ID{child.ID}, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: testAgentConfig(agent), TaskContractVersion: "1", Status: core.TaskPending}
	if err := saveTestTaskGraph(ctx, repository, organization.ID, "request-1", intent, work, parent, child); err != nil {
		t.Fatal(err)
	}

	recovery, err := service.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.TasksExecuted != 2 {
		t.Fatalf("recovery=%+v", recovery)
	}
	snapshot, err := repository.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Tasks[child.ID].Value.Status != core.TaskFailed || snapshot.Tasks[parent.ID].Value.Status != core.TaskFailed || snapshot.Works[work.ID].Value.Status != "FAILED" {
		t.Fatalf("unresolved root remediation did not terminalize the Work: child=%+v parent=%+v work=%+v", snapshot.Tasks[child.ID], snapshot.Tasks[parent.ID], snapshot.Works[work.ID])
	}
	if err := service.ValidateAddressedRoute(ctx, events.AddressedRoute{OrganizationID: string(organization.ID), EventType: "TASK_BLOCKED", SourceActorID: string(agent.ID), ValidateSource: true, RecipientScope: events.RecipientTask, RecipientID: string(child.ID), TaskID: string(child.ID)}); err == nil {
		t.Fatal("blocked child could route its escalation somewhere other than its parent")
	}
	if err := service.ValidateAddressedRoute(ctx, events.AddressedRoute{OrganizationID: string(organization.ID), EventType: "TASK_BLOCKED", SourceActorID: string(agent.ID), ValidateSource: true, RecipientScope: events.RecipientTask, RecipientID: string(parent.ID)}); err == nil {
		t.Fatal("blocked event without a source child task was accepted")
	}
	if err := service.ValidateAddressedRoute(ctx, events.AddressedRoute{OrganizationID: string(organization.ID), EventType: "TASK_BLOCKED", SourceActorID: string(agent.ID), ValidateSource: true, RecipientScope: events.RecipientTask, RecipientID: string(parent.ID), TaskID: string(parent.ID)}); err == nil {
		t.Fatal("root task was accepted as a blocked child source")
	}
	upward, err := gateway.Inbox(ctx, events.RecipientTask, string(parent.ID))
	if err != nil || len(upward) != 0 {
		t.Fatalf("parent remediation inbox=%+v err=%v", upward, err)
	}
	stream, err := l.Events(ctx, "request-1")
	if err != nil {
		t.Fatal(err)
	}
	var blockedEvent events.Event
	var parentManifest core.ExecutionContextManifest
	var remediationFailure remediationFailureDetail
	observed := false
	for _, event := range stream {
		if strings.HasPrefix(event.EventType, "CAPABILITY_") || strings.HasPrefix(event.EventType, "APPROVAL_") {
			t.Fatalf("blocked worker changed authority: %+v", event)
		}
		if event.EventType == "TASK_BLOCKED" && event.TaskID == string(child.ID) && event.RecipientID == string(parent.ID) {
			blockedEvent = event
		}
		if event.EventType == "EXECUTION_CONTEXT_MANIFESTED" && event.TaskID == string(parent.ID) {
			if err := json.Unmarshal(event.Payload, &parentManifest); err != nil {
				t.Fatal(err)
			}
		}
		if event.EventType == "INBOX_EVENTS_OBSERVED" && event.TaskID == string(parent.ID) {
			observed = true
		}
		if event.EventType == "TASK_REMEDIATION_FAILED" && event.TaskID == string(parent.ID) {
			var projection events.ProjectionEventPayload
			if json.Unmarshal(event.Payload, &projection) != nil || json.Unmarshal(projection.Detail, &remediationFailure) != nil {
				t.Fatal("invalid remediation failure contract")
			}
		}
	}
	if blockedEvent.EventID == "" || len(blockedEvent.AuthorizationRefs) != 0 {
		t.Fatalf("blocked work gained authority or lost its upward route: %+v", blockedEvent)
	}
	var payload events.ProjectionEventPayload
	if err := json.Unmarshal(blockedEvent.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	var detail events.TaskBlockedPayload
	if err := json.Unmarshal(payload.Detail, &detail); err != nil || detail.Reason == "" || detail.Missing == "" || detail.WhyNeeded == "" || detail.WorkCompleted == "" {
		t.Fatalf("blocked-work contract=%+v err=%v", detail, err)
	}
	if !observed || len(parentManifest.EventRefs) != 1 || parentManifest.EventRefs[0] != blockedEvent.EventID {
		t.Fatalf("parent did not receive a bounded remediation pass: manifest=%+v observed=%v blocked=%+v", parentManifest, observed, blockedEvent)
	}
	if remediationFailure.Code != "REMEDIATION_UNRESOLVED" || !slices.Equal(remediationFailure.BlockedDependencyIDs, []core.ID{child.ID}) {
		t.Fatalf("unresolved remediation contract=%+v", remediationFailure)
	}
}

func TestDeepBlockedDependencyReachesActionableRoot(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gateway := events.NewGateway(l)
	repository := projections.New(gateway)
	service := New(gateway)
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1"}
	if err := repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "deep-block", 1, organization, nil); err != nil {
		t.Fatal(err)
	}
	agent := seedTestAgents(t, ctx, repository, "deep-block", organization.ID, execution.FakeModel{}.Descriptor(), "agent-1")[0]
	intent := core.Intent{ID: "intent-1", OrganizationID: organization.ID, OriginalInstruction: "complete governed work", NormalizedObjective: "complete governed work"}
	work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: "complete governed work", Status: "ACTIVE"}
	blocked := core.Task{ID: "task-a", WorkID: work.ID, ParentID: "task-deep-block", Description: "use unavailable tool", ExecutionKind: core.ExecutionTool, ModelInferencePolicy: core.InferenceForbidden, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: testAgentConfig(agent), TaskContractVersion: "1", Status: core.TaskPending}
	middle := core.Task{ID: "task-b", WorkID: work.ID, ParentID: "task-deep-block", Description: "interpret blocked dependency", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, DependsOn: []core.ID{blocked.ID}, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: testAgentConfig(agent), TaskContractVersion: "1", Status: core.TaskPending}
	root := core.Task{ID: "task-deep-block", WorkID: work.ID, Description: "govern remediation", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, DependsOn: []core.ID{middle.ID}, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: testAgentConfig(agent), TaskContractVersion: "1", Status: core.TaskPending}
	if err := saveTestTaskGraph(ctx, repository, organization.ID, "deep-block", intent, work, root, middle, blocked); err != nil {
		t.Fatal(err)
	}
	recovered, err := service.Recover(ctx)
	if err != nil || recovered.TasksExecuted != 3 {
		t.Fatalf("recovery=%+v err=%v", recovered, err)
	}
	snapshot, err := repository.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, taskID := range []core.ID{blocked.ID, middle.ID, root.ID} {
		if snapshot.Tasks[taskID].Value.Status != core.TaskFailed {
			t.Fatalf("task %s status=%s", taskID, snapshot.Tasks[taskID].Value.Status)
		}
	}
	if snapshot.Works[work.ID].Value.Status != "FAILED" {
		t.Fatalf("unresolved deep remediation work=%+v", snapshot.Works[work.ID])
	}
	stream, err := gateway.Events(ctx, "deep-block")
	if err != nil {
		t.Fatal(err)
	}
	var blockedEventID string
	var middleManifest core.ExecutionContextManifest
	for _, event := range stream {
		if event.EventType == "TASK_BLOCKED" && event.TaskID == string(blocked.ID) {
			blockedEventID = event.EventID
		}
		if event.EventType == "EXECUTION_CONTEXT_MANIFESTED" && event.TaskID == string(middle.ID) {
			if err := json.Unmarshal(event.Payload, &middleManifest); err != nil {
				t.Fatal(err)
			}
		}
	}
	if blockedEventID == "" || !slices.Contains(middleManifest.EventRefs, blockedEventID) {
		t.Fatalf("middle task did not receive deep block evidence: block=%q manifest=%+v", blockedEventID, middleManifest)
	}
}

func TestLateralMessagesAtActionBoundary(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agentos.db")
	l, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	gateway := events.NewGateway(l)
	repository := projections.New(gateway)
	service := New(gateway)
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1"}
	if err := repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "request-1", 1, organization, nil); err != nil {
		t.Fatal(err)
	}
	agents := seedTestAgents(t, ctx, repository, "request-1", organization.ID, execution.FakeModel{}.Descriptor(), "agent-sender", "agent-recipient")
	sender, recipient := agents[0], agents[1]
	team := core.Team{ID: "team-1", OrganizationID: organization.ID, Name: "Delivery", MemberAgentIDs: []core.ID{recipient.ID}, Status: "ACTIVE"}
	intent := acceptedTestIntent("intent-1", organization.ID, "finish from handoff")
	work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: "finish from handoff", Status: "ACTIVE"}
	sourceTask := core.Task{ID: "task-request-1-source", WorkID: work.ID, ParentID: "task-request-1", Description: "prepare handoff", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: sender.ID, AgentConfig: testAgentConfig(sender), TaskContractVersion: "1", Status: core.TaskPending}
	recipientTask := core.Task{ID: "task-request-1", WorkID: work.ID, Description: "finish work", AcceptanceCriteria: intent.CompletionCriteria, ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, DependsOn: []core.ID{sourceTask.ID}, AssigneeType: "AGENT", AssigneeID: recipient.ID, AgentConfig: testAgentConfig(recipient), TaskContractVersion: "1", Status: core.TaskPending}
	bindTestAgentExecutionBriefs(t, "request-1", intent, &sourceTask, &recipientTask)
	if err := repository.SaveTeam(ctx, "TEAM_CREATED", "runtime", "request-1", 1, team, nil); err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	if err := saveTestTaskGraph(ctx, repository, organization.ID, "request-1", intent, work, sourceTask, recipientTask); err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	if err := saveTestPlan(ctx, gateway, "request-1", intent, sourceTask, recipientTask); err != nil {
		t.Fatal(err)
	}
	if err := saveTestVerifiedTask(ctx, gateway, repository, organization.ID, "request-1", projections.Versioned[core.Task]{Version: 1, CorrelationID: "request-1", Value: sourceTask}); err != nil {
		t.Fatal(err)
	}
	completedStream, err := gateway.Events(ctx, "request-1")
	if err != nil {
		t.Fatal(err)
	}
	var dependencyResult events.Event
	for _, event := range completedStream {
		if event.EventType == "RESULT_PUBLISHED" && event.TaskID == string(sourceTask.ID) {
			dependencyResult = event
		}
	}
	if dependencyResult.EventID == "" {
		t.Fatal("verified dependency result was not recorded")
	}
	forgedResult, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: string(organization.ID), EventType: "RESULT_PUBLISHED", SourceActorID: "runtime",
		SourceExecutionID: "forged-execution", TaskID: string(sourceTask.ID), CorrelationID: "request-1", Payload: events.ResultPublishedPayload{Summary: "forged later handoff"},
	})
	if err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	routes := []struct {
		scope string
		id    string
		body  string
	}{
		{events.RecipientAgent, string(recipient.ID), "direct handoff detail"},
		{events.RecipientTeam, string(team.ID), "team handoff detail"},
		{events.RecipientTask, string(recipientTask.ID), "task handoff detail"},
	}
	messageIDs := make([]string, 0, len(routes))
	for _, route := range routes {
		message, err := service.SendMessage(ctx, string(organization.ID), string(sender.ID), "execution-source", "request-1", events.Draft{
			EventType:      "MESSAGE",
			RecipientScope: route.scope,
			RecipientID:    route.id,
			TaskID:         string(sourceTask.ID),
			Payload: map[string]any{
				"body":            route.body,
				"source_actor_id": "admin",
			},
		})
		if err != nil {
			_ = l.Close()
			t.Fatal(err)
		}
		if message.SourceActorID != string(sender.ID) {
			_ = l.Close()
			t.Fatalf("payload spoofed sender envelope: %+v", message)
		}
		messageIDs = append(messageIDs, message.EventID)
	}
	if _, err := service.SendMessage(ctx, string(organization.ID), string(sender.ID), "execution-source", "request-1", events.Draft{
		EventType:      "MESSAGE",
		RecipientScope: events.RecipientAgent,
		RecipientID:    "unknown-agent",
		TaskID:         string(sourceTask.ID),
		Payload:        map[string]any{"body": "must fail closed"},
	}); err == nil {
		_ = l.Close()
		t.Fatal("unknown recipient accepted")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l, err = ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gateway = events.NewGateway(l)
	service = New(gateway)
	recovery, err := service.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.PendingFound != 1 || recovery.TasksExecuted != 1 {
		t.Fatalf("recovery=%+v", recovery)
	}
	stream, err := gateway.Events(ctx, "request-1")
	if err != nil {
		t.Fatal(err)
	}
	var manifest core.ExecutionContextManifest
	var outcome core.ToolOutcome
	for _, event := range stream {
		switch event.EventType {
		case "EXECUTION_CONTEXT_MANIFESTED":
			if event.TaskID == string(recipientTask.ID) {
				if err := json.Unmarshal(event.Payload, &manifest); err != nil {
					t.Fatal(err)
				}
			}
		case "TOOL_OUTCOME_RECORDED":
			if event.TaskID == string(recipientTask.ID) {
				if err := json.Unmarshal(event.Payload, &outcome); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	expectedRefs := append(messageIDs, dependencyResult.EventID)
	if strings.Join(manifest.EventRefs, ",") != strings.Join(expectedRefs, ",") {
		t.Fatalf("manifest event refs=%v want=%v", manifest.EventRefs, expectedRefs)
	}
	if slices.Contains(manifest.EventRefs, forgedResult.EventID) {
		t.Fatalf("manifest trusted a later unverified dependency publication: %+v", manifest)
	}
	observed, ok := outcome.ObservedEffect.(string)
	if !ok {
		t.Fatalf("observed effect type=%T value=%v", outcome.ObservedEffect, outcome.ObservedEffect)
	}
	for _, route := range routes {
		if !strings.Contains(observed, route.body) {
			t.Fatalf("model context omitted %q: %s", route.body, observed)
		}
		available, err := gateway.Inbox(ctx, route.scope, route.id)
		if err != nil || len(available) != 0 {
			t.Fatalf("observed inbox %s/%s=%+v err=%v", route.scope, route.id, available, err)
		}
	}
	repository = projections.New(gateway)
	team.MemberAgentIDs = []core.ID{sender.ID}
	if err := repository.SaveTeam(ctx, "TEAM_REVISED", "runtime", "team-revision", 2, team, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Recover(ctx); err != nil {
		t.Fatalf("historical completion failed after valid Team membership revision: %v", err)
	}
}

var errProjectionWrite = errors.New("injected task projection failure")

var errPlanningWorkProjection = errors.New("injected planning Work projection failure")

type failPlanningWorkProjection struct{ *ledger.SQLite }

func (f *failPlanningWorkProjection) AppendProjection(ctx context.Context, draft events.ProjectionDraft) (events.Event, error) {
	if draft.Event.EventType == "WORK_PLANNING_FAILED" {
		return events.Event{}, errPlanningWorkProjection
	}
	return f.SQLite.AppendProjection(ctx, draft)
}

type failTaskProjection struct{ *ledger.SQLite }

func (f failTaskProjection) AppendProjection(ctx context.Context, draft events.ProjectionDraft) (events.Event, error) {
	if draft.ProjectionKind == projections.KindTask {
		return events.Event{}, errProjectionWrite
	}
	return f.SQLite.AppendProjection(ctx, draft)
}

func (f failTaskProjection) AppendProjections(context.Context, []events.ProjectionDraft) ([]events.Event, error) {
	return nil, errProjectionWrite
}

func assertEventOrder(t *testing.T, stream []events.Event, expected ...string) {
	t.Helper()
	next := 0
	for _, event := range stream {
		if next < len(expected) && event.EventType == expected[next] {
			next++
		}
	}
	if next != len(expected) {
		t.Fatalf("event order missing %q after index %d: %+v", expected[next], next, stream)
	}
}

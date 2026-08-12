package app

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/completion"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/planning"
	"github.com/dominicnunez/agentos/internal/projections"
	"github.com/dominicnunez/agentos/internal/telemetry"
)

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
	if !r.Completion.Complete || r.Task.Status != core.TaskCompleted || r.Goal.Status != "COMPLETED" {
		t.Fatalf("unexpected result: %#v", r)
	}
	assertEventOrder(t, r.Events, "TASK_CREATED", "EXECUTION_STARTED", "TOOL_OUTCOME_RECORDED", "RESULT_PUBLISHED", "COMPLETION_VERIFIED", "TASK_VERIFIED_COMPLETE", "RUN_TELEMETRY_RECORDED", "GOAL_COMPLETED")
	var run telemetry.Run
	telemetryEvents := 0
	for _, event := range r.Events {
		if event.EventType != "RUN_TELEMETRY_RECORDED" {
			continue
		}
		telemetryEvents++
		if err := json.Unmarshal(event.Payload, &run); err != nil {
			t.Fatal(err)
		}
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
	if !ok || !strings.HasPrefix(observed, "fake-model: Execute only this accepted Agent OS Intent.") || !strings.Contains(observed, `"objective":"summarize"`) {
		t.Fatalf("effect=%q", r.Outcome.ObservedEffect)
	}
	if !r.Completion.Complete || r.Task.Status != core.TaskCompleted {
		t.Fatalf("fake model result was not deterministically verified: %+v", r)
	}
	assertEventOrder(t, r.Events, "EXECUTION_CONTEXT_MANIFESTED", "TOOL_OUTCOME_RECORDED", "INFERENCE_USAGE_RECORDED", "EXECUTION_FINISHED", "RUN_TELEMETRY_RECORDED")
}

type organizationLoopModel struct {
	prompts []string
}

func (*organizationLoopModel) Name() string { return "fake-model/v1" }
func (*organizationLoopModel) Descriptor() execution.ModelDescriptor {
	return execution.ModelDescriptor{Provider: "fake", Model: "fake-model/v1", ExecutionProfileVersion: "v1-fake"}
}
func (m *organizationLoopModel) Complete(_ context.Context, prompt string) (execution.ModelResponse, error) {
	m.prompts = append(m.prompts, prompt)
	text := "fake-model: " + prompt
	if strings.HasPrefix(prompt, "You are the bounded Agent OS Task-DAG planner.") {
		text = `{"tasks":[{"key":"research","description":"research the accepted objective","execution_kind":"AGENT","model_inference_policy":"REQUIRED","depends_on":[]}]}`
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

func TestAcceptedIntentBecomesDurableTaskDAGWithDependencyEvidence(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	model := &organizationLoopModel{}
	planner, err := planning.NewModelPlanner(organizationPlanningModel{model: model})
	if err != nil {
		t.Fatal(err)
	}
	service := NewWithModelAndPlanner(events.NewGateway(l), model, planner)
	submission := Submit{RequestID: "organization-loop", OrganizationID: "org-1", Statement: "prepare a verified briefing", Kind: core.ExecutionAgent}
	result, err := service.Submit(ctx, submission)
	if err != nil {
		t.Fatal(err)
	}
	if result.Task.ID == "" || result.Task.ParentID != "" || result.Task.Status != core.TaskCompleted || result.Goal.Status != "COMPLETED" {
		t.Fatalf("root result=%+v goal=%+v", result.Task, result.Goal)
	}
	if len(model.prompts) != 3 {
		t.Fatalf("model calls=%d prompts=%+v", len(model.prompts), model.prompts)
	}
	assertEventOrder(t, result.Events, "INTENT_CREATED", "PLAN_CREATED", "TASK_CREATED", "EXECUTION_STARTED", "RESULT_PUBLISHED", "TASK_VERIFIED_COMPLETE", "EXECUTION_STARTED", "RESULT_PUBLISHED", "TASK_VERIFIED_COMPLETE", "GOAL_COMPLETED")

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

type describedModel struct{}

func (describedModel) Name() string { return "codex-subscription/test-model" }
func (describedModel) Descriptor() execution.ModelDescriptor {
	return execution.ModelDescriptor{Provider: "codex-subscription", Model: "test-model", ExecutionProfileVersion: "v1-codex-subscription"}
}
func (describedModel) Complete(_ context.Context, prompt string) (execution.ModelResponse, error) {
	return execution.ModelResponse{Text: "configured-model: " + prompt, Usage: events.InferenceUsageRecordedPayload{Source: "provider_cli", Provider: "codex-subscription", Model: "test-model", InputTokens: 1, OutputTokens: 1, TotalTokens: 2}}, nil
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
	if r.Task.Status != core.TaskBlocked || r.Goal.Status != "ACTIVE" || r.Completion.Complete {
		t.Fatalf("unverified model result did not remain blocked: %+v", r)
	}
	observed, ok := r.Outcome.ObservedEffect.(string)
	if !ok || !strings.HasPrefix(observed, "configured-model: Execute only this accepted Agent OS Intent.") || !strings.Contains(observed, `"objective":"summarize"`) {
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
		if manifest.Provider != "codex-subscription" || manifest.Model != "test-model" || manifest.ExecutionProfileVersion != "v1-codex-subscription" {
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
	if err != nil || !found || !strings.HasPrefix(view.Result, "configured-model: Execute only this accepted Agent OS Intent.") || !strings.Contains(view.Result, `"objective":"summarize"`) || len(view.Request.EvidenceRefs) != 3 {
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
	if err != nil || replayed.Task.Status != core.TaskCompleted || replayed.Goal.Status != "COMPLETED" || !replayed.Completion.Complete {
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
	agent := core.Agent{ID: "agent-1", OrganizationID: organization.ID, BlueprintVersion: "v1", ExecutionProfileVersion: "v1", RuntimeAdapter: "test", Status: "ACTIVE"}
	intent := core.Intent{ID: "intent-1", OrganizationID: organization.ID, OriginalInstruction: "draft two independent updates", NormalizedObjective: "draft two independent updates"}
	goal := core.Goal{ID: "goal-1", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: "ACTIVE"}
	first := core.Task{ID: "task-1", GoalID: goal.ID, Description: "Draft the security update.", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: agent.ID, TaskContractVersion: "1", Status: core.TaskPending}
	second := core.Task{ID: "task-2", GoalID: goal.ID, Description: "Draft the release update.", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: agent.ID, TaskContractVersion: "1", Status: core.TaskPending}
	for _, save := range []func() error{
		func() error {
			return repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", correlationID, 1, organization, nil)
		},
		func() error {
			return repository.SaveAgent(ctx, "AGENT_CREATED", "runtime", correlationID, 1, agent, nil)
		},
		func() error {
			return repository.SaveIntent(ctx, "INTENT_CREATED", "runtime", correlationID, 1, intent, nil)
		},
		func() error {
			return repository.SaveGoal(ctx, organization.ID, "GOAL_CREATED", "runtime", correlationID, 1, goal, nil)
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
	goal := core.Goal{ID: "goal-1", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: "ACTIVE"}
	task := core.Task{ID: "task-1", GoalID: goal.ID, Description: "legacy model work", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, TaskContractVersion: "1", Status: core.TaskBlocked}
	for _, save := range []func() error{
		func() error {
			return repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "legacy", 1, organization, nil)
		},
		func() error { return repository.SaveIntent(ctx, "INTENT_CREATED", "runtime", "legacy", 1, intent, nil) },
		func() error {
			return repository.SaveGoal(ctx, organization.ID, "GOAL_CREATED", "runtime", "legacy", 1, goal, nil)
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
	criteria := acceptedReviewCriteria(accepted)
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
	intent := core.Intent{ID: "intent-1", OrganizationID: organization.ID, OriginalInstruction: "two steps", NormalizedObjective: "two steps"}
	goal := core.Goal{ID: "goal-1", IntentID: intent.ID, Objective: "two steps", Status: "ACTIVE"}
	first := core.Task{ID: "task-1", GoalID: goal.ID, Description: "echo first", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending}
	second := core.Task{ID: "task-2", GoalID: goal.ID, Description: "echo second", DependsOn: []core.ID{first.ID}, ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending}
	for _, save := range []func() error{
		func() error {
			return repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "request-1", 1, organization, nil)
		},
		func() error {
			return repository.SaveIntent(ctx, "INTENT_CREATED", "runtime", "request-1", 1, intent, nil)
		},
		func() error {
			return repository.SaveGoal(ctx, organization.ID, "GOAL_CREATED", "runtime", "request-1", 1, goal, nil)
		},
		func() error {
			return repository.SaveTask(ctx, organization.ID, "TASK_CREATED", "runtime", "request-1", 1, first, nil)
		},
		func() error {
			return repository.SaveTask(ctx, organization.ID, "TASK_CREATED", "runtime", "request-1", 1, second, nil)
		},
	} {
		if err := save(); err != nil {
			t.Fatal(err)
		}
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
	agent := core.Agent{ID: "agent-1", OrganizationID: organization.ID, BlueprintVersion: "v1", ExecutionProfileVersion: "v1", RuntimeAdapter: "local", Status: "ACTIVE"}
	intent := core.Intent{ID: "intent-1", OrganizationID: organization.ID, OriginalInstruction: "echo after restart", NormalizedObjective: "echo after restart"}
	goal := core.Goal{ID: "goal-1", IntentID: intent.ID, Objective: "echo after restart", Status: "ACTIVE"}
	first := core.Task{ID: "task-1", GoalID: goal.ID, Description: "echo already done", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskCompleted}
	second := core.Task{ID: "task-2", GoalID: goal.ID, Description: "echo after restart", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, DependsOn: []core.ID{first.ID}, AssigneeType: "AGENT", AssigneeID: agent.ID, TaskContractVersion: "1", Status: core.TaskPending}
	for _, save := range []func() error{
		func() error {
			return repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "request-1", 1, organization, nil)
		},
		func() error { return repository.SaveAgent(ctx, "AGENT_CREATED", "runtime", "request-1", 1, agent, nil) },
		func() error {
			return repository.SaveIntent(ctx, "INTENT_CREATED", "runtime", "request-1", 1, intent, nil)
		},
		func() error {
			return repository.SaveGoal(ctx, organization.ID, "GOAL_CREATED", "runtime", "request-1", 1, goal, nil)
		},
		func() error {
			return repository.SaveTask(ctx, organization.ID, "TASK_CREATED", "runtime", "request-1", 1, first, nil)
		},
		func() error {
			return repository.SaveTask(ctx, organization.ID, "TASK_CREATED", "runtime", "request-1", 1, second, nil)
		},
	} {
		if err := save(); err != nil {
			_ = l.Close()
			t.Fatal(err)
		}
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
	if snapshot.Tasks[second.ID].Value.Status != core.TaskCompleted || snapshot.Goals[goal.ID].Value.Status != "COMPLETED" {
		t.Fatalf("pending work not recovered: task=%+v goal=%+v", snapshot.Tasks[second.ID].Value, snapshot.Goals[goal.ID].Value)
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

func TestRecoveryIsDeterministicFirst(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agentos.db")
	l, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	repository := projections.New(events.NewGateway(l))
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1"}
	agent := core.Agent{ID: "agent-1", OrganizationID: organization.ID, BlueprintVersion: "v1", ExecutionProfileVersion: "v1", RuntimeAdapter: "local", Status: "ACTIVE"}
	if err := repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "bootstrap", 1, organization, nil); err != nil {
		t.Fatal(err)
	}
	if err := repository.SaveAgent(ctx, "AGENT_CREATED", "runtime", "bootstrap", 1, agent, nil); err != nil {
		t.Fatal(err)
	}
	seedRunning := func(requestID string, kind core.ExecutionKind, statement string) core.Task {
		t.Helper()
		intent := core.Intent{ID: core.ID("intent-" + requestID), OrganizationID: organization.ID, OriginalInstruction: statement, NormalizedObjective: statement}
		goal := core.Goal{ID: core.ID("goal-" + requestID), IntentID: intent.ID, Objective: statement, Status: "ACTIVE"}
		policy := core.InferenceForbidden
		if kind == core.ExecutionAgent {
			policy = core.InferenceAllowed
		}
		task := core.Task{ID: core.ID("task-" + requestID), GoalID: goal.ID, Description: statement, ExecutionKind: kind, ModelInferencePolicy: policy, AssigneeType: "AGENT", AssigneeID: agent.ID, TaskContractVersion: "1", Status: core.TaskRunning}
		for _, save := range []func() error{
			func() error {
				return repository.SaveIntent(ctx, "INTENT_CREATED", "runtime", requestID, 1, intent, nil)
			},
			func() error {
				return repository.SaveGoal(ctx, organization.ID, "GOAL_CREATED", "runtime", requestID, 1, goal, nil)
			},
			func() error {
				return repository.SaveTask(ctx, organization.ID, "EXECUTION_STARTED", "runtime", requestID, 1, task, nil)
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
		legacy      bool
	}{
		{name: "input_durable", stage: "input_durable"},
		{name: "task_resumed", stage: "task_resumed"},
		{name: "completion_verified", stage: "completion_verified"},
		{name: "legacy_input_durable", stage: "input_durable", legacy: true},
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
			if err != nil || result.Task.Status != core.TaskBlocked {
				t.Fatalf("submit=%+v err=%v", result, err)
			}
			correlationID := result.Events[0].CorrelationID
			inputPayload := any(events.OperatorInputReceivedPayload{
				MessageID: "message-1", Text: "approved task input", SourcePrincipalID: "external-agent",
				SourcePrincipalKind: string(core.PrincipalExternalAgent), SourceChannel: "A2A",
			})
			if test.legacy {
				inputPayload = map[string]string{"text": "approved task input", "source_external_actor": "external-agent"}
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
				if err := service.state.SaveTask(ctx, "org-1", "EXECUTION_STARTED", "runtime", correlationID, state.Version+1, task, map[string]string{"input_event_ref": inputEvent.EventID}); err != nil {
					t.Fatal(err)
				}
				state = projections.Versioned[core.Task]{Version: state.Version + 1, CorrelationID: correlationID, Value: task}
				executionID := "external-input-" + inputEvent.EventID
				now := time.Now().UTC()
				outcome := core.ToolOutcome{ToolInvocationID: core.ID("a2a-input-" + inputEvent.EventID), ToolID: "a2a.external-input", ToolVersion: "v1", Status: core.OutcomeSucceeded, ObservedEffect: map[string]string{"input_event_ref": inputEvent.EventID}, PostconditionStatus: core.PostconditionVerified, Retryability: core.NotRetryable, StartedAt: now, FinishedAt: now}
				for _, draft := range []events.TrustedDraft{
					{OrganizationID: "org-1", EventType: "TOOL_OUTCOME_RECORDED", SourceActorID: "runtime", SourceExecutionID: executionID, TaskID: string(task.ID), Payload: outcome, CorrelationID: correlationID},
					{OrganizationID: "org-1", EventType: "EXECUTION_FINISHED", SourceActorID: "runtime", SourceExecutionID: executionID, TaskID: string(task.ID), Payload: map[string]any{"status": outcome.Status}, CorrelationID: correlationID},
					{OrganizationID: "org-1", EventType: "RESULT_PUBLISHED", SourceActorID: "runtime", SourceExecutionID: executionID, TaskID: string(task.ID), Payload: events.ResultPublishedPayload{Summary: "authorized external input persisted"}, CorrelationID: correlationID},
					{OrganizationID: "org-1", EventType: "CANDIDATE_COMPLETE", SourceActorID: "runtime", SourceExecutionID: executionID, TaskID: string(task.ID), Payload: map[string]any{"tool_invocation_id": outcome.ToolInvocationID}, CorrelationID: correlationID},
				} {
					if _, err := gateway.PublishTrusted(ctx, draft); err != nil {
						t.Fatal(err)
					}
				}
				contract := core.CompletionContract{TaskID: task.ID, TaskVersion: state.Version, Criteria: []core.CompletionCriterion{{ID: "durable-external-input", Assurance: core.AssuranceDeterministic, Required: true}}}
				detail := completionDetail{Contract: contract, Result: service.completion.Evaluate(contract, outcome)}
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
			if err := service.advanceInputContinuation(ctx, test.organization, test.correlationID, test.state, map[string]string{"input_event_ref": "event-1"}); err == nil {
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
	agent := core.Agent{ID: "agent-1", OrganizationID: organization.ID, BlueprintVersion: "v1", ExecutionProfileVersion: "v1", RuntimeAdapter: "local", Status: "ACTIVE"}
	intent := core.Intent{ID: "intent-1", OrganizationID: organization.ID, OriginalInstruction: "complete governed work", NormalizedObjective: "complete governed work"}
	goal := core.Goal{ID: "goal-1", IntentID: intent.ID, Objective: "complete governed work", Status: "ACTIVE"}
	child := core.Task{ID: "task-child", GoalID: goal.ID, ParentID: "task-parent", Description: "use unavailable tool", ExecutionKind: core.ExecutionTool, ModelInferencePolicy: core.InferenceForbidden, AssigneeType: "AGENT", AssigneeID: agent.ID, TaskContractVersion: "1", Status: core.TaskPending}
	parent := core.Task{ID: "task-parent", GoalID: goal.ID, Description: "govern child remediation", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, DependsOn: []core.ID{child.ID}, AssigneeType: "AGENT", AssigneeID: agent.ID, TaskContractVersion: "1", Status: core.TaskPending}
	for _, save := range []func() error{
		func() error {
			return repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "request-1", 1, organization, nil)
		},
		func() error { return repository.SaveAgent(ctx, "AGENT_CREATED", "runtime", "request-1", 1, agent, nil) },
		func() error {
			return repository.SaveIntent(ctx, "INTENT_CREATED", "runtime", "request-1", 1, intent, nil)
		},
		func() error {
			return repository.SaveGoal(ctx, organization.ID, "GOAL_CREATED", "runtime", "request-1", 1, goal, nil)
		},
		func() error {
			return repository.SaveTask(ctx, organization.ID, "TASK_CREATED", "runtime", "request-1", 1, parent, nil)
		},
		func() error {
			return repository.SaveTask(ctx, organization.ID, "TASK_CREATED", "runtime", "request-1", 1, child, nil)
		},
	} {
		if err := save(); err != nil {
			t.Fatal(err)
		}
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
	if snapshot.Tasks[child.ID].Value.Status != core.TaskBlocked || snapshot.Tasks[parent.ID].Value.Status != core.TaskBlocked {
		t.Fatalf("blocked child or governing parent state changed unexpectedly: child=%+v parent=%+v", snapshot.Tasks[child.ID], snapshot.Tasks[parent.ID])
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
	sender := core.Agent{ID: "agent-sender", OrganizationID: organization.ID, BlueprintVersion: "v1", ExecutionProfileVersion: "v1", RuntimeAdapter: "local", Status: "ACTIVE"}
	recipient := core.Agent{ID: "agent-recipient", OrganizationID: organization.ID, BlueprintVersion: "v1", ExecutionProfileVersion: "v1", RuntimeAdapter: "local", Status: "ACTIVE"}
	team := core.Team{ID: "team-1", OrganizationID: organization.ID, Name: "Delivery", MemberAgentIDs: []core.ID{recipient.ID}, Status: "ACTIVE"}
	intent := core.Intent{ID: "intent-1", OrganizationID: organization.ID, OriginalInstruction: "finish from handoff", NormalizedObjective: "finish from handoff"}
	goal := core.Goal{ID: "goal-1", IntentID: intent.ID, Objective: "finish from handoff", Status: "ACTIVE"}
	sourceTask := core.Task{ID: "task-source", GoalID: goal.ID, Description: "prepare handoff", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: sender.ID, TaskContractVersion: "1", Status: core.TaskCompleted}
	recipientTask := core.Task{ID: "task-recipient", GoalID: goal.ID, Description: "finish work", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, DependsOn: []core.ID{sourceTask.ID}, AssigneeType: "AGENT", AssigneeID: recipient.ID, TaskContractVersion: "1", Status: core.TaskPending}
	for _, save := range []func() error{
		func() error {
			return repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "request-1", 1, organization, nil)
		},
		func() error {
			return repository.SaveAgent(ctx, "AGENT_CREATED", "runtime", "request-1", 1, sender, nil)
		},
		func() error {
			return repository.SaveAgent(ctx, "AGENT_CREATED", "runtime", "request-1", 1, recipient, nil)
		},
		func() error { return repository.SaveTeam(ctx, "TEAM_CREATED", "runtime", "request-1", 1, team, nil) },
		func() error {
			return repository.SaveIntent(ctx, "INTENT_CREATED", "runtime", "request-1", 1, intent, nil)
		},
		func() error {
			return repository.SaveGoal(ctx, organization.ID, "GOAL_CREATED", "runtime", "request-1", 1, goal, nil)
		},
		func() error {
			return repository.SaveTask(ctx, organization.ID, "TASK_CREATED", "runtime", "request-1", 1, sourceTask, nil)
		},
		func() error {
			return repository.SaveTask(ctx, organization.ID, "TASK_CREATED", "runtime", "request-1", 1, recipientTask, nil)
		},
	} {
		if err := save(); err != nil {
			_ = l.Close()
			t.Fatal(err)
		}
	}
	dependencyResult, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: string(organization.ID), EventType: "RESULT_PUBLISHED", SourceActorID: "runtime",
		TaskID: string(sourceTask.ID), CorrelationID: "request-1", Payload: events.ResultPublishedPayload{Summary: "prepared handoff"},
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
}

var errProjectionWrite = errors.New("injected task projection failure")

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

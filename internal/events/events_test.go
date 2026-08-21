package events

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestTaskProjectionTransitionsAreExact(t *testing.T) {
	base := core.Task{
		ID: "task-1", WorkID: "work-1", Description: "bounded work",
		ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden,
		TaskContractVersion: "1", Status: core.TaskPending,
	}
	blocked, running, completed, failed := base, base, base, base
	blocked.Status = core.TaskBlocked
	running.Status = core.TaskRunning
	completed.Status = core.TaskCompleted
	failed.Status = core.TaskFailed
	for name, test := range map[string]struct {
		eventType string
		version   int
		previous  *core.Task
		next      core.Task
		valid     bool
	}{
		"pending creation":            {"TASK_CREATED", 1, nil, base, true},
		"blocked creation":            {"TASK_BLOCKED", 1, nil, blocked, true},
		"execution start":             {"EXECUTION_STARTED", 2, &base, running, true},
		"runtime recovery":            {"TASK_RECOVERED", 3, &running, base, true},
		"verified completion":         {"TASK_VERIFIED_COMPLETE", 3, &running, completed, true},
		"reviewed completion":         {"TASK_VERIFIED_COMPLETE", 3, &blocked, completed, true},
		"failed dependency":           {"TASK_DEPENDENCY_FAILED", 2, &base, failed, true},
		"mislabeled completion":       {"TASK_RESUMED", 3, &running, completed, false},
		"completion from pending":     {"TASK_VERIFIED_COMPLETE", 2, &base, completed, false},
		"execution without resume":    {"EXECUTION_STARTED", 3, &blocked, running, false},
		"terminal state reopened":     {"TASK_RESUMED", 4, &completed, base, false},
		"creation label after create": {"TASK_CREATED", 2, &base, base, false},
	} {
		t.Run(name, func(t *testing.T) {
			err := ValidateTaskProjectionTransition(test.eventType, test.version, test.previous, test.next)
			if test.valid && err != nil {
				t.Fatalf("valid transition was rejected: %v", err)
			}
			if !test.valid && err == nil {
				t.Fatal("invalid transition was accepted")
			}
		})
	}
	changed := running
	changed.Description = "substituted work"
	if err := ValidateTaskProjectionTransition("EXECUTION_STARTED", 2, &base, changed); err == nil {
		t.Fatal("Task lifecycle transition changed its immutable contract")
	}
}

func TestAgentProjectionTransitionsAreExact(t *testing.T) {
	active := core.Agent{
		ID: "agent-1", OrganizationID: "org-1", BlueprintID: "blueprint-1", BlueprintVersion: "v1",
		ExecutionProfileID: "profile-1", ExecutionProfileVersion: "v1", RuntimeAdapter: "local", Status: "ACTIVE",
	}
	inactive, configured := active, active
	inactive.Status = "INACTIVE"
	configured.RuntimeAdapter = "updated"
	for name, test := range map[string]struct {
		eventType string
		version   int
		previous  *core.Agent
		next      core.Agent
		valid     bool
	}{
		"active creation":                {"AGENT_CREATED", 1, nil, active, true},
		"update without prior state":     {"AGENT_CONFIGURATION_UPDATED", 2, nil, configured, false},
		"deactivate without prior state": {"AGENT_DEACTIVATED", 2, nil, inactive, false},
		"configuration update":           {"AGENT_CONFIGURATION_UPDATED", 2, &active, configured, true},
		"deactivation":                   {"AGENT_DEACTIVATED", 2, &active, inactive, true},
		"reactivation":                   {"AGENT_REACTIVATED", 3, &inactive, active, true},
		"inactive creation":              {"AGENT_CREATED", 1, nil, inactive, false},
		"mislabeled deactivation":        {"AGENT_DEACTIVATED", 2, &active, active, false},
		"mislabeled reactivation":        {"AGENT_REACTIVATED", 2, &active, active, false},
		"status-changing config event":   {"AGENT_CONFIGURATION_UPDATED", 2, &active, inactive, false},
		"config-changing lifecycle":      {"AGENT_DEACTIVATED", 2, &active, func() core.Agent { value := configured; value.Status = "INACTIVE"; return value }(), false},
	} {
		t.Run(name, func(t *testing.T) {
			err := ValidateAgentProjectionTransition(test.eventType, test.version, test.previous, test.next)
			if test.valid && err != nil {
				t.Fatalf("valid transition was rejected: %v", err)
			}
			if !test.valid && err == nil {
				t.Fatal("invalid transition was accepted")
			}
		})
	}
}

func TestGoalProjectionTransitionsAreExact(t *testing.T) {
	now := time.Now().UTC()
	active := core.Goal{
		ID: "goal-1", OrganizationID: "org-1", MissionID: "mission-1", Objective: "deliver a bounded outcome",
		Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "verified outcome", Origin: "USER"}}, Status: core.GoalActive, CreatedAt: now,
	}
	refined, paused, retired, achieved := active, active, active, active
	refined.Objective = "deliver a verified bounded outcome"
	paused.Status = core.GoalPaused
	retired.Status = core.GoalRetired
	achieved.Status = core.GoalAchieved
	for name, test := range map[string]struct {
		eventType string
		version   int
		previous  *core.Goal
		next      core.Goal
		valid     bool
	}{
		"active creation":               {"GOAL_CREATED", 1, nil, active, true},
		"refinement":                    {"GOAL_REFINED", 2, &active, refined, true},
		"pause":                         {"GOAL_PAUSED", 2, &active, paused, true},
		"resume":                        {"GOAL_RESUMED", 3, &paused, active, true},
		"retire":                        {"GOAL_RETIRED", 2, &active, retired, true},
		"achievement":                   {"GOAL_ACHIEVED", 2, &active, achieved, true},
		"pause label with active state": {"GOAL_PAUSED", 2, &active, active, false},
		"refinement label with pause":   {"GOAL_REFINED", 2, &active, paused, false},
		"unchanged refinement":          {"GOAL_REFINED", 2, &active, active, false},
		"creation without prior state":  {"GOAL_REFINED", 2, nil, refined, false},
	} {
		t.Run(name, func(t *testing.T) {
			err := ValidateGoalProjectionTransition(test.eventType, test.version, test.previous, test.next)
			if test.valid && err != nil {
				t.Fatalf("valid transition was rejected: %v", err)
			}
			if !test.valid && err == nil {
				t.Fatal("invalid transition was accepted")
			}
		})
	}
}

func TestMissionProjectionTransitionsAreExact(t *testing.T) {
	now := time.Now().UTC()
	active := core.Mission{ID: "mission-1", OrganizationID: "org-1", Statement: "durable direction", Status: core.MissionActive, CreatedAt: now}
	revised, retired := active, active
	revised.Statement = "refined durable direction"
	retired.Status = core.MissionRetired
	for name, test := range map[string]struct {
		eventType string
		version   int
		previous  *core.Mission
		next      core.Mission
		valid     bool
	}{
		"active creation":                {"MISSION_CREATED", 1, nil, active, true},
		"active refinement":              {"MISSION_REVISED", 2, &active, revised, true},
		"retirement":                     {"MISSION_RETIRED", 2, &active, retired, true},
		"retirement label with active":   {"MISSION_RETIRED", 2, &active, active, false},
		"revision label with retirement": {"MISSION_REVISED", 2, &active, retired, false},
		"unchanged refinement":           {"MISSION_REVISED", 2, &active, active, false},
		"revision without creation":      {"MISSION_REVISED", 2, nil, revised, false},
	} {
		t.Run(name, func(t *testing.T) {
			err := ValidateMissionProjectionTransition(test.eventType, test.version, test.previous, test.next)
			if test.valid && err != nil {
				t.Fatalf("valid transition was rejected: %v", err)
			}
			if !test.valid && err == nil {
				t.Fatal("invalid transition was accepted")
			}
		})
	}
}

func TestWorkProjectionTransitionsAreExact(t *testing.T) {
	active := core.Work{ID: "work-1", IntentID: "intent-1", Objective: "bounded work", Status: core.WorkActive, CreatedAt: time.Now().UTC()}
	completed, failed := active, active
	completed.Status = core.WorkCompleted
	failed.Status = core.WorkFailed
	for name, test := range map[string]struct {
		eventType string
		version   int
		previous  *core.Work
		next      core.Work
		valid     bool
	}{
		"active creation":                 {"WORK_CREATED", 1, nil, active, true},
		"completion":                      {"WORK_COMPLETED", 2, &active, completed, true},
		"execution failure":               {"WORK_FAILED", 2, &active, failed, true},
		"planning failure":                {"WORK_PLANNING_FAILED", 2, &active, failed, true},
		"failure label with active state": {"WORK_FAILED", 2, &active, active, false},
		"completion label with failure":   {"WORK_COMPLETED", 2, &active, failed, false},
		"terminal state reopened":         {"WORK_FAILED", 3, &completed, failed, false},
		"revision without creation":       {"WORK_FAILED", 2, nil, failed, false},
	} {
		t.Run(name, func(t *testing.T) {
			err := ValidateWorkProjectionTransition(test.eventType, test.version, test.previous, test.next)
			if test.valid && err != nil {
				t.Fatalf("valid transition was rejected: %v", err)
			}
			if !test.valid && err == nil {
				t.Fatal("invalid transition was accepted")
			}
		})
	}
}

func TestLabProjectionTransitionsAreExact(t *testing.T) {
	started := time.Now().UTC()
	running := core.Experiment{
		ID: "experiment-1", OrganizationID: "org-1", WorkID: "work-1", Objective: "bounded experiment",
		SandboxRef: "sandbox-1", CapabilityProfileRef: "lab-no-effects-v1", Status: core.ExperimentRunning,
		TrustLabel: core.ExperimentTrustUnverified, StartedAt: started,
		Budget: core.ExperimentBudget{MaxExecutions: 1, MaxUsageUnits: 1, MaxWallTimeSeconds: 60, AllowedInferencePools: []string{"deterministic"}},
	}
	finished := started.Add(time.Second)
	completed, failed := running, running
	completed.Status = core.ExperimentCompleted
	completed.ResultEventRefs = []string{"work-completed-event"}
	completed.FinishedAt = &finished
	failed.Status = core.ExperimentFailed
	failed.FailureCode = core.ExperimentFailureWorkFailed
	failed.FinishedAt = &finished
	rewritten := completed
	rewritten.Budget.MaxExecutions++
	for name, test := range map[string]struct {
		eventType string
		version   int
		previous  *core.Experiment
		next      core.Experiment
		valid     bool
	}{
		"start":                         {"LAB_EXPERIMENT_STARTED", 1, nil, running, true},
		"complete unverified":           {"LAB_EXPERIMENT_COMPLETED", 2, &running, completed, true},
		"fail":                          {"LAB_EXPERIMENT_FAILED", 2, &running, failed, true},
		"completion label with running": {"LAB_EXPERIMENT_COMPLETED", 2, &running, running, false},
		"rewrite budget":                {"LAB_EXPERIMENT_COMPLETED", 2, &running, rewritten, false},
	} {
		t.Run(name, func(t *testing.T) {
			err := ValidateExperimentProjectionTransition(test.eventType, test.version, test.previous, test.next)
			if test.valid && err != nil {
				t.Fatalf("valid transition was rejected: %v", err)
			}
			if !test.valid && err == nil {
				t.Fatal("invalid transition was accepted")
			}
		})
	}
}

func TestHumanCompletionRejectsEnvelopeArtifactsAbsentFromSubmission(t *testing.T) {
	contract := core.StructuredUserCompletionContract("task-1")
	task := WorkCompletionTaskBinding{Task: core.Task{
		ID: "task-1", ExecutionKind: core.ExecutionHuman, CompletionContract: &contract,
	}, Version: 1, CorrelationID: "run-1"}
	decision := CompletionDecisionPayload{
		Contract: contract, Result: core.CompletionResult{Complete: true},
		OutcomeEventRef: "outcome-1", SubmissionEventRef: "submission-1",
	}
	outcome := core.ToolOutcome{ArtifactRefs: []string{"forged-artifact"}}
	outcomeEvent := Event{
		EventID: "outcome-1", Sequence: 2, OrganizationID: "org-1",
		TaskID: "task-1", CorrelationID: "run-1", ArtifactRefs: outcome.ArtifactRefs,
	}
	verification := Event{
		EventID: "verification-1", Sequence: 3, OrganizationID: "org-1",
		TaskID: "task-1", CorrelationID: "run-1", ArtifactRefs: outcome.ArtifactRefs,
	}
	payload, err := json.Marshal(HumanTaskCompletionSubmittedPayload{
		MessageID: "submission-message-1", Fields: map[string]string{"response": "done"},
		SourcePrincipalID: "user-1", SourceChannel: "HUMAN_DIRECT",
	})
	if err != nil {
		t.Fatal(err)
	}
	submission := Event{
		EventID: "submission-1", Sequence: 1, OrganizationID: "org-1", EventType: "HUMAN_TASK_COMPLETION_SUBMITTED",
		SourceActorID: "user-1", TaskID: "task-1", CorrelationID: "run-1",
		ArtifactRefs: outcome.ArtifactRefs, Payload: payload,
	}
	binding := WorkCompletionBinding{OrganizationID: "org-1", CorrelationID: "run-1"}
	if _, err := completionDecisionResult(binding, task, decision, outcome, outcomeEvent, verification, []Event{submission}); err == nil {
		t.Fatal("submission envelope introduced artifacts absent from the authenticated payload")
	}
}

func TestHumanCompletionRequiresExactRuntimeOutcome(t *testing.T) {
	contract := core.CompletionContract{TaskID: "task-1", TaskVersion: 1, RequiredFields: []core.CompletionFieldRequirement{{Name: "response", MinBytes: 1, MaxBytes: 64}}}
	task := WorkCompletionTaskBinding{Task: core.Task{
		ID: "task-1", ExecutionKind: core.ExecutionHuman, CompletionContract: &contract,
	}, Version: 4, CorrelationID: "run-1"}
	payload := HumanTaskCompletionSubmittedPayload{
		MessageID: "message-1", SourcePrincipalID: "user-1", SourceChannel: "HUMAN_DIRECT",
		Fields: map[string]string{"response": "done"}, Artifacts: []core.ArtifactEvidence{},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	submission := Event{
		EventID: "submission-1", Sequence: 1, OrganizationID: "org-1", EventType: "HUMAN_TASK_COMPLETION_SUBMITTED",
		SourceActorID: "user-1", TaskID: "task-1", CorrelationID: "run-1", Payload: body,
	}
	now := time.Unix(10, 0).UTC()
	outcome := core.HumanTaskCompletionOutcome(submission.EventID, nil, now)
	outcomeEvent := Event{
		EventID: "outcome-1", Sequence: 2, OrganizationID: "org-1", EventType: "TOOL_OUTCOME_RECORDED",
		SourceActorID: "runtime", SourceExecutionID: "human-completion-" + submission.EventID, TaskID: "task-1", CorrelationID: "run-1",
	}
	verification := Event{EventID: "verification-1", Sequence: 3, OrganizationID: "org-1", EventType: "COMPLETION_VERIFIED", TaskID: "task-1", CorrelationID: "run-1"}
	decision := CompletionDecisionPayload{Contract: contract, SubmissionEventRef: submission.EventID}
	binding := WorkCompletionBinding{OrganizationID: "org-1", CorrelationID: "run-1"}
	result, err := completionDecisionResult(binding, task, decision, outcome, outcomeEvent, verification, []Event{submission})
	if err != nil || !result.Complete {
		t.Fatalf("exact runtime user outcome was rejected: result=%+v err=%v", result, err)
	}

	tests := []struct {
		name   string
		event  Event
		mutate func(*core.ToolOutcome)
	}{
		{name: "unrelated execution", event: func() Event { event := outcomeEvent; event.SourceExecutionID = "human-completion-other"; return event }()},
		{name: "failed", event: outcomeEvent, mutate: func(value *core.ToolOutcome) { value.Status = core.OutcomeFailed }},
		{name: "unchecked postcondition", event: outcomeEvent, mutate: func(value *core.ToolOutcome) { value.PostconditionStatus = core.PostconditionNotChecked }},
		{name: "unrelated invocation", event: outcomeEvent, mutate: func(value *core.ToolOutcome) { value.ToolInvocationID = "other" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := outcome
			if test.mutate != nil {
				test.mutate(&candidate)
			}
			if _, err := completionDecisionResult(binding, task, decision, candidate, test.event, verification, []Event{submission}); err == nil {
				t.Fatal("non-runtime user outcome authorized completion")
			}
		})
	}
}

func TestReviewedCompletionRevalidatesExactEvidencePayloads(t *testing.T) {
	now := time.Unix(10, 0).UTC()
	task := core.Task{ID: "task-1", Description: "produce the reviewed result", ExecutionKind: core.ExecutionAgent, AssigneeID: "agent-1"}
	contract := core.ReviewedOutcomeCompletionContract(task.ID, 2, nil)
	outcome := core.ToolOutcome{
		ToolInvocationID: "invocation-1", ToolID: "model/v1", Status: core.OutcomeSucceeded,
		ObservedEffect: "actual model output", PostconditionStatus: core.PostconditionNotChecked,
		Retryability: core.NotRetryable, ArtifactRefs: []string{"artifact-1"}, StartedAt: now, FinishedAt: now,
	}
	outcomeEvent := Event{EventID: "outcome-1", Sequence: 1, OrganizationID: "org-1", EventType: "TOOL_OUTCOME_RECORDED", SourceActorID: "runtime", SourceExecutionID: "execution-task-1-v2", TaskID: "task-1", ArtifactRefs: outcome.ArtifactRefs, CorrelationID: "work-1"}
	resultEvent := Event{EventID: "result-1", Sequence: 2, OrganizationID: "org-1", EventType: "RESULT_PUBLISHED", SourceActorID: "agent-1", SourceExecutionID: outcomeEvent.SourceExecutionID, TaskID: "task-1", ArtifactRefs: outcome.ArtifactRefs, CorrelationID: "work-1"}
	resultEvent.Payload, _ = json.Marshal(ResultPublishedPayload{Summary: "actual model output", ArtifactRefs: outcome.ArtifactRefs})
	candidateEvent := Event{EventID: "candidate-1", Sequence: 3, OrganizationID: "org-1", EventType: "CANDIDATE_COMPLETE", SourceActorID: "agent-1", SourceExecutionID: outcomeEvent.SourceExecutionID, TaskID: "task-1", ArtifactRefs: outcome.ArtifactRefs, CorrelationID: "work-1"}
	candidateEvent.Payload, _ = json.Marshal(CandidateCompletePayload{ToolInvocationID: string(outcome.ToolInvocationID), ResultEventID: resultEvent.EventID, ArtifactRefs: outcome.ArtifactRefs})
	request := completionReviewRequestPayload{
		ID: "review-1", OrganizationID: "org-1", TaskID: task.ID, TaskVersion: 2, Objective: task.Description,
		Contract: contract, EvidenceRefs: []string{outcomeEvent.EventID, resultEvent.EventID, candidateEvent.EventID}, CreatedAt: now,
	}
	request.Fingerprint, _ = completionReviewRequestFingerprint(request)
	requestEvent := Event{EventID: "request-1", Sequence: 4, OrganizationID: "org-1", EventType: "COMPLETION_REVIEW_REQUESTED", SourceActorID: "runtime", SourceExecutionID: outcomeEvent.SourceExecutionID, TaskID: "task-1", CorrelationID: "work-1"}
	requestEvent.Payload, _ = json.Marshal(request)
	review := completionReviewDecisionPayload{
		ReviewID: request.ID, OrganizationID: "org-1", TaskID: task.ID, TaskVersion: 2, Fingerprint: request.Fingerprint,
		Decision: core.CompletionReviewApprove, ReviewerID: "user-1", Method: core.AssuranceHumanJudgment, EvidenceRefs: request.EvidenceRefs, DecidedAt: now.Add(time.Second),
	}
	judgmentEvent := Event{EventID: "judgment-1", Sequence: 5, OrganizationID: "org-1", EventType: "COMPLETION_REVIEW_DECIDED", SourceActorID: "user-1", TaskID: "task-1", CorrelationID: "work-1"}
	judgmentEvent.Payload, _ = json.Marshal(review)
	verification := Event{EventID: "verification-1", Sequence: 6, OrganizationID: "org-1", EventType: "COMPLETION_VERIFIED", TaskID: "task-1", CorrelationID: "work-1"}
	decision := CompletionDecisionPayload{Contract: contract, OutcomeEventRef: outcomeEvent.EventID, JudgmentRef: judgmentEvent.EventID}
	binding := WorkCompletionBinding{OrganizationID: "org-1", CorrelationID: "work-1"}
	stream := []Event{outcomeEvent, resultEvent, candidateEvent, requestEvent, judgmentEvent, verification}
	approved, err := completionDecisionApproval(binding, task, decision, outcome, outcomeEvent, verification, stream)
	if err != nil || approved == nil || !*approved {
		t.Fatalf("exact reviewed evidence was rejected: approved=%v err=%v", approved, err)
	}

	for _, mutate := range []func([]Event){
		func(events []Event) {
			events[1].Payload, _ = json.Marshal(ResultPublishedPayload{Summary: "fabricated review summary", ArtifactRefs: outcome.ArtifactRefs})
		},
		func(events []Event) {
			events[2].Payload, _ = json.Marshal(CandidateCompletePayload{ToolInvocationID: string(outcome.ToolInvocationID), ResultEventID: "other-result", ArtifactRefs: outcome.ArtifactRefs})
		},
	} {
		forged := append([]Event(nil), stream...)
		mutate(forged)
		if _, err := completionDecisionApproval(binding, task, decision, outcome, outcomeEvent, verification, forged); err == nil {
			t.Fatal("substituted reviewed evidence payload authorized completion")
		}
	}
}

func TestResolveVerifiedTaskResultBindsEverySupportedExecutionKind(t *testing.T) {
	for _, kind := range []core.ExecutionKind{core.ExecutionDeterministic, core.ExecutionAgent, core.ExecutionHuman} {
		t.Run(string(kind), func(t *testing.T) {
			now := time.Unix(10, 0).UTC()
			task := core.Task{ID: "task-1", WorkID: "work-1", Description: "produce exact dependency evidence", ExecutionKind: kind, Status: core.TaskCompleted}
			actorID := "runtime"
			executionID := "execution-task-1-v1"
			if kind == core.ExecutionAgent {
				task.AssigneeID = "agent-1"
				actorID = string(task.AssigneeID)
			}
			if kind == core.ExecutionHuman {
				executionID = "external-input-input-1"
			}
			outcome := core.ToolOutcome{
				ToolInvocationID: "invocation-1", ToolID: "bounded/test", Status: core.OutcomeSucceeded,
				ObservedEffect: "verified dependency result", PostconditionStatus: core.PostconditionVerified,
				Retryability: core.NotRetryable, ArtifactRefs: []string{"artifact-1"}, StartedAt: now, FinishedAt: now.Add(time.Second),
			}
			summary, err := core.ToolOutcomeSummary(outcome)
			if err != nil {
				t.Fatal(err)
			}
			contract := core.CompletionContract{TaskID: task.ID, TaskVersion: 1}
			decision := CompletionDecisionPayload{Contract: contract, Result: core.CompletionResult{Complete: true}, OutcomeEventRef: "outcome-1"}
			makeEvent := func(id, eventType, sourceActor, sourceExecution string, sequence int64, payload any, artifactRefs []string) Event {
				t.Helper()
				body, marshalErr := json.Marshal(payload)
				if marshalErr != nil {
					t.Fatal(marshalErr)
				}
				return Event{
					EventID: id, Sequence: sequence, OrganizationID: "org-1", EventType: eventType,
					SourceActorID: sourceActor, SourceExecutionID: sourceExecution, TaskID: string(task.ID),
					ArtifactRefs: append([]string(nil), artifactRefs...), Payload: body, CorrelationID: "work-1",
					CreatedAt: now.Add(time.Duration(sequence) * time.Second), SchemaVersion: SchemaVersion,
				}
			}
			outcomeEvent := makeEvent("outcome-1", "TOOL_OUTCOME_RECORDED", "runtime", executionID, 10, outcome, outcome.ArtifactRefs)
			resultPayload := ResultPublishedPayload{Summary: summary, ArtifactRefs: outcome.ArtifactRefs}
			resultEvent := makeEvent("result-1", "RESULT_PUBLISHED", actorID, executionID, 20, resultPayload, outcome.ArtifactRefs)
			candidateEvent := makeEvent("candidate-1", "CANDIDATE_COMPLETE", actorID, executionID, 30, CandidateCompletePayload{
				ToolInvocationID: string(outcome.ToolInvocationID), ResultEventID: resultEvent.EventID, ArtifactRefs: outcome.ArtifactRefs,
			}, outcome.ArtifactRefs)
			verification := makeEvent("verification-1", "COMPLETION_VERIFIED", "runtime", executionID, 40, decision, outcome.ArtifactRefs)
			completed := makeEvent("completion-1", "TASK_VERIFIED_COMPLETE", "runtime", "", 50, nil, nil)
			value, err := json.Marshal(task)
			if err != nil {
				t.Fatal(err)
			}
			detail, err := json.Marshal(decision)
			if err != nil {
				t.Fatal(err)
			}
			sealed, err := SealProjectionEvent(completed, ProjectionRecord{
				ProjectionKind: "task", RecordID: string(task.ID), Version: 2, CorrelationID: "work-1", Value: value,
			}, detail)
			if err != nil {
				t.Fatal(err)
			}
			completed.Payload, err = json.Marshal(sealed)
			if err != nil {
				t.Fatal(err)
			}
			stream := []Event{outcomeEvent, resultEvent, candidateEvent, verification, completed}
			selected, result, err := ResolveVerifiedTaskResult("org-1", "work-1", task, 2, stream, 60)
			if err != nil || selected.EventID != resultEvent.EventID || !reflect.DeepEqual(result, resultPayload) {
				t.Fatalf("exact verified result was not resolved: event=%+v result=%+v err=%v", selected, result, err)
			}

			for name, mutate := range map[string]func(*Event){
				"cross actor":       func(event *Event) { event.SourceActorID = "other-actor" },
				"cross execution":   func(event *Event) { event.SourceExecutionID = "other-execution" },
				"authority bearing": func(event *Event) { event.AuthorizationRefs = []string{"approval-1"} },
				"substituted payload": func(event *Event) {
					event.Payload, _ = json.Marshal(ResultPublishedPayload{Summary: "substituted", ArtifactRefs: outcome.ArtifactRefs})
				},
			} {
				t.Run(name, func(t *testing.T) {
					forged := append([]Event(nil), stream...)
					mutate(&forged[1])
					if _, _, err := ResolveVerifiedTaskResult("org-1", "work-1", task, 2, forged, 60); err == nil {
						t.Fatal("substituted dependency result was accepted")
					}
				})
			}
			later := makeEvent("result-later", "RESULT_PUBLISHED", actorID, "other-execution", 60, ResultPublishedPayload{Summary: "later unverified result"}, nil)
			selected, _, err = ResolveVerifiedTaskResult("org-1", "work-1", task, 2, append(stream, later), 70)
			if err != nil || selected.EventID != resultEvent.EventID {
				t.Fatalf("later publication replaced the verified dependency result: event=%+v err=%v", selected, err)
			}
		})
	}
}

func TestDecodeDurableOperatorInputRequiresCanonicalContract(t *testing.T) {
	now := time.Unix(10, 0).UTC()
	payload := OperatorInputReceivedPayload{
		MessageID: "message-1", Text: "bounded user input", SourcePrincipalID: "external-agent",
		SourcePrincipalKind: string(core.PrincipalExternalAgent), SourceChannel: "A2A",
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	event := Event{
		EventID: "input-1", Sequence: 1, OrganizationID: "org-1", EventType: "A2A_INPUT_RECEIVED",
		SourceActorID: payload.SourcePrincipalID, TaskID: "task-1", CorrelationID: "work-1",
		Payload: body, CreatedAt: now, SchemaVersion: SchemaVersion,
	}
	decoded, err := DecodeDurableOperatorInput(event)
	if err != nil || !reflect.DeepEqual(decoded, payload) {
		t.Fatalf("canonical operator input was rejected: payload=%+v err=%v", decoded, err)
	}

	for name, invalidBody := range map[string][]byte{
		"legacy only":           []byte(`{"text":"bounded user input","source_external_actor":"external-agent"}`),
		"legacy extension":      []byte(`{"message_id":"message-1","text":"bounded user input","source_principal_id":"external-agent","source_principal_kind":"EXTERNAL_AGENT","source_channel":"A2A","source_external_actor":"external-agent"}`),
		"duplicate declaration": []byte(`{"message_id":"message-1","message_id":"message-2","text":"bounded user input","source_principal_id":"external-agent","source_principal_kind":"EXTERNAL_AGENT","source_channel":"A2A"}`),
	} {
		t.Run(name, func(t *testing.T) {
			candidate := event
			candidate.Payload = invalidBody
			if _, err := DecodeDurableOperatorInput(candidate); err == nil {
				t.Fatal("non-canonical operator input was accepted")
			}
		})
	}
}

func TestExecutionInboxUsesPersistedSnapshotCutoff(t *testing.T) {
	task := core.Task{ID: "task-1", ExecutionKind: core.ExecutionAgent, AssigneeType: "AGENT", AssigneeID: "agent-1"}
	running := task
	running.Status = core.TaskRunning
	value, err := json.Marshal(running)
	if err != nil {
		t.Fatal(err)
	}
	detail, err := json.Marshal(ExecutionStartDetail{InboxCutoffSequence: 1, DispatchBinding: &AgentDispatchBinding{}})
	if err != nil {
		t.Fatal(err)
	}
	startPayload, err := json.Marshal(ProjectionEventPayload{Projection: ProjectionRecord{ProjectionKind: "task", RecordID: "task-1", Version: 2, CorrelationID: "work-1", Value: value}, Detail: detail})
	if err != nil {
		t.Fatal(err)
	}
	selected := Event{EventID: "message-selected", Sequence: 1, OrganizationID: "org-1", EventType: "MESSAGE", SourceActorID: "sender", RecipientScope: RecipientAgent, RecipientID: "agent-1", CreatedAt: time.Unix(1, 0).UTC(), Payload: json.RawMessage(`{"body":"selected"}`)}
	arrivedBeforeStartAfterSnapshot := Event{EventID: "message-late", Sequence: 2, OrganizationID: "org-1", EventType: "MESSAGE", SourceActorID: "sender", RecipientScope: RecipientAgent, RecipientID: "agent-1", CreatedAt: time.Unix(2, 0).UTC(), Payload: json.RawMessage(`{"body":"next execution"}`)}
	start := Event{EventID: "start-1", Sequence: 3, OrganizationID: "org-1", EventType: "EXECUTION_STARTED", SourceActorID: "runtime", TaskID: "task-1", CorrelationID: "work-1", Payload: startPayload}
	refs, inbox, err := executionInbox(WorkCompletionBinding{OrganizationID: "org-1", CorrelationID: "work-1"}, task, start, []Event{selected, arrivedBeforeStartAfterSnapshot, start})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(refs, []string{selected.EventID}) || len(inbox) != 1 || inbox[0].EventID != selected.EventID {
		t.Fatalf("execution inbox crossed its durable cutoff: refs=%v inbox=%+v", refs, inbox)
	}
}

func TestPlannedAgentTaskRequiresDerivedExecutionBrief(t *testing.T) {
	intent := core.Intent{
		ID: "intent-1", OrganizationID: "org-1", NormalizedObjective: "produce a bounded result",
		Context: []core.IntentValue{}, Deliverables: []core.IntentValue{{Value: "result", Origin: "USER"}},
		CompletionCriteria: []core.IntentValue{{Value: "verified", Origin: "USER"}}, HardConstraints: []string{},
		ConsequenceBoundaries: []string{}, ResolvedDecisions: []core.IntentDecision{}, AcceptedFingerprint: "accepted", CreatedAt: time.Unix(1, 0).UTC(),
	}
	planned := core.PlanTask{Key: "root", Description: "perform bounded work", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, DependsOn: []string{}}
	brief, err := core.AgentTaskExecutionBrief(intent, planned, "plan-fingerprint")
	if err != nil {
		t.Fatal(err)
	}
	task := core.Task{
		ID: "task-1", WorkID: "work-1", Description: planned.Description, ExecutionBrief: brief,
		AcceptanceCriteria: intent.CompletionCriteria, ExecutionKind: planned.ExecutionKind, ModelInferencePolicy: planned.ModelInferencePolicy,
		DependsOn: []core.ID{}, TaskContractVersion: "1", Status: core.TaskCompleted,
	}
	binding := WorkCompletionBinding{Intent: intent}
	if err := validatePlannedTask(binding, task, planned, map[string]core.ID{"root": task.ID}, task.ID, "plan-fingerprint"); err != nil {
		t.Fatalf("derived Agent execution brief was rejected: %v", err)
	}
	task.ExecutionBrief = "substituted model input"
	if err := validatePlannedTask(binding, task, planned, map[string]core.ID{"root": task.ID}, task.ID, "plan-fingerprint"); err == nil {
		t.Fatal("substituted Agent execution brief matched the accepted Intent and Plan")
	}
}

type memoryLedger struct{ events []Event }

func (m *memoryLedger) Append(_ context.Context, d TrustedDraft) (Event, error) {
	e := Event{
		EventID:           "1",
		EventType:         d.EventType,
		OrganizationID:    d.OrganizationID,
		SourceActorID:     d.SourceActorID,
		SourceExecutionID: d.SourceExecutionID,
		RecipientScope:    d.RecipientScope,
		RecipientID:       d.RecipientID,
		TaskID:            d.TaskID,
		AuthorizationRefs: d.AuthorizationRefs,
		ArtifactRefs:      d.ArtifactRefs,
	}
	m.events = append(m.events, e)
	return e, nil
}
func (m *memoryLedger) Events(context.Context, string) ([]Event, error) { return m.events, nil }

func TestProjectionAdmissionBindsExactEventBoundary(t *testing.T) {
	record := ProjectionRecord{
		ProjectionKind: "task", RecordID: "task-1", Version: 2,
		CorrelationID: "work-1", Value: json.RawMessage(`{"id":"task-1"}`),
	}
	draft := TrustedDraft{
		OrganizationID: "org-1", EventType: "TASK_BLOCKED", SourceActorID: "runtime",
		TaskID: "task-1", CorrelationID: "work-1",
	}
	event := Event{
		EventID: "event-1", Sequence: 1, OrganizationID: draft.OrganizationID, EventType: draft.EventType,
		SourceActorID: draft.SourceActorID, TaskID: draft.TaskID, CorrelationID: draft.CorrelationID,
		CreatedAt: time.Unix(1, 0).UTC(), SchemaVersion: SchemaVersion,
	}
	payload, err := SealProjectionEvent(event, record, json.RawMessage(`{"reason":"bounded"}`))
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	event.Payload = body
	admitted, present, err := AdmittedProjection(event)
	if err != nil || !present || admitted.Admission.EventRef != event.EventID || admitted.Admission.Fingerprint == "" {
		t.Fatalf("admitted=%+v present=%t err=%v", admitted, present, err)
	}

	for name, mutate := range map[string]func(*Event){
		"event id":       func(candidate *Event) { candidate.EventID = "event-2" },
		"organization":   func(candidate *Event) { candidate.OrganizationID = "org-2" },
		"event type":     func(candidate *Event) { candidate.EventType = "TASK_RESUMED" },
		"task":           func(candidate *Event) { candidate.TaskID = "task-2" },
		"correlation":    func(candidate *Event) { candidate.CorrelationID = "work-2" },
		"schema version": func(candidate *Event) { candidate.SchemaVersion++ },
		"sequence":       func(candidate *Event) { candidate.Sequence++ },
		"created at":     func(candidate *Event) { candidate.CreatedAt = candidate.CreatedAt.Add(time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := event
			mutate(&candidate)
			if _, present, err := AdmittedProjection(candidate); err == nil || !present {
				t.Fatalf("changed %s retained admission: present=%t err=%v", name, present, err)
			}
		})
	}

	duplicate := event
	duplicate.Payload = []byte(`{"projection":{},"projection":{},"admission":{}}`)
	if _, _, err := AdmittedProjection(duplicate); err == nil {
		t.Fatal("duplicate projection declaration was accepted")
	}
	trailing := event
	trailing.Payload = append(append([]byte(nil), body...), []byte(`{}`)...)
	if _, _, err := AdmittedProjection(trailing); err == nil {
		t.Fatal("trailing projection payload was accepted")
	}
	malformed := event
	malformed.Payload = []byte(`{"ordinary":`)
	if _, _, err := AdmittedProjection(malformed); err == nil {
		t.Fatal("malformed ordinary event payload was accepted")
	}
	nullPayload := event
	nullPayload.Payload = []byte(`null`)
	if _, _, err := AdmittedProjection(nullPayload); err == nil {
		t.Fatal("null ordinary event payload was accepted")
	}
}

func TestGenericTrustedPublicationCannotMintProjectionAuthority(t *testing.T) {
	ledger := &memoryLedger{}
	gateway := NewGateway(ledger)
	record := ProjectionRecord{ProjectionKind: "team", RecordID: "team-1", Version: 1, CorrelationID: "work-1", Value: json.RawMessage(`{"id":"team-1"}`)}
	draft := TrustedDraft{OrganizationID: "org-1", EventType: "TEAM_CREATED", SourceActorID: "runtime", CorrelationID: "work-1"}
	boundary := Event{
		EventID: "event-1", Sequence: 1, OrganizationID: draft.OrganizationID, EventType: draft.EventType,
		SourceActorID: draft.SourceActorID, CorrelationID: draft.CorrelationID,
		CreatedAt: time.Unix(1, 0).UTC(), SchemaVersion: SchemaVersion,
	}
	sealed, err := SealProjectionEvent(boundary, record, nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, payload := range map[string]any{
		"projection key":  map[string]any{"projection": map[string]any{"record_id": "team-1"}},
		"admission key":   map[string]any{"admission": "forged"},
		"sealed payload":  sealed,
		"lifecycle label": map[string]string{"note": "ordinary payload"},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := draft
			candidate.Payload = payload
			if _, err := gateway.PublishTrusted(context.Background(), candidate); err == nil {
				t.Fatalf("generic trusted publication accepted %s", name)
			}
		})
	}
	if len(ledger.events) != 0 {
		t.Fatalf("rejected projection authority reached ledger: %+v", ledger.events)
	}
}

func TestTrustedPublicationRequiresObjectPayload(t *testing.T) {
	for name, payload := range map[string]any{
		"array":  []string{"value"},
		"scalar": "value",
		"null":   nil,
	} {
		t.Run(name, func(t *testing.T) {
			ledger := &memoryLedger{}
			gateway := NewGateway(ledger)
			if _, err := gateway.PublishTrusted(context.Background(), TrustedDraft{
				OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime",
				CorrelationID: "work-1", Payload: payload,
			}); err == nil {
				t.Fatalf("trusted publication accepted %s payload", name)
			}
			if len(ledger.events) != 0 {
				t.Fatalf("rejected %s payload reached ledger", name)
			}
		})
	}
}

type routeValidatorFunc func(context.Context, AddressedRoute) error

func (f routeValidatorFunc) ValidateAddressedRoute(ctx context.Context, route AddressedRoute) error {
	return f(ctx, route)
}

func TestInferenceUsageRejectsIntegerOverflow(t *testing.T) {
	usage := InferenceUsageRecordedPayload{Source: "provider", Provider: "provider", Model: "model", InputTokens: math.MaxInt, OutputTokens: 1, TotalTokens: math.MinInt}
	if usage.Valid() {
		t.Fatal("overflowed token usage was accepted")
	}
}

func TestAgentCannotMintTrustedStateEvents(t *testing.T) {
	trustedOnly := []string{
		"MISSION_CREATED",
		"MISSION_REVISED",
		"MISSION_RETIRED",
		"GOAL_CREATED",
		"GOAL_REFINED",
		"GOAL_PROGRESS_EVALUATED",
		"GOAL_ACHIEVED",
		"WORK_CREATED",
		"WORK_COMPLETED",
		"WORK_FAILED",
		"AGENT_BLUEPRINT_CREATED",
		"EXECUTION_PROFILE_CREATED",
		"AGENT_CREATED",
		"TEAM_CREATED",
		"TASK_ASSIGNED",
		"APPROVAL_DECIDED",
		"CAPABILITY_GRANTED",
		"FREEZE_SET",
		"ACTION_ATTESTED",
		"TOOL_OUTCOME_RECORDED",
		"INFERENCE_USAGE_RECORDED",
		"EXECUTION_CONTEXT_MANIFESTED",
		"INBOX_EVENTS_OBSERVED",
		"COMPLETION_VERIFIED",
		"TASK_VERIFIED_COMPLETE",
		"WORK_COMPLETION_EVALUATED",
		"RUN_TELEMETRY_RECORDED",
	}
	for _, eventType := range trustedOnly {
		t.Run(eventType, func(t *testing.T) {
			ledger := &memoryLedger{}
			gateway := NewGateway(ledger)
			_, err := gateway.PublishAgentDraft(context.Background(), "org", "agent", "execution", "correlation", Draft{EventType: eventType, Payload: map[string]any{"forged": true}})
			if err == nil || len(ledger.events) != 0 {
				t.Fatalf("agent draft minted trusted state: type=%s events=%+v err=%v", eventType, ledger.events, err)
			}
		})
	}
}

func TestIntentCannotBypassTypedReviewAdmission(t *testing.T) {
	for _, goalID := range []core.ID{"", "goal-1"} {
		ledger := &memoryLedger{}
		gateway := NewGateway(ledger)
		draft := TrustedDraft{
			OrganizationID: "org-1", EventType: "INTENT_CONFIRMED", SourceActorID: "user-1", TaskID: "task-work-1", CorrelationID: "work-1",
			Payload: IntentConfirmedPayload{
				IntentID: "intent-work-1", GoalID: string(goalID), Version: 1, Fingerprint: "fingerprint",
				ConfirmingActorID: "user-1", ConfirmingActorKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", MessageID: "message-1",
			},
		}
		if _, err := gateway.PublishTrusted(context.Background(), draft); err == nil {
			t.Fatalf("Intent with Goal %q bypassed typed review admission", goalID)
		}
		if _, err := gateway.PublishIntentConfirmation(context.Background(), draft, goalID, ""); err == nil {
			t.Fatalf("ledger without typed review admission accepted Intent with Goal %q", goalID)
		}
		if len(ledger.events) != 0 {
			t.Fatalf("rejected Intent confirmation reached ledger: %+v", ledger.events)
		}
	}
	ledger := &memoryLedger{}
	gateway := NewGateway(ledger)
	mismatched := TrustedDraft{
		OrganizationID: "org-1", EventType: "INTENT_CONFIRMED", SourceActorID: "user-1", TaskID: "task-work-1", CorrelationID: "work-1",
		Payload: IntentConfirmedPayload{IntentID: "intent-work-1", GoalID: "goal-1", Version: 1, Fingerprint: "fingerprint", ConfirmingActorID: "user-1", ConfirmingActorKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", MessageID: "message-1"},
	}
	if _, err := gateway.PublishIntentConfirmation(context.Background(), mismatched, "goal-2", ""); err == nil {
		t.Fatal("Intent payload Goal did not match the Goal selected for typed admission")
	}
}

func TestTerminalEvidenceCannotUseGenericTrustedAdmission(t *testing.T) {
	for _, eventType := range []string{"WORK_COMPLETION_EVALUATED", "WORK_COMPLETED", "GOAL_PROGRESS_EVALUATED", "GOAL_ACHIEVED"} {
		t.Run(eventType, func(t *testing.T) {
			ledger := &memoryLedger{}
			gateway := NewGateway(ledger)
			if _, err := gateway.PublishTrusted(context.Background(), TrustedDraft{
				OrganizationID: "org-1", EventType: eventType, SourceActorID: "runtime", CorrelationID: "run-1",
			}); err == nil {
				t.Fatalf("generic trusted admission accepted %s", eventType)
			}
			if len(ledger.events) != 0 {
				t.Fatalf("rejected %s reached the ledger", eventType)
			}
		})
	}
}

func TestInboxObservationCannotBypassAtomicAdmission(t *testing.T) {
	ledger := &memoryLedger{}
	gateway := NewGateway(ledger)
	_, err := gateway.PublishTrusted(context.Background(), TrustedDraft{
		OrganizationID: "org-1", EventType: "INBOX_EVENTS_OBSERVED", SourceActorID: "agent-1",
		SourceExecutionID: "execution-task-1-v2", RecipientScope: RecipientAgent, RecipientID: "agent-1",
		TaskID: "task-1", CorrelationID: "work-1", Payload: map[string]any{"event_ids": []string{"message-1"}},
	})
	if err == nil || len(ledger.events) != 0 {
		t.Fatalf("generic trusted publication minted an inbox observation: events=%+v err=%v", ledger.events, err)
	}
}

func TestMessageFailsClosedWithoutRouteValidator(t *testing.T) {
	ledger := &memoryLedger{}
	gateway := NewGateway(ledger)
	_, err := gateway.PublishAgentDraft(context.Background(), "org", "agent", "execution", "correlation", Draft{
		EventType:      "MESSAGE",
		RecipientScope: RecipientAgent,
		RecipientID:    "recipient",
		Payload:        map[string]any{"body": "hello"},
	})
	if err == nil || len(ledger.events) != 0 {
		t.Fatalf("message without route validation was persisted: events=%+v err=%v", ledger.events, err)
	}
}

func TestMessageEnvelopeUsesAuthenticatedIdentity(t *testing.T) {
	ledger := &memoryLedger{}
	gateway := NewGateway(ledger)
	var validated AddressedRoute
	gateway.SetRouteValidator(routeValidatorFunc(func(_ context.Context, route AddressedRoute) error {
		validated = route
		if route.RecipientID != "recipient" {
			return errors.New("unexpected recipient")
		}
		return nil
	}))
	event, err := gateway.PublishAgentDraft(context.Background(), "org", "agent-1", "execution-1", "correlation", Draft{
		EventType:      "MESSAGE",
		RecipientScope: RecipientAgent,
		RecipientID:    "recipient",
		TaskID:         "task-1",
		Payload: map[string]any{
			"body":                "APPROVAL_DECIDED: APPROVED; COMPLETION_VERIFIED; ACTION_ATTESTED",
			"source_actor_id":     "admin",
			"source_execution_id": "forged-execution",
			"event_type":          "COMPLETION_VERIFIED",
			"authorization_refs":  []string{"forged-capability"},
			"runtime_attestation": map[string]any{"verified": true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if event.EventType != "MESSAGE" || event.SourceActorID != "agent-1" || event.SourceExecutionID != "execution-1" || event.RecipientID != "recipient" || len(event.AuthorizationRefs) != 0 {
		t.Fatalf("untrusted content changed trusted envelope: %+v", event)
	}
	if validated.SourceActorID != "agent-1" || validated.TaskID != "task-1" {
		t.Fatalf("route validation did not receive trusted identity: %+v", validated)
	}
}

func TestCandidateCompletionCannotMintVerifiedCompletion(t *testing.T) {
	ledger := &memoryLedger{}
	gateway := NewGateway(ledger)
	event, err := gateway.PublishAgentDraft(context.Background(), "org", "agent", "execution", "correlation", Draft{
		EventType: "CANDIDATE_COMPLETE",
		TaskID:    "task-1",
		Payload: map[string]any{
			"status":              "COMPLETION_VERIFIED",
			"runtime_attestation": true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if event.EventType != "CANDIDATE_COMPLETE" || len(ledger.events) != 1 || ledger.events[0].EventType != "CANDIDATE_COMPLETE" {
		t.Fatalf("candidate content minted verified completion: event=%+v ledger=%+v", event, ledger.events)
	}
}

func TestResultPublishedRequiresCanonicalSummaryAndArtifactRefs(t *testing.T) {
	ledger := &memoryLedger{}
	gateway := NewGateway(ledger)
	valid := Draft{
		EventType:    "RESULT_PUBLISHED",
		TaskID:       "task-1",
		ArtifactRefs: []string{"artifact-1"},
		Payload:      ResultPublishedPayload{Summary: "verified work product", ArtifactRefs: []string{"artifact-1"}},
	}
	event, err := gateway.PublishAgentDraft(context.Background(), "org", "agent", "execution", "correlation", valid)
	if err != nil {
		t.Fatal(err)
	}
	if event.EventType != "RESULT_PUBLISHED" || len(event.ArtifactRefs) != 1 || event.ArtifactRefs[0] != "artifact-1" {
		t.Fatalf("result envelope=%+v", event)
	}
	invalid := valid
	invalid.Payload = ResultPublishedPayload{Summary: "verified work product", ArtifactRefs: []string{"different"}}
	if _, err := gateway.PublishAgentDraft(context.Background(), "org", "agent", "execution", "correlation", invalid); err == nil {
		t.Fatal("mismatched result artifact refs were accepted")
	}
	invalid = valid
	invalid.Payload = ResultPublishedPayload{ArtifactRefs: valid.ArtifactRefs}
	if _, err := gateway.PublishAgentDraft(context.Background(), "org", "agent", "execution", "correlation", invalid); err == nil {
		t.Fatal("result without summary was accepted")
	}
	if len(ledger.events) != 1 {
		t.Fatalf("invalid results were persisted: %+v", ledger.events)
	}
}

func TestTaskBlockedRequiresUpwardRouteAndContract(t *testing.T) {
	ledger := &memoryLedger{}
	gateway := NewGateway(ledger)
	gateway.SetRouteValidator(routeValidatorFunc(func(_ context.Context, route AddressedRoute) error {
		if route.RecipientScope != RecipientTask || route.RecipientID != "task-parent" || route.TaskID != "task-child" {
			return errors.New("blocked work was not routed to its parent task")
		}
		return nil
	}))
	valid := Draft{
		EventType:      "TASK_BLOCKED",
		RecipientScope: RecipientTask,
		RecipientID:    "task-parent",
		TaskID:         "task-child",
		Payload:        TaskBlockedPayload{Reason: "missing access", Missing: "read invoice", WhyNeeded: "complete assigned analysis", WorkCompleted: "validated inputs"},
	}
	if _, err := gateway.PublishAgentDraft(context.Background(), "org", "agent", "execution", "correlation", valid); err != nil {
		t.Fatal(err)
	}
	invalid := valid
	invalid.RecipientID = ""
	if _, err := gateway.PublishAgentDraft(context.Background(), "org", "agent", "execution", "correlation", invalid); err == nil {
		t.Fatal("unaddressed blocked work was accepted")
	}
	invalid = valid
	invalid.TaskID = ""
	if _, err := gateway.PublishAgentDraft(context.Background(), "org", "agent", "execution", "correlation", invalid); err == nil {
		t.Fatal("blocked work without a source child task was accepted")
	}
	invalid = valid
	invalid.RecipientScope = RecipientAgent
	if _, err := gateway.PublishAgentDraft(context.Background(), "org", "agent", "execution", "correlation", invalid); err == nil {
		t.Fatal("blocked work addressed outside the parent task scope was accepted")
	}
	invalid = valid
	invalid.Payload = TaskBlockedPayload{Reason: "missing access"}
	if _, err := gateway.PublishAgentDraft(context.Background(), "org", "agent", "execution", "correlation", invalid); err == nil {
		t.Fatal("incomplete blocked-work contract was accepted")
	}
}

func TestIntentConfirmationBindsOriginalAuthenticatedSource(t *testing.T) {
	intakePayload := IntakeMessageRecordedPayload{
		MessageID: "message-1", Text: "echo hello", SourcePrincipalID: "user-1",
		SourcePrincipalKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", RequestedExecutionKind: core.ExecutionDeterministic,
	}
	intakeBody, err := json.Marshal(intakePayload)
	if err != nil {
		t.Fatal(err)
	}
	confirmationPayload := IntentConfirmedPayload{
		IntentID: "intent-run-1", Version: 1, Fingerprint: "fingerprint", ConfirmingActorID: "user-1",
		ConfirmingActorKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", MessageID: "confirmation-1",
	}
	confirmationBody, err := json.Marshal(confirmationPayload)
	if err != nil {
		t.Fatal(err)
	}
	intakeEvent := Event{EventID: "evt-1", Sequence: 1, OrganizationID: "org-1", EventType: "INTAKE_MESSAGE_RECORDED", SourceActorID: "user-1", TaskID: "task-run-1", Payload: intakeBody, CorrelationID: "run-1", CreatedAt: time.Now().UTC(), SchemaVersion: SchemaVersion}
	confirmationEvent := Event{EventID: "evt-2", Sequence: 2, OrganizationID: "org-1", EventType: "INTENT_CONFIRMED", SourceActorID: "user-1", TaskID: "task-run-1", Payload: confirmationBody, CorrelationID: "run-1", CreatedAt: time.Now().UTC(), SchemaVersion: SchemaVersion}
	intent := core.Intent{
		ID: "intent-run-1", OrganizationID: "org-1", OriginalInstruction: "echo hello", AcceptedFingerprint: "fingerprint",
		SourcePrincipalID: "user-1", SourcePrincipalKind: core.PrincipalHuman, SourceChannel: "HUMAN_DIRECT", SourceMessageID: "message-1",
	}
	evidence := []Event{intakeEvent, confirmationEvent}
	if err := ValidateIntentConfirmation(evidence, confirmationEvent, intent); err != nil {
		t.Fatalf("valid source binding: %v", err)
	}
	for name, mutate := range map[string]func(*core.Intent){
		"principal": func(value *core.Intent) { value.SourcePrincipalID = "user-2" },
		"kind":      func(value *core.Intent) { value.SourcePrincipalKind = core.PrincipalExternalAgent },
		"channel":   func(value *core.Intent) { value.SourceChannel = "A2A" },
		"message":   func(value *core.Intent) { value.SourceMessageID = "message-2" },
		"text":      func(value *core.Intent) { value.OriginalInstruction = "different" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := intent
			mutate(&changed)
			if err := ValidateIntentConfirmation(evidence, confirmationEvent, changed); err == nil {
				t.Fatal("changed durable source identity was accepted")
			}
		})
	}
}

func TestReviewedIntentEvidenceIndexUsesConfirmationBoundary(t *testing.T) {
	stream := []Event{
		{Sequence: 4, CorrelationID: "run-1", EventType: "INTAKE_MESSAGE_RECORDED"},
		{Sequence: 1, CorrelationID: "run-1", EventType: "INTAKE_MESSAGE_RECORDED"},
		{Sequence: 2, CorrelationID: "run-2", EventType: "INTENT_DRAFTED"},
		{Sequence: 3, CorrelationID: "run-1", EventType: "INTENT_CONFIRMED"},
		{Sequence: 5, CorrelationID: "run-1", EventType: "OTHER"},
	}
	evidence := IndexReviewedIntentEvidence(stream).At(stream[3])
	if len(evidence) != 2 || evidence[0].Sequence != 1 || evidence[1].Sequence != 3 {
		t.Fatalf("bounded evidence=%+v", evidence)
	}
}

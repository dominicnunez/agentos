package events

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/dominicnunez/agentos/internal/core"
)

const SchemaVersion = 2

const (
	RecipientAgent = "AGENT"
	RecipientTeam  = "TEAM"
	RecipientTask  = "TASK"
)

type Draft struct {
	EventType      string   `json:"event_type"`
	RecipientScope string   `json:"recipient_scope,omitempty"`
	RecipientID    string   `json:"recipient_id,omitempty"`
	TaskID         string   `json:"task_id,omitempty"`
	ArtifactRefs   []string `json:"artifact_refs,omitempty"`
	Payload        any      `json:"payload"`
}

type TaskBlockedPayload struct {
	Code          string   `json:"code,omitempty"`
	Reason        string   `json:"reason"`
	Missing       string   `json:"missing"`
	WhyNeeded     string   `json:"why_needed"`
	WorkCompleted string   `json:"work_completed"`
	RemainingWork string   `json:"remaining_work,omitempty"`
	EvidenceRefs  []string `json:"evidence_refs,omitempty"`
	Urgency       string   `json:"urgency,omitempty"`
}

type ResultPublishedPayload struct {
	Summary      string   `json:"summary"`
	ArtifactRefs []string `json:"artifact_refs,omitempty"`
}

type CandidateCompletePayload struct {
	ToolInvocationID string   `json:"tool_invocation_id"`
	ResultEventID    string   `json:"result_event_id"`
	ArtifactRefs     []string `json:"artifact_refs"`
}

// OperatorInputReceivedPayload is the durable, untrusted-content contract for
// one continuation message from any authenticated operator channel. MessageID
// makes delivery retries idempotent; the trusted envelope identity remains
// authoritative.
type OperatorInputReceivedPayload struct {
	MessageID           string `json:"message_id"`
	Text                string `json:"text"`
	SourcePrincipalID   string `json:"source_principal_id"`
	SourcePrincipalKind string `json:"source_principal_kind"`
	SourceChannel       string `json:"source_channel"`
}

type HumanTaskCompletionSubmittedPayload struct {
	MessageID         string                  `json:"message_id"`
	Fields            map[string]string       `json:"fields"`
	Artifacts         []core.ArtifactEvidence `json:"artifacts,omitempty"`
	SourcePrincipalID string                  `json:"source_principal_id"`
	SourceChannel     string                  `json:"source_channel"`
}

type OperatorWorkAcceptedPayload struct {
	MessageID           string `json:"message_id"`
	SourcePrincipalID   string `json:"source_principal_id"`
	SourcePrincipalKind string `json:"source_principal_kind"`
	SourceChannel       string `json:"source_channel"`
}

type IntakeMessageRecordedPayload struct {
	MessageID              string             `json:"message_id"`
	Text                   string             `json:"text"`
	SourcePrincipalID      string             `json:"source_principal_id"`
	SourcePrincipalKind    string             `json:"source_principal_kind"`
	SourceChannel          string             `json:"source_channel"`
	RequestedExecutionKind core.ExecutionKind `json:"requested_execution_kind,omitempty"`
}

type IntentDraftedPayload struct {
	SourceMessageID string           `json:"source_message_id"`
	Draft           core.IntentDraft `json:"draft"`
	Reply           string           `json:"reply"`
}

type IntentNormalizationContextPayload struct {
	SourceMessageID         string   `json:"source_message_id"`
	PromptVersion           string   `json:"prompt_version"`
	Provider                string   `json:"provider"`
	Model                   string   `json:"model"`
	ExecutionProfileVersion string   `json:"execution_profile_version"`
	InputEventRefs          []string `json:"input_event_refs"`
}

type IntentConfirmedPayload struct {
	IntentID            string `json:"intent_id"`
	GoalID              string `json:"goal_id,omitempty"`
	Version             int    `json:"version"`
	Fingerprint         string `json:"fingerprint"`
	ConfirmingActorID   string `json:"confirming_actor_id"`
	ConfirmingActorKind string `json:"confirming_actor_kind"`
	SourceChannel       string `json:"source_channel"`
	MessageID           string `json:"message_id"`
}

type WorkCompletionTransitionPayload struct {
	EvidenceEventRef string `json:"evidence_event_ref"`
	Fingerprint      string `json:"fingerprint"`
}

type WorkCompletionTaskEvidencePayload struct {
	TaskID               core.ID  `json:"task_id"`
	TaskVersion          int      `json:"task_version"`
	VerificationEventRef string   `json:"verification_event_ref"`
	CompletionEventRef   string   `json:"completion_event_ref"`
	ArtifactRefs         []string `json:"artifact_refs"`
}

// WorkCompletionEvidencePayload is the durable runtime-owned evidence contract
// that must precede and authorize one terminal Work projection transition.
type WorkCompletionEvidencePayload struct {
	WorkID            core.ID                             `json:"work_id"`
	WorkVersion       int                                 `json:"work_version"`
	GoalID            core.ID                             `json:"goal_id,omitempty"`
	IntentID          core.ID                             `json:"intent_id"`
	IntentFingerprint string                              `json:"intent_fingerprint"`
	PlanID            core.ID                             `json:"plan_id"`
	PlanVersion       int                                 `json:"plan_version"`
	Criteria          []core.IntentValue                  `json:"criteria"`
	Tasks             []WorkCompletionTaskEvidencePayload `json:"tasks"`
	ArtifactRefs      []string                            `json:"artifact_refs"`
	CreatedAt         time.Time                           `json:"created_at"`
	Fingerprint       string                              `json:"fingerprint"`
}

type WorkCompletionTaskBinding struct {
	Task          core.Task
	Version       int
	CorrelationID string
}

type TeamRevisionBinding struct {
	Team              core.Team
	Version           int
	EffectiveSequence int64
}

type InboxEventsObservedPayload struct {
	EventIDs               []string `json:"event_ids"`
	ExecutionStartEventRef string   `json:"execution_start_event_ref"`
}

type InboxObservationBinding struct {
	EventIDs               []string
	ExecutionStartEventRef string
}

type ExecutionStartDetail struct {
	Mode                string `json:"mode,omitempty"`
	InboxCutoffSequence int64  `json:"inbox_cutoff_sequence"`
}

type InboxRoute struct {
	Scope string
	ID    string
}

type InboxSelection struct {
	Route  InboxRoute
	Events []Event
}

type WorkCompletionBinding struct {
	OrganizationID    string
	CorrelationID     string
	Work              core.Work
	WorkVersion       int
	Intent            core.Intent
	Tasks             []WorkCompletionTaskBinding
	TeamRevisions     map[core.ID][]TeamRevisionBinding
	InboxObservations map[string]InboxObservationBinding
	AgentBlueprints   map[core.ID]core.AgentBlueprint
	ExecutionProfiles map[core.ID]core.ExecutionProfile
}

type CompletionDecisionResultPayload = core.CompletionResult

type CompletionDecisionPayload struct {
	Contract           core.CompletionContract         `json:"contract"`
	Result             CompletionDecisionResultPayload `json:"result"`
	OutcomeEventRef    string                          `json:"outcome_event_ref"`
	SubmissionEventRef string                          `json:"submission_event_ref,omitempty"`
	JudgmentRef        string                          `json:"judgment_ref,omitempty"`
}

type completionReviewRequestPayload struct {
	ID             core.ID                 `json:"id"`
	OrganizationID core.ID                 `json:"organization_id"`
	TaskID         core.ID                 `json:"task_id"`
	TaskVersion    int                     `json:"task_version"`
	Objective      string                  `json:"objective"`
	Contract       core.CompletionContract `json:"contract"`
	EvidenceRefs   []string                `json:"evidence_refs"`
	CreatedAt      time.Time               `json:"created_at"`
	Fingerprint    string                  `json:"fingerprint"`
}

type completionReviewDecisionPayload struct {
	ReviewID       core.ID                       `json:"review_id"`
	OrganizationID core.ID                       `json:"organization_id"`
	TaskID         core.ID                       `json:"task_id"`
	TaskVersion    int                           `json:"task_version"`
	Fingerprint    string                        `json:"fingerprint"`
	Decision       core.CompletionReviewDecision `json:"decision"`
	ReviewerID     core.ID                       `json:"reviewer_id"`
	Method         core.Assurance                `json:"method"`
	EvidenceRefs   []string                      `json:"evidence_refs"`
	Feedback       string                        `json:"feedback,omitempty"`
	DecidedAt      time.Time                     `json:"decided_at"`
}

func (p WorkCompletionEvidencePayload) Valid() bool {
	_, offset := p.CreatedAt.Zone()
	if p.WorkID == "" || p.WorkVersion < 1 || p.IntentID == "" || !validSHA256(p.IntentFingerprint) || p.PlanID == "" || p.PlanVersion < 1 || p.CreatedAt.IsZero() || offset != 0 {
		return false
	}
	if len(p.Criteria) == 0 || len(p.Criteria) > 256 || len(p.Tasks) == 0 || len(p.Tasks) > 4096 {
		return false
	}
	for _, criterion := range p.Criteria {
		if strings.TrimSpace(criterion.Value) == "" || criterion.Origin == "" || !utf8.ValidString(criterion.Value) || !utf8.ValidString(criterion.Origin) || !utf8.ValidString(criterion.SourceMessageID) {
			return false
		}
	}
	references := make(map[string]struct{}, len(p.Tasks)*2)
	for index, task := range p.Tasks {
		if task.TaskID == "" || task.TaskVersion < 1 || !validEvidenceRef(task.VerificationEventRef) || !validEvidenceRef(task.CompletionEventRef) || task.VerificationEventRef == task.CompletionEventRef || len(task.ArtifactRefs) > 1024 || !distinctEvidenceRefs(task.ArtifactRefs) {
			return false
		}
		for _, ref := range []string{task.VerificationEventRef, task.CompletionEventRef} {
			if _, exists := references[ref]; exists {
				return false
			}
			references[ref] = struct{}{}
		}
		if index > 0 && p.Tasks[index-1].TaskID >= task.TaskID {
			return false
		}
	}
	if !slices.Equal(p.ArtifactRefs, WorkCompletionArtifactRefs(p.Tasks)) {
		return false
	}
	expected, err := p.ExpectedFingerprint()
	return err == nil && validSHA256(p.Fingerprint) && p.Fingerprint == expected
}

// ValidateWorkCompletionEvidenceChain resolves every event and current-state
// claim behind one aggregate Work completion record. Structural validity alone
// is insufficient: the immutable Intent, Plan, and complete Task set must all
// match runtime-owned events that precede the aggregate evidence.
func ValidateWorkCompletionEvidenceChain(binding WorkCompletionBinding, evidenceEvent Event, stream []Event) (WorkCompletionEvidencePayload, error) {
	var evidence WorkCompletionEvidencePayload
	if binding.OrganizationID == "" || binding.CorrelationID == "" || binding.Work.ID == "" || binding.Work.Status != core.WorkCompleted || binding.WorkVersion < 2 || binding.Intent.ID == "" ||
		binding.Intent.ID != binding.Work.IntentID || binding.Intent.GoalID != binding.Work.GoalID || binding.Intent.NormalizedObjective != binding.Work.Objective || string(binding.Intent.OrganizationID) != binding.OrganizationID || json.Unmarshal(evidenceEvent.Payload, &evidence) != nil || !evidence.Valid() {
		return WorkCompletionEvidencePayload{}, fmt.Errorf("work completion binding or evidence is invalid")
	}
	if evidenceEvent.EventType != "WORK_COMPLETION_EVALUATED" || evidenceEvent.OrganizationID != binding.OrganizationID || evidenceEvent.SourceActorID != "runtime" || evidenceEvent.SourceExecutionID != "" || evidenceEvent.TaskID != "" || evidenceEvent.CorrelationID != binding.CorrelationID || !slices.Equal(evidenceEvent.ArtifactRefs, evidence.ArtifactRefs) {
		return WorkCompletionEvidencePayload{}, fmt.Errorf("work completion evidence crosses its runtime boundary")
	}
	if evidence.WorkID != binding.Work.ID || evidence.WorkVersion != binding.WorkVersion || evidence.GoalID != binding.Work.GoalID || evidence.IntentID != binding.Intent.ID || evidence.IntentFingerprint != binding.Intent.AcceptedFingerprint || !reflect.DeepEqual(evidence.Criteria, binding.Intent.CompletionCriteria) {
		return WorkCompletionEvidencePayload{}, fmt.Errorf("work completion evidence does not bind the exact work and intent")
	}
	plan, err := completionEvidencePlan(binding, evidence, evidenceEvent, stream)
	if err != nil {
		return WorkCompletionEvidencePayload{}, err
	}
	if err := completionEvidenceTasks(binding, evidence, evidenceEvent, plan, stream); err != nil {
		return WorkCompletionEvidencePayload{}, err
	}
	return evidence, nil
}

func completionEvidencePlan(binding WorkCompletionBinding, evidence WorkCompletionEvidencePayload, evidenceEvent Event, stream []Event) (core.Plan, error) {
	var planEvent Event
	var plan core.Plan
	for _, event := range stream {
		if event.EventType != "PLAN_CREATED" || event.CorrelationID != binding.CorrelationID {
			continue
		}
		if planEvent.EventID != "" {
			return core.Plan{}, fmt.Errorf("work completion evidence has multiple durable plans")
		}
		if event.Sequence >= evidenceEvent.Sequence || event.OrganizationID != binding.OrganizationID || event.SourceActorID != "runtime" || event.TaskID != "task-"+binding.CorrelationID || json.Unmarshal(event.Payload, &plan) != nil {
			return core.Plan{}, fmt.Errorf("work completion plan crosses its runtime boundary")
		}
		planEvent = event
	}
	if planEvent.EventID == "" || plan.ID != evidence.PlanID || plan.Version != evidence.PlanVersion || plan.IntentID != binding.Intent.ID || plan.IntentFingerprint != binding.Intent.AcceptedFingerprint || plan.Fingerprint == "" {
		return core.Plan{}, fmt.Errorf("work completion evidence lacks its exact durable plan")
	}
	if planEvent.SourceExecutionID != "" && planEvent.SourceExecutionID != "planning-"+string(plan.ID)+"-attempt-1" {
		return core.Plan{}, fmt.Errorf("work completion plan execution identity is invalid")
	}
	fingerprint, err := core.FingerprintPlan(plan)
	if err != nil || fingerprint != plan.Fingerprint {
		return core.Plan{}, fmt.Errorf("work completion plan fingerprint is invalid")
	}
	return plan, nil
}

func completionEvidenceTasks(binding WorkCompletionBinding, evidence WorkCompletionEvidencePayload, evidenceEvent Event, plan core.Plan, stream []Event) error {
	if len(binding.Tasks) != len(evidence.Tasks) || len(plan.Tasks) != len(evidence.Tasks) {
		return fmt.Errorf("work completion evidence does not cover the complete task set")
	}
	plannedIDs := make(map[string]core.ID, len(plan.Tasks))
	for _, item := range plan.Tasks {
		var id core.ID
		if item.Key == "root" {
			id = core.ID("task-" + binding.CorrelationID)
		} else {
			id = core.ID("task-" + binding.CorrelationID + "-" + item.Key)
		}
		if item.Key == "" || id == "" {
			return fmt.Errorf("work completion plan has an invalid task identity")
		}
		if _, exists := plannedIDs[item.Key]; exists {
			return fmt.Errorf("work completion plan has duplicate task keys")
		}
		for _, plannedID := range plannedIDs {
			if plannedID == id {
				return fmt.Errorf("work completion plan has duplicate task identities")
			}
		}
		plannedIDs[item.Key] = id
	}
	rootID, rootExists := plannedIDs["root"]
	if !rootExists || rootID != core.ID("task-"+binding.CorrelationID) {
		return fmt.Errorf("work completion plan lacks its root task")
	}
	plannedTasks := make(map[core.ID]core.PlanTask, len(plan.Tasks))
	for _, item := range plan.Tasks {
		id := plannedIDs[item.Key]
		for _, dependency := range item.DependsOn {
			if _, exists := plannedIDs[dependency]; !exists {
				return fmt.Errorf("work completion plan has an invalid task dependency")
			}
		}
		if _, exists := plannedTasks[id]; exists {
			return fmt.Errorf("work completion plan has duplicate task identities")
		}
		plannedTasks[id] = item
	}
	bindings := make(map[core.ID]WorkCompletionTaskBinding, len(binding.Tasks))
	for _, task := range binding.Tasks {
		if task.Task.ID == "" || task.Task.WorkID != binding.Work.ID || task.Task.Status != core.TaskCompleted || task.Version < 1 || task.CorrelationID != binding.CorrelationID {
			return fmt.Errorf("work completion task binding is invalid")
		}
		planned, exists := plannedTasks[task.Task.ID]
		if !exists {
			return fmt.Errorf("work completion task is outside the immutable plan")
		}
		if err := validatePlannedTask(binding, task.Task, planned, plannedIDs, rootID, plan.Fingerprint); err != nil {
			return err
		}
		if err := validateCreatedTask(binding, task.Task, evidenceEvent, stream); err != nil {
			return err
		}
		if _, exists := bindings[task.Task.ID]; exists {
			return fmt.Errorf("work completion task binding is duplicated")
		}
		bindings[task.Task.ID] = task
	}
	for _, claim := range evidence.Tasks {
		bindingTask, exists := bindings[claim.TaskID]
		if !exists || bindingTask.Version != claim.TaskVersion {
			return fmt.Errorf("work completion evidence references stale task state")
		}
		verification, found := eventWithID(stream, claim.VerificationEventRef)
		if !found || verification.EventType != "COMPLETION_VERIFIED" || verification.OrganizationID != binding.OrganizationID || verification.SourceActorID != "runtime" || verification.TaskID != string(claim.TaskID) || verification.CorrelationID != binding.CorrelationID || verification.Sequence >= evidenceEvent.Sequence || !slices.Equal(verification.ArtifactRefs, claim.ArtifactRefs) {
			return fmt.Errorf("work completion verification reference is invalid")
		}
		var decision CompletionDecisionPayload
		if json.Unmarshal(verification.Payload, &decision) != nil || decision.OutcomeEventRef == "" || !decision.Result.Complete || len(decision.Result.Reasons) != 0 || decision.Contract.TaskID != claim.TaskID || decision.Contract.TaskVersion < 1 {
			return fmt.Errorf("work completion verification decision is not satisfied")
		}
		outcomeEvent, found := eventWithID(stream, decision.OutcomeEventRef)
		if !found || outcomeEvent.EventType != "TOOL_OUTCOME_RECORDED" || outcomeEvent.OrganizationID != binding.OrganizationID || outcomeEvent.SourceActorID != "runtime" || outcomeEvent.TaskID != string(claim.TaskID) || outcomeEvent.CorrelationID != binding.CorrelationID || outcomeEvent.Sequence >= verification.Sequence || !slices.Equal(outcomeEvent.ArtifactRefs, claim.ArtifactRefs) {
			return fmt.Errorf("work completion outcome reference is invalid")
		}
		var outcome core.ToolOutcome
		if json.Unmarshal(outcomeEvent.Payload, &outcome) != nil || outcome.ToolInvocationID == "" || outcome.ToolID == "" || outcome.StartedAt.IsZero() || outcome.FinishedAt.Before(outcome.StartedAt) || !slices.Equal(outcome.ArtifactRefs, claim.ArtifactRefs) {
			return fmt.Errorf("work completion outcome is invalid")
		}
		if verification.SourceExecutionID != "" && outcomeEvent.SourceExecutionID != verification.SourceExecutionID {
			return fmt.Errorf("work completion outcome crosses its execution boundary")
		}
		expected, err := completionDecisionResult(binding, bindingTask, decision, outcome, outcomeEvent, verification, stream)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(expected, decision.Result) {
			return fmt.Errorf("work completion decision does not match its durable evidence")
		}
		completionEvent, found := eventWithID(stream, claim.CompletionEventRef)
		if !found || completionEvent.EventType != "TASK_VERIFIED_COMPLETE" || completionEvent.OrganizationID != binding.OrganizationID || completionEvent.SourceActorID != "runtime" || completionEvent.SourceExecutionID != "" || completionEvent.TaskID != string(claim.TaskID) || completionEvent.CorrelationID != binding.CorrelationID || completionEvent.Sequence <= verification.Sequence || completionEvent.Sequence >= evidenceEvent.Sequence {
			return fmt.Errorf("work completion task transition reference is invalid")
		}
		var payload ProjectionEventPayload
		var projected core.Task
		var completionDecision CompletionDecisionPayload
		if json.Unmarshal(completionEvent.Payload, &payload) != nil || payload.Projection.ProjectionKind != "task" || payload.Projection.RecordID != string(claim.TaskID) || payload.Projection.Version != claim.TaskVersion || payload.Projection.CorrelationID != binding.CorrelationID || json.Unmarshal(payload.Projection.Value, &projected) != nil || !reflect.DeepEqual(projected, bindingTask.Task) || json.Unmarshal(payload.Detail, &completionDecision) != nil || !reflect.DeepEqual(completionDecision, decision) {
			return fmt.Errorf("work completion task transition does not match current state")
		}
	}
	return nil
}

func completionDecisionResult(binding WorkCompletionBinding, task WorkCompletionTaskBinding, decision CompletionDecisionPayload, outcome core.ToolOutcome, outcomeEvent, verification Event, stream []Event) (core.CompletionResult, error) {
	if task.Task.CompletionContract != nil {
		if task.Task.ExecutionKind != core.ExecutionHuman || !reflect.DeepEqual(*task.Task.CompletionContract, decision.Contract) || decision.SubmissionEventRef == "" || decision.JudgmentRef != "" {
			return core.CompletionResult{}, fmt.Errorf("work completion user evidence reference is invalid")
		}
		submissionEvent, found := eventWithID(stream, decision.SubmissionEventRef)
		if !found || submissionEvent.EventType != "HUMAN_TASK_COMPLETION_SUBMITTED" || submissionEvent.OrganizationID != binding.OrganizationID || submissionEvent.TaskID != verification.TaskID || submissionEvent.CorrelationID != binding.CorrelationID || submissionEvent.Sequence >= outcomeEvent.Sequence || submissionEvent.SourceActorID == "" || !slices.Equal(submissionEvent.ArtifactRefs, outcome.ArtifactRefs) {
			return core.CompletionResult{}, fmt.Errorf("work completion user submission reference is invalid")
		}
		var payload HumanTaskCompletionSubmittedPayload
		if json.Unmarshal(submissionEvent.Payload, &payload) != nil || payload.MessageID == "" || payload.SourcePrincipalID != submissionEvent.SourceActorID || payload.SourceChannel != "HUMAN_DIRECT" {
			return core.CompletionResult{}, fmt.Errorf("work completion user submission is invalid")
		}
		submittedArtifactRefs := make([]string, len(payload.Artifacts))
		for index, artifact := range payload.Artifacts {
			if artifact.Origin != payload.SourcePrincipalID {
				return core.CompletionResult{}, fmt.Errorf("work completion user artifact evidence is invalid")
			}
			submittedArtifactRefs[index] = artifact.Ref
		}
		if !slices.Equal(submissionEvent.ArtifactRefs, submittedArtifactRefs) {
			return core.CompletionResult{}, fmt.Errorf("work completion user artifact references do not match the submission")
		}
		expectedExecutionID := "human-completion-" + submissionEvent.EventID
		if outcomeEvent.SourceExecutionID != expectedExecutionID || !core.ValidHumanTaskCompletionOutcome(outcome, submissionEvent.EventID, submittedArtifactRefs) {
			return core.CompletionResult{}, fmt.Errorf("work completion user outcome is invalid")
		}
		return core.EvaluateHumanTaskCompletion(decision.Contract, core.HumanTaskSubmission{MessageID: payload.MessageID, Fields: payload.Fields, Artifacts: payload.Artifacts}), nil
	}
	if decision.SubmissionEventRef != "" {
		return core.CompletionResult{}, fmt.Errorf("work completion decision has unexpected user evidence")
	}
	expectedContract, verifiedOutcome, err := completionDecisionContract(binding, task.Task, decision, outcome, outcomeEvent, stream)
	if err != nil {
		return core.CompletionResult{}, err
	}
	if !reflect.DeepEqual(expectedContract, decision.Contract) {
		return core.CompletionResult{}, fmt.Errorf("work completion decision does not match its runtime-owned contract")
	}
	approved, err := completionDecisionApproval(binding, task.Task, decision, outcome, outcomeEvent, verification, stream)
	if err != nil {
		return core.CompletionResult{}, err
	}
	return core.EvaluateCompletion(decision.Contract, verifiedOutcome, approved), nil
}

func validatePlannedTask(binding WorkCompletionBinding, task core.Task, planned core.PlanTask, plannedIDs map[string]core.ID, rootID core.ID, planFingerprint string) error {
	dependsOn := make([]core.ID, 0, len(planned.DependsOn))
	for _, dependency := range planned.DependsOn {
		dependsOn = append(dependsOn, plannedIDs[dependency])
	}
	parentID := rootID
	acceptanceCriteria := []core.IntentValue(nil)
	var completionContract *core.CompletionContract
	if planned.Key == "root" {
		parentID = ""
		acceptanceCriteria = binding.Intent.CompletionCriteria
		if planned.ExecutionKind == core.ExecutionHuman && binding.Intent.SourcePrincipalKind == core.PrincipalHuman {
			contract := core.StructuredUserCompletionContract(task.ID)
			completionContract = &contract
		}
	}
	executionBrief := ""
	if planned.ExecutionKind == core.ExecutionAgent {
		var err error
		executionBrief, err = core.AgentTaskExecutionBrief(binding.Intent, planned, planFingerprint)
		if err != nil {
			return fmt.Errorf("derive work completion Agent execution brief: %w", err)
		}
	}
	if task.Description != planned.Description || task.ExecutionBrief != executionBrief || task.ExecutionKind != planned.ExecutionKind || task.ModelInferencePolicy != planned.ModelInferencePolicy || !slices.Equal(task.DependsOn, dependsOn) || task.ParentID != parentID || !slices.Equal(task.AcceptanceCriteria, acceptanceCriteria) || task.TaskContractVersion != "1" || !reflect.DeepEqual(task.CompletionContract, completionContract) {
		return fmt.Errorf("work completion task does not match its immutable plan")
	}
	return nil
}

func validateCreatedTask(binding WorkCompletionBinding, current core.Task, evidenceEvent Event, stream []Event) error {
	var createdEvent Event
	var created core.Task
	for _, event := range stream {
		if event.EventType != "TASK_CREATED" || event.TaskID != string(current.ID) || event.CorrelationID != binding.CorrelationID {
			continue
		}
		if createdEvent.EventID != "" {
			return fmt.Errorf("work completion task has multiple creation records")
		}
		var payload ProjectionEventPayload
		if event.Sequence >= evidenceEvent.Sequence || event.OrganizationID != binding.OrganizationID || event.SourceActorID != "runtime" || event.SourceExecutionID != "" || json.Unmarshal(event.Payload, &payload) != nil || payload.Projection.ProjectionKind != "task" || payload.Projection.RecordID != string(current.ID) || payload.Projection.Version != 1 || payload.Projection.CorrelationID != binding.CorrelationID || json.Unmarshal(payload.Projection.Value, &created) != nil || created.Status != core.TaskPending {
			return fmt.Errorf("work completion task creation record is invalid")
		}
		createdEvent = event
	}
	if createdEvent.EventID == "" || !sameTaskDefinition(created, current) {
		return fmt.Errorf("work completion task does not match its immutable creation record")
	}
	return nil
}

func sameTaskDefinition(left, right core.Task) bool {
	return left.ID == right.ID && left.WorkID == right.WorkID && left.Description == right.Description &&
		left.ExecutionBrief == right.ExecutionBrief && slices.Equal(left.AcceptanceCriteria, right.AcceptanceCriteria) &&
		left.ExecutionKind == right.ExecutionKind && left.ModelInferencePolicy == right.ModelInferencePolicy &&
		slices.Equal(left.DependsOn, right.DependsOn) && left.ParentID == right.ParentID &&
		left.AssigneeType == right.AssigneeType && left.AssigneeID == right.AssigneeID && reflect.DeepEqual(left.AgentConfig, right.AgentConfig) &&
		left.RuntimeHandlerRef == right.RuntimeHandlerRef && left.TaskContractVersion == right.TaskContractVersion &&
		reflect.DeepEqual(left.CompletionContract, right.CompletionContract)
}

func completionDecisionContract(binding WorkCompletionBinding, task core.Task, decision CompletionDecisionPayload, outcome core.ToolOutcome, outcomeEvent Event, stream []Event) (core.CompletionContract, core.ToolOutcome, error) {
	startEvent, remediation, err := validateExecutionStart(binding, task, decision.Contract.TaskVersion, outcomeEvent, stream)
	if err != nil {
		return core.CompletionContract{}, core.ToolOutcome{}, err
	}
	switch task.ExecutionKind {
	case core.ExecutionDeterministic:
		expectedExecutionID := fmt.Sprintf("execution-%s-v%d", task.ID, decision.Contract.TaskVersion)
		if outcomeEvent.SourceExecutionID != expectedExecutionID || outcome.ToolID != "builtin.echo" || decision.JudgmentRef != "" {
			return core.CompletionContract{}, core.ToolOutcome{}, fmt.Errorf("work completion deterministic execution identity is invalid")
		}
		verified, available := core.VerifyPersistedPostcondition(task, outcome, "")
		if !available || verified.PostconditionStatus != core.PostconditionVerified {
			return core.CompletionContract{}, core.ToolOutcome{}, fmt.Errorf("work completion lacks its registered deterministic verification")
		}
		return core.VerifiedOutcomeCompletionContract(task.ID, decision.Contract.TaskVersion), verified, nil
	case core.ExecutionAgent:
		expectedExecutionID := fmt.Sprintf("execution-%s-v%d", task.ID, decision.Contract.TaskVersion)
		if outcomeEvent.SourceExecutionID != expectedExecutionID {
			return core.CompletionContract{}, core.ToolOutcome{}, fmt.Errorf("work completion Agent execution identity is invalid")
		}
		if remediation {
			return core.CompletionContract{}, core.ToolOutcome{}, fmt.Errorf("work completion cannot use a dependency-remediation execution")
		}
		model, err := completionExecutionModel(binding, task, expectedExecutionID, startEvent, outcomeEvent, stream)
		if err != nil {
			return core.CompletionContract{}, core.ToolOutcome{}, fmt.Errorf("work completion Agent execution manifest is invalid: %w", err)
		}
		if model.Model != outcome.ToolID && model.Provider+"/"+model.Model != outcome.ToolID {
			return core.CompletionContract{}, core.ToolOutcome{}, fmt.Errorf("work completion Agent execution manifest is invalid")
		}
		verified, available := core.VerifyPersistedPostcondition(task, outcome, model.ExecutionInputSHA256)
		if available {
			if verified.PostconditionStatus != core.PostconditionVerified || decision.JudgmentRef != "" {
				return core.CompletionContract{}, core.ToolOutcome{}, fmt.Errorf("work completion Agent verification evidence is invalid")
			}
			return core.VerifiedOutcomeCompletionContract(task.ID, decision.Contract.TaskVersion), verified, nil
		}
		if decision.JudgmentRef == "" {
			return core.CompletionContract{}, core.ToolOutcome{}, fmt.Errorf("work completion Agent result lacks independent judgment")
		}
		verified.PostconditionStatus = core.PostconditionNotChecked
		return core.ReviewedOutcomeCompletionContract(task.ID, decision.Contract.TaskVersion, task.AcceptanceCriteria), verified, nil
	case core.ExecutionHuman:
		if decision.JudgmentRef != "" || outcome.ToolID != "a2a.external-input" || !strings.HasPrefix(outcomeEvent.SourceExecutionID, "external-input-") {
			return core.CompletionContract{}, core.ToolOutcome{}, fmt.Errorf("work completion external-input evidence is invalid")
		}
		inputID := strings.TrimPrefix(outcomeEvent.SourceExecutionID, "external-input-")
		inputEvent, found := eventWithID(stream, inputID)
		if !found || !validCompletionInputEvent(binding, task.ID, inputEvent) || inputEvent.Sequence >= outcomeEvent.Sequence {
			return core.CompletionContract{}, core.ToolOutcome{}, fmt.Errorf("work completion external-input source is invalid")
		}
		var observed struct {
			InputEventRef string `json:"input_event_ref"`
		}
		body, err := json.Marshal(outcome.ObservedEffect)
		if err != nil || json.Unmarshal(body, &observed) != nil || observed.InputEventRef != inputID || outcome.Status != core.OutcomeSucceeded || outcome.ToolInvocationID != core.ID("a2a-input-"+inputID) {
			return core.CompletionContract{}, core.ToolOutcome{}, fmt.Errorf("work completion external-input outcome is invalid")
		}
		outcome.PostconditionStatus = core.PostconditionVerified
		return core.ExternalInputCompletionContract(task.ID, decision.Contract.TaskVersion), outcome, nil
	case core.ExecutionTool, core.ExecutionTeam, core.ExecutionMixed:
		return core.CompletionContract{}, core.ToolOutcome{}, fmt.Errorf("work completion uses an unavailable execution kind")
	default:
		return core.CompletionContract{}, core.ToolOutcome{}, fmt.Errorf("work completion uses an unsupported execution kind")
	}
}

type executionModel struct {
	Provider             string
	Model                string
	ExecutionInputSHA256 string
}

func completionExecutionModel(binding WorkCompletionBinding, task core.Task, executionID string, startEvent, outcomeEvent Event, stream []Event) (executionModel, error) {
	if task.AssigneeType != "AGENT" || task.AssigneeID == "" || task.AgentConfig == nil {
		return executionModel{}, fmt.Errorf("work completion Agent assignment is invalid")
	}
	var found Event
	var manifest core.ExecutionContextManifest
	for _, event := range stream {
		if event.EventType != "EXECUTION_CONTEXT_MANIFESTED" || event.TaskID != string(task.ID) || event.SourceExecutionID != executionID || event.CorrelationID != binding.CorrelationID {
			continue
		}
		if found.EventID != "" || event.Sequence <= startEvent.Sequence || event.Sequence >= outcomeEvent.Sequence || event.OrganizationID != binding.OrganizationID || json.Unmarshal(event.Payload, &manifest) != nil {
			return executionModel{}, fmt.Errorf("work completion Agent manifest record is invalid")
		}
		found = event
	}
	config := task.AgentConfig
	profile, profileFound := binding.ExecutionProfiles[config.ProfileID]
	_, createdOffset := manifest.CreatedAt.Zone()
	if found.EventID == "" || !profileFound || profile.ID != config.ProfileID || profile.OrganizationID != core.ID(binding.OrganizationID) || profile.Version != config.ProfileVersion ||
		manifest.ExecutionID != core.ID(executionID) || manifest.TaskID != task.ID || manifest.AgentID != task.AssigneeID || manifest.AgentBlueprintVersion != config.BlueprintVersion || manifest.ExecutionProfileVersion != profile.Version || manifest.RuntimeAdapter != config.RuntimeAdapter || manifest.Provider != profile.ModelProvider || manifest.Model != profile.Model || manifest.TaskContractVersion != task.TaskContractVersion || manifest.PromptVersion != profile.PromptVersion || manifest.PolicyVersion != "v1" || manifest.ContextBuilderVersion != "v1" || manifest.CreatedAt.IsZero() || createdOffset != 0 ||
		len(manifest.KnowledgeRefs) != 0 || len(manifest.SkillRefs) != 0 || len(manifest.ToolDefinitions) != 0 || len(manifest.ArtifactRefs) != 0 || len(manifest.AdditionalContextRefs) != 0 || !validSHA256(manifest.ExecutionInputSHA256) {
		return executionModel{}, fmt.Errorf("work completion Agent manifest does not match its immutable Task")
	}
	expectedInput, err := expectedAgentExecutionInput(binding, task, startEvent, found, manifest, stream)
	if err != nil || manifest.ExecutionInputSHA256 != core.FingerprintExecutionInput(expectedInput) {
		return executionModel{}, fmt.Errorf("work completion Agent manifest input does not match durable execution context")
	}
	return executionModel{Provider: manifest.Provider, Model: manifest.Model, ExecutionInputSHA256: manifest.ExecutionInputSHA256}, nil
}

func expectedAgentExecutionInput(binding WorkCompletionBinding, task core.Task, startEvent, manifestEvent Event, manifest core.ExecutionContextManifest, stream []Event) (string, error) {
	blueprint, err := executionBlueprint(binding, task)
	if err != nil {
		return "", err
	}
	dependencyRefs, dependencies, err := executionDependencies(binding, task, manifestEvent, stream)
	if err != nil {
		return "", err
	}
	inboxRefs, inbox, err := executionInbox(binding, task, startEvent, stream)
	if err != nil {
		return "", err
	}
	revisionRef, revision, err := executionRevision(binding, task, manifestEvent, stream)
	if err != nil {
		return "", err
	}
	expectedRefs := append(append([]string(nil), inboxRefs...), dependencyRefs...)
	if revisionRef != "" {
		expectedRefs = append(expectedRefs, revisionRef)
	}
	if !slices.Equal(manifest.EventRefs, expectedRefs) {
		return "", fmt.Errorf("execution context references do not match durable runtime selection")
	}
	_, input, err := core.MaterializeAgentExecutionInput(core.AgentExecutionInputContext{
		Blueprint: blueprint, Task: task, DependencyResults: dependencies, InboxEvents: inbox, Revision: revision,
	})
	return input, err
}

func executionBlueprint(binding WorkCompletionBinding, task core.Task) (core.AgentBlueprint, error) {
	blueprint, found := binding.AgentBlueprints[task.AgentConfig.BlueprintID]
	if !found || blueprint.ID != task.AgentConfig.BlueprintID || blueprint.OrganizationID != core.ID(binding.OrganizationID) || blueprint.Version != task.AgentConfig.BlueprintVersion || blueprint.Role == "" || blueprint.OperatingInstructions == "" {
		return core.AgentBlueprint{}, fmt.Errorf("execution blueprint is unavailable or invalid")
	}
	return blueprint, nil
}

func executionDependencies(binding WorkCompletionBinding, task core.Task, manifestEvent Event, stream []Event) ([]string, []core.AgentExecutionDependencyResult, error) {
	refs := make([]string, 0, len(task.DependsOn))
	results := make([]core.AgentExecutionDependencyResult, 0, len(task.DependsOn))
	for _, dependencyID := range task.DependsOn {
		var dependency *WorkCompletionTaskBinding
		for index := range binding.Tasks {
			if binding.Tasks[index].Task.ID == dependencyID {
				dependency = &binding.Tasks[index]
				break
			}
		}
		if dependency == nil || dependency.Task.WorkID != task.WorkID || dependency.Task.Status != core.TaskCompleted || dependency.CorrelationID != binding.CorrelationID {
			return nil, nil, fmt.Errorf("execution dependency is outside completed durable Work")
		}
		selected, result, err := ResolveVerifiedTaskResult(binding.OrganizationID, binding.CorrelationID, dependency.Task, dependency.Version, stream, manifestEvent.Sequence)
		if err != nil {
			return nil, nil, fmt.Errorf("execution dependency result is invalid: %w", err)
		}
		refs = append(refs, selected.EventID)
		results = append(results, core.AgentExecutionDependencyResult{
			TaskID: dependencyID, ResultEvent: selected.EventID, Summary: result.Summary,
			ArtifactRefs: append([]string(nil), result.ArtifactRefs...),
		})
	}
	return refs, results, nil
}

// ResolveVerifiedTaskResult returns the single result publication bound to an
// exact verified Task completion. Later publications cannot replace it because
// the result and candidate must precede and match the admitted verification.
func ResolveVerifiedTaskResult(organizationID, correlationID string, task core.Task, taskVersion int, stream []Event, beforeSequence int64) (Event, ResultPublishedPayload, error) {
	if organizationID == "" || correlationID == "" || task.ID == "" || task.Status != core.TaskCompleted || taskVersion < 2 {
		return Event{}, ResultPublishedPayload{}, fmt.Errorf("verified Task result boundary is incomplete")
	}
	var completionEvent Event
	var decision CompletionDecisionPayload
	for _, event := range stream {
		if event.EventType != "TASK_VERIFIED_COMPLETE" || event.TaskID != string(task.ID) || event.CorrelationID != correlationID || beforeSequence > 0 && event.Sequence >= beforeSequence {
			continue
		}
		var payload ProjectionEventPayload
		var projected core.Task
		var candidate CompletionDecisionPayload
		if event.OrganizationID != organizationID || event.SourceActorID != "runtime" || event.SourceExecutionID != "" || event.RecipientScope != "" || event.RecipientID != "" ||
			json.Unmarshal(event.Payload, &payload) != nil || payload.Projection.ProjectionKind != "task" || payload.Projection.RecordID != string(task.ID) || payload.Projection.Version != taskVersion || payload.Projection.CorrelationID != correlationID ||
			json.Unmarshal(payload.Projection.Value, &projected) != nil || !reflect.DeepEqual(projected, task) || json.Unmarshal(payload.Detail, &candidate) != nil || !candidate.Result.Complete || len(candidate.Result.Reasons) != 0 || candidate.OutcomeEventRef == "" || candidate.Contract.TaskID != task.ID || candidate.Contract.TaskVersion < 1 {
			return Event{}, ResultPublishedPayload{}, fmt.Errorf("verified Task result has an invalid terminal transition")
		}
		if completionEvent.EventID != "" {
			return Event{}, ResultPublishedPayload{}, fmt.Errorf("verified Task result has multiple terminal transitions")
		}
		completionEvent, decision = event, candidate
	}
	if completionEvent.EventID == "" {
		return Event{}, ResultPublishedPayload{}, fmt.Errorf("verified Task result lacks its terminal transition")
	}
	var verification Event
	for _, event := range stream {
		if event.EventType != "COMPLETION_VERIFIED" || event.TaskID != string(task.ID) || event.CorrelationID != correlationID || event.Sequence >= completionEvent.Sequence {
			continue
		}
		var candidate CompletionDecisionPayload
		if event.OrganizationID != organizationID || event.SourceActorID != "runtime" || event.RecipientScope != "" || event.RecipientID != "" || json.Unmarshal(event.Payload, &candidate) != nil || !reflect.DeepEqual(candidate, decision) {
			continue
		}
		if verification.EventID != "" {
			return Event{}, ResultPublishedPayload{}, fmt.Errorf("verified Task result has multiple completion verifications")
		}
		verification = event
	}
	if verification.EventID == "" {
		return Event{}, ResultPublishedPayload{}, fmt.Errorf("verified Task result lacks its completion verification")
	}
	outcomeEvent, found := eventWithID(stream, decision.OutcomeEventRef)
	var outcome core.ToolOutcome
	if !found || outcomeEvent.EventType != "TOOL_OUTCOME_RECORDED" || outcomeEvent.OrganizationID != organizationID || outcomeEvent.SourceActorID != "runtime" || outcomeEvent.SourceExecutionID == "" || outcomeEvent.RecipientScope != "" || outcomeEvent.RecipientID != "" || outcomeEvent.TaskID != string(task.ID) || outcomeEvent.CorrelationID != correlationID || outcomeEvent.Sequence >= verification.Sequence ||
		json.Unmarshal(outcomeEvent.Payload, &outcome) != nil || outcome.ToolInvocationID == "" || outcome.ToolID == "" || outcome.StartedAt.IsZero() || outcome.FinishedAt.Before(outcome.StartedAt) || !slices.Equal(outcomeEvent.ArtifactRefs, outcome.ArtifactRefs) || !slices.Equal(verification.ArtifactRefs, outcome.ArtifactRefs) || verification.SourceExecutionID != "" && verification.SourceExecutionID != outcomeEvent.SourceExecutionID {
		return Event{}, ResultPublishedPayload{}, fmt.Errorf("verified Task result outcome is invalid")
	}
	expectedSummary, err := core.ToolOutcomeSummary(outcome)
	if err != nil {
		return Event{}, ResultPublishedPayload{}, fmt.Errorf("derive verified Task result summary: %w", err)
	}
	expectedActorID := "runtime"
	if task.ExecutionKind == core.ExecutionAgent {
		expectedActorID = string(task.AssigneeID)
		if expectedActorID == "" {
			return Event{}, ResultPublishedPayload{}, fmt.Errorf("verified Agent Task result lacks its assignee")
		}
	}
	var resultEvent Event
	var result ResultPublishedPayload
	for _, event := range stream {
		if event.EventType != "RESULT_PUBLISHED" || event.TaskID != string(task.ID) || event.CorrelationID != correlationID || event.Sequence <= outcomeEvent.Sequence || event.Sequence >= verification.Sequence {
			continue
		}
		var candidate ResultPublishedPayload
		if event.OrganizationID != organizationID || event.SourceActorID != expectedActorID || event.SourceExecutionID != outcomeEvent.SourceExecutionID || event.RecipientScope != "" || event.RecipientID != "" || json.Unmarshal(event.Payload, &candidate) != nil || !candidate.ValidFor(event.ArtifactRefs) || candidate.Summary != expectedSummary || !slices.Equal(candidate.ArtifactRefs, outcome.ArtifactRefs) {
			continue
		}
		if resultEvent.EventID != "" {
			return Event{}, ResultPublishedPayload{}, fmt.Errorf("verified Task result has multiple matching publications")
		}
		resultEvent, result = event, candidate
	}
	if resultEvent.EventID == "" {
		return Event{}, ResultPublishedPayload{}, fmt.Errorf("verified Task result lacks its exact publication")
	}
	var completionCandidate Event
	for _, event := range stream {
		if event.EventType != "CANDIDATE_COMPLETE" || event.TaskID != string(task.ID) || event.CorrelationID != correlationID || event.Sequence <= resultEvent.Sequence || event.Sequence >= verification.Sequence {
			continue
		}
		var candidate CandidateCompletePayload
		if event.OrganizationID != organizationID || event.SourceActorID != expectedActorID || event.SourceExecutionID != outcomeEvent.SourceExecutionID || event.RecipientScope != "" || event.RecipientID != "" || json.Unmarshal(event.Payload, &candidate) != nil || candidate.ToolInvocationID != string(outcome.ToolInvocationID) || candidate.ResultEventID != resultEvent.EventID || !slices.Equal(candidate.ArtifactRefs, outcome.ArtifactRefs) || !slices.Equal(event.ArtifactRefs, outcome.ArtifactRefs) {
			continue
		}
		if completionCandidate.EventID != "" {
			return Event{}, ResultPublishedPayload{}, fmt.Errorf("verified Task result has multiple matching candidates")
		}
		completionCandidate = event
	}
	if completionCandidate.EventID == "" {
		return Event{}, ResultPublishedPayload{}, fmt.Errorf("verified Task result lacks its exact completion candidate")
	}
	return resultEvent, result, nil
}

func executionInbox(binding WorkCompletionBinding, task core.Task, startEvent Event, stream []Event) ([]string, []core.AgentExecutionInboxEvent, error) {
	startDetail, err := executionStartDetail(startEvent)
	if err != nil {
		return nil, nil, err
	}
	cutoff := startDetail.InboxCutoffSequence
	routes := map[string]struct{}{
		recipientKey(RecipientTask, string(task.ID)):          {},
		recipientKey(RecipientAgent, string(task.AssigneeID)): {},
	}
	for teamID, revisions := range binding.TeamRevisions {
		var effective *TeamRevisionBinding
		for index := range revisions {
			revision := &revisions[index]
			if teamID == "" || revision.Team.ID != teamID || revision.Team.OrganizationID != core.ID(binding.OrganizationID) || revision.Version != index+1 || revision.EffectiveSequence < 1 {
				return nil, nil, fmt.Errorf("execution inbox Team binding is invalid")
			}
			if index > 0 && revisions[index-1].EffectiveSequence >= revision.EffectiveSequence {
				return nil, nil, fmt.Errorf("execution inbox Team history is invalid")
			}
			if revision.EffectiveSequence < startEvent.Sequence {
				effective = revision
			}
		}
		if effective != nil && slices.Contains(effective.Team.MemberAgentIDs, task.AssigneeID) {
			routes[recipientKey(RecipientTeam, string(teamID))] = struct{}{}
		}
	}
	indexed := make(map[string]Event, len(stream))
	for _, event := range stream {
		if event.EventID == "" {
			return nil, nil, fmt.Errorf("execution inbox event identity is invalid")
		}
		if _, exists := indexed[event.EventID]; exists {
			return nil, nil, fmt.Errorf("execution inbox event identity is duplicated")
		}
		indexed[event.EventID] = event
	}
	observed := make(map[string]struct{})
	for _, event := range stream {
		if event.Sequence > cutoff || event.OrganizationID != binding.OrganizationID || event.EventType != "INBOX_EVENTS_OBSERVED" {
			continue
		}
		if _, ok := routes[recipientKey(event.RecipientScope, event.RecipientID)]; !ok {
			continue
		}
		var payload InboxEventsObservedPayload
		if event.SourceActorID == "" || event.SourceExecutionID == "" || json.Unmarshal(event.Payload, &payload) != nil || len(payload.EventIDs) == 0 || payload.ExecutionStartEventRef == "" {
			return nil, nil, fmt.Errorf("execution inbox observation is invalid")
		}
		admitted, ok := binding.InboxObservations[event.EventID]
		if !ok || admitted.ExecutionStartEventRef != payload.ExecutionStartEventRef || !slices.Equal(admitted.EventIDs, payload.EventIDs) {
			return nil, nil, fmt.Errorf("execution inbox observation lacks atomic admission")
		}
		observationStart, err := inboxObservationExecution(binding, event, payload.ExecutionStartEventRef, indexed)
		if err != nil {
			return nil, nil, err
		}
		for _, eventID := range payload.EventIDs {
			addressed, exists := indexed[eventID]
			if !exists || addressed.Sequence >= observationStart.Sequence || addressed.OrganizationID != binding.OrganizationID || addressed.RecipientScope != event.RecipientScope || addressed.RecipientID != event.RecipientID || addressed.EventType == "INBOX_EVENTS_OBSERVED" {
				return nil, nil, fmt.Errorf("execution inbox observation reference is invalid")
			}
			if _, duplicate := observed[eventID]; duplicate {
				return nil, nil, fmt.Errorf("execution inbox event was observed more than once")
			}
			observed[eventID] = struct{}{}
		}
	}
	available := make([]Event, 0)
	for _, event := range stream {
		if event.Sequence > cutoff || event.OrganizationID != binding.OrganizationID || event.EventType == "INBOX_EVENTS_OBSERVED" {
			continue
		}
		if _, ok := routes[recipientKey(event.RecipientScope, event.RecipientID)]; !ok {
			continue
		}
		if _, alreadyObserved := observed[event.EventID]; !alreadyObserved {
			available = append(available, event)
		}
	}
	sort.Slice(available, func(i, j int) bool { return available[i].Sequence < available[j].Sequence })
	refs := make([]string, 0, len(available))
	inbox := make([]core.AgentExecutionInboxEvent, 0, len(available))
	for _, event := range available {
		refs = append(refs, event.EventID)
		inbox = append(inbox, core.AgentExecutionInboxEvent{
			Sequence: event.Sequence, EventID: event.EventID, EventType: event.EventType,
			SourceActorID: event.SourceActorID, RecipientScope: event.RecipientScope, RecipientID: event.RecipientID,
			TaskID: event.TaskID, CreatedAt: event.CreatedAt, Payload: append(json.RawMessage(nil), event.Payload...),
		})
	}
	return refs, inbox, nil
}

func inboxObservationExecution(binding WorkCompletionBinding, observation Event, startEventRef string, indexed map[string]Event) (Event, error) {
	startEvent, found := indexed[startEventRef]
	if !found || startEvent.EventType != "EXECUTION_STARTED" || startEvent.Sequence >= observation.Sequence || startEvent.OrganizationID != binding.OrganizationID || startEvent.SourceActorID != "runtime" || startEvent.SourceExecutionID != "" || startEvent.RecipientScope != "" || startEvent.RecipientID != "" || startEvent.TaskID == "" || startEvent.TaskID != observation.TaskID || startEvent.CorrelationID == "" || startEvent.CorrelationID != observation.CorrelationID {
		return Event{}, fmt.Errorf("execution inbox observation start reference is invalid")
	}
	var payload ProjectionEventPayload
	var task core.Task
	if json.Unmarshal(startEvent.Payload, &payload) != nil || payload.Projection.ProjectionKind != "task" || payload.Projection.RecordID != startEvent.TaskID || payload.Projection.Version < 1 || payload.Projection.CorrelationID != startEvent.CorrelationID || json.Unmarshal(payload.Projection.Value, &task) != nil || task.ID != core.ID(startEvent.TaskID) || task.ExecutionKind != core.ExecutionAgent || task.Status != core.TaskRunning || task.AssigneeID == "" || observation.SourceActorID != string(task.AssigneeID) || observation.SourceExecutionID != fmt.Sprintf("execution-%s-v%d", task.ID, payload.Projection.Version) {
		return Event{}, fmt.Errorf("execution inbox observation is not bound to its consuming Agent execution")
	}
	if _, err := executionStartDetail(startEvent); err != nil {
		return Event{}, err
	}
	if !ExecutionRecipientAllowed(binding.TeamRevisions, task, startEvent.Sequence, observation.RecipientScope, observation.RecipientID) {
		return Event{}, fmt.Errorf("execution inbox observation recipient is outside its consuming Agent execution")
	}
	return startEvent, nil
}

// ExecutionRecipientAllowed reports whether a recipient was in the exact inbox
// boundary available to an Agent task when its execution started.
func ExecutionRecipientAllowed(teamRevisions map[core.ID][]TeamRevisionBinding, task core.Task, startSequence int64, recipientScope, recipientID string) bool {
	switch recipientScope {
	case RecipientTask:
		return recipientID == string(task.ID)
	case RecipientAgent:
		return recipientID == string(task.AssigneeID)
	case RecipientTeam:
		revisions := teamRevisions[core.ID(recipientID)]
		var effective *TeamRevisionBinding
		for index := range revisions {
			revision := &revisions[index]
			if revision.Team.ID != core.ID(recipientID) || revision.Version != index+1 || revision.EffectiveSequence < 1 || index > 0 && revisions[index-1].EffectiveSequence >= revision.EffectiveSequence {
				return false
			}
			if revision.EffectiveSequence < startSequence {
				effective = revision
			}
		}
		return effective != nil && effective.Team.OrganizationID != "" && slices.Contains(effective.Team.MemberAgentIDs, task.AssigneeID)
	default:
		return false
	}
}

// AgentExecutionRoutes returns the complete canonical recipient set available
// to an Agent at one admitted execution boundary.
func AgentExecutionRoutes(teamRevisions map[core.ID][]TeamRevisionBinding, task core.Task, startSequence int64) []InboxRoute {
	routes := []InboxRoute{{Scope: RecipientTask, ID: string(task.ID)}, {Scope: RecipientAgent, ID: string(task.AssigneeID)}}
	teamIDs := make([]core.ID, 0)
	for teamID := range teamRevisions {
		if ExecutionRecipientAllowed(teamRevisions, task, startSequence, RecipientTeam, string(teamID)) {
			teamIDs = append(teamIDs, teamID)
		}
	}
	sort.Slice(teamIDs, func(i, j int) bool { return teamIDs[i] < teamIDs[j] })
	for _, teamID := range teamIDs {
		routes = append(routes, InboxRoute{Scope: RecipientTeam, ID: string(teamID)})
	}
	return routes
}

func recipientKey(scope, id string) string {
	return scope + "\x00" + id
}

func executionRevision(binding WorkCompletionBinding, task core.Task, manifestEvent Event, stream []Event) (string, *core.AgentExecutionRevision, error) {
	requests := make(map[core.ID]completionReviewRequestPayload)
	requestSequences := make(map[core.ID]int64)
	var selected completionReviewDecisionPayload
	var selectedEvent Event
	for _, event := range stream {
		if event.Sequence >= manifestEvent.Sequence || event.CorrelationID != binding.CorrelationID || event.EventType != "COMPLETION_REVIEW_REQUESTED" {
			continue
		}
		var request completionReviewRequestPayload
		fingerprint, err := func() (string, error) {
			if json.Unmarshal(event.Payload, &request) != nil {
				return "", fmt.Errorf("decode completion revision request")
			}
			return completionReviewRequestFingerprint(request)
		}()
		if err != nil || event.OrganizationID != binding.OrganizationID || event.SourceActorID != "runtime" || event.TaskID != string(request.TaskID) || request.ID == "" || request.OrganizationID != core.ID(binding.OrganizationID) || request.TaskID == "" || request.TaskVersion < 1 || request.TaskID == task.ID && request.Objective != task.Description || request.Fingerprint == "" || request.Fingerprint != fingerprint || len(request.EvidenceRefs) != 3 {
			return "", nil, fmt.Errorf("execution completion revision request is invalid")
		}
		if _, exists := requests[request.ID]; exists {
			return "", nil, fmt.Errorf("execution completion revision request is duplicated")
		}
		requests[request.ID], requestSequences[request.ID] = request, event.Sequence
	}
	for _, event := range stream {
		if event.Sequence >= manifestEvent.Sequence || event.CorrelationID != binding.CorrelationID || event.EventType != "COMPLETION_REVIEW_DECIDED" {
			continue
		}
		var review completionReviewDecisionPayload
		if json.Unmarshal(event.Payload, &review) != nil {
			return "", nil, fmt.Errorf("decode execution completion revision")
		}
		request, exists := requests[review.ReviewID]
		if !exists || requestSequences[review.ReviewID] >= event.Sequence || event.OrganizationID != binding.OrganizationID || event.SourceActorID != string(review.ReviewerID) || event.SourceExecutionID != "" || event.TaskID != string(review.TaskID) ||
			review.OrganizationID != request.OrganizationID || review.TaskID != request.TaskID || review.TaskVersion != request.TaskVersion || review.Fingerprint != request.Fingerprint || review.ReviewerID == "" || review.Method != core.AssuranceHumanJudgment || review.DecidedAt.IsZero() || !slices.Equal(review.EvidenceRefs, request.EvidenceRefs) || len(review.Feedback) > 64<<10 || !utf8.ValidString(review.Feedback) {
			return "", nil, fmt.Errorf("execution completion revision is invalid")
		}
		if review.Decision == core.CompletionReviewRevise {
			if strings.TrimSpace(review.Feedback) == "" {
				return "", nil, fmt.Errorf("execution completion revision feedback is required")
			}
			if review.TaskID == task.ID && event.Sequence > selectedEvent.Sequence {
				selected, selectedEvent = review, event
			}
		} else if review.Decision != core.CompletionReviewApprove && review.Decision != core.CompletionReviewReject {
			return "", nil, fmt.Errorf("execution completion review decision is invalid")
		}
	}
	if selectedEvent.EventID == "" {
		return "", nil, nil
	}
	return selectedEvent.EventID, &core.AgentExecutionRevision{
		EventRef: selectedEvent.EventID, ReviewerID: selected.ReviewerID, UntrustedText: selected.Feedback,
	}, nil
}

func validCompletionInputEvent(binding WorkCompletionBinding, taskID core.ID, event Event) bool {
	if event.OrganizationID != binding.OrganizationID || event.TaskID != string(taskID) || event.CorrelationID != binding.CorrelationID || event.SourceActorID == "" || event.SourceExecutionID != "" {
		return false
	}
	var input OperatorInputReceivedPayload
	if json.Unmarshal(event.Payload, &input) != nil || input.MessageID == "" || input.Text == "" || input.SourcePrincipalID != event.SourceActorID {
		return false
	}
	switch event.EventType {
	case "A2A_INPUT_RECEIVED":
		return input.SourcePrincipalKind == string(core.PrincipalExternalAgent) && input.SourceChannel == "A2A"
	case "HUMAN_INPUT_RECEIVED":
		return input.SourcePrincipalKind == string(core.PrincipalHuman) && input.SourceChannel == "HUMAN_DIRECT"
	default:
		return false
	}
}

func validateExecutionStart(binding WorkCompletionBinding, task core.Task, version int, outcomeEvent Event, stream []Event) (Event, bool, error) {
	var found Event
	remediation := false
	for _, event := range stream {
		if event.EventType != "EXECUTION_STARTED" || event.TaskID != string(task.ID) || event.CorrelationID != binding.CorrelationID {
			continue
		}
		var payload ProjectionEventPayload
		var projected core.Task
		if json.Unmarshal(event.Payload, &payload) != nil || payload.Projection.Version != version {
			continue
		}
		if found.EventID != "" || event.Sequence >= outcomeEvent.Sequence || event.OrganizationID != binding.OrganizationID || event.SourceActorID != "runtime" || event.SourceExecutionID != "" || payload.Projection.ProjectionKind != "task" || payload.Projection.RecordID != string(task.ID) || payload.Projection.CorrelationID != binding.CorrelationID || json.Unmarshal(payload.Projection.Value, &projected) != nil || projected.Status != core.TaskRunning || !sameTaskDefinition(projected, task) {
			return Event{}, false, fmt.Errorf("work completion execution-start record is invalid")
		}
		if task.ExecutionKind == core.ExecutionAgent {
			detail, err := executionStartDetail(event)
			if err != nil {
				return Event{}, false, err
			}
			remediation = detail.Mode == "BLOCKED_DEPENDENCY_REMEDIATION"
		}
		found = event
	}
	if found.EventID == "" {
		return Event{}, false, fmt.Errorf("work completion lacks its exact execution-start version")
	}
	return found, remediation, nil
}

func executionStartDetail(event Event) (ExecutionStartDetail, error) {
	var payload ProjectionEventPayload
	var detail ExecutionStartDetail
	var fields map[string]json.RawMessage
	if event.EventType != "EXECUTION_STARTED" || json.Unmarshal(event.Payload, &payload) != nil || json.Unmarshal(payload.Detail, &detail) != nil || json.Unmarshal(payload.Detail, &fields) != nil || len(fields) < 1 || len(fields) > 2 || fields["inbox_cutoff_sequence"] == nil || detail.InboxCutoffSequence < 0 || detail.InboxCutoffSequence >= event.Sequence || detail.Mode != "" && detail.Mode != "BLOCKED_DEPENDENCY_REMEDIATION" {
		return ExecutionStartDetail{}, fmt.Errorf("work completion Agent execution-start detail is invalid")
	}
	if len(fields) == 2 && fields["mode"] == nil {
		return ExecutionStartDetail{}, fmt.Errorf("work completion Agent execution-start detail contains an unknown field")
	}
	return detail, nil
}

func completionDecisionApproval(binding WorkCompletionBinding, task core.Task, decision CompletionDecisionPayload, outcome core.ToolOutcome, outcomeEvent, verification Event, stream []Event) (*bool, error) {
	if decision.JudgmentRef == "" {
		for _, criterion := range decision.Contract.Criteria {
			if criterion.Required && criterion.Assurance == core.AssuranceHumanJudgment {
				return nil, fmt.Errorf("work completion decision lacks required user judgment")
			}
		}
		return nil, nil
	}
	judgmentEvent, found := eventWithID(stream, decision.JudgmentRef)
	if !found || judgmentEvent.EventType != "COMPLETION_REVIEW_DECIDED" || judgmentEvent.OrganizationID != binding.OrganizationID || judgmentEvent.TaskID != verification.TaskID || judgmentEvent.CorrelationID != binding.CorrelationID || judgmentEvent.Sequence >= verification.Sequence || judgmentEvent.SourceActorID == "" || judgmentEvent.SourceExecutionID != "" {
		return nil, fmt.Errorf("work completion judgment reference is invalid")
	}
	var review completionReviewDecisionPayload
	if json.Unmarshal(judgmentEvent.Payload, &review) != nil || review.ReviewID == "" || review.OrganizationID != core.ID(binding.OrganizationID) || review.TaskID != decision.Contract.TaskID || review.TaskVersion != decision.Contract.TaskVersion || review.Decision != core.CompletionReviewApprove || review.ReviewerID != core.ID(judgmentEvent.SourceActorID) || review.Method != core.AssuranceHumanJudgment || review.DecidedAt.IsZero() {
		return nil, fmt.Errorf("work completion judgment is invalid")
	}
	var requestEvent Event
	var request completionReviewRequestPayload
	for _, event := range stream {
		if event.EventType != "COMPLETION_REVIEW_REQUESTED" || event.TaskID != verification.TaskID || event.CorrelationID != binding.CorrelationID {
			continue
		}
		var candidate completionReviewRequestPayload
		if json.Unmarshal(event.Payload, &candidate) != nil || candidate.ID != review.ReviewID {
			continue
		}
		if requestEvent.EventID != "" {
			return nil, fmt.Errorf("work completion judgment has multiple review requests")
		}
		requestEvent, request = event, candidate
	}
	if requestEvent.EventID == "" || requestEvent.Sequence >= judgmentEvent.Sequence || requestEvent.OrganizationID != binding.OrganizationID || requestEvent.SourceActorID != "runtime" || requestEvent.SourceExecutionID == "" || requestEvent.SourceExecutionID != outcomeEvent.SourceExecutionID || request.OrganizationID != core.ID(binding.OrganizationID) || request.TaskID != decision.Contract.TaskID || request.TaskVersion != decision.Contract.TaskVersion || request.Objective != task.Description || !reflect.DeepEqual(request.Contract, decision.Contract) || request.Fingerprint != review.Fingerprint || !slices.Equal(request.EvidenceRefs, review.EvidenceRefs) || len(request.EvidenceRefs) != 3 || request.EvidenceRefs[0] != outcomeEvent.EventID {
		return nil, fmt.Errorf("work completion judgment does not bind its exact review request")
	}
	expectedFingerprint, err := completionReviewRequestFingerprint(request)
	if err != nil || expectedFingerprint != request.Fingerprint {
		return nil, fmt.Errorf("work completion review request fingerprint is invalid")
	}
	reviewEvidence := make([]Event, 3)
	for index, eventType := range []string{"TOOL_OUTCOME_RECORDED", "RESULT_PUBLISHED", "CANDIDATE_COMPLETE"} {
		evidenceEvent, found := eventWithID(stream, request.EvidenceRefs[index])
		if !found || evidenceEvent.EventType != eventType || evidenceEvent.OrganizationID != binding.OrganizationID || evidenceEvent.TaskID != verification.TaskID || evidenceEvent.CorrelationID != binding.CorrelationID || evidenceEvent.SourceExecutionID != requestEvent.SourceExecutionID || evidenceEvent.Sequence >= requestEvent.Sequence {
			return nil, fmt.Errorf("work completion review evidence is invalid")
		}
		if index == 0 && evidenceEvent.SourceActorID != "runtime" || index > 0 && evidenceEvent.SourceActorID != string(task.AssigneeID) {
			return nil, fmt.Errorf("work completion review evidence source is invalid")
		}
		reviewEvidence[index] = evidenceEvent
	}
	if reviewEvidence[0].EventID != outcomeEvent.EventID || reviewEvidence[0].Sequence >= reviewEvidence[1].Sequence || reviewEvidence[1].Sequence >= reviewEvidence[2].Sequence || !slices.Equal(reviewEvidence[0].ArtifactRefs, outcome.ArtifactRefs) {
		return nil, fmt.Errorf("work completion review evidence order or outcome binding is invalid")
	}
	var result ResultPublishedPayload
	var candidate CandidateCompletePayload
	expectedSummary, summaryErr := core.ToolOutcomeSummary(outcome)
	if summaryErr != nil || json.Unmarshal(reviewEvidence[1].Payload, &result) != nil || !result.ValidFor(reviewEvidence[1].ArtifactRefs) || result.Summary != expectedSummary || !slices.Equal(result.ArtifactRefs, outcome.ArtifactRefs) ||
		json.Unmarshal(reviewEvidence[2].Payload, &candidate) != nil || candidate.ToolInvocationID != string(outcome.ToolInvocationID) || candidate.ResultEventID != reviewEvidence[1].EventID || !slices.Equal(candidate.ArtifactRefs, outcome.ArtifactRefs) || !slices.Equal(reviewEvidence[2].ArtifactRefs, outcome.ArtifactRefs) {
		return nil, fmt.Errorf("work completion review evidence payload binding is invalid")
	}
	approved := true
	return &approved, nil
}

func completionReviewRequestFingerprint(request completionReviewRequestPayload) (string, error) {
	request.Fingerprint = ""
	body, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(body)
	return fmt.Sprintf("%x", digest), nil
}

func eventWithID(stream []Event, id string) (Event, bool) {
	for _, event := range stream {
		if event.EventID == id {
			return event, true
		}
	}
	return Event{}, false
}

func (p WorkCompletionEvidencePayload) ExpectedFingerprint() (string, error) {
	p.Fingerprint = ""
	body, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(body)
	return fmt.Sprintf("%x", digest), nil
}

func WorkCompletionArtifactRefs(tasks []WorkCompletionTaskEvidencePayload) []string {
	seen := make(map[string]struct{})
	for _, task := range tasks {
		for _, ref := range task.ArtifactRefs {
			seen[ref] = struct{}{}
		}
	}
	refs := make([]string, 0, len(seen))
	for ref := range seen {
		refs = append(refs, ref)
	}
	sort.Strings(refs)
	return refs
}

func distinctEvidenceRefs(values []string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !validEvidenceRef(value) {
			return false
		}
		if _, exists := seen[value]; exists {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}

func validEvidenceRef(value string) bool {
	return value != "" && len(value) <= 4096 && utf8.ValidString(value)
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}

// PlanningContextPayload records the exact trusted model identity and accepted
// Intent event/fingerprint supplied to one planning attempt. Planning output is
// still untrusted until the runtime validates and records a Plan.
type PlanningContextPayload struct {
	PlanID                  string   `json:"plan_id"`
	IntentID                string   `json:"intent_id"`
	IntentFingerprint       string   `json:"intent_fingerprint"`
	PromptVersion           string   `json:"prompt_version"`
	Provider                string   `json:"provider"`
	Model                   string   `json:"model"`
	ExecutionProfileVersion string   `json:"execution_profile_version"`
	InputEventRefs          []string `json:"input_event_refs"`
}

func (p ResultPublishedPayload) ValidFor(artifactRefs []string) bool {
	return p.Summary != "" && sameStrings(p.ArtifactRefs, artifactRefs)
}

type InferenceUsageRecordedPayload struct {
	Source       string   `json:"source"`
	Provider     string   `json:"provider"`
	Model        string   `json:"model"`
	InputTokens  int      `json:"input_tokens"`
	OutputTokens int      `json:"output_tokens"`
	TotalTokens  int      `json:"total_tokens"`
	CostUSD      *float64 `json:"cost_usd,omitempty"`
}

func (p InferenceUsageRecordedPayload) Valid() bool {
	return p.Source != "" && p.Provider != "" && p.Model != "" &&
		p.InputTokens >= 0 && p.OutputTokens >= 0 &&
		p.TotalTokens >= 0 &&
		p.TotalTokens == p.InputTokens+p.OutputTokens &&
		(p.CostUSD == nil || *p.CostUSD >= 0)
}

type TrustedDraft struct {
	OrganizationID    string
	EventType         string
	SourceActorID     string
	SourceExecutionID string
	RecipientScope    string
	RecipientID       string
	TaskID            string
	AuthorizationRefs []string
	ArtifactRefs      []string
	Payload           any
	CorrelationID     string
}

// ProjectionDraft couples one trusted event with one rebuildable projection
// update. The ledger persists both atomically; callers never publish the
// projection before its authoritative event exists.
type ProjectionDraft struct {
	Event          TrustedDraft
	ProjectionKind string
	RecordID       string
	Version        int
	Value          any
}

// ProjectionRecord is the canonical event/record representation used to
// rebuild current state. Value remains raw until a bounded projection module
// decodes it into its domain type.
type ProjectionRecord struct {
	ProjectionKind string          `json:"projection_kind"`
	RecordID       string          `json:"record_id"`
	Version        int             `json:"version"`
	CorrelationID  string          `json:"correlation_id,omitempty"`
	Value          json.RawMessage `json:"value"`
}

// ProjectionEventPayload preserves transition detail while carrying the
// complete versioned record needed for deterministic replay.
type ProjectionEventPayload struct {
	Projection ProjectionRecord `json:"projection"`
	Detail     json.RawMessage  `json:"detail,omitempty"`
}

// ResolveTeamRevisionBindings couples every Team revision admitted to the
// records projection to its exact runtime-owned ledger transition. Completion
// validation uses the resulting event sequence to reconstruct membership at
// execution time instead of applying the current roster retroactively.
func ResolveTeamRevisionBindings(organizationID string, bodies [][]byte, stream []Event) (map[core.ID][]TeamRevisionBinding, error) {
	if organizationID == "" {
		return nil, fmt.Errorf("team revision organization is required")
	}
	revisions := make(map[core.ID][]TeamRevisionBinding)
	for _, body := range bodies {
		var record ProjectionRecord
		var team core.Team
		if json.Unmarshal(body, &record) != nil || record.ProjectionKind != "team" || record.RecordID == "" || record.Version < 1 || json.Unmarshal(record.Value, &team) != nil || team.ID != core.ID(record.RecordID) || team.OrganizationID == "" {
			return nil, fmt.Errorf("admitted Team revision is invalid")
		}
		if string(team.OrganizationID) != organizationID {
			continue
		}
		eventType := "TEAM_REVISED"
		if record.Version == 1 {
			eventType = "TEAM_CREATED"
		}
		var matched Event
		for _, event := range stream {
			if event.EventType != eventType || event.OrganizationID != organizationID || event.SourceActorID != "runtime" || event.SourceExecutionID != "" || event.RecipientScope != "" || event.RecipientID != "" || event.TaskID != "" || event.CorrelationID != record.CorrelationID {
				continue
			}
			var payload ProjectionEventPayload
			if json.Unmarshal(event.Payload, &payload) != nil || !reflect.DeepEqual(payload.Projection, record) {
				continue
			}
			if matched.EventID != "" {
				return nil, fmt.Errorf("admitted Team revision has multiple ledger transitions")
			}
			matched = event
		}
		if matched.EventID == "" {
			return nil, fmt.Errorf("admitted Team revision lacks its exact ledger transition")
		}
		revisions[team.ID] = append(revisions[team.ID], TeamRevisionBinding{Team: team, Version: record.Version, EffectiveSequence: matched.Sequence})
	}
	for teamID := range revisions {
		history := revisions[teamID]
		sort.Slice(history, func(i, j int) bool { return history[i].Version < history[j].Version })
		for index, revision := range history {
			if revision.Version != index+1 || revision.EffectiveSequence < 1 || index > 0 && history[index-1].EffectiveSequence >= revision.EffectiveSequence {
				return nil, fmt.Errorf("admitted Team revision history is invalid")
			}
		}
		revisions[teamID] = history
	}
	return revisions, nil
}

type Event struct {
	EventID           string          `json:"event_id"`
	Sequence          int64           `json:"sequence"`
	OrganizationID    string          `json:"organization_id"`
	EventType         string          `json:"event_type"`
	SourceActorID     string          `json:"source_actor_id,omitempty"`
	SourceExecutionID string          `json:"source_execution_id,omitempty"`
	RecipientScope    string          `json:"recipient_scope,omitempty"`
	RecipientID       string          `json:"recipient_id,omitempty"`
	TaskID            string          `json:"task_id,omitempty"`
	AuthorizationRefs []string        `json:"authorization_refs"`
	ArtifactRefs      []string        `json:"artifact_refs"`
	CreatedAt         time.Time       `json:"created_at"`
	SchemaVersion     int             `json:"schema_version"`
	Payload           json.RawMessage `json:"payload"`
	CorrelationID     string          `json:"-"`
}

// LatestPayload decodes the newest event of a requested contract type. It is
// the shared, bounded replay primitive for projections that only need the most
// recent value of an event contract.
func LatestPayload[T any](stream []Event, eventType string) (T, bool, error) {
	var zero T
	for index := len(stream) - 1; index >= 0; index-- {
		if stream[index].EventType != eventType {
			continue
		}
		var payload T
		if err := json.Unmarshal(stream[index].Payload, &payload); err != nil {
			return zero, false, err
		}
		return payload, true, nil
	}
	return zero, false, nil
}

type Appender interface {
	Append(context.Context, TrustedDraft) (Event, error)
}
type Reader interface {
	Events(context.Context, string) ([]Event, error)
}
type ProjectionAppender interface {
	AppendProjection(context.Context, ProjectionDraft) (Event, error)
}
type ProjectionBatchAppender interface {
	AppendProjections(context.Context, []ProjectionDraft) ([]Event, error)
}
type WorkCompletionAppender interface {
	AppendWorkCompletion(context.Context, ProjectionDraft) (Event, error)
}
type ExecutionStartAppender interface {
	AppendExecutionStart(context.Context, ProjectionDraft, []InboxRoute) (Event, []InboxSelection, error)
}
type ProjectionReader interface {
	Records(context.Context, string, string) ([][]byte, error)
}
type IntentConfirmer interface {
	AppendIntentConfirmation(context.Context, TrustedDraft, core.ID) (Event, error)
}
type ExternalWorkResolver interface {
	ResolveExternalWork(context.Context, string, string) (string, bool, error)
	ResolveExternalRequest(context.Context, string, string) (string, bool, error)
	ResolveExternalTask(context.Context, string, string) (string, string, bool, error)
}
type ActiveIntakeResolver interface {
	ResolveActiveIntake(context.Context, string, string, string, string) (string, string, bool, error)
}
type ExternalWorkAllocator interface {
	ReserveExternalWork(context.Context, string, string) (string, error)
}
type InboxReader interface {
	Inbox(context.Context, string, string) ([]Event, error)
}
type InboxObserver interface {
	ObserveInbox(context.Context, TrustedDraft, string, string, []string) (Event, error)
}
type InboxObservationReader interface {
	InboxObservations(context.Context) (map[string]InboxObservationBinding, error)
}

type AddressedRoute struct {
	OrganizationID string
	EventType      string
	SourceActorID  string
	ValidateSource bool
	RecipientScope string
	RecipientID    string
	TaskID         string
}

type RouteValidator interface {
	ValidateAddressedRoute(context.Context, AddressedRoute) error
}

type Gateway struct {
	ledger interface {
		Appender
		Reader
	}
	routeValidator RouteValidator
}

func NewGateway(ledger interface {
	Appender
	Reader
}) *Gateway {
	return &Gateway{ledger: ledger}
}

// SetRouteValidator wires the organization/task identity projection into the
// gateway at the composition root. MESSAGE publication fails closed until the
// runtime supplies this validator.
func (g *Gateway) SetRouteValidator(validator RouteValidator) {
	g.routeValidator = validator
}

func (g *Gateway) ResolveExternalWork(ctx context.Context, organizationID, requestID string) (string, bool, error) {
	resolver, ok := g.ledger.(ExternalWorkResolver)
	if !ok {
		return "", false, nil
	}
	return resolver.ResolveExternalWork(ctx, organizationID, requestID)
}

func (g *Gateway) ResolveExternalRequest(ctx context.Context, organizationID, correlationID string) (string, bool, error) {
	resolver, ok := g.ledger.(ExternalWorkResolver)
	if !ok {
		return "", false, nil
	}
	return resolver.ResolveExternalRequest(ctx, organizationID, correlationID)
}

func (g *Gateway) ResolveExternalTask(ctx context.Context, organizationID, taskID string) (string, string, bool, error) {
	resolver, ok := g.ledger.(ExternalWorkResolver)
	if !ok {
		return "", "", false, nil
	}
	return resolver.ResolveExternalTask(ctx, organizationID, taskID)
}

func (g *Gateway) ResolveActiveIntake(ctx context.Context, organizationID, principalID, principalKind, sourceChannel string) (string, string, bool, error) {
	resolver, ok := g.ledger.(ActiveIntakeResolver)
	if !ok {
		return "", "", false, nil
	}
	return resolver.ResolveActiveIntake(ctx, organizationID, principalID, principalKind, sourceChannel)
}

func (g *Gateway) ReserveExternalWork(ctx context.Context, organizationID, requestID string) (string, error) {
	allocator, ok := g.ledger.(ExternalWorkAllocator)
	if !ok {
		return "", fmt.Errorf("external work allocator is unavailable")
	}
	return allocator.ReserveExternalWork(ctx, organizationID, requestID)
}

var agentTypes = map[string]bool{"MESSAGE": true, "TASK_BLOCKED": true, "EVIDENCE_PUBLISHED": true, "RESULT_PUBLISHED": true, "CANDIDATE_COMPLETE": true, "KNOWLEDGE_PROPOSED": true, "SKILL_PROPOSED": true}

func (g *Gateway) PublishAgentDraft(ctx context.Context, organizationID, actorID, executionID, correlationID string, draft Draft) (Event, error) {
	if !agentTypes[draft.EventType] {
		return Event{}, fmt.Errorf("event type %s is not agent-proposable", draft.EventType)
	}
	if draft.EventType == "TASK_BLOCKED" && (draft.TaskID == "" || draft.RecipientScope != RecipientTask || draft.RecipientID == "") {
		return Event{}, fmt.Errorf("task blocked draft requires a source child task and parent task recipient")
	}
	if draft.EventType == "RESULT_PUBLISHED" {
		var result ResultPublishedPayload
		if draft.TaskID == "" || decodePayload(draft.Payload, &result) != nil || !result.ValidFor(draft.ArtifactRefs) {
			return Event{}, fmt.Errorf("result published draft requires a task, summary, and matching artifact refs")
		}
	}
	trusted := TrustedDraft{OrganizationID: organizationID, EventType: draft.EventType, SourceActorID: actorID, SourceExecutionID: executionID, RecipientScope: draft.RecipientScope, RecipientID: draft.RecipientID, TaskID: draft.TaskID, ArtifactRefs: draft.ArtifactRefs, Payload: draft.Payload, CorrelationID: correlationID}
	if err := g.validateAddressed(ctx, trusted, true); err != nil {
		return Event{}, err
	}
	return g.ledger.Append(ctx, trusted)
}
func (g *Gateway) PublishTrusted(ctx context.Context, draft TrustedDraft) (Event, error) {
	if draft.EventType == "INBOX_EVENTS_OBSERVED" {
		return Event{}, fmt.Errorf("inbox observations require atomic inbox admission")
	}
	if draft.EventType == "INTENT_CONFIRMED" {
		var confirmation IntentConfirmedPayload
		if decodePayload(draft.Payload, &confirmation) != nil {
			return Event{}, fmt.Errorf("intent confirmation payload is invalid")
		}
		if confirmation.GoalID != "" {
			return Event{}, fmt.Errorf("goal-bound intent confirmation requires atomic Goal admission")
		}
	}
	if err := g.validateAddressed(ctx, draft, false); err != nil {
		return Event{}, err
	}
	return g.ledger.Append(ctx, draft)
}

// PublishIntentConfirmation atomically proves that an optional Goal is active
// in the same organization while appending the exact Intent confirmation.
// Later Goal lifecycle changes do not invalidate the admitted Work binding.
func (g *Gateway) PublishIntentConfirmation(ctx context.Context, draft TrustedDraft, goalID core.ID) (Event, error) {
	if draft.EventType != "INTENT_CONFIRMED" || goalID == "" {
		return Event{}, fmt.Errorf("goal-bound intent confirmation is incomplete")
	}
	var confirmation IntentConfirmedPayload
	if decodePayload(draft.Payload, &confirmation) != nil || confirmation.GoalID != string(goalID) {
		return Event{}, fmt.Errorf("goal-bound intent confirmation does not match its checked goal")
	}
	if err := g.validateAddressed(ctx, draft, false); err != nil {
		return Event{}, err
	}
	confirmer, ok := g.ledger.(IntentConfirmer)
	if !ok {
		return Event{}, fmt.Errorf("ledger does not support atomic goal-bound intent confirmation")
	}
	return confirmer.AppendIntentConfirmation(ctx, draft, goalID)
}
func (g *Gateway) PublishProjection(ctx context.Context, draft ProjectionDraft) (Event, error) {
	if draft.Event.EventType == "" || draft.ProjectionKind == "" || draft.RecordID == "" || draft.Version < 1 {
		return Event{}, fmt.Errorf("event type, projection kind, record id, and positive version are required")
	}
	if err := g.validateAddressed(ctx, draft.Event, false); err != nil {
		return Event{}, err
	}
	if err := rejectBareWorkCompletion(draft); err != nil {
		return Event{}, err
	}
	if draft.Event.EventType == "EXECUTION_STARTED" && draft.ProjectionKind == "task" {
		var task core.Task
		if decodePayload(draft.Value, &task) == nil && task.ExecutionKind == core.ExecutionAgent {
			return Event{}, fmt.Errorf("agent execution start requires atomic inbox selection")
		}
	}
	store, ok := g.ledger.(ProjectionAppender)
	if !ok {
		return Event{}, fmt.Errorf("event ledger does not support durable projections")
	}
	return store.AppendProjection(ctx, draft)
}

func (g *Gateway) PublishExecutionStart(ctx context.Context, draft ProjectionDraft, routes []InboxRoute) (Event, []InboxSelection, error) {
	if draft.Event.EventType != "EXECUTION_STARTED" || draft.Event.OrganizationID == "" || draft.Event.SourceActorID != "runtime" || draft.Event.SourceExecutionID != "" || draft.Event.RecipientScope != "" || draft.Event.RecipientID != "" || draft.Event.TaskID == "" || draft.Event.TaskID != draft.RecordID || draft.Event.CorrelationID == "" || draft.ProjectionKind != "task" || draft.RecordID == "" || draft.Version < 2 || len(routes) < 2 {
		return Event{}, nil, fmt.Errorf("complete Agent execution-start identity and inbox routes are required")
	}
	var task core.Task
	var detail ExecutionStartDetail
	if decodePayload(draft.Value, &task) != nil || task.ID != core.ID(draft.RecordID) || task.ExecutionKind != core.ExecutionAgent || task.Status != core.TaskRunning || task.AssigneeType != "AGENT" || task.AssigneeID == "" || decodePayload(draft.Event.Payload, &detail) != nil || detail.InboxCutoffSequence != 0 || detail.Mode != "" && detail.Mode != "BLOCKED_DEPENDENCY_REMEDIATION" {
		return Event{}, nil, fmt.Errorf("agent execution-start task or detail is invalid")
	}
	appender, ok := g.ledger.(ExecutionStartAppender)
	if !ok {
		return Event{}, nil, fmt.Errorf("event ledger does not support atomic Agent execution start")
	}
	return appender.AppendExecutionStart(ctx, draft, routes)
}

func (g *Gateway) PublishWorkCompletion(ctx context.Context, draft ProjectionDraft) (Event, error) {
	if draft.Event.EventType != "WORK_COMPLETED" || draft.ProjectionKind != "work" || draft.RecordID == "" || draft.Version < 2 {
		return Event{}, fmt.Errorf("complete work transition identity is required")
	}
	var work core.Work
	var detail WorkCompletionTransitionPayload
	if decodePayload(draft.Value, &work) != nil || work.ID != core.ID(draft.RecordID) || work.Status != core.WorkCompleted ||
		decodePayload(draft.Event.Payload, &detail) != nil || detail.EvidenceEventRef == "" || detail.Fingerprint == "" {
		return Event{}, fmt.Errorf("completed work requires exact durable evidence")
	}
	if err := g.validateAddressed(ctx, draft.Event, false); err != nil {
		return Event{}, err
	}
	appender, ok := g.ledger.(WorkCompletionAppender)
	if !ok {
		return Event{}, fmt.Errorf("event ledger does not support evidence-backed work completion")
	}
	return appender.AppendWorkCompletion(ctx, draft)
}

// PublishProjections atomically admits a closed set of trusted projection
// transitions. It is used for Task-DAG creation so a restart can never observe
// a graph with only some of its dependency nodes.
func (g *Gateway) PublishProjections(ctx context.Context, drafts []ProjectionDraft) ([]Event, error) {
	if len(drafts) == 0 {
		return nil, fmt.Errorf("at least one projection is required")
	}
	for _, draft := range drafts {
		if draft.Event.EventType == "" || draft.ProjectionKind == "" || draft.RecordID == "" || draft.Version < 1 {
			return nil, fmt.Errorf("event type, projection kind, record id, and positive version are required")
		}
		if err := g.validateAddressed(ctx, draft.Event, false); err != nil {
			return nil, err
		}
		if err := rejectBareWorkCompletion(draft); err != nil {
			return nil, err
		}
	}
	store, ok := g.ledger.(ProjectionBatchAppender)
	if !ok {
		return nil, fmt.Errorf("event ledger does not support atomic projection batches")
	}
	return store.AppendProjections(ctx, drafts)
}

func rejectBareWorkCompletion(draft ProjectionDraft) error {
	if draft.ProjectionKind != "work" {
		return nil
	}
	var work core.Work
	if decodePayload(draft.Value, &work) != nil {
		return fmt.Errorf("work projection value is invalid")
	}
	if work.Status == core.WorkCompleted || draft.Event.EventType == "WORK_COMPLETED" {
		return fmt.Errorf("completed work requires evidence-backed admission")
	}
	return nil
}
func (g *Gateway) ProjectionRecords(ctx context.Context, kind, id string) ([][]byte, error) {
	store, ok := g.ledger.(ProjectionReader)
	if !ok {
		return nil, fmt.Errorf("event ledger does not support durable projections")
	}
	return store.Records(ctx, kind, id)
}
func (g *Gateway) Events(ctx context.Context, correlationID string) ([]Event, error) {
	return g.ledger.Events(ctx, correlationID)
}

func (g *Gateway) Inbox(ctx context.Context, recipientScope, recipientID string) ([]Event, error) {
	if !validRecipient(recipientScope) || recipientID == "" {
		return nil, fmt.Errorf("valid recipient scope and id are required")
	}
	reader, ok := g.ledger.(InboxReader)
	if !ok {
		return nil, fmt.Errorf("event ledger does not support durable inboxes")
	}
	return reader.Inbox(ctx, recipientScope, recipientID)
}

func (g *Gateway) InboxObservations(ctx context.Context) (map[string]InboxObservationBinding, error) {
	reader, ok := g.ledger.(InboxObservationReader)
	if !ok {
		return nil, fmt.Errorf("event ledger does not support durable inbox observations")
	}
	return reader.InboxObservations(ctx)
}

func (g *Gateway) ObserveInbox(ctx context.Context, organizationID, actorID, executionID, taskID, correlationID, recipientScope, recipientID string, eventIDs []string) (Event, error) {
	if organizationID == "" || actorID == "" || executionID == "" || taskID == "" || correlationID == "" || !validRecipient(recipientScope) || recipientID == "" || len(eventIDs) == 0 {
		return Event{}, fmt.Errorf("complete execution, recipient, and event identities are required")
	}
	distinct := make(map[string]struct{}, len(eventIDs))
	for _, eventID := range eventIDs {
		if eventID == "" {
			return Event{}, fmt.Errorf("event ids must be non-empty")
		}
		distinct[eventID] = struct{}{}
	}
	if len(distinct) != len(eventIDs) {
		return Event{}, fmt.Errorf("event ids must be distinct")
	}
	observer, ok := g.ledger.(InboxObserver)
	if !ok {
		return Event{}, fmt.Errorf("event ledger does not support durable inbox observations")
	}
	draft := TrustedDraft{OrganizationID: organizationID, EventType: "INBOX_EVENTS_OBSERVED", SourceActorID: actorID, SourceExecutionID: executionID, RecipientScope: recipientScope, RecipientID: recipientID, TaskID: taskID, CorrelationID: correlationID, Payload: struct {
		EventIDs []string `json:"event_ids"`
	}{EventIDs: eventIDs}}
	return observer.ObserveInbox(ctx, draft, recipientScope, recipientID, eventIDs)
}

func (g *Gateway) validateAddressed(ctx context.Context, draft TrustedDraft, sourceRequired bool) error {
	addressed := draft.EventType == "MESSAGE" || draft.RecipientScope != "" || draft.RecipientID != ""
	if !addressed {
		return nil
	}
	validateSource := sourceRequired || draft.EventType == "MESSAGE"
	if draft.OrganizationID == "" || !validRecipient(draft.RecipientScope) || draft.RecipientID == "" {
		return fmt.Errorf("addressed event organization and valid recipient are required")
	}
	if validateSource && draft.SourceActorID == "" {
		return fmt.Errorf("addressed agent event requires an authenticated source")
	}
	if draft.EventType == "MESSAGE" {
		var content struct {
			Body string `json:"body"`
		}
		if err := decodePayload(draft.Payload, &content); err != nil || content.Body == "" {
			return fmt.Errorf("message payload requires a non-empty body")
		}
	}
	if draft.EventType == "TASK_BLOCKED" {
		var content TaskBlockedPayload
		if err := decodePayload(draft.Payload, &content); err != nil || content.Reason == "" || content.Missing == "" || content.WhyNeeded == "" || content.WorkCompleted == "" {
			return fmt.Errorf("task blocked payload requires reason, missing, why_needed, and work_completed")
		}
	}
	if g.routeValidator == nil {
		return fmt.Errorf("addressed event route validator is required")
	}
	return g.routeValidator.ValidateAddressedRoute(ctx, AddressedRoute{OrganizationID: draft.OrganizationID, EventType: draft.EventType, SourceActorID: draft.SourceActorID, ValidateSource: validateSource, RecipientScope: draft.RecipientScope, RecipientID: draft.RecipientID, TaskID: draft.TaskID})
}

func decodePayload(value any, target any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(payload, target)
}

func validRecipient(scope string) bool {
	return scope == RecipientAgent || scope == RecipientTeam || scope == RecipientTask
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

package events

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/dominicnunez/agentos/internal/core"
)

const SchemaVersion = 4

// ErrStrategicContextChanged identifies a fail-closed execution admission
// rejected because the Mission or Goal no longer matches the reviewed Plan.
var ErrStrategicContextChanged = errors.New("strategic context changed")

// ReviewedIntentEvidenceLimit bounds the durable intake/review evidence
// replayed for one Intent confirmation.
const ReviewedIntentEvidenceLimit = 1024

// ReviewedIntentEvidenceIndex is a replay-time index of bounded review events
// keyed by correlation. It prevents repeated full-ledger scans while retaining
// the exact time-of-use boundary for each confirmation.
type ReviewedIntentEvidenceIndex map[string][]Event

func IndexReviewedIntentEvidence(stream []Event) ReviewedIntentEvidenceIndex {
	index := make(ReviewedIntentEvidenceIndex)
	for _, event := range stream {
		switch event.EventType {
		case "INTAKE_MESSAGE_RECORDED", "INTENT_DRAFTED", "INTENT_CONFIRMED":
			index[event.CorrelationID] = append(index[event.CorrelationID], event)
		}
	}
	for correlationID := range index {
		sort.SliceStable(index[correlationID], func(left, right int) bool {
			return index[correlationID][left].Sequence < index[correlationID][right].Sequence
		})
	}
	return index
}

func (index ReviewedIntentEvidenceIndex) At(confirmation Event) []Event {
	evidence := index[confirmation.CorrelationID]
	if confirmation.Sequence <= 0 {
		return evidence
	}
	end := sort.Search(len(evidence), func(position int) bool {
		return evidence[position].Sequence > confirmation.Sequence
	})
	return evidence[:end]
}

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

// DecodeDurableOperatorInput accepts only the canonical operator-input Event
// Contract and its authority-free durable envelope. Authentication remains an
// ingress responsibility; the source identity copied into the payload must
// exactly match the trusted event envelope.
func DecodeDurableOperatorInput(event Event) (OperatorInputReceivedPayload, error) {
	var input OperatorInputReceivedPayload
	if decodeExactEventJSON(event.Payload, &input) != nil ||
		event.EventID == "" || event.Sequence < 1 || event.CreatedAt.IsZero() || event.SchemaVersion != SchemaVersion ||
		event.OrganizationID == "" || event.SourceActorID == "" || event.SourceExecutionID != "" || event.RecipientScope != "" || event.RecipientID != "" || event.TaskID == "" || event.CorrelationID == "" ||
		len(event.AuthorizationRefs) != 0 || len(event.ArtifactRefs) != 0 || input.MessageID == "" || strings.TrimSpace(input.Text) == "" || !utf8.ValidString(input.Text) || input.SourcePrincipalID != event.SourceActorID {
		return OperatorInputReceivedPayload{}, fmt.Errorf("operator input event contract is invalid")
	}
	switch event.EventType {
	case "A2A_INPUT_RECEIVED":
		if input.SourcePrincipalKind != string(core.PrincipalExternalAgent) || input.SourceChannel != "A2A" {
			return OperatorInputReceivedPayload{}, fmt.Errorf("operator input event contract is invalid")
		}
	case "HUMAN_INPUT_RECEIVED":
		if input.SourcePrincipalKind != string(core.PrincipalHuman) || input.SourceChannel != "HUMAN_DIRECT" {
			return OperatorInputReceivedPayload{}, fmt.Errorf("operator input event contract is invalid")
		}
	default:
		return OperatorInputReceivedPayload{}, fmt.Errorf("operator input event contract is invalid")
	}
	return input, nil
}

type HumanTaskCompletionSubmittedPayload struct {
	MessageID         string                  `json:"message_id"`
	Fields            map[string]string       `json:"fields"`
	Artifacts         []core.ArtifactEvidence `json:"artifacts,omitempty"`
	SourcePrincipalID string                  `json:"source_principal_id"`
	SourceChannel     string                  `json:"source_channel"`
}

// DecodeHumanTaskCompletion accepts only the canonical structured user
// completion Event Contract and its authority-free durable envelope.
func DecodeHumanTaskCompletion(event Event) (HumanTaskCompletionSubmittedPayload, error) {
	var submission HumanTaskCompletionSubmittedPayload
	if decodeExactEventJSON(event.Payload, &submission) != nil || event.EventID == "" || event.Sequence < 1 || event.CreatedAt.IsZero() || event.SchemaVersion != SchemaVersion ||
		event.EventType != "HUMAN_TASK_COMPLETION_SUBMITTED" || event.OrganizationID == "" || event.SourceActorID == "" || event.SourceExecutionID != "" || event.RecipientScope != "" || event.RecipientID != "" || event.TaskID == "" || event.CorrelationID == "" ||
		len(event.AuthorizationRefs) != 0 || submission.MessageID == "" || submission.SourcePrincipalID != event.SourceActorID || submission.SourceChannel != "HUMAN_DIRECT" {
		return HumanTaskCompletionSubmittedPayload{}, fmt.Errorf("structured user completion event contract is invalid")
	}
	refs := make([]string, len(submission.Artifacts))
	for index, artifact := range submission.Artifacts {
		if artifact.Ref == "" || artifact.Origin != submission.SourcePrincipalID {
			return HumanTaskCompletionSubmittedPayload{}, fmt.Errorf("structured user completion artifact evidence is invalid")
		}
		refs[index] = artifact.Ref
	}
	if !slices.Equal(event.ArtifactRefs, refs) {
		return HumanTaskCompletionSubmittedPayload{}, fmt.Errorf("structured user completion artifact references do not match")
	}
	return submission, nil
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

// ValidateIntentConfirmation proves that one reviewed confirmation exactly
// authorizes the accepted durable Intent, including an optional Goal binding.
func ValidateIntentConfirmation(stream []Event, event Event, intent core.Intent) error {
	var confirmation IntentConfirmedPayload
	if decodeExactEventJSON(event.Payload, &confirmation) != nil ||
		event.EventType != "INTENT_CONFIRMED" || event.OrganizationID != string(intent.OrganizationID) || event.SourceActorID == "" || event.SourceActorID != confirmation.ConfirmingActorID || event.SourceExecutionID != "" || event.RecipientScope != "" || event.RecipientID != "" || event.TaskID != "task-"+event.CorrelationID || len(event.AuthorizationRefs) != 0 || len(event.ArtifactRefs) != 0 || event.CorrelationID == "" || event.SchemaVersion != SchemaVersion ||
		confirmation.IntentID != string(intent.ID) || confirmation.GoalID != string(intent.GoalID) || confirmation.Version < 1 || confirmation.Fingerprint == "" || confirmation.Fingerprint != intent.AcceptedFingerprint || !core.ValidIntentSourceIdentity(intent.SourcePrincipalID, intent.SourcePrincipalKind, intent.SourceChannel) || !validReviewedOperatorIdentity(confirmation.ConfirmingActorID, confirmation.ConfirmingActorKind, confirmation.SourceChannel) || confirmation.MessageID == "" {
		return fmt.Errorf("intent confirmation is invalid")
	}
	original, found := initialReviewedIntake(stream, event)
	if !found || intent.SourcePrincipalID != core.ID(original.SourcePrincipalID) || intent.SourcePrincipalKind != core.PrincipalKind(original.SourcePrincipalKind) || intent.SourceChannel != original.SourceChannel || intent.SourceMessageID != original.MessageID || intent.OriginalInstruction != original.Text {
		return fmt.Errorf("intent confirmation is not bound to its original intake source")
	}
	return nil
}

func initialReviewedIntake(stream []Event, confirmation Event) (IntakeMessageRecordedPayload, bool) {
	var original IntakeMessageRecordedPayload
	var originalSequence int64
	found := false
	for _, event := range stream {
		if event.EventType != "INTAKE_MESSAGE_RECORDED" || event.CorrelationID != confirmation.CorrelationID || confirmation.Sequence > 0 && event.Sequence > confirmation.Sequence {
			continue
		}
		var payload IntakeMessageRecordedPayload
		if decodeExactEventJSON(event.Payload, &payload) != nil || !validReviewedIntakeMessage(event, payload, confirmation) {
			return IntakeMessageRecordedPayload{}, false
		}
		if !found || event.Sequence < originalSequence {
			original = payload
			originalSequence = event.Sequence
			found = true
		}
	}
	return original, found
}

// ValidateReviewedIntentAdmission replays the bounded intake and review
// evidence that authorizes one Intent confirmation without a Goal binding.
func ValidateReviewedIntentAdmission(stream []Event, confirmationEvent Event) error {
	return ValidateIndexedReviewedIntentAdmission(IndexReviewedIntentEvidence(stream).At(confirmationEvent), confirmationEvent)
}

// ValidateIndexedReviewedIntentAdmission validates a pre-indexed, time-bounded
// review slice without rescanning the full ledger.
func ValidateIndexedReviewedIntentAdmission(stream []Event, confirmationEvent Event) error {
	var confirmation IntentConfirmedPayload
	if decodeExactEventJSON(confirmationEvent.Payload, &confirmation) != nil ||
		!validReviewedIntentConfirmation(confirmationEvent, confirmation, "") {
		return fmt.Errorf("intent confirmation does not match its reviewed intake")
	}
	return validateReviewedIntent(stream, confirmationEvent, confirmation, "")
}

// ValidateReviewedGoalIntentAdmission replays the bounded intake and review
// evidence that authorizes one Goal-bound intent confirmation. The supplied
// Goal must be the exact durable Goal state visible at the confirmation event.
func ValidateReviewedGoalIntentAdmission(stream []Event, confirmationEvent Event, goal core.Goal) error {
	return ValidateIndexedReviewedGoalIntentAdmission(IndexReviewedIntentEvidence(stream).At(confirmationEvent), confirmationEvent, goal)
}

// ValidateIndexedReviewedGoalIntentAdmission validates a pre-indexed,
// time-bounded review slice for one Goal-bound confirmation.
func ValidateIndexedReviewedGoalIntentAdmission(stream []Event, confirmationEvent Event, goal core.Goal) error {
	var confirmation IntentConfirmedPayload
	if decodeExactEventJSON(confirmationEvent.Payload, &confirmation) != nil ||
		confirmationEvent.OrganizationID != string(goal.OrganizationID) || !validReviewedIntentConfirmation(confirmationEvent, confirmation, goal.ID) {
		return fmt.Errorf("goal-bound intent confirmation does not match its checked goal")
	}
	if goal.ID == "" || goal.Status != core.GoalActive {
		return fmt.Errorf("goal-bound intent confirmation requires its active Goal at admission")
	}
	return validateReviewedIntent(stream, confirmationEvent, confirmation, goal.ID)
}

func validReviewedIntentConfirmation(event Event, confirmation IntentConfirmedPayload, goalID core.ID) bool {
	return event.EventType == "INTENT_CONFIRMED" && event.OrganizationID != "" &&
		event.SourceActorID != "" && event.SourceActorID == confirmation.ConfirmingActorID && validReviewedOperatorIdentity(confirmation.ConfirmingActorID, confirmation.ConfirmingActorKind, confirmation.SourceChannel) &&
		event.SourceExecutionID == "" && event.RecipientScope == "" && event.RecipientID == "" && event.TaskID == "task-"+event.CorrelationID &&
		len(event.AuthorizationRefs) == 0 && len(event.ArtifactRefs) == 0 && event.CorrelationID != "" && event.SchemaVersion == SchemaVersion &&
		confirmation.IntentID == "intent-"+event.CorrelationID && confirmation.GoalID == string(goalID) && confirmation.Version >= 1 && confirmation.Fingerprint != "" && confirmation.MessageID != ""
}

func validateReviewedIntent(stream []Event, confirmationEvent Event, confirmation IntentConfirmedPayload, goalID core.ID) error {
	if len(stream) > ReviewedIntentEvidenceLimit {
		return fmt.Errorf("intent review evidence exceeds its admission bound")
	}
	intakeMessages := make(map[string]IntakeMessageRecordedPayload)
	intakeSequences := make(map[string]int64)
	var latestIntakeMessageID string
	var latestIntakeSequence int64
	var latestDraftEvent Event
	var latestDraft IntentDraftedPayload
	draftCount := 0
	for _, event := range stream {
		switch event.EventType {
		case "INTAKE_MESSAGE_RECORDED":
			var payload IntakeMessageRecordedPayload
			if decodeExactEventJSON(event.Payload, &payload) != nil || !validReviewedIntakeMessage(event, payload, confirmationEvent) {
				return fmt.Errorf("intent has invalid durable intake evidence")
			}
			if _, exists := intakeMessages[payload.MessageID]; exists {
				return fmt.Errorf("intent source message is not unique")
			}
			intakeMessages[payload.MessageID] = payload
			intakeSequences[payload.MessageID] = event.Sequence
			latestIntakeMessageID = payload.MessageID
			latestIntakeSequence = event.Sequence
		case "INTENT_DRAFTED":
			var payload IntentDraftedPayload
			if decodeExactEventJSON(event.Payload, &payload) != nil || event.OrganizationID != confirmationEvent.OrganizationID || event.SourceActorID != "runtime" || event.SourceExecutionID != "" || event.RecipientScope != "" || event.RecipientID != "" || event.TaskID != confirmationEvent.TaskID || len(event.AuthorizationRefs) != 0 || len(event.ArtifactRefs) != 0 || event.CorrelationID != confirmationEvent.CorrelationID || event.SchemaVersion != SchemaVersion {
				return fmt.Errorf("intent has invalid durable review draft")
			}
			draftCount++
			latestDraftEvent = event
			latestDraft = payload
		}
	}
	if latestDraftEvent.EventID == "" || latestIntakeMessageID == "" || latestDraftEvent.Sequence <= latestIntakeSequence || latestDraft.SourceMessageID != latestIntakeMessageID || latestDraft.Draft.CreatedAt.IsZero() || strings.TrimSpace(latestDraft.Reply) == "" {
		return fmt.Errorf("intent confirmation requires the current durable reviewed draft")
	}
	reviewed := latestDraft.Draft
	if reviewed.ID != core.ID(confirmation.IntentID) || reviewed.OrganizationID != core.ID(confirmationEvent.OrganizationID) || reviewed.Version != confirmation.Version || reviewed.Version != draftCount || reviewed.Fingerprint != confirmation.Fingerprint {
		return fmt.Errorf("intent confirmation does not match its durable reviewed draft")
	}
	switch reviewed.RequestedExecutionKind {
	case core.ExecutionDeterministic, core.ExecutionAgent, core.ExecutionHuman:
	case core.ExecutionTool, core.ExecutionTeam, core.ExecutionMixed, "":
		return fmt.Errorf("intent reviewed execution kind is unavailable")
	default:
		return fmt.Errorf("intent reviewed execution kind is unavailable")
	}
	if err := core.ValidateAcceptedIntentDraft(reviewed, core.ID(confirmationEvent.OrganizationID), reviewed.RequestedExecutionKind); err != nil {
		return fmt.Errorf("intent durable reviewed draft is invalid: %w", err)
	}
	reviewedGoalID, err := core.AcceptedIntentGoalID(reviewed)
	if err != nil || reviewedGoalID != goalID {
		return fmt.Errorf("intent reviewed Goal provenance is invalid")
	}
	if goalID == "" {
		if reviewed.Goal != nil {
			return fmt.Errorf("unbound intent contains a Goal")
		}
	} else {
		if reviewed.Goal == nil || reviewed.Goal.Origin != "EXPLICIT" && reviewed.Goal.Origin != "CONFIRMED" {
			return fmt.Errorf("goal-bound intent reviewed Goal provenance is invalid")
		}
		goalMessage, found := intakeMessages[reviewed.Goal.SourceMessageID]
		if !found || intakeSequences[reviewed.Goal.SourceMessageID] >= latestDraftEvent.Sequence || !core.ContainsExactGoalReference(goalMessage.Text, string(goalID)) {
			return fmt.Errorf("goal-bound intent Goal is not present in its attributed source message")
		}
	}
	for _, event := range stream {
		if event.EventType == "INTENT_CONFIRMED" && event.Sequence <= latestDraftEvent.Sequence {
			return fmt.Errorf("intent confirmation precedes its reviewed draft")
		}
	}
	return nil
}

func validReviewedIntakeMessage(event Event, payload IntakeMessageRecordedPayload, confirmationEvent Event) bool {
	if payload.MessageID == "" || strings.TrimSpace(payload.Text) == "" || !utf8.ValidString(payload.Text) || payload.SourcePrincipalID == "" || payload.SourcePrincipalKind == "" || payload.SourceChannel == "" ||
		event.OrganizationID != confirmationEvent.OrganizationID || event.SourceActorID != payload.SourcePrincipalID || event.SourceExecutionID != "" || event.RecipientScope != "" || event.RecipientID != "" || event.TaskID != confirmationEvent.TaskID || len(event.AuthorizationRefs) != 0 || len(event.ArtifactRefs) != 0 || event.CorrelationID != confirmationEvent.CorrelationID || event.SchemaVersion != SchemaVersion {
		return false
	}
	if !validReviewedOperatorIdentity(payload.SourcePrincipalID, payload.SourcePrincipalKind, payload.SourceChannel) {
		return false
	}
	switch payload.RequestedExecutionKind {
	case "", core.ExecutionDeterministic, core.ExecutionAgent, core.ExecutionHuman:
		return true
	case core.ExecutionTool, core.ExecutionTeam, core.ExecutionMixed:
		return false
	default:
		return false
	}
}

func validReviewedOperatorIdentity(id, kind, channel string) bool {
	principalKind := core.PrincipalKind(kind)
	return principalKind != core.PrincipalRuntime && core.ValidIntentSourceIdentity(core.ID(id), principalKind, channel)
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

type GoalProgressResult string

const (
	GoalProgressMethodExactCriteria                    = "EXACT_CRITERIA_COVERAGE_V1"
	GoalProgressTargetInProgress    GoalProgressResult = "TARGET_IN_PROGRESS"
	GoalProgressTargetAchieved      GoalProgressResult = "TARGET_ACHIEVED"
	GoalProgressContinuous          GoalProgressResult = "CONTINUOUS_PROGRESS"
)

type GoalCriterionProgressPayload struct {
	Criterion        core.IntentValue `json:"criterion"`
	Satisfied        bool             `json:"satisfied"`
	WorkEvidenceRefs []string         `json:"work_evidence_refs"`
}

// GoalProgressEvaluatedPayload binds one deterministic Goal evaluation to the
// exact Goal revision and authoritative completed-Work evidence selected by
// the ledger. It is evidence, not authority; only the specialized atomic Goal
// admission path may turn an achieved result into a terminal projection.
type GoalProgressEvaluatedPayload struct {
	GoalID           core.ID                        `json:"goal_id"`
	GoalVersion      int                            `json:"goal_version"`
	MissionID        core.ID                        `json:"mission_id"`
	Mode             core.GoalMode                  `json:"mode"`
	Criteria         []GoalCriterionProgressPayload `json:"criteria"`
	WorkEvidenceRefs []string                       `json:"work_evidence_refs"`
	Method           string                         `json:"method"`
	Result           GoalProgressResult             `json:"result"`
	EvaluatedAt      time.Time                      `json:"evaluated_at"`
	Fingerprint      string                         `json:"fingerprint"`
}

type GoalAchievementTransitionPayload struct {
	EvidenceEventRef string `json:"evidence_event_ref"`
	Fingerprint      string `json:"fingerprint"`
}

type GoalWorkEvidence struct {
	EventRef string
	EventAt  time.Time
	Evidence WorkCompletionEvidencePayload
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
	Mode                 string                `json:"mode,omitempty"`
	InputEventRef        string                `json:"input_event_ref,omitempty"`
	InboxCutoffSequence  int64                 `json:"inbox_cutoff_sequence"`
	DispatchBinding      *AgentDispatchBinding `json:"dispatch_binding,omitempty"`
	StrategicEventRefs   []string              `json:"strategic_event_refs,omitempty"`
	StrategicContextRefs []core.VersionedRef   `json:"strategic_context_refs,omitempty"`
}

// AgentDispatchBinding is the one-shot, ledger-backed admission for an exact
// Agent execution. It does not grant capabilities, effects, approvals, or
// completion authority; those remain governed by their dedicated contracts.
type AgentDispatchBinding struct {
	DispatchID                    core.ID `json:"dispatch_id"`
	OrganizationID                core.ID `json:"organization_id"`
	TaskID                        core.ID `json:"task_id"`
	TaskVersion                   int     `json:"task_version"`
	AgentID                       core.ID `json:"agent_id"`
	AgentRecordVersion            int     `json:"agent_record_version"`
	AgentEventRef                 string  `json:"agent_event_ref"`
	BlueprintID                   core.ID `json:"blueprint_id"`
	BlueprintRecordVersion        int     `json:"blueprint_record_version"`
	BlueprintVersion              string  `json:"blueprint_version"`
	BlueprintEventRef             string  `json:"blueprint_event_ref"`
	ExecutionProfileID            core.ID `json:"execution_profile_id"`
	ExecutionProfileRecordVersion int     `json:"execution_profile_record_version"`
	ExecutionProfileVersion       string  `json:"execution_profile_version"`
	ExecutionProfileEventRef      string  `json:"execution_profile_event_ref"`
	RuntimeAdapter                string  `json:"runtime_adapter"`
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

// ResolveStrategicContext selects the latest exact Mission and Goal
// projections visible at one boundary. The returned records are explanatory
// work context only. Event and version references make the selection
// independently replayable without trusting mutable current projections.
func ResolveStrategicContext(organizationID string, work core.Work, stream []Event, beforeSequence int64) (*core.StrategicContext, []string, []core.VersionedRef, error) {
	if organizationID == "" || work.ID == "" {
		return nil, nil, nil, fmt.Errorf("strategic context organization and Work are required")
	}
	if work.GoalID == "" {
		return nil, nil, nil, nil
	}
	goalEvent, goalRecord, goal, err := latestGoalProjection(organizationID, work.GoalID, stream, beforeSequence)
	if err != nil {
		return nil, nil, nil, err
	}
	missionEvent, missionRecord, mission, err := latestMissionProjection(organizationID, goal.MissionID, stream, beforeSequence)
	if err != nil {
		return nil, nil, nil, err
	}
	context := &core.StrategicContext{
		Mission: mission, MissionVersion: missionRecord.Version,
		Goal: goal, GoalVersion: goalRecord.Version,
	}
	if !core.ValidStrategicContext(*context) {
		return nil, nil, nil, fmt.Errorf("durable Mission and Goal do not form valid strategic context")
	}
	return context,
		[]string{missionEvent.EventID, goalEvent.EventID},
		[]core.VersionedRef{
			{ID: "mission/" + string(mission.ID), Version: strconv.Itoa(missionRecord.Version), MaterializationState: core.MaterializedFull},
			{ID: "goal/" + string(goal.ID), Version: strconv.Itoa(goalRecord.Version), MaterializationState: core.MaterializedFull},
		}, nil
}

// ResolveStrategicContextByRefs reconstructs the exact strategic revisions
// fingerprinted into a durable Plan. It never substitutes newer projections.
func ResolveStrategicContextByRefs(organizationID string, work core.Work, stream []Event, eventRefs []string, versionRefs []core.VersionedRef) (*core.StrategicContext, error) {
	return resolveStrategicContextByRefs(organizationID, work, stream, eventRefs, versionRefs, 0)
}

func resolveStrategicContextByRefs(organizationID string, work core.Work, stream []Event, eventRefs []string, versionRefs []core.VersionedRef, beforeSequence int64) (*core.StrategicContext, error) {
	if organizationID == "" || work.ID == "" {
		return nil, fmt.Errorf("strategic context organization and Work are required")
	}
	if work.GoalID == "" {
		if len(eventRefs) != 0 || len(versionRefs) != 0 {
			return nil, fmt.Errorf("ad hoc Work cannot bind strategic context")
		}
		return nil, nil
	}
	if len(eventRefs) != 2 || len(versionRefs) != 2 || eventRefs[0] == eventRefs[1] ||
		versionRefs[0].MaterializationState != core.MaterializedFull || versionRefs[1].MaterializationState != core.MaterializedFull {
		return nil, fmt.Errorf("strategic context references are invalid")
	}
	missionEvent, found := eventWithID(stream, eventRefs[0])
	if !found {
		return nil, fmt.Errorf("strategic Mission event is unavailable")
	}
	goalEvent, found := eventWithID(stream, eventRefs[1])
	if !found {
		return nil, fmt.Errorf("strategic Goal event is unavailable")
	}
	if beforeSequence > 0 && (missionEvent.Sequence >= beforeSequence || goalEvent.Sequence >= beforeSequence) {
		return nil, fmt.Errorf("strategic context revisions do not precede their durable Plan")
	}
	missionRecord, mission, err := exactMissionProjection(organizationID, missionEvent)
	if err != nil {
		return nil, err
	}
	goalRecord, goal, err := exactGoalProjection(organizationID, goalEvent)
	if err != nil {
		return nil, err
	}
	if goal.ID != work.GoalID || versionRefs[0].ID != "mission/"+string(mission.ID) || versionRefs[0].Version != strconv.Itoa(missionRecord.Version) ||
		versionRefs[1].ID != "goal/"+string(goal.ID) || versionRefs[1].Version != strconv.Itoa(goalRecord.Version) {
		return nil, fmt.Errorf("strategic context references do not match durable projections")
	}
	context := &core.StrategicContext{Mission: mission, MissionVersion: missionRecord.Version, Goal: goal, GoalVersion: goalRecord.Version}
	if !core.ValidStrategicContext(*context) {
		return nil, fmt.Errorf("durable Mission and Goal do not form valid strategic context")
	}
	return context, nil
}

// ResolvePlanStrategicContext validates the one runtime-owned Plan for a Work
// and reconstructs the exact Mission and Goal revisions fingerprinted into it.
func ResolvePlanStrategicContext(organizationID, correlationID string, work core.Work, intent core.Intent, stream []Event) (core.Plan, *core.StrategicContext, error) {
	plan, planEvent, err := resolvePlan(organizationID, correlationID, work, intent, stream)
	if err != nil {
		return core.Plan{}, nil, err
	}
	context, err := resolveStrategicContextByRefs(organizationID, work, stream, plan.StrategicEventRefs, plan.StrategicContextRefs, planEvent.Sequence)
	if err != nil {
		return core.Plan{}, nil, err
	}
	return plan, context, nil
}

// ResolvePlan validates the one runtime-owned Plan and binds it to the exact
// accepted Intent fingerprint. Strategic projection events may be resolved
// separately through their immutable references.
func ResolvePlan(organizationID, correlationID string, work core.Work, intent core.Intent, stream []Event) (core.Plan, error) {
	plan, _, err := resolvePlan(organizationID, correlationID, work, intent, stream)
	return plan, err
}

func resolvePlan(organizationID, correlationID string, work core.Work, intent core.Intent, stream []Event) (core.Plan, Event, error) {
	if organizationID == "" || correlationID == "" || work.ID == "" || work.IntentID == "" || intent.ID == "" || intent.AcceptedFingerprint == "" {
		return core.Plan{}, Event{}, fmt.Errorf("strategic Plan identity is incomplete")
	}
	if intent.ID != work.IntentID || intent.OrganizationID != core.ID(organizationID) {
		return core.Plan{}, Event{}, fmt.Errorf("strategic Plan Intent does not match durable Work")
	}
	var selected Event
	var plan core.Plan
	for _, event := range stream {
		if event.EventType != "PLAN_CREATED" || event.CorrelationID != correlationID {
			continue
		}
		var candidate core.Plan
		if selected.EventID != "" || event.OrganizationID != organizationID || event.SourceActorID != "runtime" || event.RecipientScope != "" || event.RecipientID != "" || event.TaskID != "task-"+correlationID || len(event.AuthorizationRefs) != 0 || len(event.ArtifactRefs) != 0 ||
			event.SourceExecutionID != "" && event.SourceExecutionID != "planning-plan-"+correlationID+"-attempt-1" || decodeExactEventJSON(event.Payload, &candidate) != nil {
			return core.Plan{}, Event{}, fmt.Errorf("strategic Plan event is invalid")
		}
		selected, plan = event, candidate
	}
	_, offset := plan.CreatedAt.Zone()
	expectedFingerprint, err := core.FingerprintPlan(plan)
	if err != nil {
		return core.Plan{}, Event{}, fmt.Errorf("fingerprint strategic Plan: %w", err)
	}
	if selected.EventID == "" {
		return core.Plan{}, Event{}, fmt.Errorf("strategic Plan is unavailable")
	}
	if plan.ID != core.ID("plan-"+correlationID) || plan.IntentID != work.IntentID || plan.IntentFingerprint != intent.AcceptedFingerprint || plan.Version != 1 || len(plan.Tasks) == 0 || plan.CreatedAt.IsZero() || offset != 0 {
		return core.Plan{}, Event{}, fmt.Errorf("strategic Plan identity or lifecycle is invalid")
	}
	if plan.Fingerprint == "" || plan.Fingerprint != expectedFingerprint {
		return core.Plan{}, Event{}, fmt.Errorf("strategic Plan fingerprint is invalid")
	}
	return plan, selected, nil
}

func exactGoalProjection(organizationID string, event Event) (ProjectionRecord, core.Goal, error) {
	return exactStrategicProjection(organizationID, event, "goal", "Goal", func(value core.Goal) core.ID { return value.ID }, func(value core.Goal) core.ID { return value.OrganizationID }, core.ValidGoal)
}

func exactMissionProjection(organizationID string, event Event) (ProjectionRecord, core.Mission, error) {
	return exactStrategicProjection(organizationID, event, "mission", "Mission", func(value core.Mission) core.ID { return value.ID }, func(value core.Mission) core.ID { return value.OrganizationID }, core.ValidMission)
}

func exactStrategicProjection[T any](organizationID string, event Event, kind, label string, identity func(T) core.ID, tenant func(T) core.ID, valid func(T) bool) (ProjectionRecord, T, error) {
	var value T
	payload, present, err := AdmittedProjection(event)
	if err != nil || !present || payload.Projection.ProjectionKind != kind || event.OrganizationID != organizationID ||
		decodeExactEventJSON(payload.Projection.Value, &value) != nil || payload.Projection.RecordID != string(identity(value)) || tenant(value) != core.ID(organizationID) || !valid(value) {
		return ProjectionRecord{}, value, fmt.Errorf("strategic %s projection is invalid", label)
	}
	return payload.Projection, value, nil
}

func latestGoalProjection(organizationID string, goalID core.ID, stream []Event, beforeSequence int64) (Event, ProjectionRecord, core.Goal, error) {
	return latestStrategicProjection(organizationID, goalID, stream, beforeSequence, "goal", "Goal", func(value core.Goal) core.ID { return value.ID }, func(value core.Goal) core.ID { return value.OrganizationID }, core.ValidGoal)
}

func latestMissionProjection(organizationID string, missionID core.ID, stream []Event, beforeSequence int64) (Event, ProjectionRecord, core.Mission, error) {
	return latestStrategicProjection(organizationID, missionID, stream, beforeSequence, "mission", "Mission", func(value core.Mission) core.ID { return value.ID }, func(value core.Mission) core.ID { return value.OrganizationID }, core.ValidMission)
}

func latestStrategicProjection[T any](organizationID string, recordID core.ID, stream []Event, beforeSequence int64, kind, label string, identity func(T) core.ID, tenant func(T) core.ID, valid func(T) bool) (Event, ProjectionRecord, T, error) {
	var selected Event
	var selectedRecord ProjectionRecord
	var selectedValue T
	for _, event := range stream {
		if beforeSequence > 0 && event.Sequence >= beforeSequence {
			continue
		}
		payload, present, err := AdmittedProjection(event)
		if err != nil {
			return Event{}, ProjectionRecord{}, selectedValue, err
		}
		if !present || payload.Projection.ProjectionKind != kind || payload.Projection.RecordID != string(recordID) {
			continue
		}
		if event.OrganizationID != organizationID {
			continue
		}
		record, value, err := exactStrategicProjection(organizationID, event, kind, label, identity, tenant, valid)
		if err != nil || identity(value) != recordID {
			return Event{}, ProjectionRecord{}, selectedValue, fmt.Errorf("strategic %s projection is invalid", label)
		}
		if event.Sequence > selected.Sequence {
			selected, selectedRecord, selectedValue = event, record, value
		}
	}
	if selected.EventID == "" {
		return Event{}, ProjectionRecord{}, selectedValue, fmt.Errorf("strategic %s projection is unavailable", label)
	}
	return selected, selectedRecord, selectedValue, nil
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

func NewGoalProgressEvaluation(goal core.Goal, goalVersion int, workEvidence []GoalWorkEvidence) (GoalProgressEvaluatedPayload, error) {
	if !core.ValidGoal(goal) || goal.Status != core.GoalActive || goalVersion < 1 || len(workEvidence) == 0 || len(workEvidence) > 4096 {
		return GoalProgressEvaluatedPayload{}, fmt.Errorf("active Goal revision and bounded Work evidence are required")
	}
	selected := append([]GoalWorkEvidence(nil), workEvidence...)
	sort.Slice(selected, func(i, j int) bool { return selected[i].EventRef < selected[j].EventRef })
	seenRefs := make(map[string]struct{}, len(selected))
	seenWorks := make(map[core.ID]struct{}, len(selected))
	evaluatedAt := time.Time{}
	for _, selectedEvidence := range selected {
		_, offset := selectedEvidence.EventAt.Zone()
		if !validEvidenceRef(selectedEvidence.EventRef) || selectedEvidence.EventAt.IsZero() || offset != 0 || !selectedEvidence.Evidence.Valid() || selectedEvidence.Evidence.GoalID != goal.ID {
			return GoalProgressEvaluatedPayload{}, fmt.Errorf("goal Work evidence is invalid")
		}
		if _, duplicate := seenRefs[selectedEvidence.EventRef]; duplicate {
			return GoalProgressEvaluatedPayload{}, fmt.Errorf("goal Work evidence reference is duplicated")
		}
		if _, duplicate := seenWorks[selectedEvidence.Evidence.WorkID]; duplicate {
			return GoalProgressEvaluatedPayload{}, fmt.Errorf("goal Work evidence identity is duplicated")
		}
		seenRefs[selectedEvidence.EventRef] = struct{}{}
		seenWorks[selectedEvidence.Evidence.WorkID] = struct{}{}
		if selectedEvidence.EventAt.After(evaluatedAt) {
			evaluatedAt = selectedEvidence.EventAt
		}
	}
	record := GoalProgressEvaluatedPayload{
		GoalID: goal.ID, GoalVersion: goalVersion, MissionID: goal.MissionID, Mode: goal.Mode,
		Criteria:         make([]GoalCriterionProgressPayload, 0, len(goal.SuccessCriteria)),
		WorkEvidenceRefs: make([]string, 0, len(selected)), Method: GoalProgressMethodExactCriteria,
		EvaluatedAt: evaluatedAt.UTC(),
	}
	allSatisfied := true
	for _, criterion := range goal.SuccessCriteria {
		progress := GoalCriterionProgressPayload{Criterion: criterion}
		for _, selectedEvidence := range selected {
			for _, completedCriterion := range selectedEvidence.Evidence.Criteria {
				if reflect.DeepEqual(completedCriterion, criterion) {
					progress.WorkEvidenceRefs = append(progress.WorkEvidenceRefs, selectedEvidence.EventRef)
					break
				}
			}
		}
		progress.Satisfied = len(progress.WorkEvidenceRefs) > 0
		allSatisfied = allSatisfied && progress.Satisfied
		record.Criteria = append(record.Criteria, progress)
	}
	for _, selectedEvidence := range selected {
		record.WorkEvidenceRefs = append(record.WorkEvidenceRefs, selectedEvidence.EventRef)
	}
	switch goal.Mode {
	case core.GoalTarget:
		record.Result = GoalProgressTargetInProgress
		if allSatisfied {
			record.Result = GoalProgressTargetAchieved
		}
	case core.GoalContinuous:
		record.Result = GoalProgressContinuous
	default:
		return GoalProgressEvaluatedPayload{}, fmt.Errorf("goal mode is invalid")
	}
	fingerprint, err := record.ExpectedFingerprint()
	if err != nil {
		return GoalProgressEvaluatedPayload{}, err
	}
	record.Fingerprint = fingerprint
	if !record.Valid() {
		return GoalProgressEvaluatedPayload{}, fmt.Errorf("goal progress evaluation is invalid")
	}
	return record, nil
}

func (p GoalProgressEvaluatedPayload) Valid() bool {
	_, offset := p.EvaluatedAt.Zone()
	if p.GoalID == "" || p.GoalVersion < 1 || p.MissionID == "" || p.Method != GoalProgressMethodExactCriteria || p.EvaluatedAt.IsZero() || offset != 0 || len(p.Criteria) == 0 || len(p.Criteria) > 256 || len(p.WorkEvidenceRefs) == 0 || len(p.WorkEvidenceRefs) > 4096 || !distinctEvidenceRefs(p.WorkEvidenceRefs) {
		return false
	}
	for index := 1; index < len(p.WorkEvidenceRefs); index++ {
		if p.WorkEvidenceRefs[index-1] >= p.WorkEvidenceRefs[index] {
			return false
		}
	}
	allSatisfied := true
	for _, criterion := range p.Criteria {
		if strings.TrimSpace(criterion.Criterion.Value) == "" || strings.TrimSpace(criterion.Criterion.Origin) == "" || !utf8.ValidString(criterion.Criterion.Value) || !utf8.ValidString(criterion.Criterion.Origin) || !utf8.ValidString(criterion.Criterion.SourceMessageID) || criterion.Satisfied != (len(criterion.WorkEvidenceRefs) > 0) || len(criterion.WorkEvidenceRefs) > len(p.WorkEvidenceRefs) || !distinctEvidenceRefs(criterion.WorkEvidenceRefs) {
			return false
		}
		for index := 1; index < len(criterion.WorkEvidenceRefs); index++ {
			if criterion.WorkEvidenceRefs[index-1] >= criterion.WorkEvidenceRefs[index] {
				return false
			}
		}
		for _, ref := range criterion.WorkEvidenceRefs {
			if _, found := slices.BinarySearch(p.WorkEvidenceRefs, ref); !found {
				return false
			}
		}
		allSatisfied = allSatisfied && criterion.Satisfied
	}
	validResult := p.Mode == core.GoalTarget && (p.Result == GoalProgressTargetInProgress && !allSatisfied || p.Result == GoalProgressTargetAchieved && allSatisfied) ||
		p.Mode == core.GoalContinuous && p.Result == GoalProgressContinuous
	if !validResult {
		return false
	}
	expected, err := p.ExpectedFingerprint()
	return err == nil && validSHA256(p.Fingerprint) && p.Fingerprint == expected
}

func (p GoalProgressEvaluatedPayload) ExpectedFingerprint() (string, error) {
	p.Fingerprint = ""
	body, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(body)
	return fmt.Sprintf("%x", digest), nil
}

func ValidateGoalProgressEvaluation(goal core.Goal, goalVersion int, workEvidence []GoalWorkEvidence, recorded GoalProgressEvaluatedPayload) error {
	expected, err := NewGoalProgressEvaluation(goal, goalVersion, workEvidence)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, recorded) {
		return fmt.Errorf("goal progress evaluation does not match authoritative Work evidence")
	}
	return nil
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
	if evidenceEvent.EventType != "WORK_COMPLETION_EVALUATED" || evidenceEvent.OrganizationID != binding.OrganizationID || evidenceEvent.SourceActorID != "runtime" || evidenceEvent.SourceExecutionID != "" || evidenceEvent.RecipientScope != "" || evidenceEvent.RecipientID != "" || evidenceEvent.TaskID != "" || len(evidenceEvent.AuthorizationRefs) != 0 || evidenceEvent.CorrelationID != binding.CorrelationID || evidenceEvent.SchemaVersion != SchemaVersion || !slices.Equal(evidenceEvent.ArtifactRefs, evidence.ArtifactRefs) {
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

// ValidateTaskCompletionEvidenceChain proves that one terminal Task projection
// is the result of the exact runtime-owned completion decision and immutable
// evidence that precede it. A completed status is not evidence by itself.
func ValidateTaskCompletionEvidenceChain(binding WorkCompletionBinding, task WorkCompletionTaskBinding, completionEvent Event, stream []Event) (CompletionDecisionPayload, error) {
	if binding.OrganizationID == "" || binding.CorrelationID == "" || binding.Work.ID == "" || binding.Work.Status != core.WorkActive || binding.Intent.ID == "" ||
		binding.Intent.ID != binding.Work.IntentID || binding.Intent.NormalizedObjective != binding.Work.Objective || string(binding.Intent.OrganizationID) != binding.OrganizationID ||
		task.Task.ID == "" || task.Task.WorkID != binding.Work.ID || task.Task.Status != core.TaskCompleted || task.Version < 2 || task.CorrelationID != binding.CorrelationID {
		return CompletionDecisionPayload{}, fmt.Errorf("task completion binding is invalid")
	}
	payload, present, err := AdmittedProjection(completionEvent)
	if err != nil || !present {
		return CompletionDecisionPayload{}, fmt.Errorf("task completion transition is not admitted")
	}
	var projected core.Task
	var decision CompletionDecisionPayload
	if completionEvent.EventType != "TASK_VERIFIED_COMPLETE" || completionEvent.OrganizationID != binding.OrganizationID || completionEvent.SourceActorID != "runtime" || completionEvent.SourceExecutionID != "" || completionEvent.RecipientScope != "" || completionEvent.RecipientID != "" || completionEvent.TaskID != string(task.Task.ID) || len(completionEvent.AuthorizationRefs) != 0 || len(completionEvent.ArtifactRefs) != 0 || completionEvent.CorrelationID != binding.CorrelationID || completionEvent.SchemaVersion != SchemaVersion ||
		payload.Projection.ProjectionKind != "task" || payload.Projection.RecordID != string(task.Task.ID) || payload.Projection.Version != task.Version || payload.Projection.CorrelationID != binding.CorrelationID ||
		decodeExactEventJSON(payload.Projection.Value, &projected) != nil || !reflect.DeepEqual(projected, task.Task) || decodeExactEventJSON(payload.Detail, &decision) != nil || decision.Contract.TaskID != task.Task.ID || decision.Contract.TaskVersion < 1 || decision.Contract.TaskVersion >= task.Version || decision.OutcomeEventRef == "" || !decision.Result.Complete || len(decision.Result.Reasons) != 0 {
		return CompletionDecisionPayload{}, fmt.Errorf("task completion transition is invalid")
	}
	var verification Event
	for _, event := range stream {
		if event.EventType != "COMPLETION_VERIFIED" || event.TaskID != string(task.Task.ID) || event.CorrelationID != binding.CorrelationID || event.Sequence >= completionEvent.Sequence {
			continue
		}
		var candidate CompletionDecisionPayload
		if event.OrganizationID != binding.OrganizationID || event.SourceActorID != "runtime" || event.RecipientScope != "" || event.RecipientID != "" || len(event.AuthorizationRefs) != 0 || event.SchemaVersion != SchemaVersion || decodeExactEventJSON(event.Payload, &candidate) != nil || !reflect.DeepEqual(candidate, decision) {
			continue
		}
		if verification.EventID != "" {
			return CompletionDecisionPayload{}, fmt.Errorf("task completion has multiple matching verification decisions")
		}
		verification = event
	}
	if verification.EventID == "" {
		return CompletionDecisionPayload{}, fmt.Errorf("task completion lacks its exact verification decision")
	}
	outcomeEvent, found := eventWithID(stream, decision.OutcomeEventRef)
	var outcome core.ToolOutcome
	if !found || outcomeEvent.EventType != "TOOL_OUTCOME_RECORDED" || outcomeEvent.OrganizationID != binding.OrganizationID || outcomeEvent.SourceActorID != "runtime" || outcomeEvent.SourceExecutionID == "" || outcomeEvent.RecipientScope != "" || outcomeEvent.RecipientID != "" || outcomeEvent.TaskID != string(task.Task.ID) || len(outcomeEvent.AuthorizationRefs) != 0 || outcomeEvent.CorrelationID != binding.CorrelationID || outcomeEvent.Sequence >= verification.Sequence || outcomeEvent.SchemaVersion != SchemaVersion ||
		decodeExactEventJSON(outcomeEvent.Payload, &outcome) != nil || outcome.ToolInvocationID == "" || outcome.ToolID == "" || outcome.StartedAt.IsZero() || outcome.FinishedAt.Before(outcome.StartedAt) || !slices.Equal(outcomeEvent.ArtifactRefs, outcome.ArtifactRefs) || !slices.Equal(verification.ArtifactRefs, outcome.ArtifactRefs) || verification.SourceExecutionID != "" && verification.SourceExecutionID != outcomeEvent.SourceExecutionID {
		return CompletionDecisionPayload{}, fmt.Errorf("task completion outcome evidence is invalid")
	}
	expected, err := completionDecisionResult(binding, task, decision, outcome, outcomeEvent, verification, stream)
	if err != nil {
		return CompletionDecisionPayload{}, err
	}
	if !reflect.DeepEqual(expected, decision.Result) {
		return CompletionDecisionPayload{}, fmt.Errorf("task completion decision does not match its durable evidence")
	}
	if _, _, err := ResolveVerifiedTaskResult(binding.OrganizationID, binding.CorrelationID, task.Task, task.Version, stream, 0); err != nil {
		return CompletionDecisionPayload{}, err
	}
	return decision, nil
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
		len(manifest.KnowledgeRefs) != 0 || len(manifest.SkillRefs) != 0 || len(manifest.ToolDefinitions) != 0 || len(manifest.ArtifactRefs) != 0 || !validSHA256(manifest.ExecutionInputSHA256) {
		return executionModel{}, fmt.Errorf("work completion Agent manifest does not match its immutable Task")
	}
	expectedInput, err := expectedAgentExecutionInput(binding, task, startEvent, found, manifest, stream)
	if err != nil {
		return executionModel{}, fmt.Errorf("work completion Agent manifest context is invalid: %w", err)
	}
	if manifest.ExecutionInputSHA256 != core.FingerprintExecutionInput(expectedInput) {
		return executionModel{}, fmt.Errorf("work completion Agent manifest input does not match durable execution context")
	}
	return executionModel{Provider: manifest.Provider, Model: manifest.Model, ExecutionInputSHA256: manifest.ExecutionInputSHA256}, nil
}

func expectedAgentExecutionInput(binding WorkCompletionBinding, task core.Task, startEvent, manifestEvent Event, manifest core.ExecutionContextManifest, stream []Event) (string, error) {
	blueprint, err := executionBlueprint(binding, task)
	if err != nil {
		return "", err
	}
	strategy, strategyEventRefs, strategyContextRefs, err := executionStrategicContext(binding, startEvent, stream)
	if err != nil {
		return "", err
	}
	if !slices.Equal(manifest.AdditionalContextRefs, strategyContextRefs) {
		return "", fmt.Errorf("execution manifest does not bind the planned strategic context")
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
	expectedRefs := append([]string(nil), strategyEventRefs...)
	expectedRefs = append(expectedRefs, inboxRefs...)
	expectedRefs = append(expectedRefs, dependencyRefs...)
	if revisionRef != "" {
		expectedRefs = append(expectedRefs, revisionRef)
	}
	if !slices.Equal(manifest.EventRefs, expectedRefs) {
		return "", fmt.Errorf("execution context references do not match durable runtime selection")
	}
	_, input, err := core.MaterializeAgentExecutionInput(core.AgentExecutionInputContext{
		Blueprint: blueprint, Task: task, Strategy: strategy, DependencyResults: dependencies, InboxEvents: inbox, Revision: revision,
	})
	return input, err
}

func executionStrategicContext(binding WorkCompletionBinding, startEvent Event, stream []Event) (*core.StrategicContext, []string, []core.VersionedRef, error) {
	startDetail, err := executionStartDetail(startEvent)
	if err != nil {
		return nil, nil, nil, err
	}
	plan, plannedStrategy, err := ResolvePlanStrategicContext(binding.OrganizationID, binding.CorrelationID, binding.Work, binding.Intent, stream)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("resolve execution Plan strategic context: %w", err)
	}
	if !slices.Equal(plan.StrategicEventRefs, startDetail.StrategicEventRefs) || !slices.Equal(plan.StrategicContextRefs, startDetail.StrategicContextRefs) {
		return nil, nil, nil, fmt.Errorf("execution start does not bind the planned strategic context")
	}
	strategy, err := ResolveStrategicContextByRefs(binding.OrganizationID, binding.Work, stream, startDetail.StrategicEventRefs, startDetail.StrategicContextRefs)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("resolve execution-start strategic context: %w", err)
	}
	if !reflect.DeepEqual(strategy, plannedStrategy) {
		return nil, nil, nil, fmt.Errorf("execution strategic context does not match its durable Plan")
	}
	return strategy, append([]string(nil), startDetail.StrategicEventRefs...), append([]core.VersionedRef(nil), startDetail.StrategicContextRefs...), nil
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
		payload, present, admissionErr := AdmittedProjection(event)
		var projected core.Task
		var candidate CompletionDecisionPayload
		if admissionErr != nil || !present || event.OrganizationID != organizationID || event.SourceActorID != "runtime" || event.SourceExecutionID != "" || event.RecipientScope != "" || event.RecipientID != "" || len(event.AuthorizationRefs) != 0 || len(event.ArtifactRefs) != 0 || event.SchemaVersion != SchemaVersion ||
			payload.Projection.ProjectionKind != "task" || payload.Projection.RecordID != string(task.ID) || payload.Projection.Version != taskVersion || payload.Projection.CorrelationID != correlationID ||
			decodeExactEventJSON(payload.Projection.Value, &projected) != nil || !reflect.DeepEqual(projected, task) || decodeExactEventJSON(payload.Detail, &candidate) != nil || !candidate.Result.Complete || len(candidate.Result.Reasons) != 0 || candidate.OutcomeEventRef == "" || candidate.Contract.TaskID != task.ID || candidate.Contract.TaskVersion < 1 || candidate.Contract.TaskVersion >= taskVersion {
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
		if event.EventID == "" || event.Sequence < 1 || event.CreatedAt.IsZero() || event.SchemaVersion != SchemaVersion || event.OrganizationID != organizationID || event.SourceActorID != "runtime" || event.RecipientScope != "" || event.RecipientID != "" || len(event.AuthorizationRefs) != 0 || decodeExactEventJSON(event.Payload, &candidate) != nil || !reflect.DeepEqual(candidate, decision) {
			return Event{}, ResultPublishedPayload{}, fmt.Errorf("verified Task result completion verification is invalid")
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
	if !found || outcomeEvent.EventID == "" || outcomeEvent.Sequence < 1 || outcomeEvent.CreatedAt.IsZero() || outcomeEvent.SchemaVersion != SchemaVersion || outcomeEvent.EventType != "TOOL_OUTCOME_RECORDED" || outcomeEvent.OrganizationID != organizationID || outcomeEvent.SourceActorID != "runtime" || outcomeEvent.SourceExecutionID == "" || outcomeEvent.RecipientScope != "" || outcomeEvent.RecipientID != "" || outcomeEvent.TaskID != string(task.ID) || len(outcomeEvent.AuthorizationRefs) != 0 || outcomeEvent.CorrelationID != correlationID || outcomeEvent.Sequence >= verification.Sequence ||
		decodeExactEventJSON(outcomeEvent.Payload, &outcome) != nil || outcome.ToolInvocationID == "" || outcome.ToolID == "" || outcome.StartedAt.IsZero() || outcome.FinishedAt.Before(outcome.StartedAt) || !slices.Equal(outcomeEvent.ArtifactRefs, outcome.ArtifactRefs) || !slices.Equal(verification.ArtifactRefs, outcome.ArtifactRefs) || verification.SourceExecutionID != "" && verification.SourceExecutionID != outcomeEvent.SourceExecutionID {
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
		if event.EventID == "" || event.CreatedAt.IsZero() || event.SchemaVersion != SchemaVersion || event.OrganizationID != organizationID || event.SourceActorID != expectedActorID || event.SourceExecutionID != outcomeEvent.SourceExecutionID || event.RecipientScope != "" || event.RecipientID != "" || len(event.AuthorizationRefs) != 0 || decodeExactEventJSON(event.Payload, &candidate) != nil || !candidate.ValidFor(event.ArtifactRefs) || candidate.Summary != expectedSummary || !slices.Equal(candidate.ArtifactRefs, outcome.ArtifactRefs) {
			return Event{}, ResultPublishedPayload{}, fmt.Errorf("verified Task result publication is invalid")
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
		if event.EventID == "" || event.CreatedAt.IsZero() || event.SchemaVersion != SchemaVersion || event.OrganizationID != organizationID || event.SourceActorID != expectedActorID || event.SourceExecutionID != outcomeEvent.SourceExecutionID || event.RecipientScope != "" || event.RecipientID != "" || len(event.AuthorizationRefs) != 0 || decodeExactEventJSON(event.Payload, &candidate) != nil || candidate.ToolInvocationID != string(outcome.ToolInvocationID) || candidate.ResultEventID != resultEvent.EventID || !slices.Equal(candidate.ArtifactRefs, outcome.ArtifactRefs) || !slices.Equal(event.ArtifactRefs, outcome.ArtifactRefs) {
			return Event{}, ResultPublishedPayload{}, fmt.Errorf("verified Task result completion candidate is invalid")
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
	if event.OrganizationID != binding.OrganizationID || event.TaskID != string(taskID) || event.CorrelationID != binding.CorrelationID {
		return false
	}
	_, err := DecodeDurableOperatorInput(event)
	return err == nil
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
		if err := ValidateTaskExecutionStart(event, projected, version, binding.Work, binding.Intent, stream); err != nil {
			return Event{}, false, err
		}
		if task.ExecutionKind == core.ExecutionAgent {
			detail, _ := executionStartDetail(event)
			remediation = detail.Mode == "BLOCKED_DEPENDENCY_REMEDIATION"
		}
		found = event
	}
	if found.EventID == "" {
		return Event{}, false, fmt.Errorf("work completion lacks its exact execution-start version")
	}
	return found, remediation, nil
}

// ValidateTaskExecutionStart replays the complete typed execution-start
// boundary. Strategic references are coordination context only, but every
// execution kind must bind the exact Plan revisions that admission accepted.
func ValidateTaskExecutionStart(start Event, task core.Task, taskVersion int, work core.Work, intent core.Intent, stream []Event) error {
	if start.OrganizationID == "" || start.SourceActorID != "runtime" || start.SourceExecutionID != "" || start.RecipientScope != "" || start.RecipientID != "" || start.TaskID != string(task.ID) || start.CorrelationID == "" || taskVersion < 2 || task.Status != core.TaskRunning || task.WorkID != work.ID || work.IntentID != intent.ID || intent.OrganizationID != core.ID(start.OrganizationID) {
		return fmt.Errorf("execution start crosses its durable Task boundary")
	}
	var detail ExecutionStartDetail
	var err error
	switch task.ExecutionKind {
	case core.ExecutionAgent:
		detail, err = executionStartDetail(start)
		if err == nil {
			err = ValidateAgentDispatchStart(start, task, taskVersion, stream)
		}
	case core.ExecutionDeterministic, core.ExecutionHuman:
		detail, err = nonAgentExecutionStartDetail(start, task.ExecutionKind)
	default:
		err = fmt.Errorf("execution start uses an unavailable execution kind")
	}
	if err != nil {
		return err
	}
	plan, _, err := ResolvePlanStrategicContext(start.OrganizationID, start.CorrelationID, work, intent, stream)
	if err != nil {
		return fmt.Errorf("resolve execution-start Plan context: %w", err)
	}
	if !slices.Equal(plan.StrategicEventRefs, detail.StrategicEventRefs) || !slices.Equal(plan.StrategicContextRefs, detail.StrategicContextRefs) {
		return fmt.Errorf("execution start does not bind its durable Plan context")
	}
	if task.ExecutionKind == core.ExecutionHuman {
		inputEvent, found := eventWithID(stream, detail.InputEventRef)
		if !found || inputEvent.Sequence >= start.Sequence || inputEvent.OrganizationID != start.OrganizationID || inputEvent.TaskID != start.TaskID || inputEvent.CorrelationID != start.CorrelationID {
			return fmt.Errorf("user execution start lacks its exact prior input event")
		}
		if detail.Mode == "OPERATOR_HUMAN_INPUT" {
			_, err = DecodeDurableOperatorInput(inputEvent)
		} else {
			_, err = DecodeHumanTaskCompletion(inputEvent)
		}
		if err != nil {
			return fmt.Errorf("user execution-start input event is invalid: %w", err)
		}
	}
	return nil
}

func executionStartDetail(event Event) (ExecutionStartDetail, error) {
	var payload ProjectionEventPayload
	var detail ExecutionStartDetail
	var fields map[string]json.RawMessage
	if event.EventType != "EXECUTION_STARTED" || json.Unmarshal(event.Payload, &payload) != nil || decodeExactEventJSON(payload.Detail, &detail) != nil || json.Unmarshal(payload.Detail, &fields) != nil || fields["inbox_cutoff_sequence"] == nil || fields["dispatch_binding"] == nil || detail.DispatchBinding == nil || detail.InboxCutoffSequence < 0 || detail.InboxCutoffSequence >= event.Sequence || detail.Mode != "" && detail.Mode != "BLOCKED_DEPENDENCY_REMEDIATION" || !validStrategicStartRefs(detail.StrategicEventRefs, detail.StrategicContextRefs) {
		return ExecutionStartDetail{}, fmt.Errorf("agent execution-start detail is invalid")
	}
	expectedFields := 2
	if detail.Mode != "" {
		expectedFields++
	}
	if len(detail.StrategicEventRefs) != 0 {
		expectedFields += 2
	}
	if len(fields) != expectedFields {
		return ExecutionStartDetail{}, fmt.Errorf("agent execution-start detail contains an unknown field")
	}
	return detail, nil
}

func nonAgentExecutionStartDetail(event Event, kind core.ExecutionKind) (ExecutionStartDetail, error) {
	var payload ProjectionEventPayload
	var detail ExecutionStartDetail
	var fields map[string]json.RawMessage
	if event.EventType != "EXECUTION_STARTED" || json.Unmarshal(event.Payload, &payload) != nil || decodeExactEventJSON(payload.Detail, &detail) != nil || json.Unmarshal(payload.Detail, &fields) != nil || fields["inbox_cutoff_sequence"] == nil || detail.InboxCutoffSequence != 0 || detail.DispatchBinding != nil || !validStrategicStartRefs(detail.StrategicEventRefs, detail.StrategicContextRefs) {
		return ExecutionStartDetail{}, fmt.Errorf("non-Agent execution-start detail is invalid")
	}
	expectedFields := 1
	switch kind {
	case core.ExecutionDeterministic:
		if detail.Mode != "" || detail.InputEventRef != "" {
			return ExecutionStartDetail{}, fmt.Errorf("deterministic execution-start detail is invalid")
		}
	case core.ExecutionHuman:
		if fields["mode"] == nil || fields["input_event_ref"] == nil || detail.InputEventRef == "" || detail.Mode != "OPERATOR_HUMAN_INPUT" && detail.Mode != "STRUCTURED_HUMAN_COMPLETION" {
			return ExecutionStartDetail{}, fmt.Errorf("user execution-start detail is invalid")
		}
		expectedFields += 2
	default:
		return ExecutionStartDetail{}, fmt.Errorf("non-Agent execution-start kind is invalid")
	}
	if len(detail.StrategicEventRefs) != 0 {
		expectedFields += 2
	}
	if len(fields) != expectedFields {
		return ExecutionStartDetail{}, fmt.Errorf("non-Agent execution-start detail contains an unknown field")
	}
	return detail, nil
}

func validStrategicStartRefs(eventRefs []string, contextRefs []core.VersionedRef) bool {
	if len(eventRefs) == 0 && len(contextRefs) == 0 {
		return true
	}
	return len(eventRefs) == 2 && eventRefs[0] != "" && eventRefs[1] != "" && eventRefs[0] != eventRefs[1] &&
		len(contextRefs) == 2 && contextRefs[0].ID != "" && contextRefs[0].Version != "" && contextRefs[0].MaterializationState == core.MaterializedFull &&
		contextRefs[1].ID != "" && contextRefs[1].Version != "" && contextRefs[1].MaterializationState == core.MaterializedFull
}

// ValidateAgentDispatchStart proves that an execution started against the
// exact active Agent, blueprint, and execution-profile revisions named in its
// immutable Task. The start transition consumes the pending Task revision, so
// this admission cannot be reused for a second invocation.
func ValidateAgentDispatchStart(start Event, task core.Task, taskVersion int, stream []Event) error {
	detail, err := executionStartDetail(start)
	if err != nil {
		return err
	}
	binding := detail.DispatchBinding
	if start.OrganizationID == "" || start.SourceActorID != "runtime" || start.SourceExecutionID != "" || start.RecipientScope != "" || start.RecipientID != "" || start.TaskID != string(task.ID) || start.CorrelationID == "" || taskVersion < 2 || task.ExecutionKind != core.ExecutionAgent || task.Status != core.TaskRunning || task.AssigneeType != "AGENT" || task.AssigneeID == "" || task.AgentConfig == nil {
		return fmt.Errorf("agent dispatch start crosses its runtime-owned Task boundary")
	}
	agentRecord, agent, err := dispatchAgentRevision(start, binding.AgentEventRef, binding.AgentID, binding.AgentRecordVersion, stream)
	if err != nil {
		return err
	}
	blueprintRecord, blueprint, err := dispatchBlueprintRevision(start, binding.BlueprintEventRef, binding.BlueprintID, binding.BlueprintRecordVersion, stream)
	if err != nil {
		return err
	}
	profileRecord, profile, err := dispatchProfileRevision(start, binding.ExecutionProfileEventRef, binding.ExecutionProfileID, binding.ExecutionProfileRecordVersion, stream)
	if err != nil {
		return err
	}
	config := task.AgentConfig
	if !core.ValidAgent(agent) || agent.Status != "ACTIVE" || blueprint.Status != "ACTIVE" || profile.Status != "ACTIVE" || agent.ID != task.AssigneeID || agent.OrganizationID != core.ID(start.OrganizationID) ||
		blueprint.ID != config.BlueprintID || blueprint.OrganizationID != agent.OrganizationID || blueprint.Version != config.BlueprintVersion || profile.ID != config.ProfileID || profile.OrganizationID != agent.OrganizationID || profile.Version != config.ProfileVersion {
		return fmt.Errorf("agent dispatch binding does not match an active exact Task assignment")
	}
	expected := AgentDispatchBinding{
		DispatchID:                    core.ID(fmt.Sprintf("execution-%s-v%d", task.ID, taskVersion)),
		OrganizationID:                core.ID(start.OrganizationID),
		TaskID:                        task.ID,
		TaskVersion:                   taskVersion,
		AgentID:                       agent.ID,
		AgentRecordVersion:            agentRecord.Version,
		AgentEventRef:                 binding.AgentEventRef,
		BlueprintID:                   blueprint.ID,
		BlueprintRecordVersion:        blueprintRecord.Version,
		BlueprintVersion:              blueprint.Version,
		BlueprintEventRef:             binding.BlueprintEventRef,
		ExecutionProfileID:            profile.ID,
		ExecutionProfileRecordVersion: profileRecord.Version,
		ExecutionProfileVersion:       profile.Version,
		ExecutionProfileEventRef:      binding.ExecutionProfileEventRef,
		RuntimeAdapter:                config.RuntimeAdapter,
	}
	if !reflect.DeepEqual(*binding, expected) {
		return fmt.Errorf("agent dispatch binding does not match its admitted roster revisions")
	}
	return nil
}

func dispatchAgentRevision(start Event, eventRef string, id core.ID, version int, stream []Event) (ProjectionRecord, core.Agent, error) {
	record, err := dispatchProjectionRevision(start, eventRef, "agent", id, version, stream)
	if err != nil {
		return ProjectionRecord{}, core.Agent{}, err
	}
	var value core.Agent
	if decodeExactEventJSON(record.Value, &value) != nil || value.ID != id {
		return ProjectionRecord{}, core.Agent{}, fmt.Errorf("agent dispatch references an invalid Agent revision")
	}
	return record, value, nil
}

func dispatchBlueprintRevision(start Event, eventRef string, id core.ID, version int, stream []Event) (ProjectionRecord, core.AgentBlueprint, error) {
	record, err := dispatchProjectionRevision(start, eventRef, "agent_blueprint", id, version, stream)
	if err != nil {
		return ProjectionRecord{}, core.AgentBlueprint{}, err
	}
	var value core.AgentBlueprint
	if decodeExactEventJSON(record.Value, &value) != nil || value.ID != id {
		return ProjectionRecord{}, core.AgentBlueprint{}, fmt.Errorf("agent dispatch references an invalid blueprint revision")
	}
	return record, value, nil
}

func dispatchProfileRevision(start Event, eventRef string, id core.ID, version int, stream []Event) (ProjectionRecord, core.ExecutionProfile, error) {
	record, err := dispatchProjectionRevision(start, eventRef, "execution_profile", id, version, stream)
	if err != nil {
		return ProjectionRecord{}, core.ExecutionProfile{}, err
	}
	var value core.ExecutionProfile
	if decodeExactEventJSON(record.Value, &value) != nil || value.ID != id {
		return ProjectionRecord{}, core.ExecutionProfile{}, fmt.Errorf("agent dispatch references an invalid execution-profile revision")
	}
	return record, value, nil
}

func dispatchProjectionRevision(start Event, eventRef, kind string, id core.ID, version int, stream []Event) (ProjectionRecord, error) {
	if eventRef == "" || id == "" || version < 1 {
		return ProjectionRecord{}, fmt.Errorf("agent dispatch roster reference is incomplete")
	}
	var found Event
	for _, candidate := range stream {
		if candidate.EventID != eventRef {
			continue
		}
		if found.EventID != "" {
			return ProjectionRecord{}, fmt.Errorf("agent dispatch roster reference is duplicated")
		}
		found = candidate
	}
	if found.EventID == "" || found.Sequence >= start.Sequence || found.OrganizationID != start.OrganizationID {
		return ProjectionRecord{}, fmt.Errorf("agent dispatch roster reference is outside its admission boundary")
	}
	payload, present, err := AdmittedProjection(found)
	if err != nil || !present || ValidateProjectionEventBoundary(found, payload) != nil || payload.Projection.ProjectionKind != kind || payload.Projection.RecordID != string(id) || payload.Projection.Version != version {
		return ProjectionRecord{}, fmt.Errorf("agent dispatch roster reference lacks exact projection admission")
	}
	for _, candidate := range stream {
		if candidate.Sequence <= found.Sequence || candidate.Sequence >= start.Sequence {
			continue
		}
		candidatePayload, candidatePresent, candidateErr := AdmittedProjection(candidate)
		if candidateErr != nil {
			return ProjectionRecord{}, fmt.Errorf("agent dispatch roster history is invalid")
		}
		if candidatePresent && candidatePayload.Projection.ProjectionKind == kind && candidatePayload.Projection.RecordID == string(id) {
			return ProjectionRecord{}, fmt.Errorf("agent dispatch references a superseded roster revision")
		}
	}
	return payload.Projection, nil
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
	PlanID                  string              `json:"plan_id"`
	IntentID                string              `json:"intent_id"`
	IntentFingerprint       string              `json:"intent_fingerprint"`
	PromptVersion           string              `json:"prompt_version"`
	Provider                string              `json:"provider"`
	Model                   string              `json:"model"`
	ExecutionProfileVersion string              `json:"execution_profile_version"`
	InputEventRefs          []string            `json:"input_event_refs"`
	StrategicContextRefs    []core.VersionedRef `json:"strategic_context_refs,omitempty"`
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

// InferencePolicyActivatedPayload records the exact reviewed organization
// budget admitted by the runtime. Limits are non-secret and the fingerprint
// binds subsequent reservations to this immutable policy revision.
type InferencePolicyActivatedPayload struct {
	PolicyFingerprint       string    `json:"policy_fingerprint"`
	Provider                string    `json:"provider"`
	Model                   string    `json:"model"`
	ExecutionProfileVersion string    `json:"execution_profile_version"`
	AccessMode              string    `json:"access_mode"`
	AuthorizedBy            string    `json:"authorized_by"`
	AuthorizedAt            time.Time `json:"authorized_at"`
	AuthorizationExpiresAt  time.Time `json:"authorization_expires_at"`
}

type InferenceReservedPayload struct {
	ReservationID           string    `json:"reservation_id"`
	RequestID               string    `json:"request_id"`
	Purpose                 string    `json:"purpose"`
	IntentID                string    `json:"intent_id,omitempty"`
	PolicyFingerprint       string    `json:"policy_fingerprint"`
	PromptSHA256            string    `json:"prompt_sha256"`
	Provider                string    `json:"provider"`
	Model                   string    `json:"model"`
	ExecutionProfileVersion string    `json:"execution_profile_version"`
	ReservedInputTokens     int64     `json:"reserved_input_tokens"`
	ReservedOutputTokens    int64     `json:"reserved_output_tokens"`
	ReservedCostNanoUSD     int64     `json:"reserved_cost_nano_usd"`
	WindowStartedAt         time.Time `json:"window_started_at"`
	WindowExpiresAt         time.Time `json:"window_expires_at"`
}

type InferenceReconciledPayload struct {
	ReservationID       string `json:"reservation_id"`
	State               string `json:"state"`
	ChargedInputTokens  int64  `json:"charged_input_tokens"`
	ChargedOutputTokens int64  `json:"charged_output_tokens"`
	ChargedCostNanoUSD  int64  `json:"charged_cost_nano_usd"`
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

const ProjectionAdmissionMethod = "EVENT_COUPLED_PROJECTION_V1"

// ProjectionAdmission proves that a projection payload was created by the
// typed ledger admission path for this exact event envelope and identity.
// Generic events reserve this contract and cannot mint it.
type ProjectionAdmission struct {
	Method      string `json:"method"`
	EventRef    string `json:"event_ref"`
	Fingerprint string `json:"fingerprint"`
}

// ProjectionEventPayload preserves transition detail while carrying the
// complete versioned record and sealed admission needed for deterministic
// replay. Presence of either top-level field is reserved to typed projection
// admission even when the payload is malformed.
type ProjectionEventPayload struct {
	Projection ProjectionRecord    `json:"projection"`
	Admission  ProjectionAdmission `json:"admission"`
	Detail     json.RawMessage     `json:"detail,omitempty"`
}

type projectionAdmissionFingerprintPayload struct {
	Method            string           `json:"method"`
	EventRef          string           `json:"event_ref"`
	Sequence          int64            `json:"sequence"`
	OrganizationID    string           `json:"organization_id"`
	EventType         string           `json:"event_type"`
	SourceActorID     string           `json:"source_actor_id"`
	SourceExecutionID string           `json:"source_execution_id"`
	RecipientScope    string           `json:"recipient_scope"`
	RecipientID       string           `json:"recipient_id"`
	TaskID            string           `json:"task_id"`
	AuthorizationRefs []string         `json:"authorization_refs"`
	ArtifactRefs      []string         `json:"artifact_refs"`
	CorrelationID     string           `json:"correlation_id"`
	CreatedAt         string           `json:"created_at"`
	SchemaVersion     int              `json:"schema_version"`
	Projection        ProjectionRecord `json:"projection"`
	Detail            json.RawMessage  `json:"detail,omitempty"`
}

func SealProjectionEvent(event Event, record ProjectionRecord, detail json.RawMessage) (ProjectionEventPayload, error) {
	if event.EventID == "" || event.Sequence < 1 || event.CreatedAt.IsZero() || event.SchemaVersion != SchemaVersion || event.EventType == "" || event.OrganizationID == "" || record.ProjectionKind == "" || record.RecordID == "" || record.Version < 1 || record.CorrelationID != event.CorrelationID || len(record.Value) == 0 {
		return ProjectionEventPayload{}, fmt.Errorf("complete projection admission identity is required")
	}
	admission := ProjectionAdmission{Method: ProjectionAdmissionMethod, EventRef: event.EventID}
	fingerprint, err := projectionAdmissionFingerprint(admission, event, record, detail)
	if err != nil {
		return ProjectionEventPayload{}, err
	}
	admission.Fingerprint = fingerprint
	return ProjectionEventPayload{Projection: record, Admission: admission, Detail: detail}, nil
}

// AdmittedProjection returns present=false only for an ordinary event. Any
// event that contains a reserved projection/admission key must carry one exact
// valid sealed contract or fail closed.
func AdmittedProjection(event Event) (ProjectionEventPayload, bool, error) {
	return admittedProjectionAtSchema(event, SchemaVersion)
}

// ResealProjectionEventForMigration validates one existing projection
// admission at its exact source Event Contract boundary and deterministically
// reseals it for a different schema. Storage migrations and their fixtures are
// its only intended callers.
func ResealProjectionEventForMigration(event Event, sourceSchemaVersion, targetSchemaVersion int) (ProjectionEventPayload, bool, error) {
	if sourceSchemaVersion < 1 || targetSchemaVersion < 1 || sourceSchemaVersion == targetSchemaVersion || event.SchemaVersion != sourceSchemaVersion || sourceSchemaVersion != SchemaVersion && targetSchemaVersion != SchemaVersion {
		return ProjectionEventPayload{}, false, fmt.Errorf("projection migration boundary is invalid")
	}
	payload, present, err := admittedProjectionAtSchema(event, sourceSchemaVersion)
	if err != nil || !present {
		return payload, present, err
	}
	event.SchemaVersion = targetSchemaVersion
	fingerprint, err := projectionAdmissionFingerprint(payload.Admission, event, payload.Projection, payload.Detail)
	if err != nil {
		return ProjectionEventPayload{}, true, err
	}
	payload.Admission.Fingerprint = fingerprint
	return payload, true, nil
}

func admittedProjectionAtSchema(event Event, expectedSchemaVersion int) (ProjectionEventPayload, bool, error) {
	if rejectDuplicateJSONKeys(event.Payload) != nil {
		return ProjectionEventPayload{}, false, fmt.Errorf("event payload is malformed")
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(event.Payload, &object) != nil || object == nil {
		return ProjectionEventPayload{}, false, fmt.Errorf("event payload is malformed")
	}
	_, hasProjection := object["projection"]
	_, hasAdmission := object["admission"]
	if !hasProjection && !hasAdmission {
		return ProjectionEventPayload{}, false, nil
	}
	var payload ProjectionEventPayload
	decoder := json.NewDecoder(bytes.NewReader(event.Payload))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&payload) != nil || decoder.Decode(&struct{}{}) != io.EOF || event.EventID == "" || event.Sequence < 1 || event.CreatedAt.IsZero() || payload.Projection.ProjectionKind == "" || payload.Projection.RecordID == "" || payload.Projection.Version < 1 || payload.Projection.CorrelationID != event.CorrelationID || len(payload.Projection.Value) == 0 || payload.Admission.Method != ProjectionAdmissionMethod || payload.Admission.EventRef != event.EventID || !validSHA256(payload.Admission.Fingerprint) {
		return ProjectionEventPayload{}, true, fmt.Errorf("projection event admission is malformed")
	}
	want, fingerprintErr := projectionAdmissionFingerprint(payload.Admission, event, payload.Projection, payload.Detail)
	if fingerprintErr != nil || want != payload.Admission.Fingerprint || event.SchemaVersion != expectedSchemaVersion {
		return ProjectionEventPayload{}, true, fmt.Errorf("projection event admission does not match its event boundary")
	}
	return payload, true, nil
}

func rejectDuplicateJSONKeys(body []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := validateUniqueJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return fmt.Errorf("unexpected trailing JSON")
	}
	return nil
}

func validateUniqueJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, compound := token.(json.Delim)
	if !compound {
		return nil
	}
	switch delimiter {
	case '{':
		seen := map[string]struct{}{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("JSON object key is invalid")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("JSON object contains duplicate key %q", key)
			}
			seen[key] = struct{}{}
			if err := validateUniqueJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return fmt.Errorf("JSON object is incomplete")
		}
	case '[':
		for decoder.More() {
			if err := validateUniqueJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return fmt.Errorf("JSON array is incomplete")
		}
	default:
		return fmt.Errorf("unsupported JSON delimiter")
	}
	return nil
}

// RequiresProjectionAdmission identifies runtime event labels that are owned
// by the typed projection writer. TASK_BLOCKED remains agent-proposable when
// the authenticated source is an Agent execution, but runtime lifecycle state
// always requires the sealed event/record transaction.
func RequiresProjectionAdmission(eventType, sourceActorID string) bool {
	switch eventType {
	case "ORGANIZATION_CREATED",
		"MISSION_CREATED", "MISSION_REVISED", "MISSION_RETIRED",
		"GOAL_CREATED", "GOAL_REFINED", "GOAL_PAUSED", "GOAL_RESUMED", "GOAL_RETIRED", "GOAL_ACHIEVED",
		"TEAM_CREATED", "TEAM_REVISED",
		"AGENT_BLUEPRINT_CREATED", "AGENT_BLUEPRINT_UPDATED",
		"EXECUTION_PROFILE_CREATED", "EXECUTION_PROFILE_UPDATED",
		"AGENT_CREATED", "AGENT_CONFIGURATION_UPDATED", "AGENT_DEACTIVATED", "AGENT_REACTIVATED",
		"INTENT_CREATED",
		"WORK_CREATED", "WORK_COMPLETED", "WORK_FAILED", "WORK_PLANNING_FAILED",
		"LAB_EXPERIMENT_STARTED", "LAB_EXPERIMENT_COMPLETED", "LAB_EXPERIMENT_FAILED", "LAB_PROMOTION_CANDIDATE_CREATED",
		"TASK_CREATED", "TASK_ASSIGNMENT_REVALIDATED", "TASK_RECOVERED", "TASK_RESUMED", "EXECUTION_STARTED", "TASK_VERIFIED_COMPLETE", "COMPLETION_REJECTED", "TASK_DEPENDENCY_FAILED", "TASK_REMEDIATION_FAILED", "TASK_WORK_FAILED":
		return true
	case "TASK_BLOCKED":
		return sourceActorID == "runtime"
	default:
		return false
	}
}

// ProjectionKindRequiresAdmission identifies the closed set of organizational
// projections carried by the current Event Contract schema.
func ProjectionKindRequiresAdmission(kind string) bool {
	switch kind {
	case "organization", "mission", "goal", "team", "agent_blueprint", "execution_profile", "agent", "intent", "work", "task", "lab_experiment", "lab_promotion_candidate":
		return true
	default:
		return false
	}
}

// ValidateProjectionEventBoundary proves that a sealed projection uses the
// runtime-owned label and routing envelope reserved for its kind and version.
func ValidateProjectionEventBoundary(event Event, payload ProjectionEventPayload) error {
	record := payload.Projection
	if event.OrganizationID == "" || event.SourceActorID != "runtime" || event.SourceExecutionID != "" || len(event.AuthorizationRefs) != 0 || len(event.ArtifactRefs) != 0 || event.SchemaVersion != SchemaVersion {
		return fmt.Errorf("projection event crosses its runtime-owned envelope")
	}
	if !validProjectionEventType(record.ProjectionKind, record.Version, event.EventType) {
		return fmt.Errorf("projection %s/%s/%d uses unsupported event %s", record.ProjectionKind, record.RecordID, record.Version, event.EventType)
	}
	if record.ProjectionKind == "organization" {
		var organization core.Organization
		if decodeExactEventJSON(record.Value, &organization) != nil || organization.ID == "" || organization.ID != core.ID(record.RecordID) || string(organization.ID) != event.OrganizationID {
			return fmt.Errorf("organization projection value is invalid")
		}
	}
	if record.ProjectionKind == "mission" {
		var mission core.Mission
		if decodeExactEventJSON(record.Value, &mission) != nil || mission.ID != core.ID(record.RecordID) || string(mission.OrganizationID) != event.OrganizationID {
			return fmt.Errorf("mission projection value is invalid")
		}
		if err := ValidateMissionProjectionTarget(event.EventType, record.Version, mission); err != nil {
			return err
		}
	}
	if record.ProjectionKind == "goal" {
		var goal core.Goal
		if decodeExactEventJSON(record.Value, &goal) != nil || goal.ID != core.ID(record.RecordID) || string(goal.OrganizationID) != event.OrganizationID {
			return fmt.Errorf("goal projection value is invalid")
		}
		if err := ValidateGoalProjectionTarget(event.EventType, record.Version, goal); err != nil {
			return err
		}
	}
	if record.ProjectionKind == "work" {
		var work core.Work
		if decodeExactEventJSON(record.Value, &work) != nil || work.ID != core.ID(record.RecordID) {
			return fmt.Errorf("work projection value is invalid")
		}
		if err := ValidateWorkProjectionTarget(event.EventType, record.Version, work); err != nil {
			return err
		}
	}
	if record.ProjectionKind == "agent" {
		var agent core.Agent
		if decodeExactEventJSON(record.Value, &agent) != nil || agent.ID != core.ID(record.RecordID) || string(agent.OrganizationID) != event.OrganizationID {
			return fmt.Errorf("agent projection value is invalid")
		}
		if err := ValidateAgentProjectionTarget(event.EventType, record.Version, agent); err != nil {
			return err
		}
	}
	if record.ProjectionKind == "intent" {
		var intent core.Intent
		if decodeExactEventJSON(record.Value, &intent) != nil || intent.ID == "" || intent.ID != core.ID(record.RecordID) || intent.OrganizationID == "" || string(intent.OrganizationID) != event.OrganizationID || record.CorrelationID == "" || record.CorrelationID != event.CorrelationID {
			return fmt.Errorf("intent projection value is invalid or lacks its correlation boundary")
		}
	}
	if record.ProjectionKind == "lab_experiment" {
		var experiment core.Experiment
		if decodeExactEventJSON(record.Value, &experiment) != nil || experiment.ID != core.ID(record.RecordID) || string(experiment.OrganizationID) != event.OrganizationID || record.CorrelationID == "" || record.CorrelationID != event.CorrelationID {
			return fmt.Errorf("lab experiment projection value is invalid or lacks its correlation boundary")
		}
		if err := ValidateExperimentProjectionTarget(event.EventType, record.Version, experiment); err != nil {
			return err
		}
	}
	if record.ProjectionKind == "lab_promotion_candidate" {
		var candidate core.PromotionCandidate
		if decodeExactEventJSON(record.Value, &candidate) != nil || candidate.ID != core.ID(record.RecordID) || string(candidate.OrganizationID) != event.OrganizationID || record.CorrelationID == "" || record.CorrelationID != event.CorrelationID {
			return fmt.Errorf("lab promotion-candidate projection value is invalid or lacks its correlation boundary")
		}
		if err := ValidatePromotionCandidateProjectionTarget(event.EventType, record.Version, candidate); err != nil {
			return err
		}
	}
	if record.ProjectionKind == "task" {
		var task core.Task
		if decodeExactEventJSON(record.Value, &task) != nil || task.ID != core.ID(record.RecordID) {
			return fmt.Errorf("task projection value is invalid")
		}
		if err := ValidateTaskProjectionTarget(event.EventType, record.Version, task); err != nil {
			return err
		}
		if event.TaskID != record.RecordID {
			return fmt.Errorf("task projection crosses its Task envelope")
		}
		if event.RecipientScope == "" && event.RecipientID == "" {
			if event.EventType == "TASK_BLOCKED" && task.ParentID != "" {
				return fmt.Errorf("blocked child Task lacks its parent route")
			}
			return nil
		}
		if event.EventType != "TASK_BLOCKED" || task.ParentID == "" || event.RecipientScope != RecipientTask || event.RecipientID != string(task.ParentID) {
			return fmt.Errorf("task projection uses unsupported routing")
		}
		return nil
	}
	if event.TaskID != "" || event.RecipientScope != "" || event.RecipientID != "" {
		return fmt.Errorf("organizational projection uses a Task or recipient route")
	}
	return nil
}

// ValidateMissionProjectionTarget couples each Mission lifecycle label to its
// only permitted materialized state.
func ValidateMissionProjectionTarget(eventType string, version int, mission core.Mission) error {
	if version < 1 || !core.ValidMission(mission) {
		return fmt.Errorf("mission projection is incomplete")
	}
	valid := eventType == "MISSION_CREATED" && version == 1 && mission.Status == core.MissionActive ||
		eventType == "MISSION_REVISED" && version > 1 && mission.Status == core.MissionActive ||
		eventType == "MISSION_RETIRED" && version > 1 && mission.Status == core.MissionRetired
	if !valid {
		return fmt.Errorf("mission lifecycle event %s cannot materialize status %s at version %d", eventType, mission.Status, version)
	}
	return nil
}

// ValidateMissionProjectionTransition preserves one durable direction and
// keeps active refinement distinct from terminal retirement.
func ValidateMissionProjectionTransition(eventType string, version int, previous *core.Mission, next core.Mission) error {
	if err := ValidateMissionProjectionTarget(eventType, version, next); err != nil {
		return err
	}
	if previous == nil {
		if version != 1 || eventType != "MISSION_CREATED" {
			return fmt.Errorf("mission history must begin with creation at version one")
		}
		return nil
	}
	if version < 2 || !core.ValidMissionRevision(*previous, next) {
		return fmt.Errorf("mission revision changes immutable identity, direction during retirement, or lifecycle order")
	}
	expected := ""
	switch {
	case previous.Status == core.MissionActive && next.Status == core.MissionActive && !reflect.DeepEqual(*previous, next):
		expected = "MISSION_REVISED"
	case previous.Status == core.MissionActive && next.Status == core.MissionRetired:
		expected = "MISSION_RETIRED"
	}
	if expected == "" || eventType != expected {
		return fmt.Errorf("mission lifecycle event %s does not match the exact state transition", eventType)
	}
	return nil
}

// ValidateWorkProjectionTarget couples each Work lifecycle label to its only
// permitted materialized state.
func ValidateWorkProjectionTarget(eventType string, version int, work core.Work) error {
	if version < 1 || !core.ValidWork(work) {
		return fmt.Errorf("work projection is incomplete")
	}
	valid := false
	switch eventType {
	case "WORK_CREATED":
		valid = version == 1 && work.Status == core.WorkActive
	case "WORK_COMPLETED":
		valid = version > 1 && work.Status == core.WorkCompleted
	case "WORK_FAILED", "WORK_PLANNING_FAILED":
		valid = version > 1 && work.Status == core.WorkFailed
	}
	if !valid {
		return fmt.Errorf("work lifecycle event %s cannot materialize status %s at version %d", eventType, work.Status, version)
	}
	return nil
}

// ValidateWorkProjectionTransition preserves one accepted Intent, optional
// Goal, objective, and correlation-owned lifecycle from active to terminal.
func ValidateWorkProjectionTransition(eventType string, version int, previous *core.Work, next core.Work) error {
	if err := ValidateWorkProjectionTarget(eventType, version, next); err != nil {
		return err
	}
	if previous == nil {
		if version != 1 || eventType != "WORK_CREATED" {
			return fmt.Errorf("work history must begin with creation at version one")
		}
		return nil
	}
	if version < 2 || !core.ValidWorkRevision(*previous, next) {
		return fmt.Errorf("work revision changes immutable identity or reopens terminal state")
	}
	valid := previous.Status == core.WorkActive && next.Status == core.WorkCompleted && eventType == "WORK_COMPLETED" ||
		previous.Status == core.WorkActive && next.Status == core.WorkFailed && (eventType == "WORK_FAILED" || eventType == "WORK_PLANNING_FAILED")
	if !valid {
		return fmt.Errorf("work lifecycle event %s does not match the exact state transition", eventType)
	}
	return nil
}

// ValidateExperimentProjectionTarget keeps experimental completion separate
// from trust. No event label in this contract can materialize an active result.
func ValidateExperimentProjectionTarget(eventType string, version int, experiment core.Experiment) error {
	if version < 1 || !core.ValidExperiment(experiment) {
		return fmt.Errorf("lab experiment projection is incomplete or unsafe")
	}
	valid := version == 1 && eventType == "LAB_EXPERIMENT_STARTED" && experiment.Status == core.ExperimentRunning ||
		version > 1 && eventType == "LAB_EXPERIMENT_COMPLETED" && experiment.Status == core.ExperimentCompleted ||
		version > 1 && eventType == "LAB_EXPERIMENT_FAILED" && experiment.Status == core.ExperimentFailed
	if !valid {
		return fmt.Errorf("lab experiment event %s cannot materialize status %s at version %d", eventType, experiment.Status, version)
	}
	return nil
}

func ValidateExperimentProjectionTransition(eventType string, version int, previous *core.Experiment, next core.Experiment) error {
	if err := ValidateExperimentProjectionTarget(eventType, version, next); err != nil {
		return err
	}
	if previous == nil {
		if version != 1 || eventType != "LAB_EXPERIMENT_STARTED" {
			return fmt.Errorf("lab experiment history must begin with runtime start at version one")
		}
		return nil
	}
	if version < 2 || !core.ValidExperimentRevision(*previous, next) {
		return fmt.Errorf("lab experiment revision changes its containment, budget, trust, or terminal lifecycle")
	}
	return nil
}

func ValidatePromotionCandidateProjectionTarget(eventType string, version int, candidate core.PromotionCandidate) error {
	if version != 1 || eventType != "LAB_PROMOTION_CANDIDATE_CREATED" || !core.ValidPromotionCandidate(candidate) {
		return fmt.Errorf("lab promotion candidate is not a valid version-one nomination")
	}
	return nil
}

// ValidateGoalProjectionTarget couples each Goal lifecycle label to the state
// that label is permitted to materialize, even before prior state is loaded.
func ValidateGoalProjectionTarget(eventType string, version int, goal core.Goal) error {
	if version < 1 || !core.ValidGoal(goal) {
		return fmt.Errorf("goal projection is incomplete")
	}
	valid := false
	switch eventType {
	case "GOAL_CREATED":
		valid = version == 1 && goal.Status == core.GoalActive
	case "GOAL_REFINED":
		valid = version > 1 && (goal.Status == core.GoalActive || goal.Status == core.GoalPaused)
	case "GOAL_PAUSED":
		valid = version > 1 && goal.Status == core.GoalPaused
	case "GOAL_RESUMED":
		valid = version > 1 && goal.Status == core.GoalActive
	case "GOAL_RETIRED":
		valid = version > 1 && goal.Status == core.GoalRetired
	case "GOAL_ACHIEVED":
		valid = version > 1 && goal.Status == core.GoalAchieved
	}
	if !valid {
		return fmt.Errorf("goal lifecycle event %s cannot materialize status %s at version %d", eventType, goal.Status, version)
	}
	return nil
}

// ValidateGoalProjectionTransition binds every Goal target to its exact prior
// state and keeps refinement distinct from pause, resume, retirement, and
// evidence-backed achievement.
func ValidateGoalProjectionTransition(eventType string, version int, previous *core.Goal, next core.Goal) error {
	if err := ValidateGoalProjectionTarget(eventType, version, next); err != nil {
		return err
	}
	if previous == nil {
		if version != 1 || eventType != "GOAL_CREATED" {
			return fmt.Errorf("goal history must begin with creation at version one")
		}
		return nil
	}
	if version < 2 || !core.ValidGoalRevision(*previous, next) {
		return fmt.Errorf("goal revision changes immutable identity, direction during a lifecycle transition, or lifecycle order")
	}
	expected := ""
	switch {
	case previous.Status == next.Status && !reflect.DeepEqual(*previous, next):
		expected = "GOAL_REFINED"
	case previous.Status == core.GoalActive && next.Status == core.GoalPaused:
		expected = "GOAL_PAUSED"
	case previous.Status == core.GoalPaused && next.Status == core.GoalActive:
		expected = "GOAL_RESUMED"
	case next.Status == core.GoalRetired && previous.Status != core.GoalRetired:
		expected = "GOAL_RETIRED"
	case previous.Status == core.GoalActive && next.Status == core.GoalAchieved:
		expected = "GOAL_ACHIEVED"
	}
	if expected == "" || eventType != expected {
		return fmt.Errorf("goal lifecycle event %s does not match the exact state transition", eventType)
	}
	return nil
}

// ValidateAgentProjectionTarget couples an Agent lifecycle label to the state
// that label is permitted to materialize.
func ValidateAgentProjectionTarget(eventType string, version int, agent core.Agent) error {
	if version < 1 || !core.ValidAgent(agent) {
		return fmt.Errorf("agent projection is incomplete")
	}
	switch eventType {
	case "AGENT_CREATED":
		if version != 1 || agent.Status != "ACTIVE" {
			return fmt.Errorf("agent creation must start ACTIVE at version one")
		}
	case "AGENT_CONFIGURATION_UPDATED":
		if version < 2 {
			return fmt.Errorf("agent configuration update requires an existing Agent")
		}
	case "AGENT_DEACTIVATED":
		if version < 2 || agent.Status != "INACTIVE" {
			return fmt.Errorf("agent deactivation must materialize INACTIVE state")
		}
	case "AGENT_REACTIVATED":
		if version < 2 || agent.Status != "ACTIVE" {
			return fmt.Errorf("agent reactivation must materialize ACTIVE state")
		}
	default:
		return fmt.Errorf("agent projection uses unsupported lifecycle event %s", eventType)
	}
	return nil
}

// ValidateAgentProjectionTransition binds Agent configuration and status
// changes to mutually exclusive runtime-owned event labels.
func ValidateAgentProjectionTransition(eventType string, version int, previous *core.Agent, next core.Agent) error {
	if err := ValidateAgentProjectionTarget(eventType, version, next); err != nil {
		return err
	}
	if previous == nil {
		if version != 1 || eventType != "AGENT_CREATED" {
			return fmt.Errorf("agent history must begin with creation at version one")
		}
		return nil
	}
	if !core.ValidAgentRevision(*previous, next) {
		return fmt.Errorf("agent revision changes immutable identity or organization")
	}
	configurationChanged := previous.BlueprintID != next.BlueprintID || previous.BlueprintVersion != next.BlueprintVersion ||
		previous.ExecutionProfileID != next.ExecutionProfileID || previous.ExecutionProfileVersion != next.ExecutionProfileVersion ||
		previous.RuntimeAdapter != next.RuntimeAdapter
	valid := false
	switch eventType {
	case "AGENT_CONFIGURATION_UPDATED":
		valid = previous.Status == next.Status && configurationChanged
	case "AGENT_DEACTIVATED":
		valid = previous.Status == "ACTIVE" && next.Status == "INACTIVE" && !configurationChanged
	case "AGENT_REACTIVATED":
		valid = previous.Status == "INACTIVE" && next.Status == "ACTIVE" && !configurationChanged
	}
	if !valid {
		return fmt.Errorf("agent lifecycle event %s does not match its configuration and status transition", eventType)
	}
	return nil
}

// ValidateTaskProjectionTarget couples every Task lifecycle label to the only
// status it is allowed to materialize. It rejects mislabeled state even when a
// caller cannot yet supply the preceding durable revision.
func ValidateTaskProjectionTarget(eventType string, version int, task core.Task) error {
	if version < 1 || !core.ValidTask(task) {
		return fmt.Errorf("task projection is incomplete")
	}
	var expected core.TaskStatus
	switch eventType {
	case "TASK_CREATED", "TASK_ASSIGNMENT_REVALIDATED", "TASK_RECOVERED", "TASK_RESUMED":
		expected = core.TaskPending
	case "TASK_BLOCKED":
		expected = core.TaskBlocked
	case "EXECUTION_STARTED":
		expected = core.TaskRunning
	case "TASK_VERIFIED_COMPLETE":
		expected = core.TaskCompleted
	case "COMPLETION_REJECTED", "TASK_DEPENDENCY_FAILED", "TASK_REMEDIATION_FAILED", "TASK_WORK_FAILED":
		expected = core.TaskFailed
	default:
		return fmt.Errorf("task projection uses unsupported lifecycle event %s", eventType)
	}
	if task.Status != expected {
		return fmt.Errorf("task lifecycle event %s cannot materialize status %s", eventType, task.Status)
	}
	if version == 1 && eventType != "TASK_CREATED" && eventType != "TASK_BLOCKED" || version > 1 && eventType == "TASK_CREATED" {
		return fmt.Errorf("task lifecycle event %s is invalid at version %d", eventType, version)
	}
	return nil
}

// ValidateTaskProjectionTransition binds a Task target to its exact preceding
// status and preserves all planned, assignment, routing, and completion fields.
func ValidateTaskProjectionTransition(eventType string, version int, previous *core.Task, next core.Task) error {
	if err := ValidateTaskProjectionTarget(eventType, version, next); err != nil {
		return err
	}
	if previous == nil {
		if version != 1 {
			return fmt.Errorf("task revision %d lacks its prior durable state", version)
		}
		return nil
	}
	if version < 2 || !core.ValidTaskRevision(*previous, next) {
		return fmt.Errorf("task revision changes its immutable contract")
	}
	valid := false
	switch eventType {
	case "TASK_ASSIGNMENT_REVALIDATED", "TASK_RESUMED":
		valid = previous.Status == core.TaskBlocked
	case "TASK_RECOVERED":
		valid = previous.Status == core.TaskRunning
	case "TASK_BLOCKED":
		valid = previous.Status == core.TaskPending || previous.Status == core.TaskRunning
	case "EXECUTION_STARTED":
		valid = previous.Status == core.TaskPending
	case "TASK_VERIFIED_COMPLETE":
		valid = previous.Status == core.TaskRunning || previous.Status == core.TaskBlocked
	case "COMPLETION_REJECTED":
		valid = previous.Status == core.TaskRunning || previous.Status == core.TaskBlocked
	case "TASK_DEPENDENCY_FAILED", "TASK_WORK_FAILED":
		valid = previous.Status == core.TaskPending || previous.Status == core.TaskBlocked
	case "TASK_REMEDIATION_FAILED":
		valid = previous.Status == core.TaskRunning
	}
	if !valid {
		return fmt.Errorf("task lifecycle event %s cannot transition %s to %s", eventType, previous.Status, next.Status)
	}
	return nil
}

func decodeExactEventJSON(data []byte, target any) error {
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("unexpected trailing JSON")
	}
	return nil
}

func validProjectionEventType(kind string, version int, eventType string) bool {
	if version < 1 {
		return false
	}
	switch kind {
	case "organization":
		return version == 1 && eventType == "ORGANIZATION_CREATED"
	case "mission":
		return version == 1 && eventType == "MISSION_CREATED" || version > 1 && (eventType == "MISSION_REVISED" || eventType == "MISSION_RETIRED")
	case "goal":
		return version == 1 && eventType == "GOAL_CREATED" || version > 1 && (eventType == "GOAL_REFINED" || eventType == "GOAL_PAUSED" || eventType == "GOAL_RESUMED" || eventType == "GOAL_RETIRED" || eventType == "GOAL_ACHIEVED")
	case "team":
		return version == 1 && eventType == "TEAM_CREATED" || version > 1 && eventType == "TEAM_REVISED"
	case "agent_blueprint":
		return version == 1 && eventType == "AGENT_BLUEPRINT_CREATED" || version > 1 && eventType == "AGENT_BLUEPRINT_UPDATED"
	case "execution_profile":
		return version == 1 && eventType == "EXECUTION_PROFILE_CREATED" || version > 1 && eventType == "EXECUTION_PROFILE_UPDATED"
	case "agent":
		return version == 1 && eventType == "AGENT_CREATED" || version > 1 && (eventType == "AGENT_CONFIGURATION_UPDATED" || eventType == "AGENT_DEACTIVATED" || eventType == "AGENT_REACTIVATED")
	case "intent":
		return version == 1 && eventType == "INTENT_CREATED"
	case "work":
		return version == 1 && eventType == "WORK_CREATED" || version > 1 && (eventType == "WORK_COMPLETED" || eventType == "WORK_FAILED" || eventType == "WORK_PLANNING_FAILED")
	case "lab_experiment":
		return version == 1 && eventType == "LAB_EXPERIMENT_STARTED" || version > 1 && (eventType == "LAB_EXPERIMENT_COMPLETED" || eventType == "LAB_EXPERIMENT_FAILED")
	case "lab_promotion_candidate":
		return version == 1 && eventType == "LAB_PROMOTION_CANDIDATE_CREATED"
	case "task":
		if version == 1 {
			return eventType == "TASK_CREATED" || eventType == "TASK_BLOCKED"
		}
		switch eventType {
		case "TASK_ASSIGNMENT_REVALIDATED", "TASK_BLOCKED", "TASK_RECOVERED", "TASK_RESUMED", "EXECUTION_STARTED", "TASK_VERIFIED_COMPLETE", "COMPLETION_REJECTED", "TASK_DEPENDENCY_FAILED", "TASK_REMEDIATION_FAILED", "TASK_WORK_FAILED":
			return true
		}
	}
	return false
}

// ValidateOrdinaryEventPayload enforces the object-shaped Event Contract
// boundary and reserves typed projection authority keys. Writers call this
// before persistence so startup never discovers a payload they admitted but
// cannot classify.
func ValidateOrdinaryEventPayload(value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode event payload: %w", err)
	}
	if rejectDuplicateJSONKeys(body) != nil {
		return fmt.Errorf("event payload is malformed")
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(body, &object) != nil || object == nil {
		return fmt.Errorf("event payload must be a JSON object")
	}
	_, hasProjection := object["projection"]
	_, hasAdmission := object["admission"]
	if hasProjection || hasAdmission {
		return fmt.Errorf("projection payloads require typed admission")
	}
	return nil
}

func projectionAdmissionFingerprint(admission ProjectionAdmission, event Event, record ProjectionRecord, detail json.RawMessage) (string, error) {
	contract := projectionAdmissionFingerprintPayload{
		Method: admission.Method, EventRef: admission.EventRef, Sequence: event.Sequence,
		OrganizationID: event.OrganizationID, EventType: event.EventType, SourceActorID: event.SourceActorID, SourceExecutionID: event.SourceExecutionID,
		RecipientScope: event.RecipientScope, RecipientID: event.RecipientID, TaskID: event.TaskID,
		AuthorizationRefs: event.AuthorizationRefs, ArtifactRefs: event.ArtifactRefs, CorrelationID: event.CorrelationID,
		CreatedAt: event.CreatedAt.UTC().Format(time.RFC3339Nano), SchemaVersion: event.SchemaVersion,
		Projection: record, Detail: detail,
	}
	body, err := json.Marshal(contract)
	if err != nil {
		return "", fmt.Errorf("encode projection admission: %w", err)
	}
	digest := sha256.Sum256(body)
	return fmt.Sprintf("%x", digest), nil
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
type WorkCompletionEvidenceAppender interface {
	AppendWorkCompletionEvidence(context.Context, TrustedDraft) (Event, error)
}
type GoalProgressAdmission struct {
	Evaluation      GoalProgressEvaluatedPayload
	EvaluationEvent Event
	GoalTransition  *Event
}
type GoalProgressAppender interface {
	AppendGoalProgress(context.Context, string, core.ID) (GoalProgressAdmission, error)
}
type GoalAchievementValidator interface {
	ValidateGoalAchievement(context.Context, string, core.ID) error
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
	if err := ValidateOrdinaryEventPayload(draft.Payload); err != nil {
		return Event{}, err
	}
	if RequiresProjectionAdmission(draft.EventType, draft.SourceActorID) {
		return Event{}, fmt.Errorf("projection lifecycle events require typed admission")
	}
	if draft.EventType == "INBOX_EVENTS_OBSERVED" {
		return Event{}, fmt.Errorf("inbox observations require atomic inbox admission")
	}
	switch draft.EventType {
	case "WORK_COMPLETION_EVALUATED", "WORK_COMPLETED", "GOAL_PROGRESS_EVALUATED", "GOAL_ACHIEVED":
		return Event{}, fmt.Errorf("terminal evidence requires its typed admission")
	}
	if draft.EventType == "INTENT_CONFIRMED" {
		return Event{}, fmt.Errorf("intent confirmation requires typed review admission")
	}
	if err := g.validateAddressed(ctx, draft, false); err != nil {
		return Event{}, err
	}
	return g.ledger.Append(ctx, draft)
}

// PublishWorkCompletionEvidence admits the aggregate evidence only through a
// ledger implementation that can validate it against current durable Work.
// The later terminal projection remains a separate atomic admission.
func (g *Gateway) PublishWorkCompletionEvidence(ctx context.Context, draft TrustedDraft) (Event, error) {
	var evidence WorkCompletionEvidencePayload
	if draft.OrganizationID == "" || draft.EventType != "WORK_COMPLETION_EVALUATED" || draft.SourceActorID != "runtime" || draft.SourceExecutionID != "" || draft.RecipientScope != "" || draft.RecipientID != "" || draft.TaskID != "" || len(draft.AuthorizationRefs) != 0 || draft.CorrelationID == "" || decodePayload(draft.Payload, &evidence) != nil || !evidence.Valid() || !slices.Equal(draft.ArtifactRefs, evidence.ArtifactRefs) {
		return Event{}, fmt.Errorf("work completion evidence requires a complete typed runtime admission")
	}
	appender, ok := g.ledger.(WorkCompletionEvidenceAppender)
	if !ok {
		return Event{}, fmt.Errorf("event ledger does not support Work completion evidence admission")
	}
	return appender.AppendWorkCompletionEvidence(ctx, draft)
}

// PublishIntentConfirmation atomically validates the exact reviewed intake and,
// when present, proves that its Goal is active in the same organization.
// Later Goal lifecycle changes do not invalidate the admitted Work binding.
func (g *Gateway) PublishIntentConfirmation(ctx context.Context, draft TrustedDraft, goalID core.ID) (Event, error) {
	if draft.EventType != "INTENT_CONFIRMED" {
		return Event{}, fmt.Errorf("intent confirmation is incomplete")
	}
	var confirmation IntentConfirmedPayload
	if decodePayload(draft.Payload, &confirmation) != nil || confirmation.GoalID != string(goalID) {
		return Event{}, fmt.Errorf("intent confirmation does not match its reviewed Goal selection")
	}
	if err := g.validateAddressed(ctx, draft, false); err != nil {
		return Event{}, err
	}
	confirmer, ok := g.ledger.(IntentConfirmer)
	if !ok {
		return Event{}, fmt.Errorf("ledger does not support typed intent confirmation")
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
	if err := rejectBareTerminalProjection(draft); err != nil {
		return Event{}, err
	}
	if draft.Event.EventType == "EXECUTION_STARTED" && draft.ProjectionKind == "task" {
		return Event{}, fmt.Errorf("execution start requires typed atomic admission")
	}
	store, ok := g.ledger.(ProjectionAppender)
	if !ok {
		return Event{}, fmt.Errorf("event ledger does not support durable projections")
	}
	return store.AppendProjection(ctx, draft)
}

func (g *Gateway) PublishExecutionStart(ctx context.Context, draft ProjectionDraft, routes []InboxRoute) (Event, []InboxSelection, error) {
	if draft.Event.EventType != "EXECUTION_STARTED" || draft.Event.OrganizationID == "" || draft.Event.SourceActorID != "runtime" || draft.Event.SourceExecutionID != "" || draft.Event.RecipientScope != "" || draft.Event.RecipientID != "" || draft.Event.TaskID == "" || draft.Event.TaskID != draft.RecordID || draft.Event.CorrelationID == "" || draft.ProjectionKind != "task" || draft.RecordID == "" || draft.Version < 2 {
		return Event{}, nil, fmt.Errorf("complete execution-start identity is required")
	}
	var task core.Task
	var detail ExecutionStartDetail
	if decodePayload(draft.Value, &task) != nil || task.ID != core.ID(draft.RecordID) || task.Status != core.TaskRunning || decodePayload(draft.Event.Payload, &detail) != nil || detail.InboxCutoffSequence != 0 || detail.DispatchBinding != nil || !validStrategicStartRefs(detail.StrategicEventRefs, detail.StrategicContextRefs) {
		return Event{}, nil, fmt.Errorf("execution-start task or detail is invalid")
	}
	switch task.ExecutionKind {
	case core.ExecutionAgent:
		if task.AssigneeType != "AGENT" || task.AssigneeID == "" || len(routes) < 2 || detail.InputEventRef != "" || detail.Mode != "" && detail.Mode != "BLOCKED_DEPENDENCY_REMEDIATION" {
			return Event{}, nil, fmt.Errorf("agent execution-start task or inbox boundary is invalid")
		}
	case core.ExecutionDeterministic:
		if len(routes) != 0 || detail.Mode != "" || detail.InputEventRef != "" {
			return Event{}, nil, fmt.Errorf("deterministic execution-start boundary is invalid")
		}
	case core.ExecutionHuman:
		if len(routes) != 0 || detail.Mode != "OPERATOR_HUMAN_INPUT" && detail.Mode != "STRUCTURED_HUMAN_COMPLETION" || detail.InputEventRef == "" {
			return Event{}, nil, fmt.Errorf("user execution-start boundary is invalid")
		}
	case core.ExecutionTool, core.ExecutionTeam, core.ExecutionMixed:
		return Event{}, nil, fmt.Errorf("execution-start kind is unavailable")
	default:
		return Event{}, nil, fmt.Errorf("execution-start kind is unavailable")
	}
	appender, ok := g.ledger.(ExecutionStartAppender)
	if !ok {
		return Event{}, nil, fmt.Errorf("event ledger does not support atomic execution start")
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

// EvaluateGoalProgress delegates evidence selection and terminal admission to
// the ledger transaction. Callers cannot supply a claimed result or evidence
// set and therefore cannot turn model or worker content into Goal authority.
func (g *Gateway) EvaluateGoalProgress(ctx context.Context, organizationID string, goalID core.ID) (GoalProgressAdmission, error) {
	if organizationID == "" || goalID == "" {
		return GoalProgressAdmission{}, fmt.Errorf("goal progress organization and identity are required")
	}
	appender, ok := g.ledger.(GoalProgressAppender)
	if !ok {
		return GoalProgressAdmission{}, fmt.Errorf("event ledger does not support evidence-backed Goal progress")
	}
	return appender.AppendGoalProgress(ctx, organizationID, goalID)
}

func (g *Gateway) ValidateGoalAchievement(ctx context.Context, organizationID string, goalID core.ID) error {
	if organizationID == "" || goalID == "" {
		return fmt.Errorf("goal achievement organization and identity are required")
	}
	validator, ok := g.ledger.(GoalAchievementValidator)
	if !ok {
		return fmt.Errorf("event ledger does not support Goal achievement validation")
	}
	return validator.ValidateGoalAchievement(ctx, organizationID, goalID)
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
		if err := rejectBareTerminalProjection(draft); err != nil {
			return nil, err
		}
	}
	store, ok := g.ledger.(ProjectionBatchAppender)
	if !ok {
		return nil, fmt.Errorf("event ledger does not support atomic projection batches")
	}
	return store.AppendProjections(ctx, drafts)
}

func rejectBareTerminalProjection(draft ProjectionDraft) error {
	switch draft.ProjectionKind {
	case "work":
		var work core.Work
		if decodePayload(draft.Value, &work) != nil {
			return fmt.Errorf("work projection value is invalid")
		}
		if work.Status == core.WorkCompleted || draft.Event.EventType == "WORK_COMPLETED" {
			return fmt.Errorf("completed work requires evidence-backed admission")
		}
	case "goal":
		var goal core.Goal
		if decodePayload(draft.Value, &goal) != nil {
			return fmt.Errorf("goal projection value is invalid")
		}
		if goal.Status == core.GoalAchieved || draft.Event.EventType == "GOAL_ACHIEVED" {
			return fmt.Errorf("achieved Goal requires evidence-backed admission")
		}
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

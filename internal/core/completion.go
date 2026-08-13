package core

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"mime"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

// ToolOutcomeSummary deterministically derives the user-reviewable result text
// from the exact structured outcome that completion admission later verifies.
func ToolOutcomeSummary(outcome ToolOutcome) (string, error) {
	if summary, ok := outcome.ObservedEffect.(string); ok && summary != "" {
		return summary, nil
	}
	if fields, ok := outcome.ObservedEffect.(map[string]string); ok && fields["status"] != "" {
		return fields["status"], nil
	}
	if fields, ok := outcome.ObservedEffect.(map[string]any); ok {
		if summary, ok := fields["status"].(string); ok && summary != "" {
			return summary, nil
		}
	}
	if outcome.ObservedEffect != nil {
		encoded, err := json.Marshal(outcome.ObservedEffect)
		if err != nil {
			return "", err
		}
		return string(encoded), nil
	}
	if outcome.ErrorDetail != "" {
		return outcome.ErrorDetail, nil
	}
	return fmt.Sprintf("task outcome: %s", outcome.Status), nil
}

// FingerprintExecutionInput binds verification to the exact runtime-selected
// input without persisting that potentially sensitive input in the manifest.
func FingerprintExecutionInput(input string) string {
	digest := sha256.Sum256([]byte(input))
	return fmt.Sprintf("%x", digest)
}

// VerifiedOutcomeCompletionContract is the runtime-owned contract for a Task
// whose successful ToolOutcome can be verified deterministically.
func VerifiedOutcomeCompletionContract(taskID ID, taskVersion int) CompletionContract {
	return CompletionContract{
		TaskID: taskID, TaskVersion: taskVersion,
		Criteria: []CompletionCriterion{{
			ID: "verified-outcome", Description: "work produced a verified successful outcome",
			Assurance: AssuranceDeterministic, Required: true,
		}},
	}
}

// ExternalInputCompletionContract is the runtime-owned contract for an
// authorized external-input continuation that has been durably recorded.
func ExternalInputCompletionContract(taskID ID, taskVersion int) CompletionContract {
	return CompletionContract{
		TaskID: taskID, TaskVersion: taskVersion,
		Criteria: []CompletionCriterion{{
			ID: "durable-external-input", Description: "authorized external input was durably recorded",
			Assurance: AssuranceDeterministic, Required: true,
		}},
	}
}

// ReviewedOutcomeCompletionContract is the runtime-owned contract for an
// adaptive result that requires independent user judgment.
func ReviewedOutcomeCompletionContract(taskID ID, taskVersion int, accepted []IntentValue) CompletionContract {
	criteria := make([]CompletionCriterion, 0, max(1, len(accepted)))
	if len(accepted) == 0 {
		criteria = append(criteria, CompletionCriterion{
			ID: "reviewed-outcome", Description: "an authorized reviewer judged the recorded candidate against the task objective",
			Assurance: AssuranceHumanJudgment, Required: true,
		})
	} else {
		for index, criterion := range accepted {
			criteria = append(criteria, CompletionCriterion{
				ID: fmt.Sprintf("accepted-criterion-%d", index+1), Description: criterion.Value,
				Assurance: AssuranceHumanJudgment, Required: true,
			})
		}
	}
	return CompletionContract{TaskID: taskID, TaskVersion: taskVersion, Criteria: criteria}
}

// StructuredUserCompletionContract is the immutable V1 response contract for
// a root user Task created from a directly submitted Intent.
func StructuredUserCompletionContract(taskID ID) CompletionContract {
	return CompletionContract{
		TaskID: taskID, TaskVersion: 1,
		RequiredFields: []CompletionFieldRequirement{{
			Name: "response", Description: "the information requested by the user task", MinBytes: 1, MaxBytes: 64 << 10,
		}},
	}
}

// VerifyRegisteredPostcondition discards a handler's postcondition claim and
// applies the runtime-owned deterministic checks registered for V1 outcomes.
// The boolean reports whether a check exists for this outcome kind.
func VerifyRegisteredPostcondition(task Task, outcome ToolOutcome) (ToolOutcome, bool) {
	outcome.PostconditionStatus = PostconditionNotChecked
	var verified bool
	switch outcome.ToolID {
	case "builtin.echo":
		if outcome.Status != OutcomeSucceeded {
			return outcome, true
		}
		value, ok := outcome.ObservedEffect.(string)
		verified = ok && task.ExecutionKind == ExecutionDeterministic && strings.HasPrefix(task.Description, "echo ") && value == strings.TrimPrefix(task.Description, "echo ")
	case "fake-model/v1":
		if outcome.Status != OutcomeSucceeded {
			return outcome, true
		}
		value, ok := outcome.ObservedEffect.(string)
		expected := task.Description
		if task.ExecutionBrief != "" {
			expected = task.ExecutionBrief
		}
		verified = ok && task.ExecutionKind == ExecutionAgent && value == "fake-model: "+expected
	default:
		return outcome, false
	}
	if verified {
		outcome.PostconditionStatus = PostconditionVerified
	}
	return outcome, true
}

// VerifyPersistedPostcondition recomputes a registered V1 postcondition from
// durable outcome data. Agent verification binds the returned execution input
// to the runtime-owned manifest without persisting the input itself.
func VerifyPersistedPostcondition(task Task, outcome ToolOutcome, executionInputSHA256 string) (ToolOutcome, bool) {
	if outcome.ToolID != "fake-model/v1" {
		return VerifyRegisteredPostcondition(task, outcome)
	}
	outcome.PostconditionStatus = PostconditionNotChecked
	if outcome.Status != OutcomeSucceeded {
		return outcome, true
	}
	value, ok := outcome.ObservedEffect.(string)
	const prefix = "fake-model: "
	if ok && task.ExecutionKind == ExecutionAgent && strings.HasPrefix(value, prefix) && executionInputSHA256 != "" && FingerprintExecutionInput(strings.TrimPrefix(value, prefix)) == executionInputSHA256 {
		outcome.PostconditionStatus = PostconditionVerified
	}
	return outcome, true
}

// EvaluateHumanTaskCompletion deterministically validates one structured user
// submission against its immutable completion contract.
func EvaluateHumanTaskCompletion(c CompletionContract, submission HumanTaskSubmission) CompletionResult {
	var reasons []string
	if c.TaskID == "" || c.TaskVersion < 1 {
		reasons = append(reasons, "completion contract identity is invalid")
	}
	if submission.MessageID == "" {
		reasons = append(reasons, "submission message identity is required")
	}
	requiredFields := make(map[string]struct{}, len(c.RequiredFields))
	for _, requirement := range c.RequiredFields {
		if requirement.Name == "" || requirement.MinBytes < 0 || requirement.MaxBytes < requirement.MinBytes || requirement.MaxBytes > 1<<20 {
			reasons = append(reasons, "field requirement is invalid")
			continue
		}
		if _, exists := requiredFields[requirement.Name]; exists {
			reasons = append(reasons, "field requirement is duplicated: "+requirement.Name)
			continue
		}
		requiredFields[requirement.Name] = struct{}{}
		value, exists := submission.Fields[requirement.Name]
		if !exists || !utf8.ValidString(value) || len(value) < requirement.MinBytes || len(value) > requirement.MaxBytes {
			reasons = append(reasons, "required field is missing or invalid: "+requirement.Name)
		}
	}
	for name := range submission.Fields {
		if _, expected := requiredFields[name]; !expected {
			reasons = append(reasons, "unexpected completion field: "+name)
		}
	}
	byRole := make(map[string][]ArtifactEvidence)
	artifactRefs := make(map[string]struct{}, len(submission.Artifacts))
	for _, artifact := range submission.Artifacts {
		if !validArtifactEvidence(artifact) {
			reasons = append(reasons, "artifact evidence is invalid")
			continue
		}
		if _, exists := artifactRefs[artifact.Ref]; exists {
			reasons = append(reasons, "artifact evidence is duplicated")
			continue
		}
		artifactRefs[artifact.Ref] = struct{}{}
		byRole[artifact.Role] = append(byRole[artifact.Role], artifact)
	}
	requiredRoles := make(map[string]struct{}, len(c.ArtifactRequirements))
	for _, requirement := range c.ArtifactRequirements {
		if requirement.Role == "" || requirement.MinCount < 0 || requirement.MaxCount < requirement.MinCount || requirement.MaxCount > 32 || len(requirement.MediaTypes) == 0 {
			reasons = append(reasons, "artifact requirement is invalid")
			continue
		}
		if _, exists := requiredRoles[requirement.Role]; exists {
			reasons = append(reasons, "artifact requirement is duplicated: "+requirement.Role)
			continue
		}
		requiredRoles[requirement.Role] = struct{}{}
		artifacts := byRole[requirement.Role]
		if len(artifacts) < requirement.MinCount || len(artifacts) > requirement.MaxCount {
			reasons = append(reasons, fmt.Sprintf("artifact role %s requires %d to %d files", requirement.Role, requirement.MinCount, requirement.MaxCount))
		}
		allowed := make(map[string]struct{}, len(requirement.MediaTypes))
		for _, mediaType := range requirement.MediaTypes {
			parsed, _, err := mime.ParseMediaType(mediaType)
			if err != nil || parsed != mediaType {
				reasons = append(reasons, "artifact media type requirement is invalid")
				continue
			}
			allowed[parsed] = struct{}{}
		}
		for _, artifact := range artifacts {
			if _, ok := allowed[artifact.MediaType]; !ok {
				reasons = append(reasons, "artifact media type is not allowed for role "+requirement.Role)
			}
		}
	}
	for role := range byRole {
		if _, expected := requiredRoles[role]; !expected {
			reasons = append(reasons, "unexpected artifact role: "+role)
		}
	}
	return CompletionResult{Complete: len(reasons) == 0, Reasons: reasons}
}

// HumanTaskCompletionOutcome materializes the only runtime-owned outcome that
// may attest to a persisted structured user submission.
func HumanTaskCompletionOutcome(submissionEventID string, artifactRefs []string, at time.Time) ToolOutcome {
	return ToolOutcome{
		ToolInvocationID:    ID("human-task-" + submissionEventID),
		ToolID:              "human.task-completion",
		ToolVersion:         "v1",
		Status:              OutcomeSucceeded,
		ObservedEffect:      map[string]any{"status": "structured user completion persisted", "completion_event_ref": submissionEventID},
		PostconditionStatus: PostconditionVerified,
		Retryability:        NotRetryable,
		ArtifactRefs:        append([]string(nil), artifactRefs...),
		StartedAt:           at,
		FinishedAt:          at,
	}
}

// ValidHumanTaskCompletionOutcome verifies the exact durable outcome emitted
// for one structured user submission. It rejects unrelated, failed, or partial
// outcomes even when their outer event envelope is valid.
func ValidHumanTaskCompletionOutcome(outcome ToolOutcome, submissionEventID string, artifactRefs []string) bool {
	if submissionEventID == "" || outcome.ToolInvocationID != ID("human-task-"+submissionEventID) || outcome.ToolID != "human.task-completion" || outcome.ToolVersion != "v1" || outcome.Status != OutcomeSucceeded || outcome.PostconditionStatus != PostconditionVerified || outcome.Retryability != NotRetryable || outcome.RecoveryAttempted || outcome.RecoveryResult != nil || outcome.ErrorClass != "" || outcome.ErrorDetail != "" || outcome.StartedAt.IsZero() || !outcome.FinishedAt.Equal(outcome.StartedAt) || !slices.Equal(outcome.ArtifactRefs, artifactRefs) {
		return false
	}
	effect, ok := outcome.ObservedEffect.(map[string]any)
	if !ok || len(effect) != 2 {
		return false
	}
	status, statusOK := effect["status"].(string)
	completionEventRef, refOK := effect["completion_event_ref"].(string)
	return statusOK && refOK && status == "structured user completion persisted" && completionEventRef == submissionEventID
}

func validArtifactEvidence(artifact ArtifactEvidence) bool {
	if artifact.Role == "" || artifact.Name == "" || artifact.Size <= 0 || artifact.Size > 16<<20 || artifact.Origin == "" || artifact.Trust != "UNTRUSTED_USER_ARTIFACT" {
		return false
	}
	if len(artifact.SHA256) != 64 || artifact.Ref != "artifact/sha256/"+artifact.SHA256 {
		return false
	}
	for _, character := range artifact.SHA256 {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	parsed, parameters, err := mime.ParseMediaType(artifact.MediaType)
	return err == nil && len(parameters) == 0 && parsed == artifact.MediaType
}

package completion

import (
	"fmt"
	"mime"
	"strings"
	"unicode/utf8"

	"github.com/dominicnunez/agentos/internal/core"
)

type Result struct {
	Complete bool     `json:"complete"`
	Reasons  []string `json:"reasons,omitempty"`
}
type Engine struct{}

func (Engine) Evaluate(c core.CompletionContract, o core.ToolOutcome) Result {
	return evaluate(c, o, nil)
}

// EvaluateHuman applies an authenticated human judgment without rewriting the
// ToolOutcome into deterministic evidence.
func (Engine) EvaluateHuman(c core.CompletionContract, o core.ToolOutcome, approved bool) Result {
	return evaluate(c, o, &approved)
}

// EvaluateHumanTask validates required structured fields and artifact evidence.
// It checks contract satisfaction, not the truth of a user's statements.
func (Engine) EvaluateHumanTask(c core.CompletionContract, submission core.HumanTaskSubmission) Result {
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
	byRole := make(map[string][]core.ArtifactEvidence)
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
	return Result{Complete: len(reasons) == 0, Reasons: reasons}
}

func validArtifactEvidence(artifact core.ArtifactEvidence) bool {
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

func evaluate(c core.CompletionContract, o core.ToolOutcome, humanApproved *bool) Result {
	var reasons []string
	if o.Status != core.OutcomeSucceeded {
		reasons = append(reasons, "tool outcome did not succeed")
	}
	for _, criterion := range c.Criteria {
		if !criterion.Required {
			continue
		}
		switch criterion.Assurance {
		case core.AssuranceDeterministic:
			if o.PostconditionStatus != core.PostconditionVerified {
				reasons = append(reasons, "postcondition is not verified for criterion "+criterion.ID)
			}
		case core.AssuranceHumanJudgment:
			if humanApproved == nil {
				reasons = append(reasons, "human judgment is required for criterion "+criterion.ID)
			} else if !*humanApproved {
				reasons = append(reasons, "human judgment rejected criterion "+criterion.ID)
			}
		default:
			reasons = append(reasons, "unsupported assurance for criterion "+criterion.ID)
		}
	}
	availableArtifacts := make(map[string]struct{}, len(o.ArtifactRefs))
	for _, ref := range o.ArtifactRefs {
		availableArtifacts[ref] = struct{}{}
	}
	for _, required := range c.RequiredArtifacts {
		if _, ok := availableArtifacts[required]; !ok {
			reasons = append(reasons, "required artifact is missing: "+required)
		}
	}
	return Result{Complete: len(reasons) == 0, Reasons: reasons}
}

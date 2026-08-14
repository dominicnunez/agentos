package core

import (
	"reflect"
	"slices"
	"strings"
	"time"
)

const ExperimentTrustUnverified = "EXPERIMENTAL_UNVERIFIED"

const (
	ExperimentFailureWorkFailed          = "WORK_FAILED"
	ExperimentFailureBudgetExceeded      = "BUDGET_EXCEEDED"
	ExperimentFailureContainmentViolated = "CONTAINMENT_VIOLATED"
)

type ExperimentStatus string

const (
	ExperimentRunning   ExperimentStatus = "RUNNING"
	ExperimentCompleted ExperimentStatus = "COMPLETED"
	ExperimentFailed    ExperimentStatus = "FAILED"
)

// ExperimentBudget is a closed, explicit ceiling. A zero metered-cost limit
// means that the experiment may not consume metered inference.
type ExperimentBudget struct {
	MaxExecutions            int      `json:"max_executions"`
	MaxUsageUnits            int64    `json:"max_usage_units"`
	MaxMeteredCostMicrounits int64    `json:"max_metered_cost_microunits"`
	MaxWallTimeSeconds       int64    `json:"max_wall_time_seconds"`
	MaxChildren              int      `json:"max_children"`
	AllowedInferencePools    []string `json:"allowed_inference_pools"`
}

// Experiment composes one existing bounded Work with containment and resource
// ceilings. Its trust label is deliberately terminal: completion does not make
// an experimental result trusted or active.
type Experiment struct {
	ID                   ID               `json:"id"`
	OrganizationID       ID               `json:"organization_id"`
	WorkID               ID               `json:"work_id"`
	Objective            string           `json:"objective"`
	SandboxRef           string           `json:"sandbox_ref"`
	CapabilityProfileRef string           `json:"capability_profile_ref"`
	Budget               ExperimentBudget `json:"budget"`
	Status               ExperimentStatus `json:"status"`
	TrustLabel           string           `json:"trust_label"`
	ResultEventRefs      []string         `json:"result_event_refs,omitempty"`
	ArtifactRefs         []string         `json:"artifact_refs,omitempty"`
	FailureCode          string           `json:"failure_code,omitempty"`
	StartedAt            time.Time        `json:"started_at"`
	FinishedAt           *time.Time       `json:"finished_at,omitempty"`
}

type PromotionTargetKind string

const (
	PromotionTargetKnowledge     PromotionTargetKind = "KNOWLEDGE"
	PromotionTargetSkill         PromotionTargetKind = "SKILL"
	PromotionTargetConfiguration PromotionTargetKind = "CONFIGURATION"
)

const PromotionCandidateStatus = "CANDIDATE"

// PromotionCandidate is only a nomination. It has no lifecycle state capable
// of activating knowledge, skills, configuration, authority, or effects.
type PromotionCandidate struct {
	ID                        ID                  `json:"id"`
	OrganizationID            ID                  `json:"organization_id"`
	ExperimentID              ID                  `json:"experiment_id"`
	ExperimentVersion         int                 `json:"experiment_version"`
	TargetKind                PromotionTargetKind `json:"target_kind"`
	TargetRef                 string              `json:"target_ref"`
	Summary                   string              `json:"summary"`
	ExperimentResultEventRefs []string            `json:"experiment_result_event_refs"`
	ReproductionEvidenceRefs  []string            `json:"reproduction_evidence_refs"`
	NominatedBy               ID                  `json:"nominated_by"`
	Status                    string              `json:"status"`
	CreatedAt                 time.Time           `json:"created_at"`
}

func ValidExperimentBudget(budget ExperimentBudget) bool {
	return budget.MaxExecutions > 0 && budget.MaxUsageUnits > 0 && budget.MaxMeteredCostMicrounits >= 0 &&
		budget.MaxWallTimeSeconds > 0 && budget.MaxChildren >= 0 && validCanonicalStrings(budget.AllowedInferencePools)
}

func ValidExperiment(experiment Experiment) bool {
	if experiment.ID == "" || experiment.OrganizationID == "" || experiment.WorkID == "" ||
		strings.TrimSpace(experiment.Objective) == "" || strings.TrimSpace(experiment.SandboxRef) == "" ||
		strings.TrimSpace(experiment.CapabilityProfileRef) == "" || experiment.TrustLabel != ExperimentTrustUnverified ||
		experiment.StartedAt.IsZero() || !ValidExperimentBudget(experiment.Budget) ||
		!validCanonicalStringsOrEmpty(experiment.ResultEventRefs) || !validCanonicalStringsOrEmpty(experiment.ArtifactRefs) {
		return false
	}
	switch experiment.Status {
	case ExperimentRunning:
		return experiment.FinishedAt == nil && len(experiment.ResultEventRefs) == 0 && len(experiment.ArtifactRefs) == 0 && experiment.FailureCode == ""
	case ExperimentCompleted:
		return validFinishedExperiment(experiment) && len(experiment.ResultEventRefs) > 0 && experiment.FailureCode == ""
	case ExperimentFailed:
		validFailure := experiment.FailureCode == ExperimentFailureWorkFailed || experiment.FailureCode == ExperimentFailureBudgetExceeded || experiment.FailureCode == ExperimentFailureContainmentViolated
		return validFinishedExperiment(experiment) && len(experiment.ResultEventRefs) == 0 && len(experiment.ArtifactRefs) == 0 && validFailure
	default:
		return false
	}
}

func ValidExperimentRevision(previous, next Experiment) bool {
	if !ValidExperiment(previous) || !ValidExperiment(next) || previous.ID != next.ID ||
		previous.OrganizationID != next.OrganizationID || previous.WorkID != next.WorkID || previous.Objective != next.Objective ||
		previous.SandboxRef != next.SandboxRef || previous.CapabilityProfileRef != next.CapabilityProfileRef ||
		!reflect.DeepEqual(previous.Budget, next.Budget) || previous.TrustLabel != next.TrustLabel || !previous.StartedAt.Equal(next.StartedAt) {
		return false
	}
	if previous.Status != ExperimentRunning || next.Status != ExperimentCompleted && next.Status != ExperimentFailed {
		return false
	}
	return true
}

func ValidPromotionCandidate(candidate PromotionCandidate) bool {
	validTarget := candidate.TargetKind == PromotionTargetKnowledge || candidate.TargetKind == PromotionTargetSkill || candidate.TargetKind == PromotionTargetConfiguration
	if candidate.ID == "" || candidate.OrganizationID == "" || candidate.ExperimentID == "" || candidate.ExperimentVersion < 2 ||
		!validTarget || strings.TrimSpace(candidate.TargetRef) == "" || strings.TrimSpace(candidate.TargetRef) != candidate.TargetRef ||
		strings.TrimSpace(candidate.Summary) == "" || strings.TrimSpace(candidate.Summary) != candidate.Summary ||
		candidate.NominatedBy == "" || candidate.Status != PromotionCandidateStatus || candidate.CreatedAt.IsZero() ||
		!validCanonicalStrings(candidate.ExperimentResultEventRefs) || !slices.IsSorted(candidate.ExperimentResultEventRefs) ||
		!validCanonicalStrings(candidate.ReproductionEvidenceRefs) || !slices.IsSorted(candidate.ReproductionEvidenceRefs) {
		return false
	}
	for _, ref := range candidate.ReproductionEvidenceRefs {
		if slices.Contains(candidate.ExperimentResultEventRefs, ref) {
			return false
		}
	}
	return true
}

func validFinishedExperiment(experiment Experiment) bool {
	return experiment.FinishedAt != nil && !experiment.FinishedAt.IsZero() && !experiment.FinishedAt.Before(experiment.StartedAt)
}

func validCanonicalStrings(values []string) bool {
	return len(values) > 0 && validCanonicalStringsOrEmpty(values)
}

func validCanonicalStringsOrEmpty(values []string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" || strings.TrimSpace(value) != value {
			return false
		}
		if _, found := seen[value]; found {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}

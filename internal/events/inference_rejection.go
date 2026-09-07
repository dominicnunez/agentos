package events

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/dominicnunez/agentos/internal/core"
)

// InferenceRouteRejectedPayload is diagnostic evidence, not an authorization,
// reservation, or proof that all possible provider configurations were searched.
// The origin links runtime work or intake evidence without copying its content.
type InferenceRouteRejectedPayload struct {
	Version                 int    `json:"version"`
	Purpose                 string `json:"purpose"`
	OriginEventRef          string `json:"origin_event_ref"`
	PlannedTaskKey          string `json:"planned_task_key,omitempty"`
	Category                string `json:"category"`
	RequirementsFingerprint string `json:"requirements_fingerprint,omitempty"`
}

func ValidateInferenceRouteRejection(d TrustedDraft) error {
	var p InferenceRouteRejectedPayload
	if d.EventType != "INFERENCE_ROUTE_REJECTED" || d.OrganizationID == "" || d.CorrelationID == "" ||
		d.SourceActorID != "runtime" || d.SourceExecutionID != "" || d.TaskID != "" ||
		d.RecipientScope != "" || d.RecipientID != "" || len(d.AuthorizationRefs) != 0 || len(d.ArtifactRefs) != 0 ||
		decodeExactPayload(d.Payload, &p) != nil || p.Version != 1 || len(p.OriginEventRef) == 0 || len(p.OriginEventRef) > 128 {
		return fmt.Errorf("invalid inference route rejection envelope")
	}
	switch p.Purpose {
	case "TASK_ASSIGNMENT":
		if len(p.PlannedTaskKey) == 0 || len(p.PlannedTaskKey) > 128 {
			return fmt.Errorf("route rejection needs a bounded planned task key")
		}
	case "PLANNING", "INTENT_NORMALIZATION":
		if p.PlannedTaskKey != "" {
			return fmt.Errorf("auxiliary rejection cannot identify a planned task")
		}
	default:
		return fmt.Errorf("unknown route rejection purpose")
	}
	switch p.Category {
	case "INVALID_REQUIREMENTS", "INVALID_CATALOG", "POLICY_DENIED", "NO_ELIGIBLE_ACCOUNT", "SHARED_BUDGET_EXHAUSTED", "CANCELED", "SELECTION_UNAVAILABLE":
	default:
		return fmt.Errorf("unknown route rejection category")
	}
	if p.RequirementsFingerprint != "" {
		if b, err := hex.DecodeString(p.RequirementsFingerprint); err != nil || len(b) != 32 || strings.ToLower(p.RequirementsFingerprint) != p.RequirementsFingerprint {
			return fmt.Errorf("invalid rejected requirements fingerprint")
		}
	}
	return nil
}

// ValidateInferenceRouteRejectionOrigin binds diagnostic attribution to earlier
// evidence in the same organization and work stream. It grants no authority.
func ValidateInferenceRouteRejectionOrigin(d TrustedDraft, origin Event) error {
	if err := ValidateInferenceRouteRejection(d); err != nil {
		return err
	}
	var p InferenceRouteRejectedPayload
	if err := decodeExactPayload(d.Payload, &p); err != nil {
		return err
	}
	if origin.EventID != p.OriginEventRef || origin.OrganizationID != d.OrganizationID || origin.CorrelationID != d.CorrelationID {
		return fmt.Errorf("route rejection origin crosses work boundary")
	}
	switch p.Purpose {
	case "TASK_ASSIGNMENT":
		var plan core.Plan
		if origin.EventType != "PLAN_CREATED" || decodeExactPayload(origin.Payload, &plan) != nil {
			return fmt.Errorf("route rejection needs its durable plan")
		}
		for _, task := range plan.Tasks {
			if task.Key == p.PlannedTaskKey && task.ExecutionKind == core.ExecutionAgent {
				return nil
			}
		}
		return fmt.Errorf("route rejection does not identify an agent task in its plan")
	case "PLANNING":
		if origin.EventType != "WORK_CREATED" {
			return fmt.Errorf("planning rejection needs durable work")
		}
	case "INTENT_NORMALIZATION":
		if origin.EventType != "INTAKE_MESSAGE_RECORDED" {
			return fmt.Errorf("normalization rejection needs its intake message")
		}
	}
	return nil
}

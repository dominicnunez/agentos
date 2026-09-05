package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/dominicnunez/agentos/internal/boundaryjson"
)

// FingerprintEffect binds every immutable field that identifies, authorizes,
// describes, or replays an effect. Runtime status, attempt, timestamp, and
// evidence fields are deliberately excluded because they change as the same
// obligation advances through execution.
func FingerprintEffect(obligation EffectObligation) (string, error) {
	canonical, err := json.Marshal(struct {
		ID                       ID                               `json:"effect_obligation_id"`
		OrganizationID           ID                               `json:"organization_id"`
		TaskID                   ID                               `json:"task_id"`
		ActorID                  ID                               `json:"actor_id"`
		ActorKind                PrincipalKind                    `json:"actor_kind"`
		Action                   string                           `json:"action"`
		Resource                 string                           `json:"resource"`
		Scope                    string                           `json:"scope"`
		ConsequenceBoundary      string                           `json:"consequence_boundary"`
		Descriptor               string                           `json:"canonical_effect_descriptor"`
		AuthorizationRefs        []string                         `json:"authorization_refs"`
		ApprovalRef              string                           `json:"approval_ref"`
		IdempotencyKey           string                           `json:"idempotency_key"`
		ReplayContext            map[string]string                `json:"replay_context"`
		RequiredCapabilities     []CapabilityRequirement          `json:"required_capabilities,omitempty"`
		ToolDefinition           *ToolDefinitionBinding           `json:"tool_definition,omitempty"`
		Influence                *ActionInfluenceBinding          `json:"influence,omitempty"`
		Trajectory               *EffectTrajectory                `json:"effect_trajectory,omitempty"`
		CodeIntroduction         *CodeIntroductionBinding         `json:"code_introduction,omitempty"`
		ExecutionSurfaceMutation *ExecutionSurfaceMutationBinding `json:"execution_surface_mutation,omitempty"`
	}{
		ID: obligation.ID, OrganizationID: obligation.OrganizationID, TaskID: obligation.TaskID,
		ActorID: obligation.ActorID, ActorKind: obligation.ActorKind, Action: obligation.Action, Resource: obligation.Resource,
		Scope: obligation.Scope, ConsequenceBoundary: obligation.ConsequenceBoundary,
		Descriptor: obligation.Descriptor, AuthorizationRefs: obligation.AuthorizationRefs,
		ApprovalRef: obligation.ApprovalRef, IdempotencyKey: obligation.IdempotencyKey,
		ReplayContext:            obligation.ReplayContext,
		RequiredCapabilities:     obligation.RequiredCapabilities,
		ToolDefinition:           obligation.ToolDefinition,
		Influence:                obligation.Influence,
		Trajectory:               obligation.Trajectory,
		CodeIntroduction:         obligation.CodeIntroduction,
		ExecutionSurfaceMutation: obligation.ExecutionSurfaceMutation,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

// DecodeEffectObligation rejects unknown or trailing security-critical state.
// Effect records are authority-bearing contracts, not extensible content.
func DecodeEffectObligation(body []byte) (EffectObligation, error) {
	var obligation EffectObligation
	if err := boundaryjson.Unmarshal(body, &obligation); err != nil {
		return EffectObligation{}, err
	}
	return obligation, nil
}

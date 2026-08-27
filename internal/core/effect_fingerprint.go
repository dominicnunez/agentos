package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// FingerprintEffect binds every immutable field that identifies, authorizes,
// describes, or replays an effect. Runtime status, attempt, timestamp, and
// evidence fields are deliberately excluded because they change as the same
// obligation advances through execution.
func FingerprintEffect(obligation EffectObligation) (string, error) {
	canonical, err := json.Marshal(struct {
		ID                  ID                `json:"effect_obligation_id"`
		OrganizationID      ID                `json:"organization_id"`
		TaskID              ID                `json:"task_id"`
		ActorID             ID                `json:"actor_id"`
		Action              string            `json:"action"`
		Resource            string            `json:"resource"`
		Scope               string            `json:"scope"`
		ConsequenceBoundary string            `json:"consequence_boundary"`
		Descriptor          string            `json:"canonical_effect_descriptor"`
		AuthorizationRefs   []string          `json:"authorization_refs"`
		ApprovalRef         string            `json:"approval_ref"`
		IdempotencyKey      string            `json:"idempotency_key"`
		ReplayContext       map[string]string `json:"replay_context"`
	}{
		ID: obligation.ID, OrganizationID: obligation.OrganizationID, TaskID: obligation.TaskID,
		ActorID: obligation.ActorID, Action: obligation.Action, Resource: obligation.Resource,
		Scope: obligation.Scope, ConsequenceBoundary: obligation.ConsequenceBoundary,
		Descriptor: obligation.Descriptor, AuthorizationRefs: obligation.AuthorizationRefs,
		ApprovalRef: obligation.ApprovalRef, IdempotencyKey: obligation.IdempotencyKey,
		ReplayContext: obligation.ReplayContext,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

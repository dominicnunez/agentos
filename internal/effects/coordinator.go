// Package effects provides persist-before-effect outbox coordination.
package effects

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

type Records interface {
	AppendRecord(context.Context, string, string, string, string, []string, []string, string, string, int, any) error
	ConsumeApproval(context.Context, string, string, string, string, string) error
}
type Adapter interface {
	Apply(context.Context, core.EffectObligation) ([]string, error)
}
type Coordinator struct {
	records Records
	adapter Adapter
}

func New(r Records, a Adapter) *Coordinator { return &Coordinator{r, a} }
func Fingerprint(action, resource string, args any) (string, error) {
	b, e := json.Marshal(struct {
		Action    string `json:"action"`
		Resource  string `json:"resource"`
		Arguments any    `json:"arguments"`
	}{action, resource, args})
	if e != nil {
		return "", e
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
func (c *Coordinator) Execute(ctx context.Context, o core.EffectObligation, approval *core.HumanApproval) (core.EffectObligation, error) {
	if o.ID == "" || o.EffectFingerprint == "" || o.IdempotencyKey == "" {
		return o, fmt.Errorf("effect id, fingerprint, and idempotency key are required")
	}
	if approval != nil && (approval.Status != "APPROVED" || approval.EffectFingerprint != o.EffectFingerprint || (approval.ExpiresAt != nil && !time.Now().Before(*approval.ExpiresAt))) {
		return o, fmt.Errorf("approval does not authorize exact effect")
	}
	if approval != nil && approval.SingleUse {
		if err := c.records.ConsumeApproval(ctx, string(o.OrganizationID), string(o.TaskID), string(approval.ID), o.EffectFingerprint, string(o.ID)); err != nil {
			return o, fmt.Errorf("single-use approval unavailable: %w", err)
		}
	}
	o.Status = core.EffectPending
	o.CreatedAt = time.Now().UTC()
	if e := c.record(ctx, o, 1); e != nil {
		return o, e
	}
	now := time.Now().UTC()
	o.Status = core.EffectAttempted
	o.AttemptCount++
	o.LastAttemptAt = &now
	if e := c.record(ctx, o, 2); e != nil {
		return o, e
	}
	evidence, e := c.adapter.Apply(ctx, o)
	if e != nil {
		o.Status = core.EffectFailed
		_ = c.record(ctx, o, 3)
		return o, e
	}
	o.Status = core.EffectConfirmed
	o.ConfirmationEvidenceRefs = evidence
	if e = c.record(ctx, o, 3); e != nil {
		return o, e
	}
	return o, nil
}

func (c *Coordinator) record(ctx context.Context, o core.EffectObligation, version int) error {
	return c.records.AppendRecord(ctx, string(o.OrganizationID), "EFFECT_OBLIGATION_TRANSITIONED", "", string(o.TaskID), o.AuthorizationRefs, o.ConfirmationEvidenceRefs, "effect", string(o.ID), version, o)
}

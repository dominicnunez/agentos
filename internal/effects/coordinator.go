// Package effects provides persist-before-effect outbox coordination.
package effects

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/dominicnunez/agentos/internal/core"
	"time"
)

type Records interface {
	PutRecord(context.Context, string, string, int, any) error
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
	o.Status = core.EffectPending
	o.CreatedAt = time.Now().UTC()
	if e := c.records.PutRecord(ctx, "effect", string(o.ID), 1, o); e != nil {
		return o, e
	}
	now := time.Now().UTC()
	o.Status = core.EffectAttempted
	o.AttemptCount++
	o.LastAttemptAt = &now
	if e := c.records.PutRecord(ctx, "effect", string(o.ID), 2, o); e != nil {
		return o, e
	}
	evidence, e := c.adapter.Apply(ctx, o)
	if e != nil {
		o.Status = core.EffectFailed
		_ = c.records.PutRecord(ctx, "effect", string(o.ID), 3, o)
		return o, e
	}
	o.Status = core.EffectConfirmed
	o.ConfirmationEvidenceRefs = evidence
	if e = c.records.PutRecord(ctx, "effect", string(o.ID), 3, o); e != nil {
		return o, e
	}
	return o, nil
}

// Package effects provides persist-before-effect outbox coordination.
package effects

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/dominicnunez/agentos/internal/approvals"
	"github.com/dominicnunez/agentos/internal/core"
)

type Records interface {
	AppendRecord(context.Context, string, string, string, string, []string, []string, string, string, int, any) error
	ConsumeApprovalAndAppendRecord(context.Context, string, string, string, string, string, []string, []string, string, string, int, any) error
	Records(context.Context, string, string) ([][]byte, error)
}
type Adapter interface {
	Apply(context.Context, core.EffectObligation) ([]string, error)
}
type ApprovalReader interface {
	Get(context.Context, core.ID) (core.HumanApproval, error)
}
type Coordinator struct {
	records   Records
	adapter   Adapter
	approvals ApprovalReader
}

func NewWithApprovals(r Records, a Adapter, approvalReader ApprovalReader) *Coordinator {
	return &Coordinator{records: r, adapter: a, approvals: approvalReader}
}

var (
	ErrEffectNotPrepared = errors.New("protected effect obligation is not prepared")
	ErrEffectUncertain   = errors.New("effect attempt has uncertain outcome")
)

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

// Prepare persists the complete protected intent before notification or human
// decision. Retrying the same preparation is idempotent; changing any material
// replay field fails closed under the existing obligation identity.
func (c *Coordinator) Prepare(ctx context.Context, obligation core.EffectObligation) (core.EffectObligation, error) {
	if err := validateObligation(obligation); err != nil {
		return obligation, err
	}
	if c == nil || c.records == nil {
		return obligation, fmt.Errorf("durable effect records are required")
	}
	requiresApproval, err := approvals.RequiresHumanApproval(obligation.ConsequenceBoundary)
	if err != nil {
		return obligation, err
	}
	if !requiresApproval || obligation.ApprovalRef == "" {
		return obligation, fmt.Errorf("protected effect and approval reference are required")
	}
	if obligation.Descriptor == "" || obligation.ReplayContext == nil {
		return obligation, fmt.Errorf("protected effect descriptor and replay context are required")
	}
	stored, _, err := c.load(ctx, obligation.ID)
	if err == nil {
		if !sameEffectIntent(stored, obligation) {
			return obligation, fmt.Errorf("effect obligation identity already has different intent")
		}
		return stored, nil
	}
	if !errors.Is(err, ErrEffectNotPrepared) {
		return obligation, err
	}
	obligation.Status = core.EffectPending
	obligation.CreatedAt = time.Now().UTC()
	obligation.AttemptCount = 0
	obligation.LastAttemptAt = nil
	obligation.ConfirmationEvidenceRefs = nil
	if err := c.record(ctx, obligation, 1); err != nil {
		return obligation, err
	}
	return obligation, nil
}

func (c *Coordinator) Execute(ctx context.Context, o core.EffectObligation) (core.EffectObligation, error) {
	if err := validateObligation(o); err != nil {
		return o, err
	}
	if c == nil || c.records == nil {
		return o, fmt.Errorf("durable effect records are required")
	}
	requiresApproval, err := approvals.RequiresHumanApproval(o.ConsequenceBoundary)
	if err != nil {
		return o, err
	}
	version := 0
	if requiresApproval {
		stored, storedVersion, err := c.load(ctx, o.ID)
		if err != nil {
			return o, err
		}
		if !sameEffectIntent(stored, o) {
			return o, fmt.Errorf("prepared effect does not match requested execution")
		}
		o, version = stored, storedVersion
		switch o.Status {
		case core.EffectConfirmed:
			return o, nil
		case core.EffectAttempted:
			return o, ErrEffectUncertain
		case core.EffectFailed, core.EffectCancelled:
			return o, fmt.Errorf("effect cannot execute from %s", o.Status)
		case core.EffectPending:
		default:
			return o, fmt.Errorf("unknown effect status %q", o.Status)
		}
	}
	var approval core.HumanApproval
	if requiresApproval || o.ApprovalRef != "" {
		if o.ApprovalRef == "" || c.approvals == nil {
			return o, fmt.Errorf("%w: durable approval is required", approvals.ErrApprovalPending)
		}
		approval, err = c.approvals.Get(ctx, core.ID(o.ApprovalRef))
		if err != nil {
			return o, fmt.Errorf("load durable approval: %w", err)
		}
		if err := validateApproval(o, approval, time.Now().UTC()); err != nil {
			if errors.Is(err, approvals.ErrApprovalDenied) && version > 0 {
				o.Status = core.EffectCancelled
				if recordErr := c.record(ctx, o, version+1); recordErr != nil {
					return o, recordErr
				}
			}
			return o, err
		}
	}
	if version == 0 {
		o.Status = core.EffectPending
		o.CreatedAt = time.Now().UTC()
		if err := c.record(ctx, o, 1); err != nil {
			return o, err
		}
		version = 1
	}
	if c.adapter == nil {
		return o, fmt.Errorf("effect adapter is required")
	}
	now := time.Now().UTC()
	o.Status = core.EffectAttempted
	o.AttemptCount++
	o.LastAttemptAt = &now
	if approval.SingleUse {
		if err := c.records.ConsumeApprovalAndAppendRecord(ctx, string(o.OrganizationID), string(o.TaskID), string(approval.ID), o.EffectFingerprint, string(o.ID), o.AuthorizationRefs, o.ConfirmationEvidenceRefs, "effect", string(o.ID), version+1, o); err != nil {
			return o, fmt.Errorf("single-use approval unavailable: %w", err)
		}
	} else if err := c.record(ctx, o, version+1); err != nil {
		return o, err
	}
	evidence, err := c.adapter.Apply(ctx, o)
	if err != nil {
		o.Status = core.EffectFailed
		_ = c.record(ctx, o, version+2)
		return o, err
	}
	o.Status = core.EffectConfirmed
	o.ConfirmationEvidenceRefs = evidence
	if err = c.record(ctx, o, version+2); err != nil {
		return o, err
	}
	return o, nil
}

func (c *Coordinator) load(ctx context.Context, effectID core.ID) (core.EffectObligation, int, error) {
	if c == nil || c.records == nil || effectID == "" {
		return core.EffectObligation{}, 0, ErrEffectNotPrepared
	}
	rows, err := c.records.Records(ctx, "effect", string(effectID))
	if err != nil {
		return core.EffectObligation{}, 0, err
	}
	if len(rows) == 0 {
		return core.EffectObligation{}, 0, ErrEffectNotPrepared
	}
	var obligation core.EffectObligation
	if err := json.Unmarshal(rows[len(rows)-1], &obligation); err != nil {
		return core.EffectObligation{}, 0, fmt.Errorf("decode effect obligation %s: %w", effectID, err)
	}
	return obligation, len(rows), nil
}

func validateObligation(obligation core.EffectObligation) error {
	if obligation.ID == "" || obligation.OrganizationID == "" || obligation.TaskID == "" || obligation.Action == "" || obligation.Resource == "" || obligation.EffectFingerprint == "" || obligation.IdempotencyKey == "" {
		return fmt.Errorf("effect identity, organization, task, action, resource, fingerprint, and idempotency key are required")
	}
	return nil
}

func sameEffectIntent(stored, requested core.EffectObligation) bool {
	return stored.ID == requested.ID &&
		stored.OrganizationID == requested.OrganizationID &&
		stored.TaskID == requested.TaskID &&
		stored.Action == requested.Action &&
		stored.Resource == requested.Resource &&
		stored.ConsequenceBoundary == requested.ConsequenceBoundary &&
		stored.Descriptor == requested.Descriptor &&
		stored.EffectFingerprint == requested.EffectFingerprint &&
		stored.ApprovalRef == requested.ApprovalRef &&
		stored.IdempotencyKey == requested.IdempotencyKey &&
		reflect.DeepEqual(stored.ReplayContext, requested.ReplayContext)
}

func validateApproval(obligation core.EffectObligation, approval core.HumanApproval, now time.Time) error {
	switch approval.Status {
	case core.ApprovalPending, core.ApprovalNotified, core.ApprovalAcknowledged, core.ApprovalPendingDecision:
		return approvals.ErrApprovalPending
	case core.ApprovalDenied:
		return approvals.ErrApprovalDenied
	case core.ApprovalApproved:
	default:
		return fmt.Errorf("unknown approval status %q", approval.Status)
	}
	if approval.OrganizationID != obligation.OrganizationID || approval.TaskID != obligation.TaskID || approval.EffectObligationID != obligation.ID || approval.Action != obligation.Action || approval.Resource != obligation.Resource || approval.Boundary != obligation.ConsequenceBoundary || approval.EffectFingerprint != obligation.EffectFingerprint {
		return fmt.Errorf("approval does not authorize exact effect")
	}
	if approval.ExpiresAt != nil && !now.Before(*approval.ExpiresAt) {
		return fmt.Errorf("approval has expired")
	}
	return nil
}

func (c *Coordinator) record(ctx context.Context, o core.EffectObligation, version int) error {
	return c.records.AppendRecord(ctx, string(o.OrganizationID), "EFFECT_OBLIGATION_TRANSITIONED", "", string(o.TaskID), o.AuthorizationRefs, o.ConfirmationEvidenceRefs, "effect", string(o.ID), version, o)
}


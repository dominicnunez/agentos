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
	"slices"
	"time"

	"github.com/dominicnunez/agentos/internal/approvals"
	"github.com/dominicnunez/agentos/internal/core"
)

type Records interface {
	AppendRecord(context.Context, string, string, string, string, []string, []string, string, string, int, any) error
	AuthorizeAndAppendEffectAttempt(context.Context, core.EffectObligation, int, any) (core.AuthorizationTrace, error)
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

func New(r Records, a Adapter, approvalReader ApprovalReader) *Coordinator {
	return &Coordinator{records: r, adapter: a, approvals: approvalReader}
}

var (
	ErrEffectNotPrepared  = errors.New("effect obligation is not persisted")
	ErrEffectUncertain    = errors.New("effect attempt has uncertain outcome")
	ErrEffectUnauthorized = errors.New("effect is not authorized at time of use")
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
	requiresApproval, err := c.validateAndClassify(obligation)
	if err != nil {
		return obligation, err
	}
	if !requiresApproval || obligation.ApprovalRef == "" {
		return obligation, fmt.Errorf("protected effect and approval reference are required")
	}
	if obligation.Descriptor == "" || obligation.ReplayContext == nil {
		return obligation, fmt.Errorf("protected effect descriptor and replay context are required")
	}
	expectedFingerprint, err := Fingerprint(obligation.Action, obligation.Resource, obligation.ReplayContext)
	if err != nil {
		return obligation, fmt.Errorf("fingerprint persisted effect arguments: %w", err)
	}
	if obligation.EffectFingerprint != expectedFingerprint {
		return obligation, fmt.Errorf("effect fingerprint does not match persisted replay context")
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
	requiresApproval, err := c.validateAndClassify(o)
	if err != nil {
		return o, err
	}
	version := 0
	stored, storedVersion, loadErr := c.load(ctx, o.ID)
	if loadErr == nil {
		if !sameEffectIntent(stored, o) {
			return o, fmt.Errorf("persisted effect does not match requested execution")
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
	} else if !errors.Is(loadErr, ErrEffectNotPrepared) {
		return o, loadErr
	} else if requiresApproval {
		return o, ErrEffectNotPrepared
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
		if err := approvals.ValidateForEffect(approval, o, time.Now().UTC()); err != nil {
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
	attempt := o
	now := time.Now().UTC()
	attempt.Status = core.EffectAttempted
	attempt.AttemptCount++
	attempt.LastAttemptAt = &now
	trace, err := c.records.AuthorizeAndAppendEffectAttempt(ctx, o, version+1, attempt)
	if err != nil {
		return o, fmt.Errorf("begin authorized effect attempt: %w", err)
	}
	if !trace.Allowed {
		return o, fmt.Errorf("%w: %s", ErrEffectUnauthorized, trace.Reason)
	}
	o = attempt
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
	if obligation.ID == "" || obligation.OrganizationID == "" || obligation.TaskID == "" || obligation.ActorID == "" || obligation.Action == "" || obligation.Resource == "" || obligation.Scope == "" || obligation.EffectFingerprint == "" || obligation.IdempotencyKey == "" || len(obligation.AuthorizationRefs) == 0 {
		return fmt.Errorf("effect identity, organization, task, actor, action, resource, scope, authorization, fingerprint, and idempotency key are required")
	}
	return nil
}

func (c *Coordinator) validateAndClassify(obligation core.EffectObligation) (bool, error) {
	if err := validateObligation(obligation); err != nil {
		return false, err
	}
	if c == nil || c.records == nil {
		return false, fmt.Errorf("durable effect records are required")
	}
	return approvals.RequiresHumanApproval(obligation.ConsequenceBoundary)
}

func sameEffectIntent(stored, requested core.EffectObligation) bool {
	return stored.ID == requested.ID &&
		stored.OrganizationID == requested.OrganizationID &&
		stored.TaskID == requested.TaskID &&
		stored.ActorID == requested.ActorID &&
		stored.Action == requested.Action &&
		stored.Resource == requested.Resource &&
		stored.Scope == requested.Scope &&
		stored.ConsequenceBoundary == requested.ConsequenceBoundary &&
		stored.Descriptor == requested.Descriptor &&
		stored.EffectFingerprint == requested.EffectFingerprint &&
		stored.ApprovalRef == requested.ApprovalRef &&
		stored.IdempotencyKey == requested.IdempotencyKey &&
		slices.Equal(stored.AuthorizationRefs, requested.AuthorizationRefs) &&
		reflect.DeepEqual(stored.ReplayContext, requested.ReplayContext)
}

func (c *Coordinator) record(ctx context.Context, o core.EffectObligation, version int) error {
	return c.records.AppendRecord(ctx, string(o.OrganizationID), "EFFECT_OBLIGATION_TRANSITIONED", "", string(o.TaskID), o.AuthorizationRefs, o.ConfirmationEvidenceRefs, "effect", string(o.ID), version, o)
}


package effects

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

type ReconciliationState string

const (
	ReconciliationUnknown   ReconciliationState = "UNKNOWN"
	ReconciliationConfirmed ReconciliationState = "CONFIRMED"
	ReconciliationFailed    ReconciliationState = "FAILED"
)

type RecoveryDisposition string

const (
	RecoveryUncertain RecoveryDisposition = "UNCERTAIN"
	RecoveryConfirmed RecoveryDisposition = "CONFIRMED"
	RecoveryFailed    RecoveryDisposition = "FAILED"
)

// ReconciliationObservation is read-only destination evidence about an
// already-attempted effect. A terminal observation requires durable evidence.
type ReconciliationObservation struct {
	State        ReconciliationState
	EvidenceRefs []string
}

// Reconciler checks destination status. It must not create, retry, or otherwise
// perform the effect it is inspecting.
type Reconciler interface {
	Check(context.Context, core.EffectObligation) (ReconciliationObservation, error)
}

// ReconcilerResolver binds a durable obligation to the status-check adapter
// that owns its destination. Absence preserves explicit uncertainty.
type ReconcilerResolver interface {
	ReconcilerFor(core.EffectObligation) (Reconciler, bool)
}

type ReconcilerResolverFunc func(core.EffectObligation) (Reconciler, bool)

func (f ReconcilerResolverFunc) ReconcilerFor(obligation core.EffectObligation) (Reconciler, bool) {
	if f == nil {
		return nil, false
	}
	return f(obligation)
}

type RecoveryItem struct {
	EffectID    core.ID
	TaskID      core.ID
	Disposition RecoveryDisposition
	Reason      string
}

type ReconciliationRecords interface {
	recordReader
	LatestRecords(context.Context, string) ([][]byte, error)
	AppendRecord(context.Context, string, string, string, string, []string, []string, string, string, int, any) error
}

type ReconciliationService struct {
	records ReconciliationRecords
	now     func() time.Time
}

func NewReconciliationService(records ReconciliationRecords) *ReconciliationService {
	return &ReconciliationService{records: records, now: time.Now}
}

// Recover discovers current ATTEMPTED obligations and performs only read-only
// status checks. It never invokes an effect Adapter or retries an effect.
func (s *ReconciliationService) Recover(ctx context.Context, resolver ReconcilerResolver) ([]RecoveryItem, error) {
	if s == nil || s.records == nil {
		return nil, fmt.Errorf("durable reconciliation records are required")
	}
	bodies, err := s.records.LatestRecords(ctx, "effect")
	if err != nil {
		return nil, fmt.Errorf("discover effect obligations: %w", err)
	}
	items := make([]RecoveryItem, 0)
	for _, body := range bodies {
		var obligation core.EffectObligation
		if err := json.Unmarshal(body, &obligation); err != nil {
			return nil, fmt.Errorf("decode latest effect obligation: %w", err)
		}
		if obligation.Status != core.EffectAttempted {
			continue
		}
		if err := validateReconciliationObligation(obligation); err != nil {
			return nil, fmt.Errorf("validate attempted effect %s: %w", obligation.ID, err)
		}
		item, err := s.reconcileOne(ctx, obligation, resolver)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func (s *ReconciliationService) reconcileOne(ctx context.Context, discovered core.EffectObligation, resolver ReconcilerResolver) (RecoveryItem, error) {
	item := RecoveryItem{EffectID: discovered.ID, TaskID: discovered.TaskID, Disposition: RecoveryUncertain}
	if resolver == nil {
		item.Reason = "reconciler unavailable"
		return item, nil
	}
	reconciler, ok := resolver.ReconcilerFor(discovered)
	if !ok || reconciler == nil {
		item.Reason = "reconciler unavailable"
		return item, nil
	}
	observation, checked := destinationObservation(ctx, reconciler, discovered)
	if !checked {
		item.Reason = "destination status check failed"
		return item, nil
	}
	if observation.State == ReconciliationUnknown {
		item.Reason = "destination status remains unknown"
		return item, nil
	}
	if observation.State != ReconciliationConfirmed && observation.State != ReconciliationFailed {
		item.Reason = "destination returned an invalid reconciliation state"
		return item, nil
	}
	evidence := normalizedEvidence(observation.EvidenceRefs)
	if len(evidence) == 0 {
		item.Reason = "terminal destination status lacks evidence"
		return item, nil
	}

	current, version, err := loadEffect(ctx, s.records, discovered.ID)
	if err != nil {
		return item, fmt.Errorf("reload attempted effect %s: %w", discovered.ID, err)
	}
	switch current.Status {
	case core.EffectConfirmed:
		item.Disposition = RecoveryConfirmed
		return item, nil
	case core.EffectFailed:
		item.Disposition = RecoveryFailed
		return item, nil
	case core.EffectAttempted:
	case core.EffectPending, core.EffectCancelled:
		item.Reason = "effect is no longer reconcilable"
		return item, nil
	default:
		item.Reason = "effect has an unknown durable status"
		return item, nil
	}
	if !sameAttempt(discovered, current) {
		item.Reason = "effect attempt changed during reconciliation"
		return item, nil
	}

	now := s.now().UTC()
	current.ReconciledAt = &now
	current.ReconciliationEvidenceRefs = evidence
	if observation.State == ReconciliationConfirmed {
		current.Status = core.EffectConfirmed
		current.ConfirmationEvidenceRefs = evidence
		item.Disposition = RecoveryConfirmed
	} else {
		current.Status = core.EffectFailed
		item.Disposition = RecoveryFailed
	}
	if err := s.records.AppendRecord(
		ctx, string(current.OrganizationID), "EFFECT_OBLIGATION_TRANSITIONED", "", string(current.TaskID),
		current.AuthorizationRefs, effectEvidenceRefs(current), "effect", string(current.ID), version+1, current,
	); err != nil {
		latest, _, loadErr := loadEffect(ctx, s.records, current.ID)
		if loadErr == nil && latest.Status == current.Status && slices.Equal(latest.ReconciliationEvidenceRefs, evidence) {
			return item, nil
		}
		return RecoveryItem{}, fmt.Errorf("persist reconciled effect %s: %w", current.ID, err)
	}
	return item, nil
}

// destinationObservation deliberately converts adapter availability errors
// into an unavailable observation. Recovery retains ATTEMPTED in that case.
func destinationObservation(ctx context.Context, reconciler Reconciler, obligation core.EffectObligation) (ReconciliationObservation, bool) {
	observation, err := reconciler.Check(ctx, obligation)
	return observation, err == nil
}

func normalizedEvidence(refs []string) []string {
	result := make([]string, 0, len(refs))
	for _, ref := range refs {
		if ref != "" && !slices.Contains(result, ref) {
			result = append(result, ref)
		}
	}
	return result
}

func sameAttempt(a, b core.EffectObligation) bool {
	if !sameEffectIntent(a, b) || a.AttemptCount != b.AttemptCount {
		return false
	}
	if a.LastAttemptAt == nil || b.LastAttemptAt == nil {
		return a.LastAttemptAt == nil && b.LastAttemptAt == nil
	}
	return a.LastAttemptAt.Equal(*b.LastAttemptAt)
}

package events

import (
	"fmt"
	"reflect"
	"slices"

	"github.com/dominicnunez/agentos/internal/core"
)

// IncidentEffectValue checks the admitted Task link and exact public event
// envelope. It does not establish historical permission or remote dispatch.
func IncidentEffectValue(event Event, tasks map[string]int64) (core.EffectObligation, error) {
	value, err := core.DecodeEffectObligation(event.Payload)
	if err != nil || event.EventType != "EFFECT_OBLIGATION_TRANSITIONED" || string(value.OrganizationID) != event.OrganizationID || string(value.TaskID) != event.TaskID || tasks[event.TaskID] == 0 || event.Sequence <= tasks[event.TaskID] || event.SchemaVersion != SchemaVersion || event.SourceActorID != "" || event.SourceExecutionID != "" || event.CorrelationID != "" || event.RecipientID != "" || event.RecipientScope != "" {
		return core.EffectObligation{}, fmt.Errorf("incident effect crosses its Task identity")
	}
	refs := slices.Clone(value.ConfirmationEvidenceRefs)
	for _, ref := range value.ReconciliationEvidenceRefs {
		if !slices.Contains(refs, ref) {
			refs = append(refs, ref)
		}
	}
	if !slices.Equal(event.AuthorizationRefs, value.AuthorizationRefs) || !slices.Equal(event.ArtifactRefs, refs) {
		return core.EffectObligation{}, fmt.Errorf("incident effect envelope differs from its evidence")
	}
	return value, nil
}

// ValidateIncidentEffect proves ordered identity and state consistency, not
// permission to dispatch or independent external confirmation. Exact backing
// records remain the verified reader's responsibility.
func ValidateIncidentEffect(value, previous core.EffectObligation, first bool) error {
	if value.ID == "" || value.OrganizationID == "" || value.TaskID == "" || value.ActorID == "" || value.Action == "" || value.Resource == "" || value.Scope == "" || value.IdempotencyKey == "" || value.EffectFingerprint == "" || len(value.AuthorizationRefs) == 0 || len(value.AuthorizationRefs) > core.MaximumEffectAuthorizationRefs {
		return fmt.Errorf("incident effect identity is incomplete")
	}
	if err := core.ValidateExecutionAuthorityEffect(value); err != nil {
		return err
	}
	for _, refs := range [][]string{value.AuthorizationRefs, value.ConfirmationEvidenceRefs, value.ReconciliationEvidenceRefs} {
		seen := make(map[string]bool, len(refs))
		for _, ref := range refs {
			if ref == "" || seen[ref] {
				return fmt.Errorf("incident effect has empty or repeated evidence")
			}
			seen[ref] = true
		}
	}
	if (len(value.ReconciliationEvidenceRefs) > 0) != (value.ReconciledAt != nil) {
		return fmt.Errorf("incident effect reconciliation evidence is incomplete")
	}
	if value.Status == core.EffectPending || value.Status == core.EffectAttempted || value.Status == core.EffectCancelled {
		if len(value.ConfirmationEvidenceRefs) != 0 || len(value.ReconciliationEvidenceRefs) != 0 || value.ReconciledAt != nil {
			return fmt.Errorf("incident unfinished effect carries terminal evidence")
		}
	}
	if value.Status == core.EffectFailed && (len(value.ConfirmationEvidenceRefs) != 0 || value.ReconciledAt == nil) {
		return fmt.Errorf("incident failed effect lacks reconciliation or claims confirmation")
	}
	if value.ActorKind != "" {
		fingerprint, err := core.FingerprintEffect(value)
		if err != nil || fingerprint != value.EffectFingerprint || !core.ValidPrincipalKind(value.ActorKind) {
			return fmt.Errorf("incident effect fingerprint is invalid")
		}
	}
	if !first {
		before, err := core.FingerprintEffect(previous)
		after, nextErr := core.FingerprintEffect(value)
		if err != nil || nextErr != nil || before != after {
			return fmt.Errorf("incident effect changes immutable intent")
		}
	}
	switch value.Status {
	case core.EffectPending:
		if !first || value.AttemptCount != 0 || value.LastAttemptAt != nil {
			return fmt.Errorf("incident effect pending history is invalid")
		}
	case core.EffectAttempted:
		if value.AttemptCount != previous.AttemptCount+1 || !first && previous.Status != core.EffectPending {
			return fmt.Errorf("incident effect attempt history is invalid")
		}
	case core.EffectConfirmed, core.EffectFailed:
		if first || previous.Status != core.EffectAttempted || value.AttemptCount != previous.AttemptCount || !reflect.DeepEqual(value.LastAttemptAt, previous.LastAttemptAt) {
			return fmt.Errorf("incident effect terminal history lacks its attempt")
		}
		if value.Status == core.EffectConfirmed && len(value.ConfirmationEvidenceRefs) == 0 {
			return fmt.Errorf("incident effect confirmation lacks evidence")
		}
		if value.ReconciledAt != nil && len(value.ReconciliationEvidenceRefs) == 0 {
			return fmt.Errorf("incident effect reconciliation lacks evidence")
		}
	case core.EffectCancelled:
		if first || previous.Status != core.EffectPending || value.AttemptCount != 0 {
			return fmt.Errorf("incident cancelled effect was not pending")
		}
	default:
		return fmt.Errorf("incident effect status is invalid")
	}
	return nil
}

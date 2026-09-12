package events

import (
	"fmt"

	"github.com/dominicnunez/agentos/internal/core"
)

// ValidateFreezeRecord verifies an exact predecessor rather than trusting a
// command's claimed event identity. Legacy metadata may form a prefix only.
func ValidateFreezeRecord(record, prior AuthorityRecord) error {
	if record.Kind != authorityKindFreeze {
		return fmt.Errorf("freeze record kind is invalid")
	}
	if err := validateAuthorityRecordTransition(record.Kind, record.RecordID, record.Version, record.Body, prior.Body, false); err != nil {
		return err
	}
	var state, before core.FreezeState
	if decodeExactEventJSON(record.Body, &state) != nil {
		return fmt.Errorf("freeze record is invalid")
	}
	if record.Version > 1 {
		if prior.Kind != record.Kind || prior.RecordID != record.RecordID || prior.Version != record.Version-1 ||
			prior.AdmissionEventID == "" || decodeExactEventJSON(prior.Body, &before) != nil {
			return fmt.Errorf("freeze predecessor is invalid")
		}
	} else if prior.Version != 0 || prior.AdmissionEventID != "" {
		return fmt.Errorf("initial freeze has a predecessor")
	}
	if state.Control == nil {
		if before.Control != nil {
			return fmt.Errorf("freeze control evidence cannot be removed")
		}
		return nil
	}
	control := state.Control
	if control.ActorID == "" || control.ActorKind != core.PrincipalHuman ||
		control.PriorVersion != record.Version-1 || control.PriorEventRef != prior.AdmissionEventID {
		return fmt.Errorf("freeze control does not bind its exact predecessor")
	}
	if !state.Frozen && (record.Version == 1 || !before.Frozen) {
		return fmt.Errorf("release requires a current hold")
	}
	return nil
}

package events

import (
	"encoding/hex"
	"fmt"
	"math"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

// DecodeInferenceReservation checks the reservation's own contract. Readers must
// additionally verify its policy, accounting row and prior execution context.
// Legacy library reservations may lack an application context and admission time.
func DecodeInferenceReservation(event Event) (InferenceReservedPayload, error) {
	var p InferenceReservedPayload
	if decodeExactEventJSON(event.Payload, &p) != nil || event.EventType != "INFERENCE_RESERVED" || event.OrganizationID == "" || event.SourceActorID != "runtime" || event.SourceExecutionID == "" || event.CorrelationID == "" || event.RecipientScope != "" || event.RecipientID != "" || len(event.AuthorizationRefs) != 0 || len(event.ArtifactRefs) != 0 || event.SchemaVersion != SchemaVersion {
		return p, fmt.Errorf("inference reservation event is invalid")
	}
	if p.ReservationID == "" || p.RequestID == "" || p.IntentID == "" && event.TaskID == "" || p.Provider == "" || p.Model == "" || p.ExecutionProfileVersion == "" || !inferenceHash(p.PolicyFingerprint) || !inferenceHash(p.PromptSHA256) || p.ConnectionID != "" && !core.ValidInferenceConnectionID(p.ConnectionID) || p.ReservedInputTokens < 1 || p.ReservedOutputTokens < 1 || p.ReservedInputTokens > int64(math.MaxInt) || p.ReservedOutputTokens > int64(math.MaxInt) || p.ReservedInputTokens > math.MaxInt64-p.ReservedOutputTokens || p.ReservedCostNanoUSD < 0 || !p.WindowStartedAt.Before(p.WindowExpiresAt) {
		return p, fmt.Errorf("inference reservation fields are invalid")
	}
	switch p.Purpose {
	case "INTENT_NORMALIZATION", "PLANNING", "TASK_EXECUTION":
	default:
		return p, fmt.Errorf("inference reservation purpose is invalid")
	}
	if p.ExecutionManifestRef != "" && p.RequestID != event.SourceExecutionID {
		return p, fmt.Errorf("inference reservation context identity is invalid")
	}
	var admitted time.Time
	if p.AdmittedAt != "" {
		var err error
		admitted, err = time.Parse(time.RFC3339Nano, p.AdmittedAt)
		if err != nil || admitted.IsZero() || admitted.Before(p.WindowStartedAt) || !admitted.Before(p.WindowExpiresAt) {
			return p, fmt.Errorf("inference reservation admission time is invalid")
		}
	} else if p.ConnectionID != "" {
		return p, fmt.Errorf("connection reservation lacks its admission time")
	}
	if p.Routing != nil {
		if _, err := p.Routing.Canonical(); err != nil || p.Routing.OrganizationID != event.OrganizationID || p.Routing.ConnectionID != "" && p.Routing.ConnectionID != p.ConnectionID {
			return p, fmt.Errorf("inference reservation routing scope is invalid")
		}
	}
	if d := p.RoutingDecision; d != nil {
		if p.Routing == nil || d.ValidateFor(*p.Routing) != nil || d.SnapshotSequence <= 0 || d.ConnectionID != p.ConnectionID || d.Provider != p.Provider || d.Model != p.Model || d.ExecutionProfileVersion != p.ExecutionProfileVersion || d.SelectedAt.After(admitted) {
			return p, fmt.Errorf("inference reservation routing decision is invalid")
		}
	}
	return p, nil
}

func inferenceHash(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

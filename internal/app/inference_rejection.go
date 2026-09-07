package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

// RecordInferenceRouteRejection records only a bounded diagnostic and a link to
// existing runtime evidence. It never authorizes retries or marks an invocation
// attempted. A canceled request gets a separate bounded opportunity to persist
// its failure; a persistence failure is returned alongside the selection error.
func (s *Service) RecordInferenceRouteRejection(ctx context.Context, organizationID, correlationID, purpose, taskKey string, cause error) error {
	if cause == nil {
		return fmt.Errorf("routing rejection requires a failure")
	}
	auditCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	stream, err := s.gateway.Events(auditCtx, correlationID)
	if err != nil {
		return errors.Join(cause, fmt.Errorf("load routing rejection origin: %w", err))
	}
	originType := map[string]string{"TASK_ASSIGNMENT": "PLAN_CREATED", "PLANNING": "WORK_CREATED", "INTENT_NORMALIZATION": "INTAKE_MESSAGE_RECORDED"}[purpose]
	for i := len(stream) - 1; i >= 0; i-- {
		origin := stream[i]
		if origin.OrganizationID != organizationID || origin.EventType != originType {
			continue
		}
		plannedKey := taskKey
		if purpose == "INTENT_NORMALIZATION" {
			var message events.IntakeMessageRecordedPayload
			if json.Unmarshal(origin.Payload, &message) != nil || message.MessageID != taskKey {
				continue
			}
			plannedKey = ""
		}
		payload := events.InferenceRouteRejectedPayload{Version: 1, Purpose: purpose, OriginEventRef: origin.EventID,
			PlannedTaskKey: plannedKey, Category: string(inference.RouteFailureCategory(cause)), RequirementsFingerprint: inference.RouteFailureFingerprint(cause)}
		_, err := s.gateway.PublishTrusted(auditCtx, events.TrustedDraft{OrganizationID: organizationID, CorrelationID: correlationID,
			SourceActorID: "runtime", EventType: "INFERENCE_ROUTE_REJECTED", Payload: payload})
		if err != nil {
			return errors.Join(cause, fmt.Errorf("persist routing rejection: %w", err))
		}
		return cause
	}
	return errors.Join(cause, fmt.Errorf("routing rejection origin is unavailable"))
}

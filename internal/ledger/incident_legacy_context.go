package ledger

import (
	"encoding/json"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

// Historical completed Agent Tasks reconstruct v2-v4 inputs without the v5
// factual classification requirement. A pending/unconsumed manifest does not
// establish that completion applicability. Revisit a previously seen start if
// its private completion evidence arrives in a later dependency iteration.
func (d *incidentDependencies) legacyCompletionContexts() (map[[3]string]bool, error) {
	manifests := map[[3]string]bool{}
	for _, event := range d.stream {
		if event.EventType != "EXECUTION_CONTEXT_MANIFESTED" {
			continue
		}
		var manifest core.ExecutionContextManifest
		if err := json.Unmarshal(event.Payload, &manifest); err != nil {
			return nil, err
		}
		switch manifest.ContextBuilderVersion {
		case "v2", "v3", "v4":
			manifests[[3]string{event.CorrelationID, event.TaskID, event.SourceExecutionID}] = true
		}
	}
	consumed := map[[3]string]bool{}
	for _, event := range d.stream {
		if event.EventType != "TASK_VERIFIED_COMPLETE" {
			continue
		}
		projection, present, err := events.AdmittedProjection(event)
		if err != nil {
			return nil, err
		}
		if !present {
			continue
		}
		var task core.Task
		var decision events.CompletionDecisionPayload
		if err := json.Unmarshal(projection.Projection.Value, &task); err != nil {
			return nil, err
		}
		if task.ExecutionKind != core.ExecutionAgent {
			continue
		}
		if err := json.Unmarshal(projection.Detail, &decision); err != nil {
			return nil, err
		}
		outcome, found := d.stream[decision.OutcomeEventRef]
		if !found {
			continue
		}
		key := [3]string{event.CorrelationID, string(task.ID), outcome.SourceExecutionID}
		if manifests[key] {
			consumed[key] = true
		}
	}
	return consumed, nil
}

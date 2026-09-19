package events

import (
	"fmt"

	"github.com/dominicnunez/agentos/internal/core"
)

type modelStopState struct {
	manifest   Event
	request    Event
	payload    ModelStopRequest
	result     Event
	usage      Event
	notStarted bool
}

func modelStopBinding(event Event) executionStopBinding {
	return executionStopBinding{event.OrganizationID, event.TaskID, event.CorrelationID, event.SourceExecutionID}
}

func validModelStopEnvelope(event Event) bool {
	return event.EventID != "" && event.Sequence > 0 && event.SchemaVersion == SchemaVersion && event.SourceActorID == "runtime" && event.OrganizationID != "" && event.CorrelationID != "" && event.SourceExecutionID != "" && event.TaskID == "task-"+event.CorrelationID && event.RecipientScope == "" && event.RecipientID == "" && len(event.AuthorizationRefs) == 0 && len(event.ArtifactRefs) == 0
}

// ValidateModelStopContext checks the existing context used as this lifecycle's
// start. Old contexts without stop evidence keep their original contract.
func ValidateModelStopContext(event Event) error {
	if !validModelStopEnvelope(event) {
		return fmt.Errorf("model stop context crosses its runtime boundary")
	}
	var provider, model, profile, prompt string
	switch event.EventType {
	case "PLANNING_CONTEXT_MANIFESTED":
		var value PlanningContextPayload
		if decodeExactPayload(event.Payload, &value) != nil || value.PlanID == "" || value.IntentID == "" || value.IntentFingerprint == "" || len(value.InputEventRefs) == 0 {
			return fmt.Errorf("invalid planning stop context")
		}
		provider, model, profile, prompt = value.Provider, value.Model, value.ExecutionProfileVersion, value.PromptVersion
	case "INTENT_NORMALIZATION_CONTEXT_MANIFESTED":
		var value IntentNormalizationContextPayload
		if decodeExactPayload(event.Payload, &value) != nil || value.SourceMessageID == "" || len(value.InputEventRefs) == 0 {
			return fmt.Errorf("invalid normalization stop context")
		}
		provider, model, profile, prompt = value.Provider, value.Model, value.ExecutionProfileVersion, value.PromptVersion
	default:
		return fmt.Errorf("model stop requires an auxiliary context")
	}
	if provider == "" || model == "" || profile == "" || prompt == "" {
		return fmt.Errorf("model stop context lacks model identity")
	}
	return nil
}

// ModelStopCompletion identifies a durable ordinary closure for the exact
// manifest. The caller also proves that it preceded any stop or hold.
func ModelStopCompletion(event, manifest Event) (bool, error) {
	if event.EventType == "PLANNING_FAILED" {
		var value struct {
			Code             string `json:"code"`
			Reason           string `json:"reason"`
			EvidenceEventRef string `json:"evidence_event_ref,omitempty"`
		}
		if decodeExactPayload(event.Payload, &value) == nil && value.EvidenceEventRef == manifest.EventID && event.OrganizationID == manifest.OrganizationID && event.CorrelationID == manifest.CorrelationID {
			if event.TaskID == "" {
				event.TaskID = manifest.TaskID
			}
			if event.SourceExecutionID == "" {
				event.SourceExecutionID = manifest.SourceExecutionID
			}
		}
	}
	if !sameExecutionStopEventBinding(event, manifest) || event.Sequence <= manifest.Sequence {
		return false, nil
	}
	matched := manifest.EventType == "PLANNING_CONTEXT_MANIFESTED" && (event.EventType == "PLAN_CREATED" || event.EventType == "PLANNING_FAILED") || manifest.EventType == "INTENT_NORMALIZATION_CONTEXT_MANIFESTED" && (event.EventType == "INTENT_DRAFTED" || event.EventType == "INTENT_NORMALIZATION_FAILED")
	if !matched {
		return false, nil
	}
	if !validModelStopEnvelope(event) {
		return false, fmt.Errorf("model completion crosses its runtime boundary")
	}
	switch event.EventType {
	case "PLANNING_FAILED":
		var value struct {
			Code             string `json:"code"`
			Reason           string `json:"reason"`
			EvidenceEventRef string `json:"evidence_event_ref,omitempty"`
		}
		if decodeExactPayload(event.Payload, &value) != nil || value.Code == "" || value.Reason == "" || value.EvidenceEventRef != manifest.EventID {
			return false, fmt.Errorf("planning failure lacks its exact context")
		}
	case "INTENT_NORMALIZATION_FAILED":
		if decodeExactPayload(event.Payload, &struct{}{}) != nil {
			return false, fmt.Errorf("invalid normalization failure")
		}
	case "PLAN_CREATED":
		var value core.Plan
		var context PlanningContextPayload
		if decodeExactPayload(event.Payload, &value) != nil || decodeExactPayload(manifest.Payload, &context) != nil || string(value.ID) != context.PlanID || string(value.IntentID) != context.IntentID || value.IntentFingerprint != context.IntentFingerprint {
			return false, fmt.Errorf("plan completion differs from context")
		}
		fingerprint, err := core.FingerprintPlan(value)
		if err != nil || fingerprint != value.Fingerprint {
			return false, fmt.Errorf("invalid completed plan fingerprint")
		}
	case "INTENT_DRAFTED":
		var value IntentDraftedPayload
		var context IntentNormalizationContextPayload
		if decodeExactPayload(event.Payload, &value) != nil || decodeExactPayload(manifest.Payload, &context) != nil || value.SourceMessageID != context.SourceMessageID || string(value.Draft.OrganizationID) != manifest.OrganizationID {
			return false, fmt.Errorf("draft completion differs from context")
		}
		fingerprint, err := core.FingerprintIntentDraft(value.Draft)
		if err != nil || fingerprint != value.Draft.Fingerprint {
			return false, fmt.Errorf("invalid completed draft fingerprint")
		}
	}
	return true, nil
}

// ValidateModelStops validates model stop claims and auxiliary retry chronology
// in one ordered pass. Ordinary legacy closures retain their existing meaning.
func ValidateModelStops(stream []Event, freezes []OrganizationFreezeAdmission) error {
	index := make(map[string]executionStopEventIndex, len(stream))
	targets := map[executionStopBinding]bool{}
	invoked := map[executionStopBinding]bool{}
	contextCounts := map[executionStopBinding]int{}
	retryCounts := map[[4]string]int{}
	notSent := map[executionStopBinding]int64{}
	invalidNotSent := map[executionStopBinding]bool{}
	for _, event := range stream {
		if event.EventType == "PLANNING_CONTEXT_MANIFESTED" || event.EventType == "INTENT_NORMALIZATION_CONTEXT_MANIFESTED" {
			contextCounts[modelStopBinding(event)]++
			retryCounts[modelRetryKey(event)]++
		}
		if event.EventType == "INFERENCE_NOT_SENT" {
			var payload struct {
				RequestID    string `json:"request_id"`
				PromptSHA256 string `json:"prompt_sha256"`
			}
			if validModelStopEnvelope(event) && decodeExactPayload(event.Payload, &payload) == nil && payload.RequestID == event.SourceExecutionID && validSHA256(payload.PromptSHA256) && notSent[modelStopBinding(event)] == 0 {
				notSent[modelStopBinding(event)] = event.Sequence
			} else {
				invalidNotSent[modelStopBinding(event)] = true
			}
		}
		if event.EventType == "INFERENCE_RESERVED" || event.EventType == "INFERENCE_RECONCILED" || event.EventType == "INFERENCE_NOT_SENT" {
			invoked[modelStopBinding(event)] = true
		}
		if RequiresModelStopAdmission(event.EventType) {
			targets[modelStopBinding(event)] = true
		}
		prior, duplicate := index[event.EventID]
		if duplicate {
			prior.duplicate = true
			index[event.EventID] = prior
		} else {
			index[event.EventID] = executionStopEventIndex{event: event}
		}
	}
	for _, event := range stream {
		if (event.EventType == "PLANNING_CONTEXT_MANIFESTED" || event.EventType == "INTENT_NORMALIZATION_CONTEXT_MANIFESTED") && retryCounts[modelRetryKey(event)] > 1 {
			targets[modelStopBinding(event)] = true
		}
	}
	for binding := range invalidNotSent {
		if targets[binding] || contextCounts[binding] != 0 {
			return fmt.Errorf("model lifecycle has invalid or duplicate non-dispatch evidence")
		}
	}
	freezeIndex := indexExecutionStopFreezes(freezes)
	states := map[executionStopBinding]*modelStopState{}
	manifests := map[executionStopBinding]Event{}
	closed := map[executionStopBinding]int64{}
	retryClosed := map[executionStopBinding]int64{}
	usages := map[executionStopBinding][]Event{}
	priorManifests := map[[4]string]Event{}
	for _, event := range stream {
		binding := modelStopBinding(event)
		if key, result := modelResultRetryKey(event); result && event.SourceExecutionID == "" && priorManifests[key].EventID != "" {
			return fmt.Errorf("model result %s omits its manifested execution", event.EventID)
		}
		if event.EventType == "PLANNING_FAILED" {
			var value struct {
				Code             string `json:"code"`
				Reason           string `json:"reason"`
				EvidenceEventRef string `json:"evidence_event_ref,omitempty"`
			}
			if decodeExactPayload(event.Payload, &value) == nil {
				manifest := index[value.EvidenceEventRef].event
				if targets[modelStopBinding(manifest)] {
					if event.OrganizationID != manifest.OrganizationID || event.CorrelationID != manifest.CorrelationID || event.TaskID != "" && event.TaskID != manifest.TaskID || event.SourceExecutionID != "" && event.SourceExecutionID != manifest.SourceExecutionID {
						return fmt.Errorf("planning failure crosses model stop context")
					}
					event.TaskID, event.SourceExecutionID = manifest.TaskID, manifest.SourceExecutionID
					binding = modelStopBinding(event)
				}
			}
		}
		if event.EventType == "PLANNING_CONTEXT_MANIFESTED" || event.EventType == "INTENT_NORMALIZATION_CONTEXT_MANIFESTED" {
			if prior := priorManifests[modelRetryKey(event)]; prior.EventID != "" {
				priorBinding := modelStopBinding(prior)
				proof := notSent[priorBinding]
				allowed := proof > prior.Sequence && proof < event.Sequence
				if finish := retryClosed[priorBinding]; !allowed && finish > prior.Sequence {
					allowed = firstExecutionStopFreeze(freezeIndex[event.OrganizationID], prior.Sequence, finish) == nil
				}
				if state := states[priorBinding]; state != nil {
					allowed = allowed || state.notStarted
					if !allowed && event.EventType == "INTENT_NORMALIZATION_CONTEXT_MANIFESTED" && state.result.EventType == "MODEL_STOP_CONFIRMED" && state.payload.ReasonClass != "security_hold" && state.payload.ReasonClass != "containment_unavailable" {
						allowed = firstExecutionStopFreeze(freezeIndex[event.OrganizationID], prior.Sequence, state.result.Sequence) == nil
					}
				}
				if !allowed {
					return fmt.Errorf("model context %s retries unresolved context %s", event.EventID, prior.EventID)
				}
			}
			priorManifests[modelRetryKey(event)] = event
		}
		if !targets[binding] {
			continue
		}
		if event.EventType == "PLANNING_CONTEXT_MANIFESTED" || event.EventType == "INTENT_NORMALIZATION_CONTEXT_MANIFESTED" {
			if prior := manifests[binding]; prior.EventID != "" && states[binding] != nil {
				return fmt.Errorf("stopped model has a duplicate context")
			}
			manifests[binding] = event
		}
		state := states[binding]
		if event.EventType == "MODEL_STOP_REQUESTED" {
			var payload ModelStopRequest
			if state != nil || index[event.EventID].duplicate || !validModelStopEnvelope(event) || decodeExactPayload(event.Payload, &payload) != nil || !ValidModelStopReason(payload.ReasonClass) {
				return fmt.Errorf("invalid or duplicate model stop request %s", event.EventID)
			}
			context, found := index[payload.ContextEventRef]
			if !found || context.duplicate || contextCounts[binding] != 1 || ValidateModelStopContext(context.event) != nil || !sameExecutionStopEventBinding(event, context.event) || context.event.Sequence >= event.Sequence || manifests[binding].EventID != context.event.EventID || closed[binding] != 0 {
				return fmt.Errorf("model stop request %s lacks its unfinished exact context", event.EventID)
			}
			first := firstExecutionStopFreeze(freezeIndex[event.OrganizationID], context.event.Sequence, event.Sequence)
			if first == nil && (payload.Hold != nil || payload.ReasonClass == "security_hold") || first != nil && (payload.Hold == nil || payload.ReasonClass != "security_hold" || !sameExecutionStopHold(*payload.Hold, *first)) {
				return fmt.Errorf("model stop request %s does not bind its earliest hold", event.EventID)
			}
			states[binding] = &modelStopState{manifest: context.event, request: event, payload: payload}
			continue
		}
		if RequiresModelStopAdmission(event.EventType) {
			var payload ModelStopResult
			if state == nil || index[event.EventID].duplicate || !validModelStopEnvelope(event) || decodeExactPayload(event.Payload, &payload) != nil || payload.StopRequestRef != state.request.EventID || event.Sequence <= state.request.Sequence || state.result.EventType == "MODEL_STOP_CONFIRMED" {
				return fmt.Errorf("model stop result %s lacks its exact open request", event.EventID)
			}
			if event.EventType == "MODEL_STOP_UNCERTAIN" {
				if state.result.EventID != "" || payload.LocalState != "" || payload.ReturnedAt != nil || payload.UsageEventRef != "" || payload.ProviderStop != nil {
					return fmt.Errorf("uncertain model stop claims local return")
				}
			} else {
				if payload.LocalState == "NOT_STARTED" {
					if payload.ReturnedAt != nil || payload.UsageEventRef != "" || payload.ProviderStop != nil || len(usages[binding]) != 0 {
						return fmt.Errorf("unstarted model stop carries invocation evidence")
					}
					if invoked[binding] {
						return fmt.Errorf("unstarted model stop contradicts inference invocation")
					}
					state.notStarted = true
				} else if payload.LocalState != "RETURNED" || payload.ReturnedAt == nil || payload.ReturnedAt.IsZero() || payload.ReturnedAt.Before(state.manifest.CreatedAt) || payload.ReturnedAt.After(event.CreatedAt) || !validExecutionProviderStop(payload.ProviderStop) {
					return fmt.Errorf("invalid local model return evidence")
				}
				if len(usages[binding]) > 1 || (len(usages[binding]) == 0) != (payload.UsageEventRef == "") {
					return fmt.Errorf("model stop has conflicting or missing usage binding")
				}
				if len(usages[binding]) == 1 {
					usage := usages[binding][0]
					if usage.EventID != payload.UsageEventRef || usage.Sequence <= state.manifest.Sequence || usage.Sequence >= event.Sequence || ValidateModelStopUsage(usage, state.manifest) != nil {
						return fmt.Errorf("invalid model stop usage reference")
					}
					state.usage = usage
				}
			}
			state.result = event
			continue
		}
		if event.EventType == "INFERENCE_USAGE_RECORDED" {
			usages[binding] = append(usages[binding], event)
		}
		if state != nil {
			switch event.EventType {
			case "INFERENCE_RECONCILED", "INFERENCE_NOT_SENT":
			case "INTENT_NORMALIZATION_SUSPENDED", "PLANNING_CONTAINMENT_SUSPENDED":
				if err := ValidateModelStopSuspension(event, state.request); err != nil {
					return err
				}
			case "INFERENCE_USAGE_RECORDED":
				if state.result.EventType == "MODEL_STOP_CONFIRMED" || ValidateModelStopUsage(event, state.manifest) != nil {
					return fmt.Errorf("invalid post-stop model accounting")
				}
			default:
				return fmt.Errorf("ordinary model activity %s follows stop %s", event.EventID, state.request.EventID)
			}
		} else if manifest := manifests[binding]; manifest.EventID != "" {
			complete, err := ModelStopCompletion(event, manifest)
			if err != nil {
				return err
			}
			if complete {
				closed[binding] = event.Sequence
				if event.EventType == "INTENT_NORMALIZATION_FAILED" {
					retryClosed[binding] = event.Sequence
				}
			}
		}
	}
	for binding, state := range states {
		for _, usage := range usages[binding] {
			if usage.Sequence > state.request.Sequence && usage.EventID != state.usage.EventID {
				return fmt.Errorf("model stop usage lacks atomic confirmation")
			}
		}
	}
	return nil
}

// ValidateModelStopSuspension preserves the older local uncertainty marker as
// bookkeeping. It grants no closure and cannot carry generated output.
func ValidateModelStopSuspension(event, request Event) error {
	var detail ModelStopRequest
	if decodeExactPayload(request.Payload, &detail) != nil || detail.ReasonClass != "containment_unavailable" || !sameExecutionStopEventBinding(event, request) || event.SourceActorID != "runtime" || event.RecipientScope != "" || event.RecipientID != "" || len(event.AuthorizationRefs) != 0 || len(event.ArtifactRefs) != 0 {
		return fmt.Errorf("model suspension lacks exact containment stop")
	}
	switch event.EventType {
	case "INTENT_NORMALIZATION_SUSPENDED":
		if decodeExactPayload(event.Payload, &struct{}{}) != nil {
			return fmt.Errorf("invalid normalization suspension")
		}
	case "PLANNING_CONTAINMENT_SUSPENDED":
		var payload struct {
			ContextEventRef string `json:"context_event_ref"`
		}
		if decodeExactPayload(event.Payload, &payload) != nil || payload.ContextEventRef != detail.ContextEventRef {
			return fmt.Errorf("invalid planning suspension context")
		}
	default:
		return fmt.Errorf("invalid model suspension type")
	}
	return nil
}

// ModelNotStartedExecutions indexes typed no-invocation evidence after ordinary
// event/stop validation. Missing or contradictory proof grants no retry. The
// caller retains the separate security authority and full-history checks.
func ModelNotStartedExecutions(stream []Event, organization, correlation string) map[string]int64 {
	index := map[string]Event{}
	duplicate := map[string]bool{}
	invoked := map[executionStopBinding]bool{}
	for _, event := range stream {
		if _, found := index[event.EventID]; found {
			duplicate[event.EventID] = true
		}
		index[event.EventID] = event
		if event.EventType == "INFERENCE_RESERVED" || event.EventType == "INFERENCE_RECONCILED" || event.EventType == "INFERENCE_NOT_SENT" || event.EventType == "INFERENCE_USAGE_RECORDED" {
			invoked[modelStopBinding(event)] = true
		}
	}
	proofs := map[string]int64{}
	for _, event := range stream {
		if event.EventType != "MODEL_STOP_CONFIRMED" || event.OrganizationID != organization || event.CorrelationID != correlation || !validModelStopEnvelope(event) || duplicate[event.EventID] || invoked[modelStopBinding(event)] {
			continue
		}
		var result ModelStopResult
		if decodeExactPayload(event.Payload, &result) != nil || result.LocalState != "NOT_STARTED" || result.ReturnedAt != nil || result.UsageEventRef != "" || result.ProviderStop != nil {
			continue
		}
		request, found := index[result.StopRequestRef]
		var detail ModelStopRequest
		if !found || duplicate[request.EventID] || request.EventType != "MODEL_STOP_REQUESTED" || !validModelStopEnvelope(request) || !sameExecutionStopEventBinding(request, event) || request.Sequence >= event.Sequence || decodeExactPayload(request.Payload, &detail) != nil || !ValidModelStopReason(detail.ReasonClass) {
			continue
		}
		manifest, found := index[detail.ContextEventRef]
		if !found || duplicate[manifest.EventID] || ValidateModelStopContext(manifest) != nil || !sameExecutionStopEventBinding(manifest, event) || manifest.Sequence >= request.Sequence {
			continue
		}
		proofs[event.SourceExecutionID] = event.Sequence
	}
	return proofs
}

func ValidateModelStopUsage(event, manifest Event) error {
	if err := validateExecutionStopUsage(event); err != nil {
		return err
	}
	if !sameExecutionStopEventBinding(event, manifest) {
		return fmt.Errorf("model usage crosses its context")
	}
	var usage InferenceUsageRecordedPayload
	if decodeExactPayload(event.Payload, &usage) != nil {
		return fmt.Errorf("invalid model usage")
	}
	var connection, provider, model string
	if manifest.EventType == "PLANNING_CONTEXT_MANIFESTED" {
		var value PlanningContextPayload
		if decodeExactPayload(manifest.Payload, &value) != nil {
			return fmt.Errorf("invalid usage context")
		}
		connection, provider, model = value.ConnectionID, value.Provider, value.Model
	} else {
		var value IntentNormalizationContextPayload
		if decodeExactPayload(manifest.Payload, &value) != nil {
			return fmt.Errorf("invalid usage context")
		}
		connection, provider, model = value.ConnectionID, value.Provider, value.Model
	}
	if usage.ConnectionID != connection || usage.Provider != provider || usage.Model != model {
		return fmt.Errorf("model usage differs from context identity")
	}
	return nil
}

func modelRetryKey(manifest Event) [4]string {
	input := ""
	if manifest.EventType == "INTENT_NORMALIZATION_CONTEXT_MANIFESTED" {
		var payload IntentNormalizationContextPayload
		if decodeExactPayload(manifest.Payload, &payload) == nil {
			input = payload.SourceMessageID
		}
	}
	return [4]string{manifest.OrganizationID, manifest.CorrelationID, manifest.EventType, input}
}

func modelResultRetryKey(event Event) ([4]string, bool) {
	switch event.EventType {
	case "PLAN_CREATED":
		return [4]string{event.OrganizationID, event.CorrelationID, "PLANNING_CONTEXT_MANIFESTED", ""}, true
	case "INTENT_DRAFTED":
		var payload IntentDraftedPayload
		if decodeExactPayload(event.Payload, &payload) == nil {
			return [4]string{event.OrganizationID, event.CorrelationID, "INTENT_NORMALIZATION_CONTEXT_MANIFESTED", payload.SourceMessageID}, true
		}
	}
	return [4]string{}, false
}

// validLegacyModelResult preserves unmanifested local results, but prevents a
// manifested input from hiding its execution identity through the legacy path.
func validLegacyModelResult(event Event, stream []Event) bool {
	key, result := modelResultRetryKey(event)
	if !result {
		return false
	}
	for _, manifest := range stream {
		if manifest.Sequence < event.Sequence && (manifest.EventType == "PLANNING_CONTEXT_MANIFESTED" || manifest.EventType == "INTENT_NORMALIZATION_CONTEXT_MANIFESTED") && modelRetryKey(manifest) == key {
			return false
		}
	}
	return true
}

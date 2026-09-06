package ledger

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/modelinput"
)

type historicalRouteBinding struct {
	requirements modelinput.RouteRequirements
	decision     modelinput.RouteDecision
}

// Exact event/row correspondence is proved by the caller first. Decisions are
// evaluated at their own cutoffs, so later refunds or policy revisions cannot
// justify earlier selection. This proves selected-account eligibility, not the
// ordering of accounts absent from the runtime's configured registry.
func validateRoutingDecisionHistory(stream []events.Event, activations map[string]inference.Policy, freezes map[core.ID][]events.OrganizationFreezeAdmission) error {
	var bindings []historicalRouteBinding
	seen := make(map[modelinput.RouteDecision]bool)
	for _, event := range stream {
		var requirements *modelinput.RouteRequirements
		var decision *modelinput.RouteDecision
		switch event.EventType {
		case "INFERENCE_RESERVED":
			var payload events.InferenceReservedPayload
			if err := decodeExactJSONBytes(event.Payload, &payload); err != nil {
				return err
			}
			requirements, decision = payload.Routing, payload.RoutingDecision
		case "PLANNING_CONTEXT_MANIFESTED":
			var payload events.PlanningContextPayload
			if err := decodeExactJSONBytes(event.Payload, &payload); err != nil {
				return err
			}
			requirements, decision = payload.Routing, payload.RoutingDecision
		case "INTENT_NORMALIZATION_CONTEXT_MANIFESTED":
			var payload events.IntentNormalizationContextPayload
			if err := decodeExactJSONBytes(event.Payload, &payload); err != nil {
				return err
			}
			requirements, decision = payload.Routing, payload.RoutingDecision
		case "EXECUTION_CONTEXT_MANIFESTED":
			var payload core.ExecutionContextManifest
			if err := decodeExactJSONBytes(event.Payload, &payload); err != nil {
				return err
			}
			requirements, decision = payload.Routing, payload.RoutingDecision
		default:
			var projection events.ProjectionEventPayload
			if decodeExactJSONBytes(event.Payload, &projection) != nil || projection.Projection.ProjectionKind != "task" {
				continue
			}
			var task core.Task
			if err := decodeExactJSONBytes(projection.Projection.Value, &task); err != nil {
				return err
			}
			requirements, decision = task.Routing, task.RoutingDecision
		}
		if decision == nil {
			continue
		}
		if requirements == nil || requirements.OrganizationID != event.OrganizationID || decision.ValidateFor(*requirements) != nil || decision.SnapshotSequence <= 0 || decision.SnapshotSequence >= event.Sequence {
			return fmt.Errorf("historical routing decision lacks its originating requirements and cutoff")
		}
		if !seen[*decision] {
			seen[*decision] = true
			bindings = append(bindings, historicalRouteBinding{requirements: *requirements, decision: *decision})
		}
	}
	sort.Slice(bindings, func(i, j int) bool {
		return bindings[i].decision.SnapshotSequence < bindings[j].decision.SnapshotSequence
	})
	policies := make(map[string]map[string]inference.Policy)
	activatedAt := make(map[string]map[string]time.Time)
	charges := make(map[string]map[string]historicalInferenceCharge)
	latestSnapshotTime := make(map[string]time.Time)
	cursor := 0
	for _, binding := range bindings {
		d := binding.decision
		organization := binding.requirements.OrganizationID
		for cursor < len(stream) && stream[cursor].Sequence <= d.SnapshotSequence {
			event := stream[cursor]
			cursor++
			if event.CreatedAt.After(latestSnapshotTime[event.OrganizationID]) {
				latestSnapshotTime[event.OrganizationID] = event.CreatedAt
			}
			switch event.EventType {
			case "INFERENCE_POLICY_ACTIVATED":
				policy, ok := activations[event.EventID]
				if !ok {
					return fmt.Errorf("routing snapshot lacks exact policy activation")
				}
				if policies[event.OrganizationID] == nil {
					policies[event.OrganizationID] = make(map[string]inference.Policy)
					activatedAt[event.OrganizationID] = make(map[string]time.Time)
				}
				policies[event.OrganizationID][policy.ConnectionID] = policy
				activatedAt[event.OrganizationID][policy.ConnectionID] = event.CreatedAt
			case "INFERENCE_RESERVED":
				var payload events.InferenceReservedPayload
				if err := decodeExactJSONBytes(event.Payload, &payload); err != nil {
					return err
				}
				at, err := time.Parse(time.RFC3339Nano, payload.AdmittedAt)
				if payload.AdmittedAt != "" && err != nil {
					return err
				}
				if at.After(latestSnapshotTime[event.OrganizationID]) {
					latestSnapshotTime[event.OrganizationID] = at
				}
				if charges[event.OrganizationID] == nil {
					charges[event.OrganizationID] = make(map[string]historicalInferenceCharge)
				}
				charges[event.OrganizationID][payload.ReservationID] = historicalInferenceCharge{admission: payload, input: payload.ReservedInputTokens, output: payload.ReservedOutputTokens, cost: payload.ReservedCostNanoUSD, outstanding: true, admittedAt: at}
			case "INFERENCE_RECONCILED":
				var payload events.InferenceReconciledPayload
				if err := decodeExactJSONBytes(event.Payload, &payload); err != nil {
					return err
				}
				charge, ok := charges[event.OrganizationID][payload.ReservationID]
				if !ok || !charge.outstanding {
					return fmt.Errorf("routing snapshot has unmatched reconciliation")
				}
				charge.input, charge.output, charge.cost, charge.outstanding = payload.ChargedInputTokens, payload.ChargedOutputTokens, payload.ChargedCostNanoUSD, false
				charges[event.OrganizationID][payload.ReservationID] = charge
			}
		}
		if cursor == 0 || stream[cursor-1].Sequence != d.SnapshotSequence {
			return fmt.Errorf("routing snapshot cutoff does not exist")
		}
		if d.SelectedAt.Before(latestSnapshotTime[organization]) {
			return fmt.Errorf("routing selection timestamp predates its organization snapshot")
		}
		frozen := false
		for _, freeze := range freezes[core.ID(organization)] {
			if freeze.Sequence <= d.SnapshotSequence {
				frozen = freeze.Frozen
			}
		}
		if frozen {
			return fmt.Errorf("routing decision selected while organization was frozen")
		}
		policy, ok := policies[organization][d.ConnectionID]
		fingerprint, err := policy.Fingerprint()
		if !ok || err != nil || fingerprint != d.PolicyFingerprint || policy.Catalog == nil || policy.Routing == nil || policy.OrganizationBudget == nil || d.SelectedAt.Before(activatedAt[organization][d.ConnectionID]) {
			return fmt.Errorf("routing decision does not identify the policy active at its cutoff")
		}
		for _, other := range policies[organization] {
			if other.Version == inference.ConnectionPolicyVersion && (!inference.SameRoutePolicy(other.Routing, policy.Routing) || other.OrganizationBudget == nil || *other.OrganizationBudget != *policy.OrganizationBudget) {
				return fmt.Errorf("routing decision observed an incomplete policy-set change")
			}
		}
		if err := validateHistoricalRouteBudget(binding, policy, charges[organization]); err != nil {
			return err
		}
	}
	return nil
}

func validateHistoricalRouteBudget(binding historicalRouteBinding, policy inference.Policy, charges map[string]historicalInferenceCharge) error {
	d := binding.decision
	metadata, err := policy.Catalog.Metadata(policy)
	if err != nil {
		return err
	}
	pool := inference.Pool{ID: d.PolicyFingerprint, Policy: policy, Available: true}
	start, _ := inferenceWindow(d.SelectedAt, time.Duration(policy.WindowDurationSeconds)*time.Second)
	orgStart, orgEnd := inferenceWindow(d.SelectedAt, time.Duration(policy.OrganizationBudget.WindowDurationSeconds)*time.Second)
	totals := historicalBudgetTotals{start: orgStart, end: orgEnd}
	for _, charge := range charges {
		if err := totals.add(charge); err != nil {
			return err
		}
		if charge.admission.ConnectionID != policy.ConnectionID {
			continue
		}
		if charge.outstanding {
			pool.ActiveRequests++
		}
		if charge.admission.Provider != policy.Provider || charge.admission.Model != policy.Model || !charge.admission.WindowStartedAt.Equal(start) {
			continue
		}
		if charge.input < 0 || charge.output < 0 || charge.cost < 0 || charge.input > math.MaxInt64-pool.ChargedTokens || charge.output > math.MaxInt64-pool.ChargedTokens-charge.input || charge.cost > math.MaxInt64-pool.ChargedCostNanoUSD {
			return fmt.Errorf("routing snapshot account charges overflow")
		}
		pool.ChargedTokens += charge.input + charge.output
		pool.ChargedCostNanoUSD += charge.cost
	}
	requirements := binding.requirements.Clone()
	requirements.ConnectionID = d.ConnectionID
	selected, err := (inference.Broker{Routes: []inference.RouteMetadata{metadata}, Manager: inference.Manager{Pools: []inference.Pool{pool}}}).Select(d.SelectedAt, requirements, *policy.Routing)
	if err != nil {
		return fmt.Errorf("historical routing selection was ineligible: %w", err)
	}
	if selected.Pool.ReservedInputTokens != d.ReservedInputTokens || selected.Pool.ReservedOutputTokens != d.ReservedOutputTokens || selected.Pool.ReservedCostNanoUSD != d.ReservedCostNanoUSD || selected.Descriptor.Provider != d.Provider || selected.Descriptor.Model != d.Model || selected.Descriptor.ExecutionProfileVersion != d.ExecutionProfileVersion || (selected.Pool.Mode == inference.Local) != d.Local {
		return fmt.Errorf("routing decision changed its historical account or reservation estimate")
	}
	if !policy.OrganizationBudget.Allows(totals.active, totals.tokens, totals.cost, d.ReservedInputTokens, d.ReservedOutputTokens, d.ReservedCostNanoUSD) {
		return fmt.Errorf("historical routing decision exceeded shared budget")
	}
	return nil
}

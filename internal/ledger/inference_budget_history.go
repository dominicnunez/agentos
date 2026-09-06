package ledger

import (
	"fmt"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

type historicalInferenceCharge struct {
	admission           events.InferenceReservedPayload
	input, output, cost int64
	outstanding         bool
	admittedAt          time.Time
}

// The caller first proves exact event/row correspondence. This second pass
// reconstructs charges at each admission, before later reconciliations can
// release resources. Current totals cannot prove historical budget compliance.
func validateOrganizationBudgetHistory(stream []events.Event, activations map[string]inference.Policy) error {
	connections := make(map[string]map[string]inference.Policy)
	charges := make(map[string]map[string]historicalInferenceCharge)
	budgets := make(map[string]*inference.OrganizationBudget)
	totals := make(map[string]*historicalBudgetTotals)
	latestAdmission := make(map[string]time.Time)
	var changingOrganization, changingAuthor string
	var changingAt time.Time
	for _, event := range stream {
		if changingOrganization != "" && (event.EventType != "INFERENCE_POLICY_ACTIVATED" || event.OrganizationID != changingOrganization) {
			return fmt.Errorf("shared budget policy set was interrupted before agreement")
		}
		switch event.EventType {
		case "INFERENCE_POLICY_ACTIVATED":
			policy, found := activations[event.EventID]
			if !found {
				return fmt.Errorf("shared budget history lacks an exact activation")
			}
			if changingOrganization != "" && (policy.AuthorizedBy != changingAuthor || !policy.AuthorizedAt.Equal(changingAt)) {
				return fmt.Errorf("shared budget policy set has mixed authorizations")
			}
			if connections[event.OrganizationID] == nil {
				connections[event.OrganizationID] = make(map[string]inference.Policy)
			}
			connections[event.OrganizationID][policy.ConnectionID] = policy
			var budget *inference.OrganizationBudget
			disagrees := false
			var routing *inference.RoutePolicy
			routingSeen := false
			for _, active := range connections[event.OrganizationID] {
				if active.Version == inference.ConnectionPolicyVersion {
					if routingSeen && !inference.SameRoutePolicy(routing, active.Routing) {
						disagrees = true
					}
					routing, routingSeen = active.Routing, true
				}
				if active.OrganizationBudget == nil {
					continue
				}
				if budget != nil && *budget != *active.OrganizationBudget {
					disagrees = true
				}
				budget = active.OrganizationBudget
			}
			// A writer transaction can replace several adjacent policy records.
			// No admission or other event may occur until all limits agree again.
			if disagrees {
				changingOrganization, changingAuthor, changingAt = event.OrganizationID, policy.AuthorizedBy, policy.AuthorizedAt
			} else {
				changingOrganization = ""
			}
			budgets[event.OrganizationID] = budget
			delete(totals, event.OrganizationID)
		case "INFERENCE_RESERVED":
			var payload events.InferenceReservedPayload
			if err := decodeExactJSONBytes(event.Payload, &payload); err != nil {
				return err
			}
			if charges[event.OrganizationID] == nil {
				charges[event.OrganizationID] = make(map[string]historicalInferenceCharge)
			}
			var at time.Time
			if payload.AdmittedAt != "" {
				var err error
				at, err = time.Parse(time.RFC3339Nano, payload.AdmittedAt)
				if err != nil {
					return err
				}
			}
			if budget := budgets[event.OrganizationID]; budget != nil {
				if at.IsZero() {
					return fmt.Errorf("shared budget reservation lacks exact admission time")
				}
				if latestAdmission[event.OrganizationID].After(at) {
					return fmt.Errorf("shared budget admission clock moved backwards")
				}
				start, end := inferenceWindow(at, time.Duration(budget.WindowDurationSeconds)*time.Second)
				current := totals[event.OrganizationID]
				if current == nil || !current.start.Equal(start) || !current.end.Equal(end) {
					current = &historicalBudgetTotals{start: start, end: end}
					for _, charge := range charges[event.OrganizationID] {
						if err := current.add(charge); err != nil {
							return err
						}
					}
					totals[event.OrganizationID] = current
				}
				if !budget.Allows(current.active, current.tokens, current.cost, payload.ReservedInputTokens, payload.ReservedOutputTokens, payload.ReservedCostNanoUSD) {
					return fmt.Errorf("historical reservation exceeded organization inference budget")
				}
			}
			charge := historicalInferenceCharge{admission: payload, admittedAt: at, input: payload.ReservedInputTokens, output: payload.ReservedOutputTokens, cost: payload.ReservedCostNanoUSD, outstanding: true}
			charges[event.OrganizationID][payload.ReservationID] = charge
			if at.After(latestAdmission[event.OrganizationID]) {
				latestAdmission[event.OrganizationID] = at
			}
			if current := totals[event.OrganizationID]; current != nil {
				if err := current.add(charge); err != nil {
					return err
				}
			}
		case "INFERENCE_RECONCILED":
			var payload events.InferenceReconciledPayload
			if err := decodeExactJSONBytes(event.Payload, &payload); err != nil {
				return err
			}
			charge, found := charges[event.OrganizationID][payload.ReservationID]
			if !found || !charge.outstanding {
				return fmt.Errorf("shared budget reconciliation lacks outstanding reservation")
			}
			current := totals[event.OrganizationID]
			if current != nil {
				current.remove(charge)
			}
			charge.input, charge.output, charge.cost, charge.outstanding = payload.ChargedInputTokens, payload.ChargedOutputTokens, payload.ChargedCostNanoUSD, false
			charges[event.OrganizationID][payload.ReservationID] = charge
			if current != nil {
				// Preserve an over-limit provider report. If its totals overflow,
				// recomputation will deny the next admission; do not lose evidence.
				if err := current.add(charge); err != nil {
					delete(totals, event.OrganizationID)
				}
			}
		}
	}
	if changingOrganization != "" {
		return fmt.Errorf("shared budget policy set is incomplete")
	}
	return nil
}

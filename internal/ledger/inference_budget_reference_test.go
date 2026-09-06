package ledger

import (
	"fmt"
	"math"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

// The caller first proves exact event/row correspondence. This second pass
// reconstructs charges at each admission, before later reconciliations can
// release resources. Current totals cannot prove historical budget compliance.
func referenceOrganizationBudgetHistory(stream []events.Event, activations map[string]inference.Policy) error {
	connections := make(map[string]map[string]inference.Policy)
	charges := make(map[string]map[string]historicalInferenceCharge)
	budgets := make(map[string]*inference.OrganizationBudget)
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
			for _, active := range connections[event.OrganizationID] {
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
		case "INFERENCE_RESERVED":
			var payload events.InferenceReservedPayload
			if err := decodeExactJSONBytes(event.Payload, &payload); err != nil {
				return err
			}
			if charges[event.OrganizationID] == nil {
				charges[event.OrganizationID] = make(map[string]historicalInferenceCharge)
			}
			if budget := budgets[event.OrganizationID]; budget != nil {
				at, err := time.Parse(time.RFC3339Nano, payload.AdmittedAt)
				if err != nil || at.IsZero() {
					return fmt.Errorf("shared budget reservation lacks exact admission time")
				}
				start, end := inferenceWindow(at, time.Duration(budget.WindowDurationSeconds)*time.Second)
				var active int
				var tokens, cost int64
				for _, charge := range charges[event.OrganizationID] {
					if charge.outstanding {
						active++
					}
					inWindow := charge.admission.WindowStartedAt.Before(end) && charge.admission.WindowExpiresAt.After(start)
					if charge.admission.AdmittedAt != "" {
						prior, err := time.Parse(time.RFC3339Nano, charge.admission.AdmittedAt)
						if err != nil || prior.After(at) {
							return fmt.Errorf("shared budget admission clock moved backwards")
						}
						inWindow = !prior.Before(start) && prior.Before(end)
					}
					if !charge.outstanding && !inWindow {
						continue
					}
					if charge.input < 0 || charge.output < 0 || charge.cost < 0 || charge.input > math.MaxInt64-tokens || charge.output > math.MaxInt64-tokens-charge.input || charge.cost > math.MaxInt64-cost {
						return fmt.Errorf("historical shared budget accounting overflow")
					}
					tokens += charge.input + charge.output
					cost += charge.cost
				}
				if !budget.Allows(active, tokens, cost, payload.ReservedInputTokens, payload.ReservedOutputTokens, payload.ReservedCostNanoUSD) {
					return fmt.Errorf("historical reservation exceeded organization inference budget")
				}
			}
			charges[event.OrganizationID][payload.ReservationID] = historicalInferenceCharge{admission: payload, input: payload.ReservedInputTokens, output: payload.ReservedOutputTokens, cost: payload.ReservedCostNanoUSD, outstanding: true}
		case "INFERENCE_RECONCILED":
			var payload events.InferenceReconciledPayload
			if err := decodeExactJSONBytes(event.Payload, &payload); err != nil {
				return err
			}
			charge, found := charges[event.OrganizationID][payload.ReservationID]
			if !found || !charge.outstanding {
				return fmt.Errorf("shared budget reconciliation lacks outstanding reservation")
			}
			charge.input, charge.output, charge.cost, charge.outstanding = payload.ChargedInputTokens, payload.ChargedOutputTokens, payload.ChargedCostNanoUSD, false
			charges[event.OrganizationID][payload.ReservationID] = charge
		}
	}
	if changingOrganization != "" {
		return fmt.Errorf("shared budget policy set is incomplete")
	}
	return nil
}

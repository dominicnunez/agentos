package inference

import (
	"fmt"
	"time"
)

// ValidateRequestRouting enforces one already-selected account's reviewed
// routing contract. Live budget totals are checked separately by reservation;
// replay calls this with the original policy and exact admission time.
func ValidateRequestRouting(now time.Time, policy Policy, request InferenceRequest) error {
	if policy.Catalog != nil && request.Scope.RoutingDecision == nil {
		return fmt.Errorf("catalog inference requires a durable routing decision")
	}
	if decision := request.Scope.RoutingDecision; decision != nil {
		if request.Scope.Validate() != nil || decision.ConnectionID != request.ConnectionID || decision.Provider != request.Descriptor.Provider ||
			decision.Model != request.Descriptor.Model || decision.ExecutionProfileVersion != request.Descriptor.ExecutionProfileVersion || decision.SelectedAt.After(now) {
			return fmt.Errorf("inference routing decision does not identify this request")
		}
	}
	if request.Scope.Routing == nil {
		if policy.Catalog != nil || policy.Routing != nil {
			return fmt.Errorf("governed inference requires explicit routing constraints")
		}
		return nil
	}
	if request.Scope.Validate() != nil || policy.Catalog == nil || policy.Routing == nil || request.Scope.OrganizationID != policy.OrganizationID ||
		request.ConnectionID != policy.ConnectionID || request.Scope.Routing.ConnectionID != "" && request.Scope.Routing.ConnectionID != request.ConnectionID {
		return fmt.Errorf("inference routing constraints do not bind the selected account")
	}
	metadata, err := policy.Catalog.Metadata(policy)
	if err != nil {
		return err
	}
	if !now.Before(metadata.ValidUntil) {
		return fmt.Errorf("inference catalog is expired")
	}
	if request.Descriptor != metadata.Descriptor {
		return fmt.Errorf("inference routing model identity differs")
	}
	constraints := request.Scope.Routing.Clone()
	constraints.ConnectionID = request.ConnectionID
	_, err = (Broker{Routes: []RouteMetadata{metadata}, Manager: Manager{Pools: []Pool{{ID: "admission", Policy: policy, Available: true}}}}).Select(now, constraints, *policy.Routing)
	return err
}

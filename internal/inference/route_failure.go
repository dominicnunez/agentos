package inference

import (
	"context"
	"errors"
)

type RouteFailureCode string

const (
	RouteInvalidRequirements   RouteFailureCode = "INVALID_REQUIREMENTS"
	RouteInvalidCatalog        RouteFailureCode = "INVALID_CATALOG"
	RoutePolicyDenied          RouteFailureCode = "POLICY_DENIED"
	RouteNoEligibleAccount     RouteFailureCode = "NO_ELIGIBLE_ACCOUNT"
	RouteSharedBudgetExhausted RouteFailureCode = "SHARED_BUDGET_EXHAUSTED"
	RouteCanceled              RouteFailureCode = "CANCELED"
	RouteSelectionUnavailable  RouteFailureCode = "SELECTION_UNAVAILABLE"
)

// routeFailure preserves error identity for trusted control flow while exposing
// only a fixed diagnostic. Raw database or provider error text is not audit data.
type routeFailure struct {
	code                    RouteFailureCode
	cause                   error
	requirementsFingerprint string
}

// RouteFailureFingerprint identifies valid selection requirements without
// exposing their contents. Invalid or unavailable requirements have no digest.
func RouteFailureFingerprint(err error) string {
	var failure *routeFailure
	if errors.As(err, &failure) {
		return failure.requirementsFingerprint
	}
	return ""
}

func (e *routeFailure) Error() string {
	switch e.code {
	case RouteInvalidRequirements:
		return "inference route requirements are invalid"
	case RouteInvalidCatalog:
		return "inference route catalog is invalid"
	case RoutePolicyDenied:
		return "no authorized inference route"
	case RouteNoEligibleAccount:
		return "no feasible inference route"
	case RouteSharedBudgetExhausted:
		return "inference organization budget exhausted"
	case RouteCanceled:
		return "inference route selection canceled"
	case RouteSelectionUnavailable:
		return "inference route selection unavailable"
	default:
		return "inference route selection unavailable"
	}
}

func (e *routeFailure) Unwrap() error { return e.cause }

// RouteFailureCategory is the bounded category suitable for an audit record.
// It deliberately does not classify failures by inspecting their message text.
func RouteFailureCategory(err error) RouteFailureCode {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return RouteCanceled
	}
	if errors.Is(err, ErrOrganizationBudgetExhausted) {
		return RouteSharedBudgetExhausted
	}
	var failure *routeFailure
	if errors.As(err, &failure) {
		return failure.code
	}
	return RouteSelectionUnavailable
}

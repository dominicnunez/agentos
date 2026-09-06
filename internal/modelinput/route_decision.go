package modelinput

import (
	"fmt"
	"math"
	"slices"
	"time"
)

type RouteReason string

const (
	RouteExplicit  RouteReason = "EXPLICIT_CONNECTION"
	RoutePreferred RouteReason = "PREFERRED_CONNECTION"
	RouteOrdered   RouteReason = "CONNECTION_ORDER"
)

// RouteDecision describes advisory selection, not admission or provider success.
// It contains no prompts, credentials, endpoints, or free-form diagnostic text.
// Persisting this record requires binding it to its originating task or attempt.
type RouteDecision struct {
	Version                 int         `json:"version"`
	RequirementsFingerprint string      `json:"requirements_fingerprint"`
	PolicyFingerprint       string      `json:"policy_fingerprint"`
	SelectedAt              time.Time   `json:"selected_at"`
	SnapshotSequence        int64       `json:"snapshot_sequence"`
	ConnectionID            string      `json:"connection_id"`
	Provider                string      `json:"provider"`
	Model                   string      `json:"model"`
	ExecutionProfileVersion string      `json:"execution_profile_version"`
	Local                   bool        `json:"local"`
	Reason                  RouteReason `json:"reason"`
	ReservedInputTokens     int64       `json:"reserved_input_tokens"`
	ReservedOutputTokens    int64       `json:"reserved_output_tokens"`
	ReservedCostNanoUSD     int64       `json:"reserved_cost_nano_usd"`
	SharedBudgetRejections  int         `json:"shared_budget_rejections"`
}

// RouteBinding carries one authoritative selection to its attempt-local caller.
type RouteBinding struct {
	Requirements RouteRequirements `json:"requirements"`
	Decision     RouteDecision     `json:"decision"`
}

func (b RouteBinding) Validate() error {
	if b.Decision.SnapshotSequence <= 0 {
		return fmt.Errorf("routing binding lacks a ledger snapshot")
	}
	return b.Decision.ValidateFor(b.Requirements)
}

func CloneRouteDecision(decision *RouteDecision) *RouteDecision {
	if decision == nil {
		return nil
	}
	copy := *decision
	return &copy
}

func SameRouteDecision(a, b *RouteDecision) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// ValidateFor checks the record's shape and correspondence with the complete
// request. Policy, catalog, and accounting authority must be checked separately.
func (d RouteDecision) ValidateFor(requirements RouteRequirements) error {
	fingerprint, err := requirements.Fingerprint()
	if err != nil {
		return err
	}
	if d.Version != 1 || d.RequirementsFingerprint != fingerprint || !routeDigest(d.PolicyFingerprint) ||
		d.SelectedAt.IsZero() || d.SelectedAt.Location() != time.UTC || d.SnapshotSequence < 0 || !validRoutingConnection(d.ConnectionID) ||
		!validRoutingValue(d.Provider) || !validRoutingValue(d.Model) || !validRoutingValue(d.ExecutionProfileVersion) ||
		d.ReservedInputTokens < requirements.InputTokens || d.ReservedOutputTokens < requirements.OutputTokens ||
		d.ReservedInputTokens > math.MaxInt64-d.ReservedOutputTokens || d.ReservedCostNanoUSD < 0 ||
		requirements.MaxCostNanoUSD != nil && d.ReservedCostNanoUSD > *requirements.MaxCostNanoUSD ||
		d.SharedBudgetRejections < 0 || d.SharedBudgetRejections > 1024 ||
		requirements.Locality == LocalOnly && !d.Local ||
		len(requirements.AllowedProviders) != 0 && !slices.Contains(requirements.AllowedProviders, d.Provider) ||
		slices.Contains(requirements.DeniedProviders, d.Provider) {
		return fmt.Errorf("inference route decision does not match its requirements")
	}
	expected := RouteOrdered
	if requirements.ConnectionID != "" {
		if d.ConnectionID != requirements.ConnectionID {
			return fmt.Errorf("inference route decision changed explicit account")
		}
		expected = RouteExplicit
	} else if slices.Contains(requirements.PreferredConnections, d.ConnectionID) {
		expected = RoutePreferred
	}
	if d.Reason != expected {
		return fmt.Errorf("inference route decision has an invalid ordering reason")
	}
	return nil
}

func routeDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

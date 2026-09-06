package inference

import (
	"fmt"
	"slices"
	"time"

	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/modelinput"
)

type Capability = modelinput.Capability
type Locality = modelinput.Locality

const (
	Text             = modelinput.Text
	Vision           = modelinput.Vision
	ToolCalling      = modelinput.ToolCalling
	StructuredOutput = modelinput.StructuredOutput
	Streaming        = modelinput.Streaming
	Reasoning        = modelinput.Reasoning
	LocalOnly        = modelinput.LocalOnly
	CloudAllowed     = modelinput.CloudAllowed
)

// RouteMetadata is trusted, reviewed configuration, not a provider response or
// a guess based on a model name. Zero token limits mean unknown, never unlimited.
// Local metadata describes placement; it does not attest confinement or identity.
type RouteMetadata struct {
	ConnectionID  string
	Descriptor    execution.ModelDescriptor
	Capabilities  []Capability
	Local         bool
	ContextTokens int64
	OutputTokens  int64
	DataClasses   []string
	ValidUntil    time.Time
}

type RouteRequirements = modelinput.RouteRequirements

// RoutePolicy is a separate hard boundary. A caller's requirements cannot relax
// installation policy by changing its preferred provider or locality.
type RoutePolicy struct {
	OrganizationID   string   `json:"organization_id"`
	Locality         Locality `json:"locality"`
	AllowedProviders []string `json:"allowed_providers,omitempty"`
	DeniedProviders  []string `json:"denied_providers,omitempty"`
	DataClasses      []string `json:"data_classes"`
}

func (p RoutePolicy) Validate() error {
	if !validValue(p.OrganizationID) || !validLocality(p.Locality) || !validRouteValues(p.AllowedProviders) ||
		!validRouteValues(p.DeniedProviders) || len(p.DataClasses) == 0 || !validRouteValues(p.DataClasses) {
		return fmt.Errorf("organization inference routing policy is invalid")
	}
	return nil
}

func SameRoutePolicy(a, b *RoutePolicy) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.OrganizationID == b.OrganizationID && a.Locality == b.Locality && slices.Equal(a.AllowedProviders, b.AllowedProviders) &&
		slices.Equal(a.DeniedProviders, b.DeniedProviders) && slices.Equal(a.DataClasses, b.DataClasses)
}

type RouteSelection struct {
	Decision     modelinput.RouteDecision
	ConnectionID string
	Descriptor   execution.ModelDescriptor
	Pool         PoolSelection
}

func (s RouteSelection) ValidateFor(request RouteRequirements) error {
	if err := s.Decision.ValidateFor(request); err != nil {
		return err
	}
	d := s.Decision
	if d.ConnectionID != s.ConnectionID || d.Provider != s.Descriptor.Provider || d.Model != s.Descriptor.Model ||
		d.ExecutionProfileVersion != s.Descriptor.ExecutionProfileVersion || d.PolicyFingerprint != s.Pool.PolicyFingerprint ||
		d.Local != (s.Pool.Mode == Local) || d.ReservedInputTokens != s.Pool.ReservedInputTokens ||
		d.ReservedOutputTokens != s.Pool.ReservedOutputTokens || d.ReservedCostNanoUSD != s.Pool.ReservedCostNanoUSD {
		return fmt.Errorf("inference decision does not identify its selected pool")
	}
	return nil
}

// Broker filters configured metadata before applying the existing pool budget
// calculation. Select is advisory: the ledger must still reserve atomically at
// dispatch, and local execution must still pass its independent prerequisites.
type Broker struct {
	Routes  []RouteMetadata
	Manager Manager
}

// SelectRoute uses only catalog entries bound at registry construction. Pool
// state is an admission snapshot, not a second mutable accounting store.
func (r *ConnectionRegistry) SelectRoute(now time.Time, pools []Pool, request RouteRequirements, policy RoutePolicy) (RouteSelection, error) {
	return (Broker{Routes: r.Catalog(), Manager: Manager{Pools: pools}}).Select(now, request, policy)
}

func (r RouteMetadata) Validate() error {
	if !ValidConnectionID(r.ConnectionID) || !validValue(r.Descriptor.Provider) ||
		!validValue(r.Descriptor.Model) || !validValue(r.Descriptor.ExecutionProfileVersion) ||
		!validCapabilities(r.Capabilities) || !validRouteValues(r.DataClasses) ||
		r.ContextTokens < 0 || r.OutputTokens < 0 || r.ContextTokens > 0 && r.OutputTokens > r.ContextTokens ||
		r.ValidUntil.IsZero() || r.ValidUntil.Location() != time.UTC {
		return fmt.Errorf("inference route metadata is invalid")
	}
	return nil
}

func validCapabilities(values []Capability) bool {
	if len(values) > 6 {
		return false
	}
	seen := make(map[Capability]bool, len(values))
	for _, value := range values {
		if seen[value] || !slices.Contains([]Capability{Text, Vision, ToolCalling, StructuredOutput, Streaming, Reasoning}, value) {
			return false
		}
		seen[value] = true
	}
	return true
}

func validRouteValues(values []string) bool {
	if len(values) > 1024 {
		return false
	}
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		if !validValue(value) || seen[value] {
			return false
		}
		seen[value] = true
	}
	return true
}

func validLocality(value Locality) bool { return value == LocalOnly || value == CloudAllowed }

func providerAllowed(provider string, allow, deny []string) bool {
	return (len(allow) == 0 || slices.Contains(allow, provider)) && !slices.Contains(deny, provider)
}

func (b Broker) Select(now time.Time, request RouteRequirements, policy RoutePolicy) (RouteSelection, error) {
	if _, err := request.Canonical(); err != nil {
		return RouteSelection{}, &routeFailure{code: RouteInvalidRequirements, cause: err}
	}
	if !validValue(request.OrganizationID) || request.OrganizationID != policy.OrganizationID ||
		!validLocality(request.Locality) || !validLocality(policy.Locality) || !validValue(request.DataClass) ||
		request.ConnectionID != "" && !ValidConnectionID(request.ConnectionID) ||
		request.InputTokens < 1 || request.OutputTokens < 1 || !validCapabilities(request.Capabilities) ||
		request.MaxCostNanoUSD != nil && *request.MaxCostNanoUSD < 0 ||
		!validRouteValues(request.AllowedProviders) || !validRouteValues(request.DeniedProviders) ||
		!validRouteValues(request.PreferredConnections) || !validRouteValues(policy.AllowedProviders) ||
		!validRouteValues(policy.DeniedProviders) || !validRouteValues(policy.DataClasses) {
		return RouteSelection{}, &routeFailure{code: RouteInvalidRequirements}
	}
	if len(b.Routes) == 0 || len(b.Routes) > 1024 || !slices.Contains(policy.DataClasses, request.DataClass) {
		return RouteSelection{}, &routeFailure{code: RoutePolicyDenied}
	}
	seen := make(map[string]bool, len(b.Routes))
	var eligible []RouteSelection
	for _, route := range b.Routes {
		if route.Validate() != nil || seen[route.ConnectionID] {
			return RouteSelection{}, &routeFailure{code: RouteInvalidCatalog}
		}
		seen[route.ConnectionID] = true
		if request.ConnectionID != "" && request.ConnectionID != route.ConnectionID || !now.Before(route.ValidUntil) ||
			(request.Locality == LocalOnly || policy.Locality == LocalOnly) && !route.Local ||
			!providerAllowed(route.Descriptor.Provider, request.AllowedProviders, request.DeniedProviders) ||
			!providerAllowed(route.Descriptor.Provider, policy.AllowedProviders, policy.DeniedProviders) ||
			!slices.Contains(route.DataClasses, request.DataClass) || request.OutputTokens > route.OutputTokens ||
			request.InputTokens > route.ContextTokens || request.OutputTokens > route.ContextTokens-request.InputTokens {
			continue
		}
		if !allCapabilities(route.Capabilities, request.Capabilities) {
			continue
		}
		// Scope pools to this organization before asking the existing manager;
		// matching account/model names in another tenant are never candidates.
		var pools []Pool
		for _, pool := range b.Manager.Pools {
			if pool.Policy.OrganizationID != request.OrganizationID || pool.Policy.ConnectionID != route.ConnectionID || pool.Policy.Catalog == nil {
				continue
			}
			admitted, err := pool.Policy.Catalog.Metadata(pool.Policy)
			if err != nil || !sameRouteMetadata(route, admitted) {
				continue
			}
			governance := pool.Policy.Routing
			if governance == nil || governance.Locality == LocalOnly && !route.Local ||
				!providerAllowed(route.Descriptor.Provider, governance.AllowedProviders, governance.DeniedProviders) || !slices.Contains(governance.DataClasses, request.DataClass) {
				continue
			}
			pools = append(pools, pool)
		}
		selection, err := (Manager{Pools: pools}).Select(now, PoolRequest{ConnectionID: route.ConnectionID, Descriptor: route.Descriptor})
		if err != nil || route.Local != (selection.Mode == Local) || request.InputTokens > selection.ReservedInputTokens || request.OutputTokens > selection.ReservedOutputTokens ||
			request.MaxCostNanoUSD != nil && selection.ReservedCostNanoUSD > *request.MaxCostNanoUSD {
			continue
		}
		eligible = append(eligible, RouteSelection{ConnectionID: route.ConnectionID, Descriptor: route.Descriptor, Pool: selection})
	}
	if len(eligible) == 0 {
		return RouteSelection{}, &routeFailure{code: RouteNoEligibleAccount}
	}
	preference := func(id string) int {
		if rank := slices.Index(request.PreferredConnections, id); rank >= 0 {
			return rank
		}
		return len(request.PreferredConnections)
	}
	slices.SortFunc(eligible, func(a, b RouteSelection) int {
		if delta := preference(a.ConnectionID) - preference(b.ConnectionID); delta != 0 {
			return delta
		}
		if a.ConnectionID < b.ConnectionID {
			return -1
		}
		if a.ConnectionID > b.ConnectionID {
			return 1
		}
		return 0
	})
	selected := eligible[0]
	fingerprint, err := request.Fingerprint()
	if err != nil {
		return RouteSelection{}, err
	}
	reason := modelinput.RouteOrdered
	if request.ConnectionID != "" {
		reason = modelinput.RouteExplicit
	} else if slices.Contains(request.PreferredConnections, selected.ConnectionID) {
		reason = modelinput.RoutePreferred
	}
	selected.Decision = modelinput.RouteDecision{
		Version: 1, RequirementsFingerprint: fingerprint, PolicyFingerprint: selected.Pool.PolicyFingerprint,
		SelectedAt: now, ConnectionID: selected.ConnectionID, Provider: selected.Descriptor.Provider,
		Model: selected.Descriptor.Model, ExecutionProfileVersion: selected.Descriptor.ExecutionProfileVersion,
		Local: selected.Pool.Mode == Local, Reason: reason,
		ReservedInputTokens: selected.Pool.ReservedInputTokens, ReservedOutputTokens: selected.Pool.ReservedOutputTokens,
		ReservedCostNanoUSD: selected.Pool.ReservedCostNanoUSD,
	}
	if err := selected.ValidateFor(request); err != nil {
		return RouteSelection{}, err
	}
	return selected, nil
}

func sameRouteMetadata(a, b RouteMetadata) bool {
	return a.ConnectionID == b.ConnectionID && a.Descriptor == b.Descriptor && a.Local == b.Local &&
		a.ContextTokens == b.ContextTokens && a.OutputTokens == b.OutputTokens && a.ValidUntil.Equal(b.ValidUntil) &&
		slices.Equal(a.Capabilities, b.Capabilities) && slices.Equal(a.DataClasses, b.DataClasses)
}

func allCapabilities(available, required []Capability) bool {
	for _, capability := range required {
		if !slices.Contains(available, capability) {
			return false
		}
	}
	return true
}

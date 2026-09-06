package modelinput

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Capability string

const (
	Text             Capability = "text"
	Vision           Capability = "vision"
	ToolCalling      Capability = "tool_calling"
	StructuredOutput Capability = "structured_output"
	Streaming        Capability = "streaming"
	Reasoning        Capability = "reasoning"
)

type Locality string

const (
	LocalOnly    Locality = "LOCAL_ONLY"
	CloudAllowed Locality = "CLOUD_ALLOWED"
)

// RouteRequirements are runtime-owned constraints, not model-authored authority.
// Canonical bytes can be bound in a manifest and checked by admission/replay.
type RouteRequirements struct {
	OrganizationID       string       `json:"organization_id"`
	ConnectionID         string       `json:"connection_id,omitempty"`
	Capabilities         []Capability `json:"capabilities"`
	InputTokens          int64        `json:"input_tokens"`
	OutputTokens         int64        `json:"output_tokens"`
	Locality             Locality     `json:"locality"`
	DataClass            string       `json:"data_class"`
	AllowedProviders     []string     `json:"allowed_providers,omitempty"`
	DeniedProviders      []string     `json:"denied_providers,omitempty"`
	MaxCostNanoUSD       *int64       `json:"max_cost_nano_usd,omitempty"`
	PreferredConnections []string     `json:"preferred_connections,omitempty"`
}

func (r RouteRequirements) Clone() RouteRequirements {
	r.Capabilities = slices.Clone(r.Capabilities)
	r.AllowedProviders = slices.Clone(r.AllowedProviders)
	r.DeniedProviders = slices.Clone(r.DeniedProviders)
	r.PreferredConnections = slices.Clone(r.PreferredConnections)
	if r.MaxCostNanoUSD != nil {
		cost := *r.MaxCostNanoUSD
		r.MaxCostNanoUSD = &cost
	}
	return r
}

func (r RouteRequirements) Validate() error {
	if !validRoutingValue(r.OrganizationID) || !validRoutingValue(r.DataClass) ||
		r.ConnectionID != "" && !validRoutingConnection(r.ConnectionID) ||
		r.InputTokens < 1 || r.OutputTokens < 1 || r.InputTokens > math.MaxInt64-r.OutputTokens ||
		(r.Locality != LocalOnly && r.Locality != CloudAllowed) ||
		r.MaxCostNanoUSD != nil && *r.MaxCostNanoUSD < 0 || len(r.Capabilities) > 6 {
		return fmt.Errorf("inference route requirements are invalid")
	}
	seen := map[Capability]bool{}
	for _, c := range r.Capabilities {
		if seen[c] || !slices.Contains([]Capability{Text, Vision, ToolCalling, StructuredOutput, Streaming, Reasoning}, c) {
			return fmt.Errorf("inference capability requirement is invalid")
		}
		seen[c] = true
	}
	for _, values := range [][]string{r.AllowedProviders, r.DeniedProviders, r.PreferredConnections} {
		if len(values) > 1024 {
			return fmt.Errorf("too many inference route constraints")
		}
		seen := map[string]bool{}
		for _, value := range values {
			if !validRoutingValue(value) || seen[value] {
				return fmt.Errorf("inference route list is invalid")
			}
			seen[value] = true
		}
	}
	for _, id := range r.PreferredConnections {
		if !validRoutingConnection(id) {
			return fmt.Errorf("inference route preference is invalid")
		}
	}
	return nil
}

func (r RouteRequirements) Canonical() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	body, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	if len(body) > 64<<10 {
		return nil, fmt.Errorf("inference route requirements exceed byte limit")
	}
	return body, nil
}

func (r RouteRequirements) Fingerprint() (string, error) {
	body, err := r.Canonical()
	if err != nil {
		return "", err
	}
	return TextDigest(string(body)), nil
}

func SameRouteRequirements(a, b *RouteRequirements) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	aHash, aErr := a.Fingerprint()
	bHash, bErr := b.Fingerprint()
	return aErr == nil && bErr == nil && aHash == bHash
}

func CloneRouteRequirements(r *RouteRequirements) *RouteRequirements {
	if r == nil {
		return nil
	}
	clone := r.Clone()
	return &clone
}

func validRoutingValue(value string) bool {
	return value != "" && len(value) <= 512 && utf8.ValidString(value) && strings.TrimSpace(value) == value &&
		strings.IndexFunc(value, func(r rune) bool { return unicode.IsControl(r) || unicode.Is(unicode.Cf, r) }) < 0
}

func validRoutingConnection(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for _, c := range value {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' && c != '_' {
			return false
		}
	}
	return true
}

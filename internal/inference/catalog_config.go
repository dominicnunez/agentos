package inference

import (
	"fmt"
	"time"

	"github.com/dominicnunez/agentos/internal/execution"
)

// CatalogDefinition extends an account's reviewed policy without duplicating
// provider or model identity. Limits describe verified model capabilities;
// they are not inferred from the configured budget or model name.
type CatalogDefinition struct {
	Capabilities  []Capability `json:"capabilities"`
	Local         bool         `json:"local"`
	ContextTokens int64        `json:"context_tokens"`
	OutputTokens  int64        `json:"output_tokens"`
	DataClasses   []string     `json:"data_classes"`
	ValidUntil    time.Time    `json:"valid_until"`
}

func (c CatalogDefinition) Metadata(policy Policy) (RouteMetadata, error) {
	if err := policy.Validate(); err != nil {
		return RouteMetadata{}, err
	}
	return c.validateForPolicy(policy)
}

func (c CatalogDefinition) validateForPolicy(policy Policy) (RouteMetadata, error) {
	metadata := RouteMetadata{
		ConnectionID: policy.ConnectionID,
		Descriptor:   execution.ModelDescriptor{Provider: policy.Provider, Model: policy.Model, ExecutionProfileVersion: policy.ExecutionProfileVersion},
		Capabilities: append([]Capability(nil), c.Capabilities...), Local: c.Local,
		ContextTokens: c.ContextTokens, OutputTokens: c.OutputTokens,
		DataClasses: append([]string(nil), c.DataClasses...), ValidUntil: c.ValidUntil,
	}
	if metadata.Validate() != nil || c.Local != (policy.Mode == Local) ||
		!c.ValidUntil.After(policy.AuthorizedAt) || c.ValidUntil.After(policy.AuthorizationExpiresAt) {
		return RouteMetadata{}, fmt.Errorf("catalog definition does not match its reviewed account policy")
	}
	// The current model-only adapters implement text completion. Advanced
	// transports must be added before configuration can advertise them.
	for _, capability := range c.Capabilities {
		if capability != Text {
			return RouteMetadata{}, fmt.Errorf("catalog capability is not implemented by current adapters")
		}
	}
	return metadata, nil
}

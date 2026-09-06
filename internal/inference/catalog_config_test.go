package inference

import (
	"testing"
	"time"
)

func TestCatalogDefinitionDerivesExactAccountAndCopiesInput(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	b, _, _ := brokerFixture(now)
	policy := b.Manager.Pools[0].Policy
	definition := CatalogDefinition{Capabilities: []Capability{Text}, ContextTokens: 120, OutputTokens: 20, DataClasses: []string{"internal"}, ValidUntil: policy.AuthorizationExpiresAt}
	metadata, err := definition.Metadata(policy)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.ConnectionID != policy.ConnectionID || metadata.Descriptor.Provider != policy.Provider || metadata.Descriptor.Model != policy.Model || metadata.Descriptor.ExecutionProfileVersion != policy.ExecutionProfileVersion {
		t.Fatal("catalog identity changed")
	}
	definition.DataClasses[0], definition.Capabilities[0] = "secret", Vision
	if metadata.DataClasses[0] != "internal" || metadata.Capabilities[0] != Text {
		t.Fatal("mutable catalog input changed metadata")
	}
}

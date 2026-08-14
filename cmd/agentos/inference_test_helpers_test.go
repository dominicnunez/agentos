package main

import (
	"strconv"
	"time"

	"github.com/dominicnunez/agentos/internal/bootstrap"
	"github.com/dominicnunez/agentos/internal/inference"
)

func testOpenAIProvider(config bootstrap.Config, model, secretRef string) bootstrap.Provider {
	now := config.CreatedAt
	return bootstrap.Provider{
		Kind: bootstrap.ProviderOpenAIAPI, Model: model, SecretRef: secretRef,
		InferencePolicy: inference.Policy{
			Version: inference.PolicyVersion, OrganizationID: config.Organization, Provider: "openai-api", Model: model,
			ExecutionProfileVersion: "v1-openai-responses-model-only", Mode: inference.MeteredAPI,
			MaxInputTokensPerRequest: 100, MaxOutputTokensPerRequest: 100, MaxTokensPerWindow: 10_000,
			ContinuityReserveTokens: 100, WindowDurationSeconds: 3600, MaxConcurrentRequests: 1, MaxAttemptsPerRequest: 1,
			AuthorizedBy: "local-uid-" + strconv.Itoa(config.Owner.UID), AuthorizedAt: now, AuthorizationExpiresAt: now.Add(time.Hour),
			Pricing: &inference.Pricing{
				InputNanoUSDPerMillionTokens: 1, OutputNanoUSDPerMillionTokens: 1,
				MaxCostNanoUSDPerRequest: 2, MaxCostNanoUSDPerWindow: 100, ExpiresAt: now.Add(time.Hour),
			},
		},
	}
}

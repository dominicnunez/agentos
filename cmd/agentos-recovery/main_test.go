package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/bootstrap"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/ledger"
	ledgeranchor "github.com/dominicnunez/agentos/internal/ledger/anchor"
	"github.com/dominicnunez/agentos/internal/ledger/recovery"
)

func TestRunRequiresKnownCommand(t *testing.T) {
	for _, args := range [][]string{nil, {"unknown"}, {"verify", "extra"}} {
		if err := run(context.Background(), args, &bytes.Buffer{}); err == nil {
			t.Fatalf("run(%v) succeeded", args)
		}
	}
}

func TestRunPrintsVersionWithoutOpeningDatabase(t *testing.T) {
	for _, args := range [][]string{{"--version"}, {"version"}} {
		var output bytes.Buffer
		if err := run(context.Background(), args, &output); err != nil {
			t.Fatal(err)
		}
		if output.String() != version+"\n" {
			t.Fatalf("run(%v) output=%q", args, output.String())
		}
	}
}

func TestRunVerifyReturnsStructuredResult(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source.db")
	store, err := ledger.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	state, err := store.IntegrityAnchorState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	privateKey := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	publicKey := privateKey.Public().(ed25519.PublicKey)
	encodedPublic, _ := ledgeranchor.EncodePublicKey(publicKey)
	keyID, _ := ledgeranchor.PublicKeyID(publicKey)
	installationID := "install-" + strings.Repeat("ab", 32)
	checkpoint := filepath.Join(directory, "ledger-anchor.json")
	if _, err := ledgeranchor.Initialize(checkpoint, installationID, privateKey, state, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	paths, err := bootstrap.UserPaths(filepath.Join(directory, "home"), filepath.Join(directory, "run"), 1000)
	if err != nil {
		t.Fatal(err)
	}
	paths.Database = source
	paths.StateDir = directory
	now := time.Now().UTC()
	config := bootstrap.NewConfig(bootstrap.ModeUser, bootstrap.Owner{Username: "alice", UID: 1000, GID: 1000}, paths, now)
	config.Integrity = bootstrap.IntegrityAnchor{InstallationID: installationID, CheckpointFile: checkpoint, PublicKey: encodedPublic, KeyID: keyID, SecretRef: "ledger-anchor-signing-key", SignatureAlgorithm: ledgeranchor.SignatureAlgorithm}
	config.Providers = []bootstrap.Provider{{
		Kind: bootstrap.ProviderOpenAIAPI, Model: "gpt-test-2026-08-01", SecretRef: "openai-api-key",
		InferencePolicy: inference.Policy{
			Version: inference.PolicyVersion, OrganizationID: config.Organization, Provider: "openai-api", Model: "gpt-test-2026-08-01", ExecutionProfileVersion: "v1-openai-responses-model-only", Mode: inference.MeteredAPI,
			MaxInputTokensPerRequest: 100, MaxOutputTokensPerRequest: 100, MaxTokensPerWindow: 1000, ContinuityReserveTokens: 100,
			WindowDurationSeconds: 3600, MaxConcurrentRequests: 1, MaxAttemptsPerRequest: 1,
			AuthorizedBy: "local-uid-1000", AuthorizedAt: now, AuthorizationExpiresAt: now.Add(time.Hour),
			Pricing: &inference.Pricing{InputNanoUSDPerMillionTokens: 1, OutputNanoUSDPerMillionTokens: 1, MaxCostNanoUSDPerRequest: 2, MaxCostNanoUSDPerWindow: 100, ExpiresAt: now.Add(time.Hour)},
		},
	}}
	configPath := filepath.Join(directory, "config.json")
	if err := bootstrap.SaveConfig(configPath, config); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := run(context.Background(), []string{"verify", "--config", configPath, "--database", source}, &output); err != nil {
		t.Fatal(err)
	}
	var result recovery.Result
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Path != source || result.CheckpointPath != checkpoint || result.SHA256 == "" || result.SizeBytes == 0 {
		t.Fatalf("result=%+v", result)
	}
}

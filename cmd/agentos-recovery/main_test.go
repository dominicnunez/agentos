package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"os"
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
	publicKey, err := ledgeranchor.PublicKeyFromPrivate(privateKey)
	if err != nil {
		t.Fatal(err)
	}
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
	if result.CheckpointKeyID != keyID || result.CheckpointPublicKey != encodedPublic {
		t.Fatalf("checkpoint trust result=%+v", result)
	}
}

func TestTrustedRecoveryCheckpointAcceptsOnlyRetainedAncestor(t *testing.T) {
	directory := t.TempDir()
	transitions := filepath.Join(directory, "ledger-anchor-transitions")
	if err := os.Mkdir(transitions, 0o700); err != nil {
		t.Fatal(err)
	}
	installationID := "install-" + strings.Repeat("ac", 32)
	oldPrivate := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	oldPublic, _ := ledgeranchor.PublicKeyFromPrivate(oldPrivate)
	nextSeed := make([]byte, ed25519.SeedSize)
	nextSeed[0] = 1
	nextPrivate := ed25519.NewKeyFromSeed(nextSeed)
	nextPublic, _ := ledgeranchor.PublicKeyFromPrivate(nextPrivate)
	now := time.Date(2026, 8, 26, 18, 0, 0, 0, time.UTC)
	state := ledgeranchor.LedgerState{ApplicationID: ledger.StorageApplicationID, StorageVersion: ledger.CurrentStorageVersion, EventSchemaVersion: 1, ChainAlgorithm: ledger.EventIntegrityAlgorithm}
	oldCheckpointPath := filepath.Join(directory, "old.anchor.json")
	oldCheckpoint, err := ledgeranchor.Initialize(oldCheckpointPath, installationID, oldPrivate, state, now)
	if err != nil {
		t.Fatal(err)
	}
	oldBody, err := os.ReadFile(oldCheckpointPath)
	if err != nil {
		t.Fatal(err)
	}
	_, _, transitionBody, err := ledgeranchor.NewAuthorizedRotation(oldCheckpoint, oldBody, oldPrivate, nextPrivate, "local-uid-1000", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(transitions, "rotation.json"), transitionBody, 0o600); err != nil {
		t.Fatal(err)
	}
	encodedNext, _ := ledgeranchor.EncodePublicKey(nextPublic)
	config := bootstrap.Config{Paths: bootstrap.Paths{StateDir: directory}, Integrity: bootstrap.IntegrityAnchor{InstallationID: installationID, PublicKey: encodedNext}}
	encodedOld, oldKeyID, err := trustedRecoveryCheckpoint(config, oldCheckpointPath)
	if err != nil {
		t.Fatal(err)
	}
	wantEncodedOld, _ := ledgeranchor.EncodePublicKey(oldPublic)
	wantOldKeyID, _ := ledgeranchor.PublicKeyID(oldPublic)
	if encodedOld != wantEncodedOld || oldKeyID != wantOldKeyID {
		t.Fatalf("historical key=%q/%q want=%q/%q", encodedOld, oldKeyID, wantEncodedOld, wantOldKeyID)
	}
}

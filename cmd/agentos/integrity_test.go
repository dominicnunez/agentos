package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/bootstrap"
	"github.com/dominicnunez/agentos/internal/ledger"
	ledgeranchor "github.com/dominicnunez/agentos/internal/ledger/anchor"
	"github.com/dominicnunez/agentos/internal/secrets"
)

func TestConfiguredLedgerAnchorRequiresMatchingAvailableSigningCredential(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	database := filepath.Join(directory, "agentos.db")
	ledgerStore, err := ledger.Open(database)
	if err != nil {
		t.Fatal(err)
	}
	state, err := ledgerStore.IntegrityAnchorState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledgerStore.Close(); err != nil {
		t.Fatal(err)
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	installationID := "install-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	checkpoint := filepath.Join(directory, "ledger-anchor.json")
	if _, err := ledgeranchor.Initialize(checkpoint, installationID, privateKey, state, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	encodedPublic, err := ledgeranchor.EncodePublicKey(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	keyID, err := ledgeranchor.PublicKeyID(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	config := bootstrap.Config{Integrity: bootstrap.IntegrityAnchor{
		InstallationID: installationID, CheckpointFile: checkpoint, PublicKey: encodedPublic,
		KeyID: keyID, SecretRef: "ledger-anchor-signing-key", SignatureAlgorithm: ledgeranchor.SignatureAlgorithm,
	}}
	credentialBody, err := json.Marshal(ledgerAnchorCredential{
		Version: 1, InstallationID: installationID, PrivateKey: base64.StdEncoding.EncodeToString(privateKey),
	})
	if err != nil {
		t.Fatal(err)
	}
	store, err := configuredLedgerAnchor(ctx, config, testSecrets{"ledger-anchor-signing-key": secrets.Value(credentialBody)}, state)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := configuredLedgerAnchor(ctx, config, testSecrets{}, state); err == nil {
		t.Fatal("runtime accepted a missing ledger anchor signing credential")
	}
	_, wrongPrivateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	wrongBody, err := json.Marshal(ledgerAnchorCredential{
		Version: 1, InstallationID: installationID, PrivateKey: base64.StdEncoding.EncodeToString(wrongPrivateKey),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := configuredLedgerAnchor(ctx, config, testSecrets{"ledger-anchor-signing-key": secrets.Value(wrongBody)}, state); err == nil {
		t.Fatal("runtime accepted a signing credential outside the configured trust root")
	}
}

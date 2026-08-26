package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/bootstrap"
	"github.com/dominicnunez/agentos/internal/events"
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
	if !privateKeyMatchesPublicKey(privateKey, publicKey) {
		t.Fatal("matching maintenance credential was rejected")
	}
	if privateKeyMatchesPublicKey(wrongPrivateKey, publicKey) {
		t.Fatal("substituted maintenance credential was treated as the current key")
	}
}

func TestPendingResolutionEvidenceIsRetrySafeAndCollisionResistant(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("private evidence mode semantics are Linux-only")
	}
	directory := t.TempDir()
	checkpointPath := filepath.Join(directory, "ledger-anchor.json")
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	installationID := "install-" + strings.Repeat("ab", 32)
	committedState := ledgeranchor.LedgerState{
		ApplicationID: ledger.StorageApplicationID, StorageVersion: ledger.CurrentStorageVersion,
		EventSchemaVersion: events.SchemaVersion, ChainAlgorithm: "SHA-256", AuthorityAlgorithm: "SHA-256", AuthoritySHA256: strings.Repeat("0", 64),
	}
	observedAt := time.Date(2026, 8, 26, 18, 0, 0, 0, time.UTC)
	if _, err := ledgeranchor.Initialize(checkpointPath, installationID, privateKey, committedState, observedAt); err != nil {
		t.Fatal(err)
	}
	committed, committedBody, err := ledgeranchor.Read(checkpointPath, installationID, publicKey)
	if err != nil {
		t.Fatal(err)
	}
	firstPending, firstBody := preparePendingCheckpoint(t, checkpointPath, installationID, publicKey, privateKey, committedState, ledgeranchor.LedgerState{
		ApplicationID: ledger.StorageApplicationID, StorageVersion: ledger.CurrentStorageVersion,
		EventSchemaVersion: events.SchemaVersion, EventCount: 1, Sequence: 1, EventID: "evt-1",
		ChainAlgorithm: "SHA-256", ChainHead: strings.Repeat("a", 64),
		AuthorityAlgorithm: "SHA-256", AuthoritySHA256: strings.Repeat("0", 64),
	}, observedAt.Add(time.Minute))
	firstPath, err := preservePendingResolution(directory, "local-uid-1000", committed, committedBody, firstPending, firstBody, committedState, privateKey, publicKey, observedAt.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(firstPath)
	if err != nil {
		t.Fatal(err)
	}
	retriedPath, err := preservePendingResolution(directory, "local-uid-1000", committed, committedBody, firstPending, firstBody, committedState, privateKey, publicKey, observedAt.Add(3*time.Minute))
	if err != nil || retriedPath != firstPath {
		t.Fatalf("retry path=%q want=%q err=%v", retriedPath, firstPath, err)
	}
	after, err := os.ReadFile(firstPath)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("retry changed signed evidence: %v", err)
	}
	if err := os.Remove(checkpointPath + ".pending"); err != nil {
		t.Fatal(err)
	}
	secondPending, secondBody := preparePendingCheckpoint(t, checkpointPath, installationID, publicKey, privateKey, committedState, ledgeranchor.LedgerState{
		ApplicationID: ledger.StorageApplicationID, StorageVersion: ledger.CurrentStorageVersion,
		EventSchemaVersion: events.SchemaVersion, EventCount: 1, Sequence: 1, EventID: "evt-2",
		ChainAlgorithm: "SHA-256", ChainHead: strings.Repeat("b", 64),
		AuthorityAlgorithm: "SHA-256", AuthoritySHA256: strings.Repeat("0", 64),
	}, observedAt.Add(4*time.Minute))
	secondPath, err := preservePendingResolution(directory, "local-uid-1000", committed, committedBody, secondPending, secondBody, committedState, privateKey, publicKey, observedAt.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if secondPath == firstPath {
		t.Fatal("different discarded checkpoint reused existing resolution evidence path")
	}
}

func preparePendingCheckpoint(t *testing.T, checkpointPath, installationID string, publicKey ed25519.PublicKey, privateKey ed25519.PrivateKey, committedState, nextState ledgeranchor.LedgerState, observedAt time.Time) (ledgeranchor.Checkpoint, []byte) {
	t.Helper()
	store, err := ledgeranchor.Open(checkpointPath, installationID, publicKey, privateKey, committedState, func() time.Time { return observedAt })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Prepare(nextState); err != nil {
		t.Fatal(err)
	}
	pending, body, err := ledgeranchor.Read(checkpointPath+".pending", installationID, publicKey)
	if err != nil {
		t.Fatal(err)
	}
	_ = store.Close()
	return pending, body
}

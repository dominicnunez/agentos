package anchor

import (
	"crypto/ed25519"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestTrustedPublicKeyHistoryVerifiesRetainedAncestor(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "ledger-anchor-transitions")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	installationID := "install-" + strings.Repeat("7a", 32)
	oldPrivate := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	oldPublic, _ := PublicKeyFromPrivate(oldPrivate)
	nextSeed := make([]byte, ed25519.SeedSize)
	nextSeed[0] = 1
	nextPrivate := ed25519.NewKeyFromSeed(nextSeed)
	nextPublic, _ := PublicKeyFromPrivate(nextPrivate)
	now := time.Date(2026, 8, 26, 18, 0, 0, 0, time.UTC)
	checkpoint, checkpointBody, err := newCheckpoint(installationID, 0, state(0, "", ""), now, "", oldPrivate)
	if err != nil {
		t.Fatal(err)
	}
	_, _, transitionBody, err := NewAuthorizedRotation(checkpoint, checkpointBody, oldPrivate, nextPrivate, "local-uid-1000", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "rotation.json"), transitionBody, 0o600); err != nil {
		t.Fatal(err)
	}
	trusted, err := TrustedPublicKeyHistory(directory, installationID, nextPublic)
	if err != nil || len(trusted) != 2 || !containsPublicKey(trusted, oldPublic) {
		t.Fatalf("trusted keys=%d err=%v", len(trusted), err)
	}
	checkpointPath := filepath.Join(t.TempDir(), "old.anchor.json")
	if err := os.WriteFile(checkpointPath, checkpointBody, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, selected, err := ReadTrustedCheckpoint(checkpointPath, installationID, trusted); err != nil || !containsPublicKey([]ed25519.PublicKey{oldPublic}, selected) {
		t.Fatalf("selected historical checkpoint key=%x err=%v", selected, err)
	}
}

func TestTrustedPublicKeyHistoryIgnoresUnrelatedValidEvidence(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "ledger-anchor-transitions")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	installationID := "install-" + strings.Repeat("7b", 32)
	currentSeed := make([]byte, ed25519.SeedSize)
	currentSeed[0] = 1
	currentPrivate := ed25519.NewKeyFromSeed(currentSeed)
	currentPublic, _ := PublicKeyFromPrivate(currentPrivate)
	unrelatedPrivate := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	unrelatedNextSeed := make([]byte, ed25519.SeedSize)
	unrelatedNextSeed[0] = 2
	unrelatedNext := ed25519.NewKeyFromSeed(unrelatedNextSeed)
	now := time.Date(2026, 8, 26, 18, 0, 0, 0, time.UTC)
	checkpoint, body, err := newCheckpoint(installationID, 0, state(0, "", ""), now, "", unrelatedPrivate)
	if err != nil {
		t.Fatal(err)
	}
	_, _, transitionBody, err := NewAuthorizedRotation(checkpoint, body, unrelatedPrivate, unrelatedNext, "local-uid-1000", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "unrelated.json"), transitionBody, 0o600); err != nil {
		t.Fatal(err)
	}
	trusted, err := TrustedPublicKeyHistory(directory, installationID, currentPublic)
	if err != nil || len(trusted) != 1 || !containsPublicKey(trusted, currentPublic) {
		t.Fatalf("unrelated evidence expanded trust: keys=%d err=%v", len(trusted), err)
	}
}

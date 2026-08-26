package anchor

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStoreCompletesOnlyUnambiguousCommitRecovery(t *testing.T) {
	path, installationID, publicKey, privateKey, genesis, now := fixture(t)
	if _, err := Initialize(path, installationID, privateKey, genesis, now); err != nil {
		t.Fatal(err)
	}
	store, err := Open(path, installationID, publicKey, privateKey, genesis, func() time.Time { return now.Add(time.Minute) })
	if err != nil {
		t.Fatal(err)
	}
	first := state(1, "evt-1", strings.Repeat("a", 64))
	prepared, err := store.Prepare(first)
	if err != nil || !prepared {
		t.Fatalf("prepare=%v err=%v", prepared, err)
	}
	if _, err := os.Stat(path + ".pending"); err != nil {
		t.Fatalf("pending checkpoint missing: %v", err)
	}

	if _, err := Open(path, installationID, publicKey, privateKey, genesis, nil); !errors.Is(err, ErrAmbiguousPending) {
		t.Fatalf("older database did not fail closed on ambiguous pending checkpoint: %v", err)
	}
	recovered, err := Open(path, installationID, publicKey, privateKey, first, nil)
	if err != nil {
		t.Fatalf("finish committed pending checkpoint: %v", err)
	}
	if _, err := os.Stat(path + ".pending"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("promoted pending checkpoint remains: %v", err)
	}

	second := state(2, "evt-2", strings.Repeat("b", 64))
	if prepared, err := recovered.Prepare(second); err != nil || !prepared {
		t.Fatalf("prepare second=%v err=%v", prepared, err)
	}
	if err := recovered.CommitPrepared(); err != nil {
		t.Fatal(err)
	}
	checkpoint, _, err := Read(path, installationID, publicKey)
	if err != nil || checkpoint.Generation != 2 || !checkpoint.Ledger.Equal(second) || checkpoint.PreviousCheckpointSHA256 == "" {
		t.Fatalf("committed checkpoint=%+v err=%v", checkpoint, err)
	}
}

func TestStoreRejectsRollbackSubstitutionAndWrongTrustRoot(t *testing.T) {
	path, installationID, publicKey, privateKey, genesis, now := fixture(t)
	if _, err := Initialize(path, installationID, privateKey, genesis, now); err != nil {
		t.Fatal(err)
	}
	store, err := Open(path, installationID, publicKey, privateKey, genesis, func() time.Time { return now.Add(time.Minute) })
	if err != nil {
		t.Fatal(err)
	}
	first := state(1, "evt-1", strings.Repeat("c", 64))
	if _, err := store.Prepare(first); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitPrepared(); err != nil {
		t.Fatal(err)
	}
	for name, candidate := range map[string]LedgerState{
		"truncation":   genesis,
		"substitution": state(1, "evt-other", strings.Repeat("d", 64)),
		"application":  {ApplicationID: 2, StorageVersion: 6, EventSchemaVersion: 4, EventCount: 1, Sequence: 1, EventID: "evt-1", ChainAlgorithm: "SHA-256", ChainHead: strings.Repeat("c", 64)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Open(path, installationID, publicKey, privateKey, candidate, nil); err == nil {
				t.Fatal("mismatched database was accepted")
			}
		})
	}
	otherPublic, otherPrivate := keyPair(9)
	if _, err := Open(path, installationID, otherPublic, otherPrivate, first, nil); err == nil {
		t.Fatal("substituted trust root was accepted")
	}
}

func TestStoreRejectsTamperMalformedDataAndClockRollback(t *testing.T) {
	path, installationID, publicKey, privateKey, genesis, now := fixture(t)
	if _, err := Initialize(path, installationID, privateKey, genesis, now); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var checkpoint map[string]any
	if err := json.Unmarshal(body, &checkpoint); err != nil {
		t.Fatal(err)
	}
	checkpoint["unexpected"] = true
	malformed, _ := json.Marshal(checkpoint)
	if err := os.WriteFile(path, append(malformed, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Read(path, installationID, publicKey); err == nil {
		t.Fatal("unknown checkpoint field was accepted")
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	body[len(body)/2] ^= 1
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Read(path, installationID, publicKey); err == nil {
		t.Fatal("tampered checkpoint was accepted")
	}

	path, installationID, publicKey, privateKey, genesis, now = fixture(t)
	if _, err := Initialize(path, installationID, privateKey, genesis, now); err != nil {
		t.Fatal(err)
	}
	store, err := Open(path, installationID, publicKey, privateKey, genesis, func() time.Time { return now.Add(-time.Second) })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Prepare(state(1, "evt-1", strings.Repeat("e", 64))); err == nil || !strings.Contains(err.Error(), "wall clock") {
		t.Fatalf("clock rollback was not rejected: %v", err)
	}
}

func TestInitializeNeverOverwritesCheckpoint(t *testing.T) {
	path, installationID, _, privateKey, genesis, now := fixture(t)
	if _, err := Initialize(path, installationID, privateKey, genesis, now); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	if _, err := Initialize(path, installationID, privateKey, genesis, now); err == nil {
		t.Fatal("existing checkpoint was overwritten")
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Fatal("failed initialization changed checkpoint bytes")
	}
}

func TestCloseRetainsUnresolvedPendingCheckpointAndDisablesWriter(t *testing.T) {
	path, installationID, publicKey, privateKey, genesis, now := fixture(t)
	if _, err := Initialize(path, installationID, privateKey, genesis, now); err != nil {
		t.Fatal(err)
	}
	store, err := Open(path, installationID, publicKey, privateKey, genesis, func() time.Time { return now.Add(time.Minute) })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Prepare(state(1, "evt-1", strings.Repeat("f", 64))); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err == nil || !strings.Contains(err.Error(), "unresolved") {
		t.Fatalf("close did not report unresolved checkpoint: %v", err)
	}
	if _, err := os.Stat(path + ".pending"); err != nil {
		t.Fatalf("unresolved pending checkpoint was discarded: %v", err)
	}
	if _, err := store.Prepare(state(2, "evt-2", strings.Repeat("e", 64))); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("closed anchor writer continued after uncertainty: %v", err)
	}
}

func fixture(t *testing.T) (string, string, ed25519.PublicKey, ed25519.PrivateKey, LedgerState, time.Time) {
	t.Helper()
	publicKey, privateKey := keyPair(1)
	return filepath.Join(t.TempDir(), "ledger-anchor.json"), "install-" + strings.Repeat("1a", 32), publicKey, privateKey,
		LedgerState{ApplicationID: 0x41474f53, StorageVersion: 6, EventSchemaVersion: 4, ChainAlgorithm: "SHA-256"},
		time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
}

func keyPair(fill byte) (ed25519.PublicKey, ed25519.PrivateKey) {
	seed := make([]byte, ed25519.SeedSize)
	for index := range seed {
		seed[index] = fill
	}
	privateKey := ed25519.NewKeyFromSeed(seed)
	return append(ed25519.PublicKey(nil), privateKey[ed25519.SeedSize:]...), privateKey
}

func state(sequence int64, eventID, head string) LedgerState {
	return LedgerState{
		ApplicationID: 0x41474f53, StorageVersion: 6, EventSchemaVersion: 4,
		EventCount: sequence, Sequence: sequence, EventID: eventID,
		ChainAlgorithm: "SHA-256", ChainHead: head,
	}
}

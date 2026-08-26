package anchor

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"testing"
	"time"
)

func TestPendingResolutionPreservesSignedDiscardedSuccessor(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	committed, committedBody, err := newCheckpoint(testInstallationID, 2, state(1, "event-1", testSHA256("one")), now, testSHA256("older"), privateKey)
	if err != nil {
		t.Fatal(err)
	}
	pending, pendingBody, err := newCheckpoint(testInstallationID, 3, state(2, "event-2", testSHA256("two")), now.Add(time.Minute), checkpointSHA256(committedBody), privateKey)
	if err != nil {
		t.Fatal(err)
	}
	record, body, err := NewPendingResolution(committed, committedBody, pending, pendingBody, committed.Ledger, privateKey, "local-uid-1000", now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	verified, err := VerifyPendingResolution(body, publicKey)
	if err != nil {
		t.Fatal(err)
	}
	if verified.Resolution != ResolutionRetainCommittedHead || verified.DiscardedCheckpoint != pending || record.CommittedCheckpoint != committed {
		t.Fatalf("resolution=%+v", verified)
	}
}

func TestPendingResolutionRejectsTamperAndWrongSQLiteHead(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	committed, committedBody, err := newCheckpoint(testInstallationID, 0, state(0, "", ""), now, "", privateKey)
	if err != nil {
		t.Fatal(err)
	}
	pending, pendingBody, err := newCheckpoint(testInstallationID, 1, state(1, "event-1", testSHA256("one")), now.Add(time.Minute), checkpointSHA256(committedBody), privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := NewPendingResolution(committed, committedBody, pending, pendingBody, pending.Ledger, privateKey, "local-uid-1000", now.Add(2*time.Minute)); err == nil {
		t.Fatal("pending successor was discarded while SQLite matched it")
	}
	_, body, err := NewPendingResolution(committed, committedBody, pending, pendingBody, committed.Ledger, privateKey, "local-uid-1000", now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	var tampered PendingResolution
	if err := json.Unmarshal(body, &tampered); err != nil {
		t.Fatal(err)
	}
	tampered.AuthorizedBy = "local-uid-0"
	tamperedBody, err := marshalPendingResolution(tampered)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyPendingResolution(tamperedBody, publicKey); err == nil {
		t.Fatal("tampered pending resolution was accepted")
	}
}

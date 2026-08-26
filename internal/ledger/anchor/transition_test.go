package anchor

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"
)

const testInstallationID = "install-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestAuthorizedRotationPreservesHeadAndProvesPriorKeyAuthorization(t *testing.T) {
	previousPublic, previousPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, nextPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	previous, previousBody, err := newCheckpoint(testInstallationID, 7, testNonEmptyState(), now, testSHA256("previous"), previousPrivate)
	if err != nil {
		t.Fatal(err)
	}
	record, nextBody, recordBody, err := NewAuthorizedRotation(previous, previousBody, previousPrivate, nextPrivate, "local-uid-1000", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	verified, err := VerifyKeyTransition(recordBody, previousPublic)
	if err != nil {
		t.Fatal(err)
	}
	if verified.Continuity != TransitionAuthorizedRotation || !verified.NextCheckpoint.Ledger.Equal(previous.Ledger) || verified.NextCheckpoint.Generation != previous.Generation+1 {
		t.Fatalf("transition=%+v", verified)
	}
	nextPublic, err := PublicKeyFromPrivate(nextPrivate)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyCheckpoint(record.NextCheckpoint, testInstallationID, nextPublic); err != nil {
		t.Fatal(err)
	}
	if err := exactCheckpointBytes(record.NextCheckpoint, nextBody); err != nil {
		t.Fatal(err)
	}

	otherPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyKeyTransition(recordBody, otherPublic); err == nil {
		t.Fatal("rotation accepted an untrusted previous key")
	}

	var tampered KeyTransition
	if err := json.Unmarshal(recordBody, &tampered); err != nil {
		t.Fatal(err)
	}
	tampered.AuthorizedBy = "local-uid-0"
	tamperedBody, err := marshalTransition(tampered)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyKeyTransition(tamperedBody, previousPublic); err == nil {
		t.Fatal("rotation accepted changed authorization evidence")
	}
}

func TestReviewedTrustResetIsExplicitlyNotPriorKeyAuthorized(t *testing.T) {
	previousPublic, previousPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, nextPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	previous, previousBody, err := newCheckpoint(testInstallationID, 3, testNonEmptyState(), now, testSHA256("previous"), previousPrivate)
	if err != nil {
		t.Fatal(err)
	}
	record, _, recordBody, err := NewReviewedTrustReset(previous, previousBody, previousPublic, nextPrivate, "local-uid-0", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	verified, err := VerifyKeyTransition(recordBody, previousPublic)
	if err != nil {
		t.Fatal(err)
	}
	if verified.Continuity != TransitionReviewedTrustReset || verified.Reason != RecoveryReasonKeyUnavailable || verified.PreviousAuthorization != "" || !verified.NextCheckpoint.Ledger.Equal(previous.Ledger) {
		t.Fatalf("transition=%+v", verified)
	}
	tampered := record
	tampered.AuthorizedBy = "local-uid-1000"
	tamperedBody, err := marshalTransition(tampered)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyKeyTransition(tamperedBody, previousPublic); err == nil {
		t.Fatal("trust reset accepted changed local-authority evidence")
	}

	record.PreviousAuthorization = "forged"
	forged, err := marshalTransition(record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyKeyTransition(forged, previousPublic); err == nil {
		t.Fatal("trust reset concealed a prior-key authorization")
	}
}

func TestKeyTransitionRejectsInvalidAuthorityTimeAndCheckpointBytes(t *testing.T) {
	_, previousPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, nextPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	previous, previousBody, err := newCheckpoint(testInstallationID, 1, testNonEmptyState(), now, testSHA256("previous"), previousPrivate)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name       string
		authority  string
		observedAt time.Time
		body       []byte
	}{
		{name: "authority", authority: "agent-1", observedAt: now.Add(time.Minute), body: previousBody},
		{name: "clock rollback", authority: "local-uid-1000", observedAt: now.Add(-time.Second), body: previousBody},
		{name: "noncanonical checkpoint", authority: "local-uid-1000", observedAt: now.Add(time.Minute), body: append([]byte(" "), previousBody...)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, _, _, err := NewAuthorizedRotation(previous, test.body, previousPrivate, nextPrivate, test.authority, test.observedAt); err == nil {
				t.Fatal("invalid transition was accepted")
			}
		})
	}
}

func testNonEmptyState() LedgerState {
	return LedgerState{
		ApplicationID: 1, StorageVersion: 6, EventSchemaVersion: 1,
		EventCount: 2, Sequence: 2, EventID: "event-2",
		ChainAlgorithm: "SHA-256", ChainHead: testSHA256("head"),
		AuthorityAlgorithm: "SHA-256", AuthoritySHA256: testSHA256("authority"),
	}
}

func testSHA256(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

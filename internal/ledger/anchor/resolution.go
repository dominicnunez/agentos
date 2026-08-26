package anchor

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash"
	"time"
)

const (
	PendingResolutionSchemaVersion = 1
	ResolutionRetainCommittedHead  = "RETAIN_COMMITTED_SQLITE_HEAD"
	pendingResolutionDomain        = "agentos.ledger-anchor-pending-resolution.v1"
)

// PendingResolution preserves the exact abandoned successor checkpoint when
// reviewed local recovery decides that SQLite did not retain the prepared
// transaction. This is recovery evidence, not proof that the discarded event
// never occurred elsewhere.
type PendingResolution struct {
	SchemaVersion       int        `json:"schema_version"`
	Resolution          string     `json:"resolution"`
	InstallationID      string     `json:"installation_id"`
	AuthorizedBy        string     `json:"authorized_by"`
	ObservedAt          time.Time  `json:"observed_at"`
	TimeEvidence        string     `json:"time_evidence"`
	CommittedCheckpoint Checkpoint `json:"committed_checkpoint"`
	DiscardedCheckpoint Checkpoint `json:"discarded_checkpoint"`
	SignatureAlgorithm  string     `json:"signature_algorithm"`
	KeyID               string     `json:"key_id"`
	Signature           string     `json:"signature"`
}

func NewPendingResolution(committed Checkpoint, committedBody []byte, pending Checkpoint, pendingBody []byte, current LedgerState, privateKey ed25519.PrivateKey, authorizedBy string, observedAt time.Time) (PendingResolution, []byte, error) {
	if !validLocalAuthority(authorizedBy) || observedAt.IsZero() || observedAt.Location() != time.UTC || len(privateKey) != ed25519.PrivateKeySize {
		return PendingResolution{}, nil, fmt.Errorf("pending checkpoint resolution authority, time, or key is invalid")
	}
	publicKey := privateKey.Public().(ed25519.PublicKey)
	if err := exactCheckpointBytes(committed, committedBody); err != nil {
		return PendingResolution{}, nil, err
	}
	if err := exactCheckpointBytes(pending, pendingBody); err != nil {
		return PendingResolution{}, nil, err
	}
	if err := verifyCheckpoint(committed, committed.InstallationID, publicKey); err != nil {
		return PendingResolution{}, nil, err
	}
	if err := verifyCheckpoint(pending, committed.InstallationID, publicKey); err != nil {
		return PendingResolution{}, nil, err
	}
	if err := validateSuccessor(committed, committedBody, pending); err != nil || !committed.Ledger.Equal(current) || observedAt.Before(pending.ObservedAt) {
		return PendingResolution{}, nil, fmt.Errorf("pending checkpoint cannot be resolved against the current SQLite head")
	}
	keyID, _ := PublicKeyID(publicKey)
	record := PendingResolution{
		SchemaVersion: PendingResolutionSchemaVersion, Resolution: ResolutionRetainCommittedHead,
		InstallationID: committed.InstallationID, AuthorizedBy: authorizedBy,
		ObservedAt: observedAt, TimeEvidence: TimeEvidence,
		CommittedCheckpoint: committed, DiscardedCheckpoint: pending,
		SignatureAlgorithm: SignatureAlgorithm, KeyID: keyID,
	}
	record.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, pendingResolutionPayload(record)))
	body, err := marshalPendingResolution(record)
	return record, body, err
}

func VerifyPendingResolution(body []byte, trustedPublicKey ed25519.PublicKey) (PendingResolution, error) {
	var record PendingResolution
	if err := decodeCanonicalDocument(body, &record, func() ([]byte, error) { return marshalPendingResolution(record) }, "pending checkpoint resolution"); err != nil {
		return PendingResolution{}, err
	}
	keyID, err := PublicKeyID(trustedPublicKey)
	if err != nil {
		return PendingResolution{}, err
	}
	if record.SchemaVersion != PendingResolutionSchemaVersion || record.Resolution != ResolutionRetainCommittedHead || !validInstallationID(record.InstallationID) || !validLocalAuthority(record.AuthorizedBy) || record.ObservedAt.IsZero() || record.ObservedAt.Location() != time.UTC || record.TimeEvidence != TimeEvidence || record.SignatureAlgorithm != SignatureAlgorithm || record.KeyID != keyID || record.CommittedCheckpoint.InstallationID != record.InstallationID || record.DiscardedCheckpoint.InstallationID != record.InstallationID {
		return PendingResolution{}, fmt.Errorf("pending checkpoint resolution envelope is invalid")
	}
	committedBody, err := marshalCheckpoint(record.CommittedCheckpoint)
	if err != nil || verifyCheckpoint(record.CommittedCheckpoint, record.InstallationID, trustedPublicKey) != nil || verifyCheckpoint(record.DiscardedCheckpoint, record.InstallationID, trustedPublicKey) != nil || validateSuccessor(record.CommittedCheckpoint, committedBody, record.DiscardedCheckpoint) != nil || record.ObservedAt.Before(record.DiscardedCheckpoint.ObservedAt) {
		return PendingResolution{}, fmt.Errorf("pending checkpoint resolution evidence is invalid")
	}
	signature, err := base64.StdEncoding.DecodeString(record.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize || !ed25519.Verify(trustedPublicKey, pendingResolutionPayload(record), signature) {
		return PendingResolution{}, fmt.Errorf("pending checkpoint resolution signature is invalid")
	}
	return record, nil
}

func marshalPendingResolution(record PendingResolution) ([]byte, error) {
	body, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(body, '\n'), nil
}

func pendingResolutionPayload(record PendingResolution) []byte {
	digest := sha256.New()
	writeResolutionField(digest, []byte(pendingResolutionDomain))
	writeResolutionField(digest, []byte(record.Resolution))
	writeResolutionField(digest, []byte(record.InstallationID))
	writeResolutionField(digest, []byte(record.AuthorizedBy))
	writeResolutionField(digest, []byte(record.ObservedAt.Format(time.RFC3339Nano)))
	committed, _ := marshalCheckpoint(record.CommittedCheckpoint)
	discarded, _ := marshalCheckpoint(record.DiscardedCheckpoint)
	writeResolutionField(digest, committed)
	writeResolutionField(digest, discarded)
	writeResolutionField(digest, []byte(record.SignatureAlgorithm))
	writeResolutionField(digest, []byte(record.KeyID))
	return digest.Sum(nil)
}

func writeResolutionField(digest hash.Hash, value []byte) {
	writeInt(digest, int64(len(value)))
	_, _ = digest.Write(value)
}

package anchor

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"strconv"
	"strings"
	"time"
)

const (
	TransitionSchemaVersion      = 1
	TransitionAuthorizedRotation = "AUTHORIZED_ROTATION"
	TransitionReviewedTrustReset = "REVIEWED_TRUST_RESET"
	RecoveryReasonKeyUnavailable = "SIGNING_KEY_UNAVAILABLE"
	transitionDomain             = "agentos.ledger-anchor-key-transition.v1"
)

// KeyTransition is durable public evidence for changing the checkpoint trust
// root. A reviewed trust reset is deliberately distinguishable from a
// continuity-preserving rotation and never claims that the unavailable key
// authorized its replacement.
type KeyTransition struct {
	SchemaVersion         int        `json:"schema_version"`
	Continuity            string     `json:"continuity"`
	InstallationID        string     `json:"installation_id"`
	AuthorizedBy          string     `json:"authorized_by"`
	Reason                string     `json:"reason,omitempty"`
	ObservedAt            time.Time  `json:"observed_at"`
	TimeEvidence          string     `json:"time_evidence"`
	PreviousPublicKey     string     `json:"previous_public_key"`
	PreviousCheckpoint    Checkpoint `json:"previous_checkpoint"`
	NextPublicKey         string     `json:"next_public_key"`
	NextCheckpoint        Checkpoint `json:"next_checkpoint"`
	PreviousAuthorization string     `json:"previous_authorization,omitempty"`
	NextAuthorization     string     `json:"next_authorization"`
}

// NewAuthorizedRotation creates a dual-key transition. The prior key
// explicitly authorizes the exact successor checkpoint and public key.
func NewAuthorizedRotation(previous Checkpoint, previousBody []byte, previousPrivateKey, nextPrivateKey ed25519.PrivateKey, authorizedBy string, observedAt time.Time) (KeyTransition, []byte, []byte, error) {
	return newKeyTransition(TransitionAuthorizedRotation, "", previous, previousBody, previousPrivateKey, nextPrivateKey, authorizedBy, observedAt)
}

// NewReviewedTrustReset creates explicit evidence that continuity could not be
// proven because the prior signing key was unavailable. The caller must apply
// the separate reviewed local-authority procedure before invoking this
// function.
func NewReviewedTrustReset(previous Checkpoint, previousBody []byte, trustedPreviousPublicKey ed25519.PublicKey, nextPrivateKey ed25519.PrivateKey, authorizedBy string, observedAt time.Time) (KeyTransition, []byte, []byte, error) {
	if len(trustedPreviousPublicKey) != ed25519.PublicKeySize {
		return KeyTransition{}, nil, nil, fmt.Errorf("previous ledger anchor public key is invalid")
	}
	return newKeyTransition(TransitionReviewedTrustReset, RecoveryReasonKeyUnavailable, previous, previousBody, nil, nextPrivateKey, authorizedBy, observedAt, trustedPreviousPublicKey)
}

func newKeyTransition(continuity, reason string, previous Checkpoint, previousBody []byte, previousPrivateKey, nextPrivateKey ed25519.PrivateKey, authorizedBy string, observedAt time.Time, trustedPreviousPublicKey ...ed25519.PublicKey) (KeyTransition, []byte, []byte, error) {
	if !validLocalAuthority(authorizedBy) || observedAt.IsZero() || observedAt.Location() != time.UTC || len(nextPrivateKey) != ed25519.PrivateKeySize {
		return KeyTransition{}, nil, nil, fmt.Errorf("ledger anchor transition authority, time, or replacement key is invalid")
	}
	var previousPublicKey ed25519.PublicKey
	switch continuity {
	case TransitionAuthorizedRotation:
		if reason != "" || len(previousPrivateKey) != ed25519.PrivateKeySize {
			return KeyTransition{}, nil, nil, fmt.Errorf("authorized rotation requires the prior signing key")
		}
		previousPublicKey, _ = PublicKeyFromPrivate(previousPrivateKey)
	case TransitionReviewedTrustReset:
		if reason != RecoveryReasonKeyUnavailable || len(previousPrivateKey) != 0 || len(trustedPreviousPublicKey) != 1 {
			return KeyTransition{}, nil, nil, fmt.Errorf("reviewed trust reset evidence is invalid")
		}
		previousPublicKey = trustedPreviousPublicKey[0]
	default:
		return KeyTransition{}, nil, nil, fmt.Errorf("ledger anchor transition continuity is invalid")
	}
	if err := exactCheckpointBytes(previous, previousBody); err != nil {
		return KeyTransition{}, nil, nil, err
	}
	if err := verifyCheckpoint(previous, previous.InstallationID, previousPublicKey); err != nil {
		return KeyTransition{}, nil, nil, fmt.Errorf("verify previous ledger anchor checkpoint: %w", err)
	}
	if observedAt.Before(previous.ObservedAt) {
		return KeyTransition{}, nil, nil, fmt.Errorf("ledger anchor transition time precedes its checkpoint")
	}
	next, nextBody, err := newCheckpoint(previous.InstallationID, previous.Generation+1, previous.Ledger, observedAt, checkpointSHA256(previousBody), nextPrivateKey)
	if err != nil {
		return KeyTransition{}, nil, nil, err
	}
	previousEncoded, _ := EncodePublicKey(previousPublicKey)
	nextPublicKey, err := PublicKeyFromPrivate(nextPrivateKey)
	if err != nil {
		return KeyTransition{}, nil, nil, err
	}
	nextEncoded, _ := EncodePublicKey(nextPublicKey)
	record := KeyTransition{
		SchemaVersion: TransitionSchemaVersion, Continuity: continuity, InstallationID: previous.InstallationID,
		AuthorizedBy: authorizedBy, Reason: reason, ObservedAt: observedAt, TimeEvidence: TimeEvidence,
		PreviousPublicKey: previousEncoded, PreviousCheckpoint: previous,
		NextPublicKey: nextEncoded, NextCheckpoint: next,
	}
	if continuity == TransitionAuthorizedRotation {
		record.PreviousAuthorization = base64.StdEncoding.EncodeToString(ed25519.Sign(previousPrivateKey, transitionAuthorizationPayload(record)))
	}
	record.NextAuthorization = base64.StdEncoding.EncodeToString(ed25519.Sign(nextPrivateKey, transitionAuthorizationPayload(record)))
	recordBody, err := marshalTransition(record)
	if err != nil {
		return KeyTransition{}, nil, nil, err
	}
	return record, nextBody, recordBody, nil
}

// VerifyKeyTransition requires the caller's already-trusted prior public key;
// an embedded key never establishes its own trust.
func VerifyKeyTransition(body []byte, trustedPreviousPublicKey ed25519.PublicKey) (KeyTransition, error) {
	var record KeyTransition
	if err := decodeCanonicalDocument(body, &record, func() ([]byte, error) { return marshalTransition(record) }, "ledger anchor key transition"); err != nil {
		return KeyTransition{}, err
	}
	if len(trustedPreviousPublicKey) != ed25519.PublicKeySize || !validLocalAuthority(record.AuthorizedBy) || record.SchemaVersion != TransitionSchemaVersion || !validInstallationID(record.InstallationID) || record.InstallationID != record.PreviousCheckpoint.InstallationID || record.InstallationID != record.NextCheckpoint.InstallationID || record.ObservedAt.IsZero() || record.ObservedAt.Location() != time.UTC || record.TimeEvidence != TimeEvidence {
		return KeyTransition{}, fmt.Errorf("ledger anchor key transition envelope is invalid")
	}
	previousEncoded, _ := EncodePublicKey(trustedPreviousPublicKey)
	if record.PreviousPublicKey != previousEncoded {
		return KeyTransition{}, fmt.Errorf("ledger anchor key transition does not use the trusted prior key")
	}
	previousBody, err := marshalCheckpoint(record.PreviousCheckpoint)
	if err != nil || verifyCheckpoint(record.PreviousCheckpoint, record.InstallationID, trustedPreviousPublicKey) != nil {
		return KeyTransition{}, fmt.Errorf("ledger anchor key transition has an invalid prior checkpoint")
	}
	nextPublicKey, err := DecodePublicKey(record.NextPublicKey)
	if err != nil || verifyCheckpoint(record.NextCheckpoint, record.InstallationID, nextPublicKey) != nil {
		return KeyTransition{}, fmt.Errorf("ledger anchor key transition has an invalid successor checkpoint")
	}
	nextAuthorization, err := base64.StdEncoding.DecodeString(record.NextAuthorization)
	if err != nil || len(nextAuthorization) != ed25519.SignatureSize || !ed25519.Verify(nextPublicKey, transitionAuthorizationPayload(record), nextAuthorization) {
		return KeyTransition{}, fmt.Errorf("replacement ledger anchor key did not authorize the transition evidence")
	}
	if record.NextCheckpoint.Generation != record.PreviousCheckpoint.Generation+1 || !record.NextCheckpoint.Ledger.Equal(record.PreviousCheckpoint.Ledger) || record.NextCheckpoint.PreviousCheckpointSHA256 != checkpointSHA256(previousBody) || record.NextCheckpoint.ObservedAt != record.ObservedAt || record.ObservedAt.Before(record.PreviousCheckpoint.ObservedAt) {
		return KeyTransition{}, fmt.Errorf("ledger anchor key transition does not preserve the exact ledger head")
	}
	switch record.Continuity {
	case TransitionAuthorizedRotation:
		if record.Reason != "" {
			return KeyTransition{}, fmt.Errorf("authorized ledger anchor rotation has a recovery reason")
		}
		signature, decodeErr := base64.StdEncoding.DecodeString(record.PreviousAuthorization)
		if decodeErr != nil || len(signature) != ed25519.SignatureSize || !ed25519.Verify(trustedPreviousPublicKey, transitionAuthorizationPayload(record), signature) {
			return KeyTransition{}, fmt.Errorf("prior ledger anchor key did not authorize its replacement")
		}
	case TransitionReviewedTrustReset:
		if record.Reason != RecoveryReasonKeyUnavailable || record.PreviousAuthorization != "" {
			return KeyTransition{}, fmt.Errorf("reviewed ledger anchor trust reset evidence is invalid")
		}
	default:
		return KeyTransition{}, fmt.Errorf("ledger anchor key transition continuity is invalid")
	}
	return record, nil
}

func decodeCanonicalDocument(body []byte, target any, marshal func() ([]byte, error), label string) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode %s: %w", label, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%s has trailing content", label)
	}
	canonical, err := marshal()
	if err != nil || !bytes.Equal(canonical, body) {
		return fmt.Errorf("%s is not canonical", label)
	}
	return nil
}

// EncodeCheckpoint returns the exact canonical checkpoint representation used
// for signatures and durable publication.
func EncodeCheckpoint(checkpoint Checkpoint) ([]byte, error) {
	return marshalCheckpoint(checkpoint)
}

func exactCheckpointBytes(checkpoint Checkpoint, body []byte) error {
	canonical, err := marshalCheckpoint(checkpoint)
	if err != nil || !bytes.Equal(canonical, body) {
		return fmt.Errorf("previous ledger anchor checkpoint bytes are not canonical")
	}
	return nil
}

func marshalTransition(record KeyTransition) ([]byte, error) {
	body, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(body, '\n'), nil
}

func transitionAuthorizationPayload(record KeyTransition) []byte {
	digest := sha256.New()
	writeTransitionField(digest, []byte(transitionDomain))
	writeInt(digest, int64(record.SchemaVersion))
	writeTransitionField(digest, []byte(record.Continuity))
	writeTransitionField(digest, []byte(record.InstallationID))
	writeTransitionField(digest, []byte(record.AuthorizedBy))
	writeTransitionField(digest, []byte(record.Reason))
	writeTransitionField(digest, []byte(record.ObservedAt.Format(time.RFC3339Nano)))
	writeTransitionField(digest, []byte(record.TimeEvidence))
	writeTransitionField(digest, []byte(record.PreviousPublicKey))
	previousBody, _ := marshalCheckpoint(record.PreviousCheckpoint)
	writeTransitionField(digest, previousBody)
	writeTransitionField(digest, []byte(record.NextPublicKey))
	nextBody, _ := marshalCheckpoint(record.NextCheckpoint)
	writeTransitionField(digest, nextBody)
	return digest.Sum(nil)
}

func writeTransitionField(digest hash.Hash, value []byte) {
	writeInt(digest, int64(len(value)))
	_, _ = digest.Write(value)
}

func validLocalAuthority(value string) bool {
	const prefix = "local-uid-"
	if !strings.HasPrefix(value, prefix) || len(value) == len(prefix) {
		return false
	}
	uid, err := strconv.ParseUint(value[len(prefix):], 10, 31)
	return err == nil && strconv.FormatUint(uid, 10) == value[len(prefix):]
}

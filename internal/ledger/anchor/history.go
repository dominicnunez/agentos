package anchor

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/dominicnunez/agentos/internal/fileguard"
)

const maximumTransitionHistory = 128

type verifiedKeyTransition struct {
	continuity string
	previous   ed25519.PublicKey
	next       ed25519.PublicKey
}

// TrustedPublicKeyHistory reconstructs only the key graph cryptographically
// connected to the currently pinned public key. A reviewed reset is traversed
// backward from its trusted replacement; an authorized rotation can then be
// traversed in either direction. Unrelated evidence never becomes trusted
// merely because it is present on disk.
func TrustedPublicKeyHistory(directory, installationID string, current ed25519.PublicKey) ([]ed25519.PublicKey, error) {
	if !validInstallationID(installationID) || len(current) != ed25519.PublicKeySize || !filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
		return nil, fmt.Errorf("ledger anchor transition history boundary is invalid")
	}
	if err := fileguard.ValidateDirectoryChain(directory); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return []ed25519.PublicKey{append(ed25519.PublicKey(nil), current...)}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read ledger anchor transition history: %w", err)
	}
	if len(entries) > maximumTransitionHistory {
		return nil, fmt.Errorf("ledger anchor transition history exceeds its bound")
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	records := make([]verifiedKeyTransition, 0, len(entries))
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") || strings.HasSuffix(entry.Name(), ".pending.json") {
			continue
		}
		if !entry.Type().IsRegular() {
			return nil, fmt.Errorf("ledger anchor transition evidence must be a regular file")
		}
		body, err := readPrivateRegularFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read ledger anchor transition evidence: %w", err)
		}
		var untrusted KeyTransition
		if err := decodeCanonicalDocument(body, &untrusted, func() ([]byte, error) { return marshalTransition(untrusted) }, "ledger anchor key transition"); err != nil {
			return nil, err
		}
		if untrusted.InstallationID != installationID {
			return nil, fmt.Errorf("ledger anchor transition evidence belongs to another installation")
		}
		previous, err := DecodePublicKey(untrusted.PreviousPublicKey)
		if err != nil {
			return nil, err
		}
		record, err := VerifyKeyTransition(body, previous)
		if err != nil {
			return nil, err
		}
		next, err := DecodePublicKey(record.NextPublicKey)
		if err != nil {
			return nil, err
		}
		records = append(records, verifiedKeyTransition{continuity: record.Continuity, previous: previous, next: next})
	}

	trusted := []ed25519.PublicKey{append(ed25519.PublicKey(nil), current...)}
	for {
		changed := false
		for index := range records {
			record := records[index]
			if containsPublicKey(trusted, record.next) && !containsPublicKey(trusted, record.previous) {
				if transitionForksIntoTrustedKey(records, record) {
					return nil, fmt.Errorf("ledger anchor transition history forks into a trusted key")
				}
				trusted = append(trusted, append(ed25519.PublicKey(nil), record.previous...))
				changed = true
			}
			if record.continuity == TransitionAuthorizedRotation && containsPublicKey(trusted, record.previous) && !containsPublicKey(trusted, record.next) {
				if authorizedRotationForksFromTrustedKey(records, record) {
					return nil, fmt.Errorf("ledger anchor rotation history forks from a trusted key")
				}
				trusted = append(trusted, append(ed25519.PublicKey(nil), record.next...))
				changed = true
			}
		}
		if !changed {
			break
		}
	}
	return trusted, nil
}

func transitionForksIntoTrustedKey(records []verifiedKeyTransition, selected verifiedKeyTransition) bool {
	for _, record := range records {
		if bytes.Equal(record.next, selected.next) && !bytes.Equal(record.previous, selected.previous) {
			return true
		}
	}
	return false
}

func authorizedRotationForksFromTrustedKey(records []verifiedKeyTransition, selected verifiedKeyTransition) bool {
	for _, record := range records {
		if record.continuity == TransitionAuthorizedRotation && bytes.Equal(record.previous, selected.previous) && !bytes.Equal(record.next, selected.next) {
			return true
		}
	}
	return false
}

// ReadTrustedCheckpoint selects the single retained key that verifies a
// checkpoint. The caller must first derive trustedKeys from the current pin.
func ReadTrustedCheckpoint(path, installationID string, trustedKeys []ed25519.PublicKey) (Checkpoint, []byte, ed25519.PublicKey, error) {
	if len(trustedKeys) == 0 || len(trustedKeys) > maximumTransitionHistory+1 {
		return Checkpoint{}, nil, nil, fmt.Errorf("trusted ledger anchor key set is invalid")
	}
	var selected Checkpoint
	var selectedBody []byte
	var selectedKey ed25519.PublicKey
	for _, key := range trustedKeys {
		checkpoint, body, err := Read(path, installationID, key)
		if err != nil {
			continue
		}
		if selectedKey != nil {
			return Checkpoint{}, nil, nil, fmt.Errorf("ledger checkpoint verifies under multiple trusted keys")
		}
		selected, selectedBody = checkpoint, body
		selectedKey = append(ed25519.PublicKey(nil), key...)
	}
	if selectedKey == nil {
		return Checkpoint{}, nil, nil, fmt.Errorf("ledger checkpoint is not signed by the current or a retained ancestor key")
	}
	return selected, selectedBody, selectedKey, nil
}

func containsPublicKey(keys []ed25519.PublicKey, candidate ed25519.PublicKey) bool {
	for _, key := range keys {
		if bytes.Equal(key, candidate) {
			return true
		}
	}
	return false
}

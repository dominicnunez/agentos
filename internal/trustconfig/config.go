// Package trustconfig contains shared fail-closed parsing and lifecycle checks
// for trusted startup registries. It has no domain or transport dependencies.
package trustconfig

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
)

const registryLimit = 1 << 20

// DecodeObject decodes exactly one size-limited JSON object and rejects unknown
// fields or trailing content.
func DecodeObject(reader io.Reader, name string, target any) error {
	decoder := json.NewDecoder(io.LimitReader(reader, registryLimit))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode %s: %w", name, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("%s must contain one JSON object", name)
	}
	return nil
}

// DecodeEntries decodes one trusted registry object and requires at least one
// entry. The entries pointer must refer to a field in target.
func DecodeEntries[T any](reader io.Reader, name, entryName string, target any, entries *[]T) ([]T, error) {
	if err := DecodeObject(reader, name, target); err != nil {
		return nil, err
	}
	if len(*entries) == 0 {
		return nil, fmt.Errorf("%s must contain at least one %s", name, entryName)
	}
	return *entries, nil
}

// ValidateCredentialLifecycle validates the shared lifecycle fields for a
// credential-backed trusted registry entry.
func ValidateCredentialLifecycle(status, credential string, expiresAt *time.Time) error {
	if status != "ACTIVE" && status != "SUSPENDED" && status != "REVOKED" {
		return fmt.Errorf("status must be ACTIVE, SUSPENDED, or REVOKED")
	}
	if len(credential) < 32 {
		return fmt.Errorf("resolved bearer credential must contain at least 32 characters")
	}
	if expiresAt == nil || expiresAt.IsZero() {
		return fmt.Errorf("expires_at is required")
	}
	return nil
}

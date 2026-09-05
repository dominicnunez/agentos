// Package trustconfig contains shared fail-closed parsing and lifecycle checks
// for trusted startup registries. It has no domain or transport dependencies.
package trustconfig

import (
	"fmt"
	"io"
	"time"

	"github.com/dominicnunez/agentos/internal/boundaryjson"
)

const registryLimit = 1 << 20

// DecodeObject decodes exactly one size-limited JSON object and rejects unknown
// fields or trailing content.
func DecodeObject(reader io.Reader, name string, target any) error {
	content, err := io.ReadAll(io.LimitReader(reader, registryLimit+1))
	if err != nil {
		return fmt.Errorf("read %s: %w", name, err)
	}
	if len(content) > registryLimit {
		return fmt.Errorf("%s exceeds %d bytes", name, registryLimit)
	}
	if err := boundaryjson.Unmarshal(content, target); err != nil {
		return fmt.Errorf("decode %s: %w", name, err)
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

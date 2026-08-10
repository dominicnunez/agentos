package core

import (
	"crypto/sha256"
	"fmt"
)

// ExternalWorkID derives a compact tenant-scoped correlation key from an
// authenticated organization and caller-visible request identifier.
func ExternalWorkID(organizationID, requestID string) string {
	digest := sha256.Sum256([]byte("agentos.external-work.v1\x00" + organizationID + "\x00" + requestID))
	return fmt.Sprintf("w-%x", digest[:16])
}

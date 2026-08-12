package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// FingerprintIntentDraft binds confirmation to the complete canonical draft,
// including its version and creation time, while excluding the fingerprint
// field itself.
func FingerprintIntentDraft(draft IntentDraft) (string, error) {
	draft.Fingerprint = ""
	draft.CreatedAt = draft.CreatedAt.UTC()
	body, err := json.Marshal(draft)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:]), nil
}

package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// FingerprintPlan binds durable Task materialization to the complete validated
// plan while excluding the fingerprint field itself.
func FingerprintPlan(plan Plan) (string, error) {
	plan.Fingerprint = ""
	plan.CreatedAt = plan.CreatedAt.UTC()
	body, err := json.Marshal(plan)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:]), nil
}

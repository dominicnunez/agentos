// Package modeloutput validates bounded structured model responses. It owns no
// domain policy; callers must separately validate the decoded candidate.
package modeloutput

import (
	"fmt"

	"github.com/dominicnunez/agentos/internal/boundaryjson"
)

// DecodeJSON accepts exactly one closed-schema JSON value within maxBytes.
// Unknown fields, ambiguous keys, and trailing content fail closed.
func DecodeJSON[T any](text string, maxBytes int) (T, error) {
	var value T
	if maxBytes <= 0 || len(text) > maxBytes {
		return value, fmt.Errorf("structured model response exceeds limit")
	}
	if err := boundaryjson.Unmarshal([]byte(text), &value); err != nil {
		return value, fmt.Errorf("structured model response is invalid JSON: %w", err)
	}
	return value, nil
}

// Package modeloutput validates bounded structured model responses. It owns no
// domain policy; callers must separately validate the decoded candidate.
package modeloutput

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// DecodeJSON accepts exactly one closed-schema JSON value within maxBytes.
// Unknown fields and trailing content fail closed.
func DecodeJSON[T any](text string, maxBytes int) (T, error) {
	var value T
	if maxBytes <= 0 || len(text) > maxBytes {
		return value, fmt.Errorf("structured model response exceeds limit")
	}
	decoder := json.NewDecoder(bytes.NewBufferString(text))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, fmt.Errorf("structured model response is invalid JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return value, fmt.Errorf("structured model response contains trailing content")
	}
	return value, nil
}

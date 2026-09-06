package modelinput

import (
	"encoding/json"
	"unicode/utf8"
)

// QuoteData retains the lower-trust classification on transports without a
// native data role. JSON escaping prevents payload bytes from adding fields to
// this envelope. This is representation, not a model-compliance guarantee or
// an authority grant. The runtime must still validate every proposed action.
func QuoteData(text string) (string, error) {
	if len(text) > MaximumBytes {
		return "", ErrLimit
	}
	if !utf8.ValidString(text) {
		return "", ErrInvalid
	}
	body, err := json.Marshal(struct {
		Class   string `json:"class"`
		Content string `json:"content"`
	}{"LOW_PRIVILEGE_DATA", text})
	if err != nil {
		return "", ErrInvalid
	}
	if len(body) > MaximumBytes {
		return "", ErrLimit
	}
	return string(body), nil
}

package modelinput

import (
	"encoding/json"
	"unicode/utf8"
)

// WireText exposes only the runtime-issued handle, class, and source text.
// Internal source IDs and digests remain in the manifest. Embedded claims in
// Content cannot create or replace an envelope field.
func WireText(message Message) (string, error) {
	if len(message.Text) > MaximumBytes {
		return "", ErrLimit
	}
	if !utf8.ValidString(message.Text) {
		return "", ErrInvalid
	}
	if message.Source.Handle != "" && !validHandle(message.Source.Handle) {
		return "", ErrInvalid
	}
	class := ""
	switch message.Role {
	case System:
		return message.Text, nil
	case User:
		class = "USER"
	case Data:
		class = "LOW_PRIVILEGE_DATA"
	case Assistant:
		class = "PRIOR_MODEL_OUTPUT"
	default:
		return "", ErrInvalid
	}
	if message.Source.Handle == "" {
		if message.Role == Data {
			return QuoteData(message.Text)
		}
		return message.Text, nil
	}
	body, err := json.Marshal(struct {
		Class        string `json:"class"`
		SourceHandle string `json:"source_handle"`
		Content      string `json:"content"`
	}{class, message.Source.Handle, message.Text})
	if err != nil {
		return "", ErrInvalid
	}
	if len(body) > MaximumBytes {
		return "", ErrLimit
	}
	return string(body), nil
}

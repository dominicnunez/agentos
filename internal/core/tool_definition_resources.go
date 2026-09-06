package core

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"
)

// These fixed limits bound the existing fingerprint helper. Dynamic discovery
// must additionally bound transport reads, tool counts and cumulative work.
const (
	MaximumToolDefinitionBytes  = 256 << 10
	MaximumToolNameBytes        = 256
	MaximumToolDescriptionBytes = 16 << 10
	MaximumToolSchemaBytes      = 64 << 10
	MaximumToolMetadataBytes    = 16 << 10
	MaximumToolSchemaDepth      = 32
	MaximumToolSchemaProperties = 1024
)

var (
	ErrToolDefinitionLimit = errors.New("tool definition exceeds resource limits")
	ErrToolDefinitionJSON  = errors.New("tool definition contains invalid or ambiguous JSON")
)

func validateToolDefinitionResources(name, description string, input, output, metadata json.RawMessage) error {
	// Check all raw lengths before parsing any field. Encoding can expand strings;
	// the caller also bounds the encoded definition before hashing it.
	if len(name) > MaximumToolNameBytes || len(description) > MaximumToolDescriptionBytes ||
		len(input) > MaximumToolSchemaBytes || len(output) > MaximumToolSchemaBytes || len(metadata) > MaximumToolMetadataBytes {
		return ErrToolDefinitionLimit
	}
	if !utf8.ValidString(name) || !utf8.ValidString(description) {
		return ErrToolDefinitionJSON
	}
	for _, data := range []json.RawMessage{input, output, metadata} {
		if data == nil { // Preserve the existing optional-field null representation.
			continue
		}
		if !utf8.Valid(data) {
			return ErrToolDefinitionJSON
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		properties := 0
		if err := toolDefinitionJSONValue(decoder, 0, &properties); err != nil {
			return err
		}
		if _, err := decoder.Token(); err != io.EOF {
			return ErrToolDefinitionJSON
		}
	}
	return nil
}

// Count every object member, including nested metadata and schema extensions,
// rather than trusting a server's interpretation of JSON Schema properties.
func toolDefinitionJSONValue(decoder *json.Decoder, depth int, properties *int) error {
	token, err := decoder.Token()
	if err != nil {
		return ErrToolDefinitionJSON
	}
	delim, container := token.(json.Delim)
	if !container {
		return nil
	}
	if depth >= MaximumToolSchemaDepth {
		return ErrToolDefinitionLimit
	}
	if delim != '{' && delim != '[' {
		return ErrToolDefinitionJSON
	}
	seen := make(map[string]struct{})
	for decoder.More() {
		if delim == '{' {
			*properties++
			if *properties > MaximumToolSchemaProperties {
				return ErrToolDefinitionLimit
			}
			keyToken, err := decoder.Token()
			if err != nil {
				return ErrToolDefinitionJSON
			}
			key, ok := keyToken.(string)
			if !ok {
				return ErrToolDefinitionJSON
			}
			if _, duplicate := seen[key]; duplicate {
				return ErrToolDefinitionJSON
			}
			seen[key] = struct{}{}
		}
		if err := toolDefinitionJSONValue(decoder, depth+1, properties); err != nil {
			return err
		}
	}
	end, err := decoder.Token()
	if err != nil || delim == '{' && end != json.Delim('}') || delim == '[' && end != json.Delim(']') {
		return ErrToolDefinitionJSON
	}
	return nil
}

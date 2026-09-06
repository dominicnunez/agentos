package execution

import (
	"encoding/json"
	"strings"

	"github.com/dominicnunez/agentos/internal/boundaryjson"
)

// Provider responses contain evolving metadata. Keep that metadata open while
// rejecting aliases of every field consumed by the model-only validator.
func validOpenAIResponseJSON(body []byte) bool {
	if len(body) > openAIMaximumBodyBytes || boundaryjson.Validate(body) != nil {
		return false
	}
	root, ok := openAIExactFields(body, "id", "object", "status", "error", "incomplete_details", "model", "output", "store", "tool_choice", "tools", "truncation", "max_output_tokens", "usage")
	if !ok {
		return false
	}
	if usage := root["usage"]; len(usage) != 0 && string(usage) != "null" {
		if _, ok := openAIExactFields(usage, "input_tokens", "output_tokens", "total_tokens"); !ok {
			return false
		}
	}
	var output []json.RawMessage
	if json.Unmarshal(root["output"], &output) != nil {
		return false
	}
	for _, item := range output {
		fields, ok := openAIExactFields(item, "type", "role", "status", "content")
		if !ok {
			return false
		}
		if raw := fields["content"]; len(raw) != 0 && string(raw) != "null" {
			var content []json.RawMessage
			if json.Unmarshal(raw, &content) != nil {
				return false
			}
			for _, part := range content {
				if _, ok := openAIExactFields(part, "type", "text", "refusal", "annotations"); !ok {
					return false
				}
			}
		}
	}
	return true
}

func openAIExactFields(body []byte, names ...string) (map[string]json.RawMessage, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil || fields == nil {
		return nil, false
	}
	for key := range fields {
		for _, name := range names {
			if key != name && strings.EqualFold(key, name) {
				return nil, false
			}
		}
	}
	return fields, true
}

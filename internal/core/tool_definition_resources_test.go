package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestToolDefinitionResourceBoundary(t *testing.T) {
	object := func(count int) json.RawMessage {
		members := make([]string, count)
		for i := range members {
			members[i] = fmt.Sprintf(`"k%d":0`, i)
		}
		return json.RawMessage("{" + strings.Join(members, ",") + "}")
	}
	quoted := func(size int) json.RawMessage { return json.RawMessage(`"` + strings.Repeat("x", size-2) + `"`) }
	for _, tc := range []struct {
		name                    string
		toolName, description   string
		input, output, metadata json.RawMessage
		want                    error
	}{
		{name: "optional fields"},
		{name: "exact name", toolName: strings.Repeat("n", MaximumToolNameBytes)},
		{name: "long name", toolName: strings.Repeat("n", MaximumToolNameBytes+1), want: ErrToolDefinitionLimit},
		{name: "exact description", description: strings.Repeat("d", MaximumToolDescriptionBytes)},
		{name: "long description", description: strings.Repeat("d", MaximumToolDescriptionBytes+1), want: ErrToolDefinitionLimit},
		{name: "exact schema", input: quoted(MaximumToolSchemaBytes), output: quoted(MaximumToolSchemaBytes)},
		{name: "large input", input: quoted(MaximumToolSchemaBytes + 1), want: ErrToolDefinitionLimit},
		{name: "large output", output: quoted(MaximumToolSchemaBytes + 1), want: ErrToolDefinitionLimit},
		{name: "exact metadata", metadata: quoted(MaximumToolMetadataBytes)},
		{name: "large metadata", metadata: quoted(MaximumToolMetadataBytes + 1), want: ErrToolDefinitionLimit},
		{name: "exact depth", input: json.RawMessage(strings.Repeat("[", MaximumToolSchemaDepth) + "0" + strings.Repeat("]", MaximumToolSchemaDepth))},
		{name: "deep schema", input: json.RawMessage(strings.Repeat("[", MaximumToolSchemaDepth+1) + "0" + strings.Repeat("]", MaximumToolSchemaDepth+1)), want: ErrToolDefinitionLimit},
		{name: "exact members", input: object(MaximumToolSchemaProperties)},
		{name: "many members", input: object(MaximumToolSchemaProperties + 1), want: ErrToolDefinitionLimit},
		{name: "nested member total", input: append(append(json.RawMessage(`{"nested":`), object(MaximumToolSchemaProperties)...), '}'), want: ErrToolDefinitionLimit},
		{name: "metadata members", metadata: object(MaximumToolSchemaProperties + 1), want: ErrToolDefinitionLimit},
		{name: "escaped duplicate", input: json.RawMessage(`{"type":0,"\u0074ype":1}`), want: ErrToolDefinitionJSON},
		{name: "nested duplicate", output: json.RawMessage(`{"x":[{"a":0,"a":1}]}`), want: ErrToolDefinitionJSON},
		{name: "invalid utf8", metadata: json.RawMessage{'"', 255, '"'}, want: ErrToolDefinitionJSON},
		{name: "invalid description", description: string([]byte{255}), want: ErrToolDefinitionJSON},
		{name: "trailing value", input: json.RawMessage(`{} {}`), want: ErrToolDefinitionJSON},
		{name: "malformed private key", input: json.RawMessage(`{"PRIVATE_CANARY":`), want: ErrToolDefinitionJSON},
		{name: "check lengths first", input: json.RawMessage(`invalid`), output: quoted(MaximumToolSchemaBytes + 1), want: ErrToolDefinitionLimit},
		{name: "encoded expansion", description: strings.Repeat("<", MaximumToolDescriptionBytes), input: json.RawMessage(`"` + strings.Repeat("<", MaximumToolSchemaBytes-2) + `"`), want: ErrToolDefinitionLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := tc.toolName
			if name == "" {
				name = "inspect"
			}
			digest, err := FingerprintToolDefinition("tool", "v1", "server", "endpoint", name, tc.description, tc.input, tc.output, tc.metadata, []CapabilityRequirement{{Action: "inspect", Resource: "repo", Scope: "org"}})
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			if err != nil && digest != "" {
				t.Fatal("rejected definition received a digest")
			}
			if err == nil && len(digest) != 64 {
				t.Fatal("valid definition missing digest")
			}
		})
	}
}

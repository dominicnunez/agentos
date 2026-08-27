package core

import (
	"encoding/json"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestKnowledgeRecordMatchesActiveSchemaProperties(t *testing.T) {
	body, err := os.ReadFile("../../docs/schemas/knowledge-record.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		AdditionalProperties bool                       `json:"additionalProperties"`
		Required             []string                   `json:"required"`
		Properties           map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(body, &schema); err != nil {
		t.Fatal(err)
	}
	if schema.AdditionalProperties {
		t.Fatal("knowledge schema must remain closed")
	}
	typeOfRecord := reflect.TypeFor[KnowledgeRecord]()
	properties := make([]string, 0, typeOfRecord.NumField())
	required := make([]string, 0, typeOfRecord.NumField())
	for index := range typeOfRecord.NumField() {
		tag := typeOfRecord.Field(index).Tag.Get("json")
		name, option, _ := strings.Cut(tag, ",")
		if name == "" || name == "-" {
			t.Fatalf("KnowledgeRecord field %s lacks a wire name", typeOfRecord.Field(index).Name)
		}
		properties = append(properties, name)
		if option != "omitempty" {
			required = append(required, name)
		}
	}
	slices.Sort(properties)
	slices.Sort(required)
	schemaProperties := make([]string, 0, len(schema.Properties))
	for name := range schema.Properties {
		schemaProperties = append(schemaProperties, name)
	}
	slices.Sort(schemaProperties)
	slices.Sort(schema.Required)
	if !slices.Equal(properties, schemaProperties) {
		t.Fatalf("KnowledgeRecord properties=%v schema=%v", properties, schemaProperties)
	}
	if !slices.Equal(required, schema.Required) {
		t.Fatalf("KnowledgeRecord required=%v schema=%v", required, schema.Required)
	}
	encoded, err := json.Marshal(KnowledgeRecord{})
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	if string(wire["evidence_artifact_refs"]) != "[]" {
		t.Fatalf("required evidence_artifact_refs serialized as %s", wire["evidence_artifact_refs"])
	}
}

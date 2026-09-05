package boundaryjson

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

type testChild struct {
	Value string `json:"value"`
}
type testDocument struct {
	Child testChild            `json:"child"`
	Items []testChild          `json:"items"`
	Data  map[string]testChild `json:"data"`
	Raw   json.RawMessage      `json:"raw"`
	When  time.Time            `json:"when"`
}

func TestRejectAmbiguityAtEveryDepthWithoutDisclosingInput(t *testing.T) {
	for name, input := range map[string]string{
		"duplicate":         `{"child":{"value":"first","value":"synthetic-secret"}}`,
		"escaped duplicate": `{"child":{"value":"first","\u0076alue":"synthetic-secret"}}`,
		"alias":             `{"Child":{"value":"synthetic-secret"}}`,
		"nested alias":      `{"items":[{"VALUE":"synthetic-secret"}]}`,
		"map value alias":   `{"data":{"open-key":{"VALUE":"synthetic-secret"}}}`,
		"unknown":           `{"synthetic-secret":true}`,
		"raw duplicate":     `{"raw":{"x":1,"x":2}}`,
		"invalid UTF8":      "{\"child\":{\"value\":\"\xff\"}}",
		"trailing":          `{} {}`,
		"type mismatch":     `{"items":"synthetic-secret"}`,
	} {
		t.Run(name, func(t *testing.T) {
			value := testDocument{Child: testChild{Value: "retained"}}
			before := value
			err := Unmarshal([]byte(input), &value)
			if err == nil || strings.Contains(err.Error(), "synthetic-secret") {
				t.Fatal("unsafe input or diagnostic accepted")
			}
			if !reflect.DeepEqual(value, before) {
				t.Fatal("failure partially mutated destination")
			}
		})
	}
}

func TestExactSchemaSupportsOpenKeysEmbeddedFieldsAndScalarContracts(t *testing.T) {
	var value testDocument
	input := `{"child":{"value":"ok"},"items":[{"value":"ok"}],"data":{"VALUE":{"value":"ok"}},"raw":{"VALUE":1,"value":2},"when":"2026-09-05T00:00:00Z"}`
	if err := Unmarshal([]byte(input), &value); err != nil || value.Data["VALUE"].Value != "ok" || value.When.IsZero() {
		t.Fatalf("valid schema: %v", err)
	}
	if err := Unmarshal([]byte(`{}`), &value); err != nil || !reflect.DeepEqual(value, testDocument{}) {
		t.Fatal("omitted fields retained earlier data")
	}
	type Embedded struct {
		Name string `json:"name"`
	}
	type outer struct {
		Embedded
		Child *testChild `json:"child"`
	}
	var out outer
	if err := Unmarshal([]byte(`{"name":"ok","child":{"value":"yes"}}`), &out); err != nil || out.Name != "ok" || out.Child.Value != "yes" {
		t.Fatal("embedded fields rejected")
	}
}

func TestLimitsAndExactNumberMode(t *testing.T) {
	if err := Validate([]byte(strings.Repeat(" ", MaximumBytes+1))); !errors.Is(err, ErrLimit) {
		t.Fatal("byte limit not enforced")
	}
	deep := strings.Repeat("[", MaximumDepth+1) + "0" + strings.Repeat("]", MaximumDepth+1)
	if err := Validate([]byte(deep)); !errors.Is(err, ErrLimit) {
		t.Fatal("depth limit not enforced")
	}
	var value map[string]any
	if err := UnmarshalNumbers([]byte(`{"id":9007199254740993}`), &value); err != nil || value["id"] != json.Number("9007199254740993") {
		t.Fatal("number lost precision")
	}
	if err := Unmarshal([]byte(`{}`), nil); !errors.Is(err, ErrSchema) {
		t.Fatal("nil destination accepted")
	}
}

func TestEmbeddedFieldDominanceMatchesDeclaredJSONSchema(t *testing.T) {
	type Left struct {
		Value string
	}
	type Right struct {
		Value string
	}
	type ambiguous struct {
		Left
		Right
	}
	var conflict ambiguous
	if Unmarshal([]byte(`{"Value":"x"}`), &conflict) == nil {
		t.Fatal("ambiguous schema field accepted")
	}
	type dominant struct {
		Left
		Right
		Value string
	}
	var direct dominant
	if err := Unmarshal([]byte(`{"Value":"x"}`), &direct); err != nil || direct.Value != "x" {
		t.Fatal("direct field did not dominate")
	}
}

func FuzzBoundaryJSON(f *testing.F) {
	f.Add([]byte(`{"child":{"value":"ok"}}`))
	f.Add([]byte(`{"raw":{"x":1,"x":2}}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		var value testDocument
		if err := Unmarshal(data, &value); err == nil {
			encoded, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			var replay testDocument
			if err := Unmarshal(encoded, &replay); err != nil {
				t.Fatal("accepted value cannot round-trip")
			}
		}
	})
}

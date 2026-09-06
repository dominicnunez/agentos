package execution

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestOpenAIRejectsAmbiguousModelEvidence(t *testing.T) {
	valid := string(openAITestResponse("answer"))
	for _, body := range []string{
		strings.Replace(valid, `"model":"`+openAITestModel+`"`, `"model":"wrong-model","model":"`+openAITestModel+`"`, 1),
		strings.Replace(valid, `"model":"`+openAITestModel+`"`, `"model":"wrong-model","Model":"`+openAITestModel+`"`, 1),
	} {
		response, err := decodeOpenAIResponse([]byte(body))
		if err == nil {
			_, err = (&OpenAIAPI{model: openAITestModel}).validateResponse(response)
		}
		if err == nil {
			t.Fatal("ambiguous model evidence accepted")
		}
	}
}

func TestOpenAIBoundaryRejectsNestedAmbiguity(t *testing.T) {
	valid := string(openAITestResponse("answer"))
	for _, tc := range []struct{ name, old, replacement string }{
		{"usage duplicate", `"input_tokens":12`, `"input_tokens":999999,"input_tokens":12`},
		{"usage alias", `"input_tokens":12`, `"input_tokens":999999,"INPUT_TOKENS":12`},
		{"output type alias", `"type":"message"`, `"type":"function_call","Type":"message"`},
		{"text alias", `"text":"answer"`, `"text":"other","Text":"answer"`},
		{"escaped duplicate", `"text":"answer"`, `"text":"other","\u0074ext":"answer"`},
		{"profile alias", `"store":false`, `"store":true,"Store":false`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := strings.Replace(valid, tc.old, tc.replacement, 1)
			if body == valid {
				t.Fatal("fixture did not exercise mutation")
			}
			if _, err := decodeOpenAIResponse([]byte(body)); err == nil {
				t.Fatal("ambiguous evidence decoded")
			}
		})
	}
}

func TestOpenAIBoundaryPreservesMetadataButRejectsItsDuplicates(t *testing.T) {
	object := openAITestResponseObject("answer")
	object["provider_metadata"] = map[string]any{"future_field": "value"}
	usage, ok := object["usage"].(map[string]any)
	if !ok {
		t.Fatal("invalid fixture usage")
	}
	usage["input_tokens_details"] = map[string]any{"cached_tokens": 0}
	body, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	response, err := decodeOpenAIResponse(body)
	if err == nil {
		_, err = (&OpenAIAPI{model: openAITestModel}).validateResponse(response)
	}
	if err != nil {
		t.Fatal(err)
	}
	body = []byte(strings.Replace(string(body), `"future_field":"value"`, `"future_field":"first","future_field":"value"`, 1))
	if _, err = decodeOpenAIResponse(body); err == nil {
		t.Fatal("metadata duplicate accepted")
	}
}

func TestOpenAIBoundaryErrorsDoNotExposeResponseValues(t *testing.T) {
	const canary = "sensitive-response-canary"
	valid := string(openAITestResponse("answer"))
	for _, body := range []string{
		strings.Replace(valid, `"store":false`, `"store":"`+canary+`"`, 1),
		`{"` + canary + `":`,
		strings.Repeat("x", openAIMaximumBodyBytes+1),
		valid + ` {"extra":"` + canary + `"}`,
		strings.Replace(valid, "answer", string([]byte{0xff}), 1),
	} {
		_, err := decodeOpenAIResponse([]byte(body))
		if err == nil || strings.Contains(err.Error(), canary) {
			t.Fatal("missing rejection or unsafe diagnostic")
		}
	}
}

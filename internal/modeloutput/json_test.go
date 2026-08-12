package modeloutput

import "testing"

func TestDecodeJSONRequiresOneClosedBoundedValue(t *testing.T) {
	type payload struct {
		Value string `json:"value"`
	}
	valid, err := DecodeJSON[payload](`{"value":"ok"}`, 64)
	if err != nil || valid.Value != "ok" {
		t.Fatalf("value=%+v err=%v", valid, err)
	}
	for name, text := range map[string]string{
		"unknown":   `{"value":"ok","authority":"admin"}`,
		"trailing":  `{"value":"ok"}{}`,
		"oversized": `{"value":"this response is too large"}`,
	} {
		t.Run(name, func(t *testing.T) {
			limit := 64
			if name == "oversized" {
				limit = 8
			}
			if _, err := DecodeJSON[payload](text, limit); err == nil {
				t.Fatal("unsafe structured response was accepted")
			}
		})
	}
}

package ledger

import (
	"encoding/json"
	"github.com/dominicnunez/agentos/internal/core"
	"testing"
)

func TestRuntimeAuthorityDecoderRejectsAmbiguousStoredContracts(t *testing.T) {
	for _, body := range []string{
		`{"effect_obligation_id":"first","effect_obligation_id":"second"}`,
		`{"EFFECT_OBLIGATION_ID":"effect"}`,
		`{"replay_context":{"body":"first","body":"second"}}`,
	} {
		for _, decode := range []func(any) error{
			func(target any) error { return decodeExactJSONBytes([]byte(body), target) },
			func(target any) error { return decodeExactJSON(json.RawMessage(body), target) },
		} {
			value := core.EffectObligation{ID: "retained"}
			if err := decode(&value); err == nil || value.ID != "retained" {
				t.Fatal("runtime accepted ambiguous authority or partially changed destination")
			}
		}
	}
}

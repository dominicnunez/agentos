package recovery

import (
	"github.com/dominicnunez/agentos/internal/core"
	"testing"
)

func TestRecoveryAuthorityDecoderRejectsAmbiguousStoredContracts(t *testing.T) {
	for _, body := range []string{
		`{"effect_obligation_id":"first","effect_obligation_id":"second"}`,
		`{"EFFECT_OBLIGATION_ID":"effect"}`,
		`{"replay_context":{"body":"first","body":"second"}}`,
	} {
		value := core.EffectObligation{ID: "retained"}
		if err := decodeExactJSON([]byte(body), &value); err == nil || value.ID != "retained" {
			t.Fatal("recovery accepted ambiguous authority or partially changed destination")
		}
	}
}

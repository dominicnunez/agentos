package ledger

import (
	"reflect"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

// SQLite's integer affinity still permits corrupt retained text. Bound every
// stored accounting column before scanning numeric values into Go.
func TestIncidentAccountingNumericBytePreflight(t *testing.T) {
	parallelIncidentTest(t)
	for _, field := range []string{"reserved_input_tokens", "reserved_output_tokens", "reserved_cost_nano_usd", "charged_input_tokens", "charged_output_tokens", "charged_cost_nano_usd"} {
		t.Run(field, func(t *testing.T) {
			store, policy, correlation := appendIncidentBudgetHistory(t, 1)
			if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
				t.Fatalf("healthy full owner: %v", err)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), policy.OrganizationID, correlation, 256); err != nil {
				t.Fatalf("healthy public reader: %v", err)
			}
			if _, err := store.db.ExecContext(t.Context(), `UPDATE inference_reservations SET `+field+`=?`, strings.Repeat("x", 33<<20)); err != nil {
				t.Fatal(err)
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err == nil {
				t.Fatal("full accounting owner accepted nonnumeric retained column")
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), policy.OrganizationID, correlation, 256)
			if err == nil {
				t.Fatal("incident accepted oversized accounting metadata")
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("invalid accounting returned partial incident")
			}
			if !strings.Contains(err.Error(), "accounting exceeds byte limit") {
				t.Fatal("oversized accounting column reached numeric scanning before byte preflight")
			}
		})
	}
}

package ledger

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"unicode/utf8"
)

func TestIncidentEscapedClaimPath(t *testing.T) {
	cases := []struct {
		name, body, path string
		array, want      bool
	}{
		{"scalar", `{"task\u005fid":"selected"}`, "$.task_id", false, true},
		{"array", `{"occurrence\u005fevent_refs":["selected"]}`, "$.occurrence_event_refs", true, true},
		{"intermediate", `{"projec\u0074ion":{"va\u006cue":{"validation\u005frefs":["selected"]}}}`, "$.projection.value.validation_refs", true, true},
		{"record-id", `{"projection":{"record\u005fid":"selected"}}`, "$.projection.record_id", false, true},
		{"effect-id", `{"effect\u005fobligation_id":"selected"}`, "$.effect_obligation_id", false, true},
		{"alias", `{"task_id":"decoy","task\u005fid":"selected"}`, "$.task_id", false, true},
		{"nested-note", `{"notes":{"task\u005fid":"selected"}}`, "$.task_id", false, false},
		{"wrong-array", `{"occurrence\u005fevent_refs":{"note":"selected"}}`, "$.occurrence_event_refs", true, false},
		{"nested-element", `{"occurrence\u005fevent_refs":[["selected"],{"note":"selected"}]}`, "$.occurrence_event_refs", true, false},
		{"wrong-parent", `{"projection":[{"record\u005fid":"selected"}]}`, "$.projection.record_id", false, false},
	}
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			claim := incidentScalarClaim("document.body", tc.path, `='selected'`)
			if tc.array {
				claim = incidentArrayClaim("document.body", tc.path, `='selected'`)
			}
			var got bool
			if err := store.db.QueryRowContext(t.Context(), `WITH document(body) AS (VALUES (?)) SELECT `+claim+` FROM document`, tc.body).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("owned path match=%v want %v", got, tc.want)
			}
		})
	}
}

// Encoded member names have the same owned semantics as their decoded names.
// A canonical decoy and escaped alias must both survive, in either order.
func FuzzIncidentSQLClaimAliases(f *testing.F) {
	f.Add("selected", uint8(0), false)
	f.Add("世界", uint8(1), true)
	f.Add("quote\"slash\\", uint8(7), false)
	store, err := Open(":memory:")
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { _ = store.Close() })
	f.Fuzz(func(t *testing.T, value string, position uint8, array bool) {
		if len(value) > 1024 || !utf8.ValidString(value) {
			t.Skip()
		}
		leaf, path := "record_id", "$.projection.record_id"
		encoded, _ := json.Marshal(value)
		child := string(encoded)
		if array {
			leaf, path = "validation_refs", "$.projection.value.validation_refs"
			child = "[" + child + "]"
		}
		field := func(name string) string {
			index := int(position) % len(name)
			return `"` + name[:index] + fmt.Sprintf(`\u%04x`, name[index]) + name[index+1:] + `"`
		}
		claimed := `{` + field(leaf) + `:` + child + `}`
		if array {
			claimed = `{` + field("value") + `:` + claimed + `}`
		}
		escaped := field("projection") + `:` + claimed
		for _, body := range []string{`{"projection":{},` + escaped + `}`, `{` + escaped + `,"projection":{}}`} {
			comparison := `=?`
			query := incidentScalarClaim("document.body", path, comparison)
			if array {
				query = incidentArrayClaim("document.body", path, comparison)
			}
			var matched bool
			if err := store.db.QueryRowContext(t.Context(), `WITH document(body) AS (VALUES (?)) SELECT `+query+` FROM document`, body, value).Scan(&matched); err != nil {
				t.Fatal(err)
			}
			if !matched {
				t.Fatalf("escaped owned path lost a reference: body=%s", body)
			}
			if !array {
				var raw string
				if err := store.db.QueryRowContext(t.Context(), `WITH document(body) AS (VALUES (?)) SELECT `+incidentClaimJSON("document.body", path)+` FROM document`, body).Scan(&raw); err != nil {
					t.Fatal(err)
				}
				var got []string
				if err := json.Unmarshal([]byte(raw), &got); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, []string{value}) {
					t.Fatalf("identity enumeration=%v want %v", got, []string{value})
				}
			}
		}
	})
}

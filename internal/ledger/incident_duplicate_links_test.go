package ledger

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentDuplicateLinkGrammar(t *testing.T) {
	cases := []struct {
		name, kind, body string
		record           bool
		current, legacy  []string
	}{
		{"scalar", "task", `{"value":{"work_id":"first","work_id":"selected"}}`, true, []string{"work:first", "work:selected"}, []string{"work:first"}},
		{"arrays", "task", `{"value":{"depends_on":["first"],"depends_on":["selected"]}}`, true, []string{"task:first", "task:selected"}, []string{"task:first"}},
		{"nested-object", "task", `{"value":{"agent_config":{"blueprint_id":"first"},"agent_config":{"blueprint_id":"selected","blueprint_id":"last"}}}`, true, []string{"agent_blueprint:first", "agent_blueprint:selected", "agent_blueprint:last"}, []string{"agent_blueprint:first"}},
		{"element", "knowledge", `{"value":{"derived_knowledge_refs":[{"id":"first","id":"selected"}]}}`, true, []string{"knowledge:first", "knowledge:selected"}, []string{"knowledge:first"}},
		{"value", "task", `{"value":{"work_id":"first"},"value":{"work_id":"selected"}}`, true, []string{"work:first", "work:selected"}, []string{"work:first"}},
		{"discriminator", "task", `{"value":{"assignee_type":"TEAM","assignee_type":"AGENT","assignee_id":"first","assignee_id":"selected"}}`, true, []string{"team:first", "team:selected", "agent:first", "agent:selected"}, []string{"team:first"}},
		{"projection", "AUDIT_NOTE", `{"projection":{"projection_kind":"note","record_id":"first"},"projection":{"projection_kind":"task","record_id":"selected","value":{"work_id":"linked"}}}`, false, []string{"task:selected", "work:linked"}, []string{"note:first"}},
		{"projection-identities", "AUDIT_NOTE", `{"projection":{"projection_kind":"note","projection_kind":"task","record_id":"first","record_id":"selected","value":{"work_id":"linked"}}}`, false, []string{"task:first", "task:selected", "work:linked"}, []string{"note:first"}},
		{"detail", "WORK_COMPLETED", `{"projection":{"projection_kind":"work","record_id":"self"},"detail":{"evidence_event_ref":"first"},"detail":{"evidence_event_ref":"selected"}}`, false, []string{"work:self", "event:first", "event:selected"}, []string{"work:self"}},
		{"nested-arrays", "GOAL_PROGRESS_EVALUATED", `{"criteria":[{"work_evidence_refs":["first"],"work_evidence_refs":["selected"]}],"criteria":[{"work_evidence_refs":["last"]}]}`, false, []string{"event:first", "event:selected", "event:last"}, nil},
		{"prefix", "EXECUTION_CONTEXT_MANIFESTED", `{"additional_context_refs":[{"id":"task/ignored","id":"goal/selected","id":"mission/first"}],"notes":{"id":"goal/opaque"}}`, false, []string{"goal:selected", "mission:first"}, nil},
		{"requires", "TASK_EXECUTION_SUSPENDED", `{"projection":{"projection_kind":"task","record_id":"self"},"detail":{},"detail":{"stop_request_ref":null,"execution_start_ref":"selected"}}`, false, []string{"task:self", "event:selected"}, []string{"task:self"}},
	}
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, version := range []string{"v1", "v2"} {
				var encoded string
				if err := store.db.QueryRowContext(t.Context(), "SELECT agentos_incident_links_"+version+"(?,?,?)", tc.record, tc.kind, tc.body).Scan(&encoded); err != nil {
					t.Fatal(err)
				}
				var links []incidentSelector
				if err := json.Unmarshal([]byte(encoded), &links); err != nil {
					t.Fatal(err)
				}
				var got []string
				for _, link := range links {
					got = append(got, link.Kind+":"+link.ID)
				}
				want := tc.current
				if version == "v1" {
					want = tc.legacy
				}
				want = append([]string(nil), want...)
				sort.Strings(want)
				sort.Strings(got)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("%s got %v want %v", version, got, want)
				}
				for _, link := range links {
					var match bool
					if err := store.db.QueryRowContext(t.Context(), "SELECT agentos_incident_link_match_"+version+"(?,?,?,?,?)", tc.record, tc.kind, tc.body, link.Kind, link.ID).Scan(&match); err != nil {
						t.Fatal(err)
					}
					if !match {
						t.Fatalf("%s extraction/match disagreement for %+v", version, link)
					}
				}
			}
		})
	}
}

// A member permutation cannot erase an otherwise typed incoming reference.
// Seed empty, escaped, and Unicode IDs; the expected set follows this specific
// Work identity contract independently of the visitor's traversal.
func FuzzIncidentDuplicateReferences(f *testing.F) {
	for _, pair := range [][2]string{{"first", "selected"}, {"", "selected"}, {"selected", "selected"}, {"quote\"slash\\", "世界"}} {
		f.Add(pair[0], pair[1])
	}
	f.Fuzz(func(t *testing.T, first, second string) {
		if len(first)+len(second) > 2048 || !utf8.ValidString(first) || !utf8.ValidString(second) {
			t.Skip()
		}
		a, _ := json.Marshal(first)
		b, _ := json.Marshal(second)
		expected := map[string]bool{}
		if first != "" {
			expected[first] = true
		}
		if second != "" {
			expected[second] = true
		}
		for _, body := range []string{`{"value":{"work_id":` + string(a) + `,"work_id":` + string(b) + `}}`, `{"value":{"work_id":` + string(b) + `,"work_id":` + string(a) + `}}`, `{"value":{"work_id":` + string(a) + `},"value":{"work_id":` + string(b) + `}}`} {
			got := map[string]bool{}
			visitAllIncidentLinks([]driver.Value{int64(1), "task", body}, "work", incidentLinkRules, func(kind, id string) bool {
				if kind != "work" {
					t.Fatalf("unexpected kind %q", kind)
				}
				got[id] = true
				return false
			})
			if !reflect.DeepEqual(got, expected) {
				t.Fatalf("duplicate permutation lost identity: got %v want %v", got, expected)
			}
			for id := range expected {
				if !visitAllIncidentLinks([]driver.Value{int64(1), "task", body}, "work", incidentLinkRules, func(kind, candidate string) bool { return kind == "work" && candidate == id }) {
					t.Fatalf("match omitted %q", id)
				}
			}
		}
	})
}

func TestIncidentDuplicateProjectionKinds(t *testing.T) {
	// Only admitted projection kinds can be selected typed record identities.
	// Unsupported duplicate discriminators must not multiply every record ID.
	for _, size := range []int{32, 1000} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			var members []string
			for index := range size {
				members = append(members, fmt.Sprintf(`"projection_kind":"unsupported-%d"`, index), fmt.Sprintf(`"record_id":"record-%d"`, index))
			}
			kinds := []string{"organization", "mission", "goal", "team", "agent_blueprint", "execution_profile", "agent", "intent", "work", "lab_experiment", "lab_promotion_candidate", "knowledge", "task"}
			for _, kind := range kinds {
				members = append(members, `"projection_kind":"`+kind+`"`)
			}
			body := `{"projection":{` + strings.Join(members, ",") + `}}`
			count := 0
			visitAllIncidentLinks([]driver.Value{int64(0), "AUDIT_NOTE", body}, "", incidentLinkRules, func(kind, id string) bool {
				if !events.ProjectionKindRequiresAdmission(kind) {
					t.Errorf("unsupported projection identity %q", kind)
					return true
				}
				count++
				return false
			})
			if count != len(kinds)*size {
				t.Fatalf("candidate identities=%d want %d", count, len(kinds)*size)
			}
		})
	}
}

func TestIncidentDuplicateDetailGrowth(t *testing.T) {
	for _, size := range []int{16, 1000} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			var members []string
			for index := range size {
				members = append(members, `"projection":{"projection_kind":"work","record_id":"self"}`, fmt.Sprintf(`"detail":{"evidence_event_ref":"event-%d"}`, index))
			}
			body := `{` + strings.Join(members, ",") + `}`
			count := 0
			rules := append(append([]incidentLinkRule(nil), incidentLinkRules...), incidentDetailLinkRules...)
			visitAllIncidentLinks([]driver.Value{int64(0), "WORK_COMPLETED", body}, "event", rules, func(kind, id string) bool { count++; return false })
			if count != size {
				t.Fatalf("shared detail visits=%d want %d distinct typed claims", count, size)
			}
		})
	}
}

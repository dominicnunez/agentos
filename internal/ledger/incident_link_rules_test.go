package ledger

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentLinkSelectFamilies(t *testing.T) {
	tests := []struct {
		name, kind, value string
		want              []string
	}{
		{"goal", "goal", `{"mission_id":"m"}`, []string{"mission:m"}},
		{"work", "work", `{"goal_id":"g","intent_id":"i","replaces_work_id":"w"}`, []string{"goal:g", "intent:i", "work:w"}},
		{"intent", "intent", `{"goal_id":"g","replaces_work_id":"w"}`, []string{"goal:g", "work:w"}},
		{"task agent", "task", `{"work_id":"w","parent_id":"p","depends_on":["d","d",7,null,{},"p"],"assignee_type":"AGENT","assignee_id":"a","agent_config":{"blueprint_id":"b","profile_id":"e"}}`, []string{"work:w", "task:p", "task:d", "agent:a", "agent_blueprint:b", "execution_profile:e"}},
		{"task team", "task", `{"assignee_type":"TEAM","assignee_id":"t"}`, []string{"team:t"}},
		{"experiment", "lab_experiment", `{"work_id":"w"}`, []string{"work:w"}},
		{"candidate", "lab_promotion_candidate", `{"experiment_id":"x"}`, []string{"lab_experiment:x"}},
		{"knowledge agent", "knowledge", `{"derived_knowledge_refs":[{"id":"k"},{"id":8},"opaque",7,null,{}],"scope":"AGENT","scope_id":"a","created_by_kind":"AGENT","created_by":"c"}`, []string{"knowledge:k", "agent:a", "agent:c"}},
		{"knowledge team", "knowledge", `{"scope":"TEAM","scope_id":"t","created_by_kind":"HUMAN","created_by":"h"}`, []string{"team:t"}},
		{"knowledge organization", "knowledge", `{"scope":"ORGANIZATION","scope_id":"o","created_by_kind":"RUNTIME","created_by":"r"}`, nil},
		{"roster", "team", `{"member_agent_ids":["a","a",8,{},null]}`, []string{"agent:a"}},
		{"agent", "agent", `{"blueprint_id":"b","execution_profile_id":"e"}`, []string{"agent_blueprint:b", "execution_profile:e"}},
		{"notes", "note", `{"goal_id":"g","work_id":"w","parent_id":"p","scope":"AGENT","scope_id":"a"}`, nil},
		{"wrong types", "task", `{"work_id":7,"parent_id":{},"depends_on":"opaque","assignee_type":"AGENT","assignee_id":false,"agent_config":[]}`, nil},
		{"wrong array", "knowledge", `{"derived_knowledge_refs":{"id":"k"}}`, nil},
		{"duplicate field", "task", `{"work_id":"w","work_id":"other"}`, []string{"work:w"}},
		{"duplicate member", "knowledge", `{"derived_knowledge_refs":[{"id":"k","id":"other"}]}`, []string{"knowledge:k"}},
	}
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	for _, tc := range tests {
		for _, record := range []bool{false, true} {
			t.Run(tc.name+map[bool]string{true: " record", false: " event"}[record], func(t *testing.T) {
				body := `{"value":` + tc.value + `}`
				if !record {
					body = `{"projection":{"projection_kind":"` + tc.kind + `","record_id":"self","value":` + tc.value + `}}`
				}
				rows, err := incidentTestLinks(t, store, record, tc.kind, body)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = rows.Close() }()
				var got []string
				for rows.Next() {
					var k, id string
					if err := rows.Scan(&k, &id); err != nil {
						t.Fatal(err)
					}
					got = append(got, k+":"+id)
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				want := append([]string(nil), tc.want...)
				if !record {
					want = append(want, tc.kind+":self")
				}
				sort.Strings(want)
				sort.Strings(got)
				assertIncidentLinkMatch(t, store, record, tc.kind, body, want)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("got %v, want %v", got, want)
				}
			})
		}
	}
}

func TestIncidentLinkSelectInvalidJSON(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	for _, record := range []bool{false, true} {
		for _, body := range []string{`{`, `null`, `[]`, `"opaque"`, `7`, `{"projection":{"projection_kind":7,"record_id":"self"}}`} {
			assertIncidentLinkMatch(t, store, record, "task", body, nil)
			rows, err := incidentTestLinks(t, store, record, "task", body)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = rows.Close() }()
			if rows.Next() {
				t.Fatalf("invalid input yielded a link: %q", body)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
		}
	}
	// A field name inside opaque event notes is not a projection link.
	body, _ := json.Marshal(map[string]string{"work_id": "w", "projection": "opaque"})
	rows, err := incidentTestLinks(t, store, false, "", string(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	if rows.Next() {
		t.Fatal("opaque note yielded link")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

// Exercise the exact maintenance shape: extraction must not abort raw writes.
func incidentTestLinks(t *testing.T, store *SQLite, record bool, kind, body string) (*sql.Rows, error) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TEMP TABLE IF NOT EXISTS test_link_source(kind TEXT,body TEXT,payload TEXT,record INTEGER)`,
		`CREATE TEMP TABLE IF NOT EXISTS test_links(target_kind TEXT,target_id TEXT)`,
		`CREATE TEMP TRIGGER IF NOT EXISTS test_record_links AFTER INSERT ON test_link_source WHEN NEW.record=1 BEGIN INSERT INTO test_links ` + incidentLinkSelect(true, "NEW") + `; END`,
		`CREATE TEMP TRIGGER IF NOT EXISTS test_event_links AFTER INSERT ON test_link_source WHEN NEW.record=0 BEGIN INSERT INTO test_links ` + incidentLinkSelect(false, "NEW") + `; END`,
		`DELETE FROM test_links`,
	} {
		if _, err := store.db.ExecContext(t.Context(), statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.db.ExecContext(t.Context(), `INSERT INTO test_link_source VALUES (?,?,?,?)`, kind, body, body, record); err != nil {
		t.Fatal(err)
	}
	return store.db.QueryContext(t.Context(), `SELECT target_kind,target_id FROM test_links`)
}

func assertIncidentLinkMatch(t *testing.T, store *SQLite, record bool, kind, body string, want []string) {
	t.Helper()
	expected := map[string]bool{}
	for _, link := range want {
		expected[link] = true
	}
	candidates := []string{"mission:m", "goal:g", "intent:i", "work:w", "task:p", "task:d", "task:7", "agent:a", "agent:c", "agent:h", "team:t", "lab_experiment:x", "knowledge:k", "knowledge:8", "agent_blueprint:b", "execution_profile:e", "organization:o", kind + ":self", "task:absent", "task:"}
	for _, candidate := range candidates {
		parts := strings.SplitN(candidate, ":", 2)
		var matches bool
		query := `WITH source(kind,body,payload) AS (VALUES (?,?,?)), target(kind,id) AS (VALUES (?,?)) SELECT ` + incidentLinkMatch(record, "source", "target.kind", "target.id") + ` FROM source,target`
		if err := store.db.QueryRowContext(t.Context(), query, kind, body, body, parts[0], parts[1]).Scan(&matches); err != nil {
			t.Fatal(err)
		}
		if matches != expected[candidate] {
			t.Fatalf("match %q=%v, want %v", candidate, matches, expected[candidate])
		}
	}
}

// Ordinary notes have no typed links, but SQLite must still compile their source
// triggers. This benchmark covers that regression through the owning writer.
func BenchmarkIncidentOrdinaryAppend(b *testing.B) {
	store, err := Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	draft := events.TrustedDraft{OrganizationID: "org", EventType: "AUDIT_NOTE", CorrelationID: "notes", Payload: map[string]string{"note": "unrelated"}}
	b.ResetTimer()
	for b.Loop() {
		if _, err := store.Append(b.Context(), draft); err != nil {
			b.Fatal(err)
		}
	}
}

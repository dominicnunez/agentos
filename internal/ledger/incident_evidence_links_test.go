package ledger

import (
	"reflect"
	"sort"
	"testing"
)

func TestIncidentEvidenceLinkGrammar(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for _, field := range []string{"provenance_event_refs", "occurrence_event_refs", "validation_refs"} {
		for _, record := range []bool{false, true} {
			value := `{"` + field + `":["first","first",7,null,{},["nested"],{"id":"typed"},"","second"],"evidence_artifact_refs":["artifact"]}`
			body := `{"value":` + value + `}`
			if !record {
				body = `{"projection":{"projection_kind":"knowledge","record_id":"incoming","value":` + value + `}}`
			}
			rows, err := incidentTestLinks(t, store, record, "knowledge", body)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for rows.Next() {
				var kind, id string
				if err := rows.Scan(&kind, &id); err != nil {
					t.Fatal(err)
				}
				if kind == "event" {
					got = append(got, id)
				}
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			sort.Strings(got)
			if !reflect.DeepEqual(got, []string{"first", "second"}) {
				t.Fatalf("%s record=%v: event links %v", field, record, got)
			}
			var legacy, current, match int
			if err := store.db.QueryRowContext(t.Context(), `SELECT (SELECT COUNT(*) FROM json_each(agentos_incident_links_v1(?,?,?)) WHERE json_extract(value,'$.kind')='event'),(SELECT COUNT(*) FROM json_each(agentos_incident_links_v2(?,?,?)) WHERE json_extract(value,'$.kind')='event'),agentos_incident_link_match_v2(?,?,?,'event','second')`, record, "knowledge", body, record, "knowledge", body, record, "knowledge", body).Scan(&legacy, &current, &match); err != nil {
				t.Fatal(err)
			}
			if legacy != 0 || current != 2 || match != 1 {
				t.Fatalf("versioned extraction %d/%d, match %d", legacy, current, match)
			}
		}
	}
}

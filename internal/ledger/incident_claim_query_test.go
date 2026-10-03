package ledger

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// Frozen original helper, including root-scalar optimization, ancestor
// relationships and immediate-array-element gates. It does not call the
// production helper or derive its traversal from the staged implementation.
func incidentPriorClaimQuery(source, path string, array bool, comparison string) string {
	fields := strings.Split(strings.TrimPrefix(path, "$."), ".")
	tree := `json_tree(CASE WHEN json_valid(` + source + `) THEN ` + source + ` ELSE '{}' END)`
	nodes := `WITH claims AS MATERIALIZED (SELECT id,parent,key,type,value FROM ` + tree + `) `
	from, leaf, value, typed := `claims claim`, "claim", "claim.value", `claim.type='text'`
	if array {
		from = `claims ref JOIN claims claim ON claim.id=ref.parent`
		value = "ref.value"
		typed = `ref.type='text' AND claim.type='array'`
	}
	if !array && len(fields) == 1 {
		nodes, from = "", tree+` claim`
	}
	predicate := typed + ` AND claim.key='` + fields[len(fields)-1] + `'`
	for index := len(fields) - 2; index >= 0; index-- {
		parent := fmt.Sprintf("parent%d", index)
		from += ` JOIN claims ` + parent + ` ON ` + parent + `.id=` + leaf + `.parent`
		predicate += ` AND ` + parent + `.type='object' AND ` + parent + `.key='` + fields[index] + `'`
		leaf = parent
	}
	predicate += ` AND ` + leaf + `.parent=0`
	if comparison != "" {
		predicate += ` AND ` + value + comparison
	}
	return nodes + `SELECT ` + value + ` AS value FROM ` + from + ` WHERE ` + predicate
}

func TestIncidentClaimPriorEquivalence(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	accepted, excluded := 0, 0
	check := func(name string, raw []byte, path string, array bool, expectedCount ...int) {
		t.Helper()
		var priorValues, currentValues []string
		var priorDecision, currentDecision bool
		for index, helper := range []func(string, string, bool, string) string{incidentPriorClaimQuery, incidentClaimQuery} {
			// Hex preserves invalid UTF8/NUL bytes; json_quote also observes SQL
			// subtype behavior. Sorting preserves duplicate multiplicity without
			// imposing ordering that no production enumeration consumer owns.
			values := helper("document.body", path, array, "")
			grouped := `(SELECT json_group_array(value) FROM (` + values + `))`
			if !array && index == 1 {
				grouped = incidentClaimJSON("document.body", path)
			}
			query := `WITH document(body) AS (VALUES (?1)) SELECT
				(SELECT json_group_array(json_array(hex(CAST(value AS BLOB)),typeof(value),hex(CAST(json_quote(value) AS BLOB)))) FROM (` + values + `)),
				(SELECT json_group_array(json_array(hex(CAST(value AS BLOB)),type,hex(CAST(json_quote(value) AS BLOB)))) FROM json_each(` + grouped + `)),
				EXISTS (` + helper("document.body", path, array, "=?2") + `) FROM document`
			var rowsJSON, enumerationJSON string
			var decision bool
			if err := store.db.QueryRowContext(t.Context(), query, raw, "selected").Scan(&rowsJSON, &enumerationJSON, &decision); err != nil {
				t.Fatalf("%s path=%s array=%v helper%d: %v raw=%q", name, path, array, index, err, raw)
			}
			canonical := func(encoded string) []string {
				var rows [][]string
				if err := json.Unmarshal([]byte(encoded), &rows); err != nil {
					t.Fatal(err)
				}
				var values []string
				for _, row := range rows {
					values = append(values, strings.Join(row, "/"))
				}
				sort.Strings(values)
				return values
			}
			rows, enumerated := canonical(rowsJSON), canonical(enumerationJSON)
			// JSON aggregation can replace invalid UTF8, so compare its output
			// independently across helpers rather than against raw SQL bytes.
			combined := append(append([]string(nil), rows...), "enumerated")
			combined = append(combined, enumerated...)
			if len(expectedCount) != 0 && len(rows) != expectedCount[0] {
				t.Fatalf("%s lost the explicit occurrence count: got%d want%d", name, len(rows), expectedCount[0])
			}
			if index == 0 {
				priorValues, priorDecision = combined, decision
			} else {
				currentValues, currentDecision = combined, decision
			}
		}
		if !reflect.DeepEqual(priorValues, currentValues) || priorDecision != currentDecision {
			t.Fatalf("%s path=%s array=%v old/new values=%v/%v decisions=%v/%v raw=%q", name, path, array, priorValues, currentValues, priorDecision, currentDecision, raw)
		}
		if priorDecision {
			accepted++
		} else {
			excluded++
		}
	}
	paths := []struct {
		path  string
		array bool
	}{
		{"$.projection.projection_kind", false}, {"$.projection.record_id", false}, {"$.projection.value.created_by_kind", false},
		{"$.projection.value.validation_method", false}, {"$.projection.value.validated_by_kind", false}, {"$.projection.value.status", false},
		{"$.projection.value.context_use", false}, {"$.projection.value.scope", false}, {"$.projection.value.scope_id", false}, {"$.projection.value.organization_id", false},
		{"$.value.created_by_kind", false}, {"$.value.validation_method", false}, {"$.value.validated_by_kind", false}, {"$.value.status", false},
		{"$.value.context_use", false}, {"$.value.scope", false}, {"$.value.scope_id", false}, {"$.value.organization_id", false},
		{"$.detail.evidence_event_ref", false}, {"$.id", false}, {"$.task_id", false}, {"$.connection_id", false}, {"$.reservation_id", false},
		{"$.effect_obligation_id", false}, {"$.outcome_event_ref", false}, {"$.capability_check_event_id", false}, {"$.knowledge_id", false},
		{"$.goal_id", false}, {"$.intent_id", false}, {"$.replaces_work_id", false},
		{"$.occurrence_event_refs", true}, {"$.projection.value.provenance_event_refs", true}, {"$.projection.value.validation_refs", true},
		{"$.value.provenance_event_refs", true}, {"$.value.validation_refs", true}, {"$.projection.value.member_agent_ids", true}, {"$.value.member_agent_ids", true},
	}
	for _, path := range paths {
		fields := strings.Split(strings.TrimPrefix(path.path, "$."), ".")
		var value any = "selected"
		if path.array {
			value = []any{"selected", "selected", 123, true, nil, []any{"selected"}, map[string]string{"id": "selected"}}
		}
		for index := len(fields) - 1; index >= 0; index-- {
			value = map[string]any{fields[index]: value, "opaque": map[string]any{"deep": []any{0, []any{1, 2, 3}}}}
		}
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		check("production-path", raw, path.path, path.array)
		check("root-array", append(append([]byte{'['}, raw...), ']'), path.path, path.array, 0)
	}
	check("scalar-occurrences", []byte(`{"projection":{"record_id":"first","record\u005fid":"selected"},"projec\u0074ion":{"record_id":"selected"}}`), "$.projection.record_id", false, 3)
	check("array-occurrences", []byte(`{"projection":{"value":{"validation_refs":["first","selected",5,["selected"],{"id":"selected"}],"validation_refs":["selected"]},"value":{"validation_refs":["third"]}},"projec\u0074ion":{"value":{"validation_refs":["fourth"]}}}`), "$.projection.value.validation_refs", true, 5)
	seeds := [][]byte{
		[]byte(`{"projection":{"record_id":"selected","value":{"validation_refs":["selected"]}},"id":"selected"}`),
		[]byte(`{"projection":"{\"record_id\":\"selected\"}"}`), []byte(`{"projection":{"value":"{\"validation_refs\":[\"selected\"]}"}}`),
		[]byte(`{"projection":null,"projection":1,"projection":true,"projection":[],"projection":{}}`),
		[]byte(`{"projection":{"record_id":123,"record_id":[],"record_id":{},"record_id":true,"record_id":null,"record_id":"123"}}`),
		[]byte(`{"projection":{"record_id":"世界\u2603\u0000","value":{"validation_refs":["\ud83d\ude00","\ud800","\udc00"]}},"id":"{\"literal\":true}"}`),
		[]byte(`{"projection":{"record_id":"quote\"slash\\","value":{"validation_refs":["selected",1e999]}}}`),
		[]byte(`{"projection":{"value":{"validation_refs":{},"validation_refs":null,"validation_refs":"selected"}}}`),
		[]byte(`null`), []byte(`true`), []byte(`1e999`), []byte(`"selected"`), []byte(`[]`), []byte(`{`), []byte(`{} trailing`),
		append([]byte(`{"projection":{"record_id":"selected"}}`), 0),
		append(append([]byte(`{"projection":{"record_id":"selected"}}`), 0), []byte(`trailing`)...),
		append([]byte{0}, []byte(`{"projection":{"record_id":"selected"}}`)...),
		append(append([]byte(`{"projection":{"record_id":"`), 0xff, 0xc0, 0x80), []byte(`"}}`)...),
	}
	for index, raw := range seeds {
		for _, path := range []struct {
			path  string
			array bool
		}{{"$.projection.record_id", false}, {"$.projection.value.validation_refs", true}, {"$.id", false}} {
			check(fmt.Sprintf("boundary%d", index), raw, path.path, path.array)
		}
	}
	random := rand.New(rand.NewSource(41_718))
	for index := range 40 {
		raw := append([]byte(nil), seeds[0]...)
		if index < 32 {
			raw = append(raw, byte(index))
		} else {
			raw[random.Intn(len(raw))] = byte(random.Intn(256))
		}
		check(fmt.Sprintf("generated%d", index), raw, "$.projection.value.validation_refs", true)
	}
	if accepted == 0 || excluded == 0 {
		t.Fatalf("vacuous prior-equivalence decisions: accepted=%d excluded=%d", accepted, excluded)
	}
}

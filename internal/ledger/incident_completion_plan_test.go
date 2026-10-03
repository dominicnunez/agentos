package ledger

import (
	"strings"
	"testing"
)

// Inspect the production discovery query, including its canonical window probes.
// Virtual/materialized boundary scans are permitted; retained event scans are not.
func TestIncidentCompletionCandidatePlan(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	rows, err := store.db.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+incidentCompletionCandidatesSQL,
		`[{"task":"task","correlation":"correlation","organization":"org","terminal":100,"outcome":"outcome","agent":true}]`, `[]`, 4097)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
		for _, alias := range []string{"events", "e", "v", "r", "review", "manifest", "outcome"} {
			if detail == "SCAN "+alias || strings.HasPrefix(detail, "SCAN "+alias+" ") {
				t.Fatalf("unindexed retained completion scan: %s", detail)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(plan, "\n")
	for _, alias := range []string{"e", "v", "r", "review"} {
		if !strings.Contains(joined, "SEARCH "+alias+" USING INDEX events_correlation_idx") {
			t.Fatalf("missing indexed completion probe %s:\n%s", alias, joined)
		}
	}
	if !strings.Contains(joined, "SEARCH manifest USING INDEX events_execution_idx") {
		t.Fatalf("missing exact execution manifest probe:\n%s", joined)
	}

}

package ledger

import "testing"

func TestIncidentAggregateLinkGrammar(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for _, tc := range []struct {
		name, kind, body string
		want             int
	}{
		{"goal-witness", "GOAL_PROGRESS_EVALUATED", `{"work_evidence_refs":["selected"]}`, 1},
		{"goal-criterion", "GOAL_PROGRESS_EVALUATED", `{"criteria":[{"work_evidence_refs":["other","selected"]}]}`, 1},
		{"multiple-criteria", "GOAL_PROGRESS_EVALUATED", `{"criteria":[{"work_evidence_refs":["other"]},{"work_evidence_refs":["selected","selected"]}]}`, 1},
		{"verification", "WORK_COMPLETION_EVALUATED", `{"tasks":[{"verification_event_ref":"selected"}]}`, 1},
		{"completion", "WORK_COMPLETION_EVALUATED", `{"tasks":[{"completion_event_ref":"selected"}]}`, 1},
		{"artifact", "WORK_COMPLETION_EVALUATED", `{"tasks":[{"artifact_refs":["selected"]}],"artifact_refs":["selected"]}`, 0},
		{"criterion-text", "GOAL_PROGRESS_EVALUATED", `{"criteria":[{"criterion":{"value":"selected","work_evidence_refs":["selected"]}}]}`, 0},
		{"nested-note", "GOAL_PROGRESS_EVALUATED", `{"note":{"work_evidence_refs":["selected"]}}`, 0},
		{"wrong-array-shape", "GOAL_PROGRESS_EVALUATED", `{"criteria":[{"work_evidence_refs":"selected"}]}`, 0},
		{"non-string", "GOAL_PROGRESS_EVALUATED", `{"criteria":[{"work_evidence_refs":[7,{"event_ref":"selected"}]}]}`, 0},
		{"ordinary-note", "AUDIT_NOTE", `{"work_evidence_refs":["selected"],"criteria":[{"work_evidence_refs":["selected"]}],"tasks":[{"verification_event_ref":"selected"}]}`, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var count, match, legacy int
			if err := store.db.QueryRowContext(t.Context(), `SELECT (SELECT COUNT(*) FROM json_each(agentos_incident_links_v2(0,?,?)) WHERE json_extract(value,'$.kind')='event' AND json_extract(value,'$.id')='selected'),agentos_incident_link_match_v2(0,?,?,'event','selected'),(SELECT COUNT(*) FROM json_each(agentos_incident_links_v1(0,?,?)) WHERE json_extract(value,'$.kind')='event')`, tc.kind, tc.body, tc.kind, tc.body, tc.kind, tc.body).Scan(&count, &match, &legacy); err != nil {
				t.Fatal(err)
			}
			if count != tc.want || match != tc.want || legacy != 0 {
				t.Fatalf("links=%d match=%d legacy=%d want=%d", count, match, legacy, tc.want)
			}
		})
	}
}

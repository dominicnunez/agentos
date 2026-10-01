package ledger

import (
	"strings"
	"testing"
)

// Frozen pre-optimization SQL: retain the full json_tree value column and the
// complete closed owner relation. This oracle must not call the production
// node builder, or both sides could silently acquire the same omission.
func projectionPriorValueNodes(source string) string {
	const kinds = `'organization','mission','goal','team','agent_blueprint','execution_profile','agent','intent','work','lab_experiment','lab_promotion_candidate','knowledge','task'`
	const lifecycle = `('ORGANIZATION_CREATED','organization'),
		('MISSION_CREATED','mission'),('MISSION_REVISED','mission'),('MISSION_RETIRED','mission'),
		('GOAL_CREATED','goal'),('GOAL_REFINED','goal'),('GOAL_PAUSED','goal'),('GOAL_RESUMED','goal'),('GOAL_RETIRED','goal'),('GOAL_ACHIEVED','goal'),
		('TEAM_CREATED','team'),('TEAM_REVISED','team'),
		('AGENT_BLUEPRINT_CREATED','agent_blueprint'),('AGENT_BLUEPRINT_UPDATED','agent_blueprint'),
		('EXECUTION_PROFILE_CREATED','execution_profile'),('EXECUTION_PROFILE_UPDATED','execution_profile'),
		('AGENT_CREATED','agent'),('AGENT_CONFIGURATION_UPDATED','agent'),('AGENT_DEACTIVATED','agent'),('AGENT_REACTIVATED','agent'),
		('INTENT_CREATED','intent'),
		('WORK_CREATED','work'),('WORK_COMPLETED','work'),('WORK_FAILED','work'),('WORK_PLANNING_FAILED','work'),
		('LAB_EXPERIMENT_STARTED','lab_experiment'),('LAB_EXPERIMENT_COMPLETED','lab_experiment'),('LAB_EXPERIMENT_FAILED','lab_experiment'),
		('LAB_PROMOTION_CANDIDATE_CREATED','lab_promotion_candidate'),
		('KNOWLEDGE_PROPOSED','knowledge'),('KNOWLEDGE_ACTIVATED','knowledge'),('KNOWLEDGE_SUPERSEDED','knowledge'),('KNOWLEDGE_STALE','knowledge'),('KNOWLEDGE_QUARANTINED','knowledge'),
		('TASK_CREATED','task'),('TASK_BLOCKED','task'),('TASK_ASSIGNMENT_REVALIDATED','task'),('TASK_EXECUTION_SUSPENDED','task'),('TASK_RECOVERED','task'),('TASK_RESUMED','task'),('EXECUTION_STARTED','task'),('TASK_VERIFIED_COMPLETE','task'),('COMPLETION_REJECTED','task'),('TASK_DEPENDENCY_FAILED','task'),('TASK_REMEDIATION_FAILED','task'),('TASK_WORK_FAILED','task')`
	body, projection := "e.payload", `SELECT id FROM nodes WHERE parent=0 AND key='projection' AND type='object'`
	owners := `SELECT p.id,k.value AS kind FROM projection_nodes p JOIN nodes k ON k.parent=p.id WHERE k.key='projection_kind' AND k.type='text' AND k.value IN (` + kinds + `)`
	if source == "record" {
		body, projection = "r.body", `SELECT id FROM nodes WHERE parent IS NULL AND type='object'`
		owners += ` UNION SELECT p.id,r.kind FROM projection_nodes p WHERE r.kind IN (` + kinds + `)`
	} else {
		owners += ` UNION SELECT p.id,r.kind FROM projection_nodes p JOIN records r ON r.admission_event_id=e.event_id AND r.admission_event_id<>'' WHERE r.kind IN (` + kinds + `)`
	}
	owners += ` UNION SELECT p.id,l.kind FROM projection_nodes p JOIN lifecycle l ON l.label=e.event_type`
	return `nodes AS MATERIALIZED (SELECT id,parent,key,type,value FROM json_tree(CASE WHEN json_valid(` + body + `) THEN ` + body + ` ELSE '{}' END)),
		projection_nodes AS (` + projection + `),lifecycle(label,kind) AS (VALUES ` + lifecycle + `),owners AS (` + owners + `)`
}

func projectionPriorValueSQL(t *testing.T, query string) string {
	t.Helper()
	for _, source := range []string{"event", "record"} {
		if strings.Count(query, projectionClaimNodes(source)) != 1 {
			t.Fatalf("expected exactly one %s node relation in compared query", source)
		}
		query = strings.Replace(query, projectionClaimNodes(source), projectionPriorValueNodes(source), 1)
	}
	return query
}

func TestProjectionNumericTextIdentity(t *testing.T) {
	for _, source := range []string{"event", "record"} {
		t.Run(source, func(t *testing.T) {
			store := projectionScopeFixture(t)
			var eventID string
			if err := store.db.QueryRowContext(t.Context(), `SELECT admission_event_id FROM records WHERE kind='team'`).Scan(&eventID); err != nil {
				t.Fatal(err)
			}
			payload, body := []byte(`{"note":"safe"}`), []byte(`{"note":"safe"}`)
			doc := `{"projection_kind":"task","record_id":"foreign","value":{"id":"123","routing":{"organization_id":"org-1"},"number":123,"array":[123],"object":{"number":123}}}`
			if source == "event" {
				payload = []byte(`{"projection":` + doc + `}`)
			} else {
				body = []byte(doc)
			}
			if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET payload=?,event_type='AUDIT_NOTE' WHERE event_id=?`, payload, eventID); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.ExecContext(t.Context(), `UPDATE records SET body=?,kind='task',record_id='foreign' WHERE admission_event_id=?`, body, eventID); err != nil {
				t.Fatal(err)
			}
			for _, query := range []string{projectionIdentitySQL(), projectionPriorValueSQL(t, projectionIdentitySQL())} {
				var conflict bool
				if err := store.db.QueryRowContext(t.Context(), query, `[{"kind":"task","id":"123"}]`).Scan(&conflict); err != nil || !conflict {
					t.Fatalf("numeric-looking owned text identity was omitted: conflict=%v err=%v", conflict, err)
				}
			}
		})
	}
}

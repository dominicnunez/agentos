package recovery

import (
	"database/sql"
	"testing"

	"github.com/dominicnunez/agentos/internal/ledger"
)

func TestCompletionTransitionSelection(t *testing.T) {
	for _, damage := range []string{"missing", "duplicate", "foreign-duplicate", "older-version", "string-version"} {
		t.Run(damage, func(t *testing.T) {
			db, err := sql.Open("sqlite", recoveryCompletedTaskHistory(t, 2))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			if err := ledger.ValidateTaskCompletionAdmissions(t.Context(), db); err != nil {
				t.Fatalf("valid baseline: %v", err)
			}
			switch damage {
			case "missing":
				_, err = db.ExecContext(t.Context(), `DELETE FROM events WHERE event_type='TASK_VERIFIED_COMPLETE' AND task_id='task-0'`)
			case "older-version":
				_, err = db.ExecContext(t.Context(), `UPDATE events SET payload=json_set(payload,'$.projection.version',2) WHERE event_type='TASK_VERIFIED_COMPLETE' AND task_id='task-0'`)
			default:
				organization := "org-1"
				if damage == "foreign-duplicate" {
					organization = "foreign-org"
				}
				_, err = db.ExecContext(t.Context(), `INSERT INTO events(event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version)
SELECT 'duplicate-completion',(SELECT MAX(sequence)+1 FROM events),?,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE event_type='TASK_VERIFIED_COMPLETE' AND task_id='task-0'`, organization)
			}
			if err != nil {
				t.Fatal(err)
			}
			if damage == "string-version" {
				if _, err := db.ExecContext(t.Context(), `UPDATE events SET payload=CAST(json_set(payload,'$.projection.version','3') AS BLOB) WHERE event_id='duplicate-completion'`); err != nil {
					t.Fatal(err)
				}
				// Even though the exact SQL selector excludes a string version,
				// the malformed Task claim must fail full evidence validation.
			}
			// Exercise the admission reader directly so integrity-chain failure
			// cannot mask a missing or ambiguous transition-selection failure.
			if err := ledger.ValidateTaskCompletionAdmissions(t.Context(), db); err == nil {
				t.Fatal("accepted damaged completion transition")
			}
		})
	}
}

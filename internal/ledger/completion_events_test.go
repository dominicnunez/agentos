package ledger

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestCompletionSelectionTypes(t *testing.T) {
	for _, tc := range []struct {
		name, id, version string
		match             bool
	}{
		{"exact", `"123"`, `3`, true},
		{"real-version", `"123"`, `3.0`, true},
		{"string-version", `"123"`, `"3"`, false},
		{"numeric-id", `123`, `3`, false},
		{"older-version", `"123"`, `2`, false},
		{"null-version", `"123"`, `null`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			// Minimal storage values isolate SQL candidate selection from the
			// subsequent typed projection and completion evidence validators.
			payload := []byte(fmt.Sprintf(`{"projection":{"projection_kind":"task","record_id":%s,"version":%s}}`, tc.id, tc.version))
			_, err = store.db.ExecContext(t.Context(), `INSERT INTO events(event_id,organization_id,event_type,authorization_refs,artifact_refs,payload,created_at,schema_version) VALUES('completion','org','TASK_VERIFIED_COMPLETE','[]','[]',?,'2026-09-01T00:00:00Z',?)`, payload, events.SchemaVersion)
			if err != nil {
				t.Fatal(err)
			}
			for _, version := range []int{2, 3} {
				_, err = store.db.ExecContext(t.Context(), `INSERT INTO records(kind,record_id,version,body,admission_event_id,created_at) VALUES('task','123',?,?,?,'2026-09-01T00:00:00Z')`, version, []byte(`{}`), fmt.Sprintf("admission-%d", version))
				if err != nil {
					t.Fatal(err)
				}
			}
			tx, err := store.db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			old, err := collectEvents(tx.QueryContext(t.Context(), `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE event_type=? AND json_extract(payload,'$.projection.projection_kind')=? AND json_extract(payload,'$.projection.record_id')=? AND json_extract(payload,'$.projection.version')=? ORDER BY sequence LIMIT 2`, "TASK_VERIFIED_COMPLETE", "task", "123", 3))
			if err != nil {
				t.Fatal(err)
			}
			batched, err := taskCompletionEvents(t.Context(), tx)
			if err != nil {
				t.Fatal(err)
			}
			if (len(old) == 1) != tc.match {
				t.Fatalf("original selector matches=%d, want match=%v", len(old), tc.match)
			}
			if len(old) != len(batched["123"]) || (len(old) > 0 && !reflect.DeepEqual(old, batched["123"])) {
				t.Fatalf("batched selection differs from exact lookup: old=%v batched=%v", old, batched["123"])
			}
		})
	}
}

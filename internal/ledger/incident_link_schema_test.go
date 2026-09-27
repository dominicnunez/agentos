package ledger

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentLinkSchemaMaintainsSources(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.db.ExecContext(t.Context(), `INSERT INTO events(event_id,organization_id,event_type,authorization_refs,artifact_refs,payload,created_at,schema_version)
VALUES('incoming','org-1','KNOWLEDGE_PROPOSED','[]','[]',?, '2026-09-01T00:00:00Z',?)`,
		`{"projection":{"projection_kind":"knowledge","record_id":"child","value":{"derived_knowledge_refs":[{"id":"root"}]}}}`, events.SchemaVersion); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM incident_event_links WHERE target_kind='knowledge' AND target_id='root' AND event_id='incoming'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("incoming event links=%d, want 1", count)
	}
}

// Source bytes are intentionally minimal: these fixtures exercise storage
// maintenance independently of admission/replay validation in reader tests.
func incidentLinkSourceFixtures(t *testing.T, db *sql.DB) {
	t.Helper()
	for n := 1; n <= 2; n++ {
		if _, err := db.ExecContext(t.Context(), `INSERT INTO events(sequence,event_id,organization_id,event_type,authorization_refs,artifact_refs,payload,created_at,schema_version)
VALUES(?,?,'org-1','KNOWLEDGE_PROPOSED','[]','[]',?,'2026-09-01T00:00:00Z',?)`, n, fmt.Sprintf("e%d", n), incidentLinkEventBody(n, fmt.Sprintf("root%d", n)), events.SchemaVersion); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(t.Context(), `INSERT INTO records(kind,record_id,version,body,admission_event_id,created_at) VALUES('knowledge',?,1,?,?,'2026-09-01T00:00:00Z')`, fmt.Sprintf("child%d", n), incidentLinkRecordBody(fmt.Sprintf("root%d", n)), fmt.Sprintf("e%d", n)); err != nil {
			t.Fatal(err)
		}
	}
}

func incidentLinkEventBody(id int, target string) string {
	return fmt.Sprintf(`{"projection":{"projection_kind":"knowledge","record_id":"child%d","value":{"derived_knowledge_refs":[{"id":%q}]}}}`, id, target)
}

func incidentLinkRecordBody(target string) string {
	return fmt.Sprintf(`{"value":{"derived_knowledge_refs":[{"id":%q}]}}`, target)
}

func incidentLinkRows(t *testing.T, query storageQueryer, record bool) []string {
	t.Helper()
	statement := `SELECT target_kind||':'||target_id||':'||event_id||':'||event_sequence FROM incident_event_links ORDER BY 1`
	if record {
		statement = `SELECT target_kind||':'||target_id||':'||record_kind||':'||record_id||':'||record_version||':'||admission_event_id FROM incident_record_links ORDER BY 1`
	}
	rows, err := query.QueryContext(t.Context(), statement)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var got []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		got = append(got, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return got
}

func checkIncidentLinkRows(t *testing.T, query storageQueryer, record bool, want []string) {
	t.Helper()
	sort.Strings(want)
	if got := incidentLinkRows(t, query, record); !reflect.DeepEqual(got, want) {
		t.Fatalf("record=%t links=%v, want %v", record, got, want)
	}
}

func TestIncidentLinkSourceMutations(t *testing.T) {
	tests := []struct {
		name, statement string
		args            []any
		record          bool
		want            []string
	}{
		{"event body", `UPDATE events SET payload=? WHERE event_id='e1'`, []any{incidentLinkEventBody(1, "changed")}, false, []string{"knowledge:child1:e1:1", "knowledge:changed:e1:1", "knowledge:child2:e2:2", "knowledge:root2:e2:2"}},
		{"event identity", `UPDATE events SET event_id='moved',sequence=11 WHERE event_id='e1'`, nil, false, []string{"knowledge:child1:moved:11", "knowledge:root1:moved:11", "knowledge:child2:e2:2", "knowledge:root2:e2:2"}},
		{"event delete", `DELETE FROM events WHERE event_id='e1'`, nil, false, []string{"knowledge:child2:e2:2", "knowledge:root2:e2:2"}},
		{"event malformed", `UPDATE events SET payload='{' WHERE event_id='e1'`, nil, false, []string{"knowledge:child2:e2:2", "knowledge:root2:e2:2"}},
		{"event insert replace", `INSERT OR REPLACE INTO events(sequence,event_id,organization_id,event_type,authorization_refs,artifact_refs,payload,created_at,schema_version) VALUES(1,'e2','org-1','KNOWLEDGE_PROPOSED','[]','[]',?,'2026-09-01T00:00:00Z',?)`, []any{incidentLinkEventBody(3, "changed"), events.SchemaVersion}, false, []string{"knowledge:child3:e2:1", "knowledge:changed:e2:1"}},
		{"event update replace", `UPDATE OR REPLACE events SET event_id='e2',sequence=2,payload=? WHERE event_id='e1'`, []any{incidentLinkEventBody(3, "changed")}, false, []string{"knowledge:child3:e2:2", "knowledge:changed:e2:2"}},
		{"event replace unrelated", `INSERT OR REPLACE INTO events(sequence,event_id,organization_id,event_type,authorization_refs,artifact_refs,payload,created_at,schema_version) VALUES(1,'e2','org-1','AUDIT_NOTE','[]','[]','{}','2026-09-01T00:00:00Z',?)`, []any{events.SchemaVersion}, false, nil},
		{"record body", `UPDATE records SET body=? WHERE record_id='child1'`, []any{incidentLinkRecordBody("changed")}, true, []string{"knowledge:changed:knowledge:child1:1:e1", "knowledge:root2:knowledge:child2:1:e2"}},
		{"record identity", `UPDATE records SET record_id='moved',version=2,admission_event_id='moved-event' WHERE record_id='child1'`, nil, true, []string{"knowledge:root1:knowledge:moved:2:moved-event", "knowledge:root2:knowledge:child2:1:e2"}},
		{"record delete", `DELETE FROM records WHERE record_id='child1'`, nil, true, []string{"knowledge:root2:knowledge:child2:1:e2"}},
		{"record malformed", `UPDATE records SET kind='task',body='{' WHERE record_id='child1'`, nil, true, []string{"knowledge:root2:knowledge:child2:1:e2"}},
		{"record insert replace", `INSERT OR REPLACE INTO records(kind,record_id,version,body,admission_event_id,created_at) VALUES('knowledge','child1',1,?,'e2','2026-09-01T00:00:00Z')`, []any{incidentLinkRecordBody("changed")}, true, []string{"knowledge:changed:knowledge:child1:1:e2"}},
		{"record update replace", `UPDATE OR REPLACE records SET record_id='child2',admission_event_id='e2',body=? WHERE record_id='child1'`, []any{incidentLinkRecordBody("changed")}, true, []string{"knowledge:changed:knowledge:child2:1:e2"}},
		{"record replace unrelated", `INSERT OR REPLACE INTO records(kind,record_id,version,body,admission_event_id,created_at) VALUES('note','replacement',1,'{}','e1','2026-09-01T00:00:00Z')`, nil, true, []string{"knowledge:root2:knowledge:child2:1:e2"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if _, err := store.db.ExecContext(t.Context(), `PRAGMA recursive_triggers=OFF`); err != nil {
				t.Fatal(err)
			}
			incidentLinkSourceFixtures(t, store.db)
			independent := incidentLinkRows(t, store.db, !tc.record)
			if _, err := store.db.ExecContext(t.Context(), tc.statement, tc.args...); err != nil {
				t.Fatal(err)
			}
			checkIncidentLinkRows(t, store.db, tc.record, tc.want)
			checkIncidentLinkRows(t, store.db, !tc.record, independent)
		})
	}
}

func TestIncidentLinkGuardsRejectTampering(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	incidentLinkSourceFixtures(t, store.db)
	beforeEvents, beforeRecords := incidentLinkRows(t, store.db, false), incidentLinkRows(t, store.db, true)
	for _, statement := range []string{
		`DELETE FROM incident_event_links WHERE target_id='root1'`,
		`UPDATE incident_event_links SET target_id='missing' WHERE target_id='root1'`,
		`INSERT INTO incident_event_links VALUES('knowledge','invented',1,'e1')`,
		`INSERT INTO incident_event_links VALUES('knowledge','root1',2,'e2')`,
		`INSERT OR REPLACE INTO incident_event_links VALUES('knowledge','root1',1,'wrong-event')`,
		`DELETE FROM incident_record_links WHERE target_id='root1'`,
		`UPDATE incident_record_links SET target_id='missing' WHERE target_id='root1'`,
		`INSERT INTO incident_record_links VALUES('knowledge','invented','knowledge','child1',1,'e1')`,
		`INSERT OR REPLACE INTO incident_record_links VALUES('knowledge','root1','knowledge','child1',1,'wrong-event')`,
	} {
		if _, err := store.db.ExecContext(t.Context(), statement); err == nil {
			t.Fatalf("tampering accepted: %s", statement)
		}
		checkIncidentLinkRows(t, store.db, false, beforeEvents)
		checkIncidentLinkRows(t, store.db, true, beforeRecords)
	}
	for _, statement := range []string{
		`INSERT OR REPLACE INTO incident_event_links SELECT * FROM incident_event_links`,
		`INSERT OR REPLACE INTO incident_record_links SELECT * FROM incident_record_links`,
	} {
		if _, err := store.db.ExecContext(t.Context(), statement); err != nil {
			t.Fatal(err)
		}
	}
	checkIncidentLinkRows(t, store.db, false, beforeEvents)
	checkIncidentLinkRows(t, store.db, true, beforeRecords)
}

func TestIncidentLinkRollbackAndVacuum(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "links.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	incidentLinkSourceFixtures(t, store.db)
	beforeEvents, beforeRecords := incidentLinkRows(t, store.db, false), incidentLinkRows(t, store.db, true)
	tx, err := store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(t.Context(), `DELETE FROM events; DELETE FROM records`); err != nil {
		t.Fatal(err)
	}
	checkIncidentLinkRows(t, tx, false, nil)
	checkIncidentLinkRows(t, tx, true, nil)
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	checkIncidentLinkRows(t, store.db, false, beforeEvents)
	checkIncidentLinkRows(t, store.db, true, beforeRecords)
	if _, err := store.db.ExecContext(t.Context(), `UPDATE records SET rowid=rowid+100; VACUUM`); err != nil {
		t.Fatal(err)
	}
	checkIncidentLinkRows(t, store.db, false, beforeEvents)
	checkIncidentLinkRows(t, store.db, true, beforeRecords)
}

func TestIncidentLinkSchemaDrift(t *testing.T) {
	for _, mutation := range []string{"missing", "noop", "table", "index", "extra index", "extra source trigger", "changed source trigger", "temporary source trigger", "temporary link table"} {
		t.Run(mutation, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			statement := `DROP TRIGGER incident_event_links_source_insert`
			switch mutation {
			case "noop":
				statement += `; CREATE TRIGGER incident_event_links_source_insert AFTER INSERT ON events BEGIN SELECT 1; END`
			case "table":
				statement = `ALTER TABLE incident_record_links ADD COLUMN ignored TEXT`
			case "index":
				statement = `DROP INDEX incident_event_link_source_idx; CREATE INDEX incident_event_link_source_idx ON incident_event_links(target_id)`
			case "extra index":
				statement = `CREATE UNIQUE INDEX unwanted_link_constraint ON incident_event_links(target_kind)`
			case "extra source trigger":
				statement = `CREATE TRIGGER skip_link_maintenance AFTER INSERT ON events BEGIN SELECT RAISE(IGNORE); END`
			case "changed source trigger":
				statement = `DROP TRIGGER freeze_events_insert_change; CREATE TRIGGER freeze_events_insert_change AFTER INSERT ON events BEGIN SELECT RAISE(IGNORE); END`
			case "temporary source trigger":
				statement = `CREATE TEMP TRIGGER skip_link_maintenance AFTER INSERT ON main.events BEGIN SELECT RAISE(IGNORE); END`
			case "temporary link table":
				statement = `CREATE TEMP TABLE incident_event_links AS SELECT * FROM main.incident_event_links`
			}
			if _, err := store.db.ExecContext(t.Context(), statement); err != nil {
				t.Fatal(err)
			}
			// Rewriting the mutable general fingerprint cannot bless changed
			// maintenance definitions for an incident selection transaction.
			fingerprint, err := storageSchemaFingerprint(t.Context(), store.db)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.ExecContext(t.Context(), `UPDATE agentos_storage SET schema_fingerprint=?`, fingerprint); err != nil {
				t.Fatal(err)
			}
			if err := validateIncidentLinkSchema(t.Context(), store.db); err == nil {
				t.Fatal("changed incident index definitions accepted")
			}
			if _, err := ValidateStorageContract(t.Context(), store.db); err == nil {
				t.Fatal("changed index definitions accepted by storage contract")
			}
		})
	}
}

func removeIncidentLinkSchemaForTest(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, object := range incidentLinkObjects {
		if object.kind == "trigger" {
			if _, err := db.ExecContext(t.Context(), "DROP TRIGGER IF EXISTS "+object.name); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := db.ExecContext(t.Context(), `DROP TABLE IF EXISTS incident_event_links; DROP TABLE IF EXISTS incident_record_links`); err != nil {
		t.Fatal(err)
	}
}

func TestIncidentLinkMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "links-v12.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	removeIncidentLinkSchemaForTest(t, store.db)
	if _, err := store.db.ExecContext(t.Context(), `DROP INDEX records_replaced_work_idx; DROP INDEX events_incident_execution_idx; DROP INDEX events_message_idx; DROP INDEX events_source_message_idx`); err != nil {
		t.Fatal(err)
	}
	incidentLinkSourceFixtures(t, store.db)
	// A moved event and retained record must seed their own independent links.
	if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id='e1'; DELETE FROM records WHERE record_id='child2'`, incidentLinkEventBody(1, "changed")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `INSERT INTO records(kind,record_id,version,body,admission_event_id,created_at) VALUES('knowledge','child1',2,?,'missing-event','2026-09-01T00:00:00Z');
INSERT INTO records(kind,record_id,version,body,created_at) VALUES('task','malformed',1,'{','2026-09-01T00:00:00Z')`, incidentLinkRecordBody("later")); err != nil {
		t.Fatal(err)
	}
	fingerprint, err := storageSchemaFingerprint(t.Context(), store.db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `UPDATE agentos_storage SET storage_version=12,schema_fingerprint=?; PRAGMA user_version=12`, fingerprint); err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateStorageContract(t.Context(), store.db); err != nil {
		t.Fatal(err)
	}
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error { return rebuildEventIntegrity(t.Context(), tx) }); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateIncidentLinkSchema(t.Context(), store.db); err != nil {
		t.Fatal(err)
	}
	checkIncidentLinkRows(t, store.db, false, []string{"knowledge:child1:e1:1", "knowledge:changed:e1:1", "knowledge:child2:e2:2", "knowledge:root2:e2:2"})
	checkIncidentLinkRows(t, store.db, true, []string{"knowledge:root1:knowledge:child1:1:e1", "knowledge:later:knowledge:child1:2:missing-event"})
	var body string
	if err := store.db.QueryRowContext(t.Context(), `SELECT body FROM records WHERE record_id='malformed'`).Scan(&body); err != nil || body != "{" {
		t.Fatalf("migration altered retained body: %q, %v", body, err)
	}
	if _, err := store.db.ExecContext(t.Context(), `DELETE FROM incident_record_links WHERE target_id='root1'`); err == nil || !strings.Contains(err.Error(), "required incident link") {
		t.Fatalf("migrated index is unguarded: %v", err)
	}
}

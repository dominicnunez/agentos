package ledger

import (
	"bytes"
	"database/sql"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

type freezeChange struct {
	generation []byte
	rewrite    []byte
}

func readFreezeChange(t *testing.T, db *sql.DB, organization string) freezeChange {
	t.Helper()
	var change freezeChange
	if err := db.QueryRowContext(t.Context(), `SELECT generation,rewrite_generation FROM freeze_changes WHERE organization_id=?`, organization).Scan(&change.generation, &change.rewrite); err != nil {
		t.Fatal(err)
	}
	if len(change.generation) != 32 || len(change.rewrite) != 32 {
		t.Fatalf("freeze change token lengths=%d/%d", len(change.generation), len(change.rewrite))
	}
	return change
}

func sameFreezeChange(left, right freezeChange) bool {
	return bytes.Equal(left.generation, right.generation) && bytes.Equal(left.rewrite, right.rewrite)
}

func removeFreezeChangesForLegacyFixture(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(t.Context(), `DROP TRIGGER freeze_events_delete_change;
DROP TRIGGER freeze_events_insert_change;
DROP TRIGGER freeze_events_insert_conflict;
DROP TRIGGER freeze_events_update_change;
DROP TRIGGER freeze_records_delete_change;
DROP TRIGGER freeze_records_insert_change;
DROP TRIGGER freeze_records_update_change;
DROP TABLE freeze_changes;`); err != nil {
		t.Fatal(err)
	}
}

func TestFreezeChangeMigrationPreservesHistory(t *testing.T) {
	path := t.TempDir() + "/freeze-v10.db"
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	appendHistoricalInferenceFreeze(t, store, "org-1", 1, true)
	appendInferenceFreeze(t, store, "org-1", 2, false)
	beforeEvents, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	beforeRecords, err := store.Records(t.Context(), "organization_freeze", "org-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	removeFreezeChangesForLegacyFixture(t, db)
	fingerprint, err := storageSchemaFingerprint(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `UPDATE agentos_storage SET storage_version=10,schema_fingerprint=?; PRAGMA user_version=10`, fingerprint); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	migrated, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = migrated.Close() })
	afterEvents, err := migrated.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	afterRecords, err := migrated.Records(t.Context(), "organization_freeze", "org-1")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(beforeEvents, afterEvents) || !reflect.DeepEqual(beforeRecords, afterRecords) {
		t.Fatal("freeze freshness migration changed durable history")
	}
	_ = readFreezeChange(t, migrated.db, "org-1")
	var contract StorageContract
	contract, err = ValidateStorageContract(t.Context(), migrated.db)
	if err != nil || contract.StorageVersion != CurrentStorageVersion {
		t.Fatalf("migrated contract=%+v err=%v", contract, err)
	}
}

func TestFreezeChangeTriggersClassifyAppendAndRewrite(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	appendHistoricalInferenceFreeze(t, store, "org-1", 1, true)
	initial := readFreezeChange(t, store.db, "org-1")
	appendInferenceFreeze(t, store, "org-1", 2, false)
	appended := readFreezeChange(t, store.db, "org-1")
	if bytes.Equal(initial.generation, appended.generation) || !bytes.Equal(initial.rewrite, appended.rewrite) {
		t.Fatal("tail append did not change only the freeze generation")
	}
	if _, err := store.db.ExecContext(t.Context(), `UPDATE records SET body=body WHERE kind='organization_freeze' AND record_id='org-1' AND version=1`); err != nil {
		t.Fatal(err)
	}
	rewritten := readFreezeChange(t, store.db, "org-1")
	if bytes.Equal(appended.generation, rewritten.generation) || bytes.Equal(appended.rewrite, rewritten.rewrite) {
		t.Fatal("historical record update did not change both generations")
	}
	beforeAudit := rewritten
	if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-2", EventType: "AUDIT_NOTE", SourceActorID: "runtime", Payload: map[string]string{"state": "unrelated"}}); err != nil {
		t.Fatal(err)
	}
	if afterAudit := readFreezeChange(t, store.db, "org-1"); !sameFreezeChange(beforeAudit, afterAudit) {
		t.Fatal("unrelated event invalidated freeze history")
	}
}

func TestFreezeChangeFastPathInitializesFirstFreeze(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", Payload: map[string]string{"state": "ordinary"}}); err != nil {
		t.Fatal(err)
	}
	var changes int
	if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM freeze_changes`).Scan(&changes); err != nil {
		t.Fatal(err)
	}
	if changes != 0 {
		t.Fatalf("ordinary history created %d freeze change rows", changes)
	}
	appendHistoricalInferenceFreeze(t, store, "org-1", 1, true)
	_ = readFreezeChange(t, store.db, "org-1")
}

func TestFreezeEventChangesDirtyEveryAffectedTenant(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	first := appendHistoricalInferenceFreeze(t, store, "org-1", 1, true)
	appendHistoricalInferenceFreeze(t, store, "org-2", 1, true)
	beforeOne := readFreezeChange(t, store.db, "org-1")
	beforeTwo := readFreezeChange(t, store.db, "org-2")
	if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET organization_id='org-2' WHERE event_id=?`, first.EventRef); err != nil {
		t.Fatal(err)
	}
	afterOne := readFreezeChange(t, store.db, "org-1")
	afterTwo := readFreezeChange(t, store.db, "org-2")
	if sameFreezeChange(beforeOne, afterOne) || sameFreezeChange(beforeTwo, afterTwo) ||
		bytes.Equal(beforeOne.rewrite, afterOne.rewrite) || bytes.Equal(beforeTwo.rewrite, afterTwo.rewrite) {
		t.Fatal("event tenant/type move did not rewrite every affected tenant")
	}
	if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET event_type='AUDIT_NOTE' WHERE event_id=?`, first.EventRef); err != nil {
		t.Fatal(err)
	}
	beforeReferenced := readFreezeChange(t, store.db, "org-1")
	if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET payload=payload WHERE event_id=?`, first.EventRef); err != nil {
		t.Fatal(err)
	}
	afterReferenced := readFreezeChange(t, store.db, "org-1")
	if sameFreezeChange(beforeReferenced, afterReferenced) || bytes.Equal(beforeReferenced.rewrite, afterReferenced.rewrite) {
		t.Fatal("referenced non-freeze event update did not dirty its freeze record owner")
	}
}

func TestFreezeChangeTriggersCatchReplaceAndOldSequenceInsert(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	appendHistoricalInferenceFreeze(t, store, "org-1", 1, true)
	beforeReplace := readFreezeChange(t, store.db, "org-1")
	if _, err := store.db.ExecContext(t.Context(), `INSERT OR REPLACE INTO records(kind,record_id,version,body,admission_event_id,admission_fingerprint,created_at)
SELECT kind,record_id,version,body,admission_event_id,admission_fingerprint,created_at FROM records
WHERE kind='organization_freeze' AND record_id='org-1' AND version=1`); err != nil {
		t.Fatal(err)
	}
	afterReplace := readFreezeChange(t, store.db, "org-1")
	if sameFreezeChange(beforeReplace, afterReplace) || bytes.Equal(beforeReplace.rewrite, afterReplace.rewrite) {
		t.Fatal("record replacement was misclassified as an append")
	}
	var eventID string
	if err := store.db.QueryRowContext(t.Context(), `SELECT admission_event_id FROM records
WHERE kind='organization_freeze' AND record_id='org-1' AND version=1`).Scan(&eventID); err != nil {
		t.Fatal(err)
	}
	beforeEventReplace := readFreezeChange(t, store.db, "org-1")
	if _, err := store.db.ExecContext(t.Context(), `INSERT OR REPLACE INTO events(sequence,event_id,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version)
SELECT sequence,event_id,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version
FROM events WHERE event_id=?`, eventID); err != nil {
		t.Fatal(err)
	}
	afterEventReplace := readFreezeChange(t, store.db, "org-1")
	if sameFreezeChange(beforeEventReplace, afterEventReplace) || bytes.Equal(beforeEventReplace.rewrite, afterEventReplace.rewrite) {
		t.Fatal("event replacement was misclassified as an append")
	}
	if _, err := store.db.ExecContext(t.Context(), `INSERT INTO events(sequence,event_id,organization_id,event_type,source_actor_id,authorization_refs,artifact_refs,payload,created_at,schema_version)
VALUES(10,'later-audit','org-2','AUDIT_NOTE','runtime','[]','[]','{}','2026-09-12T00:00:00Z',?)`, events.SchemaVersion); err != nil {
		t.Fatal(err)
	}
	beforeOld := readFreezeChange(t, store.db, "org-1")
	if _, err := store.db.ExecContext(t.Context(), `INSERT INTO events(sequence,event_id,organization_id,event_type,source_actor_id,authorization_refs,artifact_refs,payload,created_at,schema_version)
VALUES(9,'old-freeze','org-1','FREEZE_SET','runtime','[]','[]','{}','2026-09-12T00:00:00Z',?)`, events.SchemaVersion); err != nil {
		t.Fatal(err)
	}
	afterOld := readFreezeChange(t, store.db, "org-1")
	if sameFreezeChange(beforeOld, afterOld) || bytes.Equal(beforeOld.rewrite, afterOld.rewrite) {
		t.Fatal("old-sequence freeze event was misclassified as an append")
	}
}

func TestFreezeChangeTriggersDirtyRecordKeysAndFutureEvent(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	appendHistoricalInferenceFreeze(t, store, "org-1", 1, true)
	beforeOld := readFreezeChange(t, store.db, "org-1")
	if _, err := store.db.ExecContext(t.Context(), `UPDATE records SET record_id='org-moved'
WHERE kind='organization_freeze' AND record_id='org-1' AND version=1`); err != nil {
		t.Fatal(err)
	}
	afterOld := readFreezeChange(t, store.db, "org-1")
	afterNew := readFreezeChange(t, store.db, "org-moved")
	if sameFreezeChange(beforeOld, afterOld) || bytes.Equal(beforeOld.rewrite, afterOld.rewrite) ||
		len(afterNew.generation) != 32 || len(afterNew.rewrite) != 32 {
		t.Fatal("record tenant move did not dirty both record keys")
	}
	if _, err := store.db.ExecContext(t.Context(), `UPDATE records SET admission_event_id='future-event'
WHERE kind='organization_freeze' AND record_id='org-moved' AND version=1`); err != nil {
		t.Fatal(err)
	}
	beforeFuture := readFreezeChange(t, store.db, "org-moved")
	if _, err := store.db.ExecContext(t.Context(), `INSERT INTO events(event_id,organization_id,event_type,source_actor_id,authorization_refs,artifact_refs,payload,created_at,schema_version)
VALUES('future-event','org-elsewhere','AUDIT_NOTE','runtime','[]','[]','{}','2026-09-12T00:00:00Z',?)`, events.SchemaVersion); err != nil {
		t.Fatal(err)
	}
	afterFuture := readFreezeChange(t, store.db, "org-moved")
	if sameFreezeChange(beforeFuture, afterFuture) || bytes.Equal(beforeFuture.rewrite, afterFuture.rewrite) {
		t.Fatal("new event referenced by an existing freeze record did not dirty its owner")
	}
}

func TestFreezeChangeDeletesDirtyOnlyAffectedTenant(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	first := appendHistoricalInferenceFreeze(t, store, "org-1", 1, true)
	appendHistoricalInferenceFreeze(t, store, "org-2", 1, true)
	beforeRecord := readFreezeChange(t, store.db, "org-1")
	other := readFreezeChange(t, store.db, "org-2")
	if _, err := store.db.ExecContext(t.Context(), `DELETE FROM records
WHERE kind='organization_freeze' AND record_id='org-1' AND version=1`); err != nil {
		t.Fatal(err)
	}
	afterRecord := readFreezeChange(t, store.db, "org-1")
	if bytes.Equal(beforeRecord.generation, afterRecord.generation) || bytes.Equal(beforeRecord.rewrite, afterRecord.rewrite) {
		t.Fatal("freeze record deletion did not change both tokens")
	}
	if afterOther := readFreezeChange(t, store.db, "org-2"); !sameFreezeChange(other, afterOther) {
		t.Fatal("freeze record deletion invalidated another tenant")
	}
	if _, err := store.db.ExecContext(t.Context(), `DELETE FROM events WHERE event_id=?`, first.EventRef); err != nil {
		t.Fatal(err)
	}
	afterEvent := readFreezeChange(t, store.db, "org-1")
	if bytes.Equal(afterRecord.generation, afterEvent.generation) || bytes.Equal(afterRecord.rewrite, afterEvent.rewrite) {
		t.Fatal("freeze event deletion did not change both tokens")
	}
	if afterOther := readFreezeChange(t, store.db, "org-2"); !sameFreezeChange(other, afterOther) {
		t.Fatal("freeze event deletion invalidated another tenant")
	}
}

func TestFreezeChangeReplaceDirtiesEventIDAndSequenceVictims(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	first := appendHistoricalInferenceFreeze(t, store, "org-1", 1, true)
	second := appendHistoricalInferenceFreeze(t, store, "org-2", 1, true)
	appendHistoricalInferenceFreeze(t, store, "org-other", 1, true)
	var secondSequence int64
	if err := store.db.QueryRowContext(t.Context(), `SELECT sequence FROM events WHERE event_id=?`, second.EventRef).Scan(&secondSequence); err != nil {
		t.Fatal(err)
	}
	beforeOne := readFreezeChange(t, store.db, "org-1")
	beforeTwo := readFreezeChange(t, store.db, "org-2")
	beforeOther := readFreezeChange(t, store.db, "org-other")
	if _, err := store.db.ExecContext(t.Context(), `INSERT OR REPLACE INTO events(sequence,event_id,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version)
VALUES(?,?,'org-replacement','AUDIT_NOTE','runtime','','','','','[]','[]','{}','','2026-09-12T00:00:00Z',?)`, secondSequence, first.EventRef, events.SchemaVersion); err != nil {
		t.Fatal(err)
	}
	afterOne := readFreezeChange(t, store.db, "org-1")
	afterTwo := readFreezeChange(t, store.db, "org-2")
	if bytes.Equal(beforeOne.generation, afterOne.generation) || bytes.Equal(beforeOne.rewrite, afterOne.rewrite) ||
		bytes.Equal(beforeTwo.generation, afterTwo.generation) || bytes.Equal(beforeTwo.rewrite, afterTwo.rewrite) {
		t.Fatal("two-victim event replacement did not change both tenants' tokens")
	}
	if afterOther := readFreezeChange(t, store.db, "org-other"); !sameFreezeChange(beforeOther, afterOther) {
		t.Fatal("two-victim event replacement invalidated an unrelated tenant")
	}
}

func TestFreezeChangesRejectInvalidTokenShape(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.db.ExecContext(t.Context(), `INSERT INTO freeze_changes(organization_id,generation,rewrite_generation) VALUES('org-1',randomblob(31),randomblob(32))`); err == nil {
		t.Fatal("short freeze generation accepted")
	}
	if _, err := store.db.ExecContext(t.Context(), `INSERT INTO freeze_changes(organization_id,generation,rewrite_generation) VALUES('org-1',?,randomblob(32))`, "not-a-blob"); err == nil {
		t.Fatal("text freeze generation accepted")
	}
}

func TestFreezeChangeTriggerLookupsUseIndexes(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	tests := []struct {
		name      string
		statement string
		arguments []any
		want      []string
	}{
		{
			name: "record admission conflict",
			statement: `SELECT r.record_id FROM records r
WHERE r.kind='organization_freeze' AND r.record_id<>'' AND r.admission_event_id<>'' AND r.admission_event_id=?`,
			arguments: []any{"event-1"},
			want:      []string{"records_admission_event_idx (admission_event_id=?)"},
		},
		{
			name:      "record key conflict",
			statement: `SELECT 1 FROM records r WHERE r.kind=? AND r.record_id=? AND r.version=?`,
			arguments: []any{"organization_freeze", "org-1", 1},
			want:      []string{"sqlite_autoindex_records_1 (kind=? AND record_id=? AND version=?)"},
		},
		{
			name:      "record tail",
			statement: `SELECT COALESCE(MAX(version),0)+1 FROM records WHERE kind=? AND record_id=?`,
			arguments: []any{"organization_freeze", "org-1"},
			want:      []string{"sqlite_autoindex_records_1 (kind=? AND record_id=?)"},
		},
		{
			name:      "event id conflict",
			statement: `SELECT e.organization_id FROM events e WHERE e.event_id=?`,
			arguments: []any{"event-1"},
			want:      []string{"sqlite_autoindex_events_1 (event_id=?)"},
		},
		{
			name: "event sequence record owner",
			statement: `SELECT r.record_id FROM events e
JOIN records r ON r.admission_event_id=e.event_id AND r.admission_event_id<>''
WHERE e.sequence=? AND e.event_id<>? AND r.kind='organization_freeze' AND r.record_id<>''`,
			arguments: []any{1, "event-1"},
			want:      []string{"INTEGER PRIMARY KEY (rowid=?)", "records_admission_event_idx (admission_event_id=?)"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows, err := store.db.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+test.statement, test.arguments...)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = rows.Close() }()
			var details []string
			for rows.Next() {
				var id, parent, unused int
				var detail string
				if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
					t.Fatal(err)
				}
				details = append(details, detail)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			plan := strings.Join(details, "\n")
			if strings.Contains(plan, "SCAN r") || strings.Contains(plan, "SCAN e") {
				t.Fatalf("freeze change lookup scans history:\n%s", plan)
			}
			for _, want := range test.want {
				if !strings.Contains(plan, want) {
					t.Fatalf("freeze change lookup lacks %q:\n%s", want, plan)
				}
			}
		})
	}
}

func TestFreezeChangeTriggerDriftIsRejected(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.db.ExecContext(t.Context(), `DROP TRIGGER freeze_events_delete_change;
CREATE TRIGGER freeze_events_delete_change BEFORE DELETE ON events BEGIN SELECT 1; END;`); err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateStorageContract(t.Context(), store.db); err == nil || !strings.Contains(err.Error(), "schema fingerprint does not match") {
		t.Fatalf("changed freeze trigger was not rejected: %v", err)
	}
}

func BenchmarkFreezeOrdinaryWrites(b *testing.B) {
	store, err := Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })
	tx, err := store.db.BeginTx(b.Context(), nil)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	created := "2026-09-12T00:00:00Z"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := "ordinary-" + strconv.Itoa(i)
		if _, err := tx.ExecContext(b.Context(), `INSERT INTO events(event_id,organization_id,event_type,source_actor_id,authorization_refs,artifact_refs,payload,created_at,schema_version)
VALUES(?,'org-1','AUDIT_NOTE','runtime','[]','[]','{}',?,?)`, id, created, events.SchemaVersion); err != nil {
			b.Fatal(err)
		}
		if _, err := tx.ExecContext(b.Context(), `UPDATE events SET payload=payload WHERE event_id=?`, id); err != nil {
			b.Fatal(err)
		}
		if _, err := tx.ExecContext(b.Context(), `INSERT INTO records(kind,record_id,version,body,admission_event_id,admission_fingerprint,created_at)
VALUES('benchmark',?,1,'{}',?,'benchmark',?)`, id, id, created); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

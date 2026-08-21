package ledger

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/dominicnunez/agentos/internal/events"
)

const (
	// StorageApplicationID is the SQLite application_id for an Agent OS ledger
	// (ASCII "AGOS"). It prevents an unrelated SQLite database from being
	// initialized or migrated as Agent OS state.
	StorageApplicationID = 0x41474f53

	// OldestSupportedStorageVersion is the oldest durable layout contract that
	// the current runtime and recovery tooling can validate and migrate. It does
	// not identify or publish an Agent OS release.
	OldestSupportedStorageVersion = 1
	// CurrentStorageVersion is the only layout accepted after runtime startup.
	CurrentStorageVersion = 3
	// LegacyEventSchemaVersion identifies the pre-Intent-mode Event Contract.
	// Migration may advance only ledgers without review evidence whose
	// fingerprint semantics changed.
	LegacyEventSchemaVersion = 3

	storageSchemaV1Fingerprint = "ce7fe300685bcbc66821ca3692d962eda27cdd1ca9e1642972cccc8e2b4736cb"
)

// StorageContract identifies the durable layout and Event Contract versions
// carried by a verified Agent OS database.
type StorageContract struct {
	StorageVersion     int
	EventSchemaVersion int
}

var storageColumnsV1 = map[string][]string{
	"consumed_approvals":     {"approval_id", "effect_fingerprint", "consumed_at"},
	"events":                 {"sequence", "event_id", "organization_id", "event_type", "source_actor_id", "source_execution_id", "recipient_scope", "recipient_id", "task_id", "authorization_refs", "artifact_refs", "payload", "correlation_id", "created_at", "schema_version"},
	"external_tasks":         {"organization_id", "task_id", "correlation_id"},
	"external_work":          {"organization_id", "request_id", "correlation_id", "intent_id"},
	"inbox":                  {"recipient_scope", "recipient_id", "event_id", "organization_id", "task_id", "available_at", "observed_at", "observation_event_id"},
	"inference_policies":     {"organization_id", "policy_fingerprint", "body", "activation_event_id", "activated_at", "active"},
	"inference_reservations": {"reservation_id", "request_id", "organization_id", "purpose", "intent_id", "task_id", "execution_id", "correlation_id", "prompt_sha256", "provider", "model", "execution_profile_version", "policy_fingerprint", "state", "reserved_input_tokens", "reserved_output_tokens", "reserved_cost_nano_usd", "charged_input_tokens", "charged_output_tokens", "charged_cost_nano_usd", "window_started_at", "window_expires_at", "created_at", "updated_at"},
	"records":                {"kind", "record_id", "version", "body", "admission_event_id", "admission_fingerprint", "created_at"},
}

var storageIndexes = map[string]string{
	"events_correlation_idx":            "events",
	"events_intake_actor_idx":           "events",
	"external_tasks_correlation_idx":    "external_tasks",
	"inbox_available_idx":               "inbox",
	"inference_policies_active_idx":     "inference_policies",
	"inference_reservations_window_idx": "inference_reservations",
	"records_admission_event_idx":       "records",
	"records_kind_idx":                  "records",
}

const storageSchemaV1SQL = `CREATE TABLE events (
sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE, organization_id TEXT NOT NULL,
event_type TEXT NOT NULL, source_actor_id TEXT NOT NULL DEFAULT '', source_execution_id TEXT NOT NULL DEFAULT '', recipient_scope TEXT NOT NULL DEFAULT '', recipient_id TEXT NOT NULL DEFAULT '', task_id TEXT NOT NULL DEFAULT '', authorization_refs BLOB NOT NULL, artifact_refs BLOB NOT NULL, payload BLOB NOT NULL,
correlation_id TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, schema_version INTEGER NOT NULL);
CREATE INDEX events_correlation_idx ON events(correlation_id, sequence);
CREATE INDEX events_intake_actor_idx ON events(organization_id,event_type,source_actor_id,sequence);
CREATE TABLE records (
kind TEXT NOT NULL, record_id TEXT NOT NULL, version INTEGER NOT NULL, body BLOB NOT NULL,
admission_event_id TEXT NOT NULL DEFAULT '', admission_fingerprint TEXT NOT NULL DEFAULT '',
created_at TEXT NOT NULL, PRIMARY KEY(kind, record_id, version));
CREATE INDEX records_kind_idx ON records(kind, created_at);
CREATE UNIQUE INDEX records_admission_event_idx ON records(admission_event_id) WHERE admission_event_id<>'';
CREATE TABLE external_work (
organization_id TEXT NOT NULL, request_id TEXT NOT NULL, correlation_id TEXT NOT NULL, intent_id TEXT NOT NULL,
PRIMARY KEY(organization_id, request_id), UNIQUE(organization_id, correlation_id), UNIQUE(intent_id));
CREATE TABLE external_tasks (
organization_id TEXT NOT NULL, task_id TEXT NOT NULL, correlation_id TEXT NOT NULL,
PRIMARY KEY(organization_id, task_id));
CREATE INDEX external_tasks_correlation_idx ON external_tasks(organization_id, correlation_id);
CREATE TABLE inbox (
recipient_scope TEXT NOT NULL, recipient_id TEXT NOT NULL, event_id TEXT NOT NULL UNIQUE,
organization_id TEXT NOT NULL, task_id TEXT NOT NULL DEFAULT '', available_at TEXT NOT NULL,
observed_at TEXT NOT NULL DEFAULT '', observation_event_id TEXT NOT NULL DEFAULT '',
PRIMARY KEY(recipient_scope, recipient_id, event_id));
CREATE INDEX inbox_available_idx ON inbox(recipient_scope, recipient_id, observed_at, available_at);
CREATE TABLE consumed_approvals (
approval_id TEXT PRIMARY KEY, effect_fingerprint TEXT NOT NULL, consumed_at TEXT NOT NULL);
CREATE TABLE inference_policies (
organization_id TEXT NOT NULL, policy_fingerprint TEXT NOT NULL, body BLOB NOT NULL,
activation_event_id TEXT NOT NULL UNIQUE, activated_at TEXT NOT NULL, active INTEGER NOT NULL,
PRIMARY KEY(organization_id,policy_fingerprint));
CREATE UNIQUE INDEX inference_policies_active_idx ON inference_policies(organization_id) WHERE active=1;
CREATE TABLE inference_reservations (
reservation_id TEXT PRIMARY KEY, request_id TEXT NOT NULL, organization_id TEXT NOT NULL,
purpose TEXT NOT NULL, intent_id TEXT NOT NULL DEFAULT '', task_id TEXT NOT NULL DEFAULT '',
execution_id TEXT NOT NULL, correlation_id TEXT NOT NULL, prompt_sha256 TEXT NOT NULL,
provider TEXT NOT NULL, model TEXT NOT NULL, execution_profile_version TEXT NOT NULL,
policy_fingerprint TEXT NOT NULL, state TEXT NOT NULL,
reserved_input_tokens INTEGER NOT NULL, reserved_output_tokens INTEGER NOT NULL,
reserved_cost_nano_usd INTEGER NOT NULL, charged_input_tokens INTEGER NOT NULL,
charged_output_tokens INTEGER NOT NULL, charged_cost_nano_usd INTEGER NOT NULL,
window_started_at TEXT NOT NULL, window_expires_at TEXT NOT NULL,
created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
UNIQUE(organization_id,request_id));
CREATE INDEX inference_reservations_window_idx
ON inference_reservations(organization_id,provider,model,window_started_at,state);`

const storageSchemaV2SQL = `CREATE TABLE agentos_storage (
singleton INTEGER PRIMARY KEY CHECK(singleton=1),
storage_version INTEGER NOT NULL,
event_schema_version INTEGER NOT NULL,
application_id INTEGER NOT NULL,
schema_fingerprint TEXT NOT NULL);`

func migrateStorage(ctx context.Context, db *sql.DB) error {
	if ctx == nil || db == nil {
		return fmt.Errorf("storage migration requires context and database")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin storage migration: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	applicationID, version, err := sqliteStorageHeader(ctx, tx)
	if err != nil {
		return err
	}
	objects, err := userStorageObjects(ctx, tx)
	if err != nil {
		return err
	}
	if version == 0 {
		if len(objects) != 0 {
			return fmt.Errorf("unversioned nonempty database is unsupported; preserve it and use an explicitly reviewed migration")
		}
		if applicationID == 0 {
			if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA application_id=%d", StorageApplicationID)); err != nil {
				return fmt.Errorf("set Agent OS SQLite application id: %w", err)
			}
		} else if applicationID != StorageApplicationID {
			return fmt.Errorf("database application id %d is not Agent OS", applicationID)
		}
	} else if applicationID != StorageApplicationID {
		return fmt.Errorf("database application id %d is not Agent OS", applicationID)
	}

	if version != 0 {
		if version < OldestSupportedStorageVersion {
			return fmt.Errorf("storage schema version %d is older than supported version %d", version, OldestSupportedStorageVersion)
		}
		if version > CurrentStorageVersion {
			return fmt.Errorf("storage schema version %d is newer than supported version %d", version, CurrentStorageVersion)
		}
		if _, err := validateStorageLayout(ctx, tx, version); err != nil {
			return fmt.Errorf("validate storage schema version %d before migration: %w", version, err)
		}
	}

	for version < CurrentStorageVersion {
		next := version + 1
		if err := applyStorageMigration(ctx, tx, version, next); err != nil {
			return fmt.Errorf("migrate storage schema %d to %d: %w", version, next, err)
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version=%d", next)); err != nil {
			return fmt.Errorf("record storage schema version %d: %w", next, err)
		}
		version = next
		if _, err := validateStorageLayout(ctx, tx, version); err != nil {
			return fmt.Errorf("validate migrated storage schema version %d: %w", version, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit storage migration: %w", err)
	}
	return nil
}

func applyStorageMigration(ctx context.Context, tx *sql.Tx, from, to int) error {
	switch {
	case from == 0 && to == 1:
		_, err := tx.ExecContext(ctx, storageSchemaV1SQL)
		return err
	case from == 1 && to == 2:
		if _, err := tx.ExecContext(ctx, storageSchemaV2SQL); err != nil {
			return err
		}
		fingerprint, err := storageSchemaFingerprint(ctx, tx)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO agentos_storage(singleton,storage_version,event_schema_version,application_id,schema_fingerprint) VALUES(1,?,?,?,?)`, to, LegacyEventSchemaVersion, StorageApplicationID, fingerprint)
		return err
	case from == 2 && to == 3:
		var incompatible int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_type IN ('INTENT_DRAFTED','INTENT_CONFIRMED')`).Scan(&incompatible); err != nil {
			return fmt.Errorf("inspect pre-mode Intent review evidence: %w", err)
		}
		if incompatible != 0 {
			return fmt.Errorf("storage contains pre-mode Intent review evidence that cannot be migrated without changing authoritative fingerprints")
		}
		if err := resealLegacyProjectionAdmissions(ctx, tx); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE events SET schema_version=? WHERE schema_version=?`, events.SchemaVersion, LegacyEventSchemaVersion); err != nil {
			return fmt.Errorf("advance durable Event Contract version: %w", err)
		}
		result, err := tx.ExecContext(ctx, `UPDATE agentos_storage SET storage_version=?,event_schema_version=? WHERE singleton=1 AND storage_version=? AND event_schema_version=?`, to, events.SchemaVersion, from, LegacyEventSchemaVersion)
		if err != nil {
			return fmt.Errorf("advance storage contract metadata: %w", err)
		}
		rows, err := result.RowsAffected()
		if err != nil || rows != 1 {
			return fmt.Errorf("storage contract metadata did not match the reviewed migration boundary")
		}
		return nil
	default:
		return fmt.Errorf("no reviewed storage migration exists")
	}
}

type projectionReseal struct {
	eventID        string
	oldPayload     []byte
	newPayload     []byte
	oldFingerprint string
	newFingerprint string
}

func resealLegacyProjectionAdmissions(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,CAST(payload AS BLOB),correlation_id,created_at,schema_version FROM events ORDER BY sequence`)
	if err != nil {
		return fmt.Errorf("read legacy projection admissions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var reseals []projectionReseal
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return fmt.Errorf("decode legacy projection admission: %w", err)
		}
		payload, present, err := events.ResealProjectionEventForMigration(event, LegacyEventSchemaVersion, events.SchemaVersion)
		if err != nil {
			return fmt.Errorf("reseal legacy event %s: %w", event.EventID, err)
		}
		if !present {
			continue
		}
		body, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("encode resealed legacy event %s: %w", event.EventID, err)
		}
		var original events.ProjectionEventPayload
		if decodeExactJSONBytes(event.Payload, &original) != nil {
			return fmt.Errorf("decode original legacy event %s", event.EventID)
		}
		reseals = append(reseals, projectionReseal{
			eventID: event.EventID, oldPayload: event.Payload, newPayload: body,
			oldFingerprint: original.Admission.Fingerprint, newFingerprint: payload.Admission.Fingerprint,
		})
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close legacy projection admissions: %w", err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("scan legacy projection admissions: %w", err)
	}
	for _, reseal := range reseals {
		result, err := tx.ExecContext(ctx, `UPDATE events SET payload=?,schema_version=? WHERE event_id=? AND payload=? AND schema_version=?`, reseal.newPayload, events.SchemaVersion, reseal.eventID, reseal.oldPayload, LegacyEventSchemaVersion)
		if err != nil {
			return fmt.Errorf("persist resealed legacy event %s: %w", reseal.eventID, err)
		}
		changed, err := result.RowsAffected()
		if err != nil || changed != 1 {
			return fmt.Errorf("legacy event %s changed across its migration boundary", reseal.eventID)
		}
		result, err = tx.ExecContext(ctx, `UPDATE records SET admission_fingerprint=? WHERE admission_event_id=? AND admission_fingerprint=?`, reseal.newFingerprint, reseal.eventID, reseal.oldFingerprint)
		if err != nil {
			return fmt.Errorf("persist resealed legacy record %s: %w", reseal.eventID, err)
		}
		changed, err = result.RowsAffected()
		if err != nil || changed != 1 {
			return fmt.Errorf("legacy record %s does not match its sealed admission", reseal.eventID)
		}
	}
	return nil
}

// ValidateStorageContract verifies a supported offline database without
// modifying it. Runtime startup subsequently migrates supported older layouts
// and requires CurrentStorageVersion before serving work.
func ValidateStorageContract(ctx context.Context, db *sql.DB) (StorageContract, error) {
	if ctx == nil || db == nil {
		return StorageContract{}, fmt.Errorf("storage validation requires context and database")
	}
	applicationID, version, err := sqliteStorageHeader(ctx, db)
	if err != nil {
		return StorageContract{}, err
	}
	if applicationID != StorageApplicationID {
		return StorageContract{}, fmt.Errorf("database application id %d is not Agent OS", applicationID)
	}
	if version < OldestSupportedStorageVersion || version > CurrentStorageVersion {
		return StorageContract{}, fmt.Errorf("storage schema version %d is unsupported; supported range is %d through %d", version, OldestSupportedStorageVersion, CurrentStorageVersion)
	}
	return validateStorageLayout(ctx, db, version)
}

type storageQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func sqliteStorageHeader(ctx context.Context, query storageQueryer) (applicationID, version int, err error) {
	if err := query.QueryRowContext(ctx, `PRAGMA application_id`).Scan(&applicationID); err != nil {
		return 0, 0, fmt.Errorf("read SQLite application id: %w", err)
	}
	if err := query.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return 0, 0, fmt.Errorf("read SQLite storage version: %w", err)
	}
	return applicationID, version, nil
}

func validateStorageLayout(ctx context.Context, query storageQueryer, version int) (StorageContract, error) {
	expectedEventVersion := events.SchemaVersion
	if version < CurrentStorageVersion {
		expectedEventVersion = LegacyEventSchemaVersion
	}
	expected := make(map[string][]string, len(storageColumnsV1)+1)
	for table, columns := range storageColumnsV1 {
		expected[table] = columns
	}
	if version >= 2 {
		expected["agentos_storage"] = []string{"singleton", "storage_version", "event_schema_version", "application_id", "schema_fingerprint"}
	}
	tables, err := userStorageTables(ctx, query)
	if err != nil {
		return StorageContract{}, err
	}
	wantTables := make([]string, 0, len(expected))
	for table := range expected {
		wantTables = append(wantTables, table)
	}
	slices.Sort(wantTables)
	if !slices.Equal(tables, wantTables) {
		return StorageContract{}, fmt.Errorf("storage tables do not match version %d: got %v want %v", version, tables, wantTables)
	}
	for table, want := range expected {
		columns, err := storageTableColumns(ctx, query, table)
		if err != nil {
			return StorageContract{}, err
		}
		if !slices.Equal(columns, want) {
			return StorageContract{}, fmt.Errorf("storage table %s columns do not match version %d", table, version)
		}
	}
	for index, table := range storageIndexes {
		var count int
		if err := query.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_schema WHERE type='index' AND name=? AND tbl_name=?`, index, table).Scan(&count); err != nil {
			return StorageContract{}, fmt.Errorf("inspect storage index %s: %w", index, err)
		}
		if count != 1 {
			return StorageContract{}, fmt.Errorf("storage schema version %d lacks exact index %s", version, index)
		}
	}
	var unsupportedEvents int
	if err := query.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE schema_version<>?`, expectedEventVersion).Scan(&unsupportedEvents); err != nil {
		return StorageContract{}, fmt.Errorf("inspect durable Event Contract versions: %w", err)
	}
	if unsupportedEvents != 0 {
		return StorageContract{}, fmt.Errorf("storage schema version %d contains %d events outside supported Event Contract schema v%d", version, unsupportedEvents, expectedEventVersion)
	}
	contract := StorageContract{StorageVersion: version, EventSchemaVersion: expectedEventVersion}
	if version < 2 {
		fingerprint, err := storageSchemaFingerprint(ctx, query)
		if err != nil {
			return StorageContract{}, err
		}
		if fingerprint != storageSchemaV1Fingerprint {
			return StorageContract{}, fmt.Errorf("storage schema version 1 fingerprint does not match its frozen layout")
		}
		return contract, nil
	}
	var rows, storageVersion, eventVersion, applicationID int
	var storedFingerprint string
	if err := query.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MAX(storage_version),0),COALESCE(MAX(event_schema_version),0),COALESCE(MAX(application_id),0),COALESCE(MAX(schema_fingerprint),'') FROM agentos_storage`).Scan(&rows, &storageVersion, &eventVersion, &applicationID, &storedFingerprint); err != nil {
		return StorageContract{}, fmt.Errorf("read Agent OS storage metadata: %w", err)
	}
	if rows != 1 || storageVersion != version || eventVersion != expectedEventVersion || applicationID != StorageApplicationID || storedFingerprint == "" {
		return StorageContract{}, fmt.Errorf("agent OS storage metadata does not match runtime contract")
	}
	fingerprint, err := storageSchemaFingerprint(ctx, query)
	if err != nil {
		return StorageContract{}, err
	}
	if storedFingerprint != fingerprint {
		return StorageContract{}, fmt.Errorf("agent OS storage schema fingerprint does not match its reviewed layout")
	}
	contract.EventSchemaVersion = eventVersion
	return contract, nil
}

func userStorageObjects(ctx context.Context, query storageQueryer) ([]string, error) {
	return storageNames(ctx, query, `SELECT type || ':' || name FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name`)
}

func userStorageTables(ctx context.Context, query storageQueryer) ([]string, error) {
	return storageNames(ctx, query, `SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
}

func storageNames(ctx context.Context, query storageQueryer, statement string) (names []string, finalErr error) {
	rows, err := query.QueryContext(ctx, statement)
	if err != nil {
		return nil, fmt.Errorf("inspect Agent OS storage schema: %w", err)
	}
	defer func() { finalErr = errors.Join(finalErr, rows.Close()) }()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("read Agent OS storage schema: %w", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate Agent OS storage schema: %w", err)
	}
	return names, nil
}

func storageTableColumns(ctx context.Context, query storageQueryer, table string) (columns []string, finalErr error) {
	rows, err := query.QueryContext(ctx, `SELECT name FROM pragma_table_info(?) ORDER BY cid`, table)
	if err != nil {
		return nil, fmt.Errorf("inspect storage table %s: %w", table, err)
	}
	defer func() { finalErr = errors.Join(finalErr, rows.Close()) }()
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("read storage table %s: %w", table, err)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate storage table %s: %w", table, err)
	}
	return columns, nil
}

func storageSchemaFingerprint(ctx context.Context, query storageQueryer) (fingerprint string, finalErr error) {
	rows, err := query.QueryContext(ctx, `SELECT type,name,tbl_name,COALESCE(sql,'') FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name,tbl_name`)
	if err != nil {
		return "", fmt.Errorf("inspect storage schema fingerprint: %w", err)
	}
	defer func() { finalErr = errors.Join(finalErr, rows.Close()) }()
	hash := sha256.New()
	for rows.Next() {
		var kind, name, table, statement string
		if err := rows.Scan(&kind, &name, &table, &statement); err != nil {
			return "", fmt.Errorf("read storage schema fingerprint: %w", err)
		}
		_, _ = fmt.Fprintf(hash, "%s\x00%s\x00%s\x00%s\n", kind, name, table, strings.TrimSpace(statement))
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("iterate storage schema fingerprint: %w", err)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

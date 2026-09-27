package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

var storageColumnsV13 = map[string][]string{
	"incident_event_links":  {"target_kind", "target_id", "event_sequence", "event_id"},
	"incident_record_links": {"target_kind", "target_id", "record_kind", "record_id", "record_version", "admission_event_id"},
}

type incidentSchemaObject struct {
	kind, name, table, sql string
}

var incidentLinkObjects = incidentLinkSchemaObjects()

var incidentLinkChecks = incidentLinkSchemaChecks()

func incidentLinkSchemaChecks() []incidentSchemaObject {
	objects := append([]incidentSchemaObject(nil), incidentLinkObjects...)
	// An unrelated source trigger can suppress later AFTER maintenance with
	// RAISE(IGNORE), so the existing source triggers belong to this proof too.
	for _, definition := range strings.Split(storageSchemaV11SQL, "\nCREATE TRIGGER ")[1:] {
		statement := strings.TrimSuffix(strings.TrimSpace("CREATE TRIGGER "+definition), ";")
		name := strings.Fields(statement)[2]
		objects = append(objects, incidentSchemaObject{"trigger", name, storageTriggersV11[name], statement})
	}
	return objects
}

func incidentLinkSchemaObjects() []incidentSchemaObject {
	objects := []incidentSchemaObject{
		{"table", "incident_event_links", "incident_event_links", `CREATE TABLE incident_event_links (
target_kind TEXT NOT NULL,target_id TEXT NOT NULL,event_sequence INTEGER NOT NULL,event_id TEXT NOT NULL,
PRIMARY KEY(target_kind,target_id,event_sequence,event_id)) WITHOUT ROWID`},
		{"table", "incident_record_links", "incident_record_links", `CREATE TABLE incident_record_links (
target_kind TEXT NOT NULL,target_id TEXT NOT NULL,record_kind TEXT NOT NULL,record_id TEXT NOT NULL,record_version INTEGER NOT NULL,admission_event_id TEXT NOT NULL,
PRIMARY KEY(target_kind,target_id,record_kind,record_id,record_version,admission_event_id)) WITHOUT ROWID`},
		{"index", "incident_event_link_source_idx", "incident_event_links", `CREATE INDEX incident_event_link_source_idx ON incident_event_links(event_sequence,event_id)`},
		{"index", "incident_event_link_id_idx", "incident_event_links", `CREATE INDEX incident_event_link_id_idx ON incident_event_links(event_id,event_sequence)`},
		{"index", "incident_record_link_source_idx", "incident_record_links", `CREATE INDEX incident_record_link_source_idx ON incident_record_links(record_kind,record_id,record_version,admission_event_id)`},
		{"index", "incident_record_link_event_idx", "incident_record_links", `CREATE INDEX incident_record_link_event_idx ON incident_record_links(admission_event_id,record_kind,record_id,record_version) WHERE admission_event_id<>''`},
	}
	trigger := func(name, table, timing, body string) {
		objects = append(objects, incidentSchemaObject{"trigger", name, table, "CREATE TRIGGER " + name + " " + timing + " ON " + table + " BEGIN\n" + body + "\nEND"})
	}
	for _, record := range []bool{false, true} {
		source, table, prefix := "events", "incident_event_links", "incident_event_links_"
		columns, values := "target_kind,target_id,event_sequence,event_id", "target_kind,target_id,NEW.sequence,NEW.event_id"
		newSources := `(event_sequence=NEW.sequence OR event_id=NEW.event_id)`
		oldSources := `(event_sequence=OLD.sequence OR event_id=OLD.event_id)`
		if record {
			source, table, prefix = "records", "incident_record_links", "incident_record_links_"
			columns = "target_kind,target_id,record_kind,record_id,record_version,admission_event_id"
			values = "target_kind,target_id,NEW.kind,NEW.record_id,NEW.version,NEW.admission_event_id"
			newSources = `((record_kind=NEW.kind AND record_id=NEW.record_id AND record_version=NEW.version) OR (NEW.admission_event_id<>'' AND admission_event_id<>'' AND admission_event_id=NEW.admission_event_id))`
			oldSources = `(record_kind=OLD.kind AND record_id=OLD.record_id AND record_version=OLD.version)`
		}
		// Guard against omissions as well as invented relationships. Maintenance
		// runs after source changes: only obsolete references may then be deleted.
		// INSERT OR REPLACE can conflict only with the same complete reference.
		trigger(prefix+"insert_guard", table, "BEFORE INSERT", `SELECT CASE WHEN NOT (`+incidentCanonicalLink(record, "NEW")+`) THEN RAISE(ABORT,'invalid incident link insertion') END;`)
		trigger(prefix+"delete_guard", table, "BEFORE DELETE", `SELECT CASE WHEN `+incidentCanonicalLink(record, "OLD")+` THEN RAISE(ABORT,'required incident link deletion') END;`)
		trigger(prefix+"update_guard", table, "BEFORE UPDATE", `SELECT RAISE(ABORT,'incident link updates are forbidden');`)
		insert := "INSERT OR IGNORE INTO " + table + "(" + columns + ") SELECT " + values + " FROM (" + incidentLinkSelect(record, "NEW") + ");"
		cleanup := func(sources string) string {
			return "DELETE FROM " + table + " WHERE " + sources + " AND NOT (" + incidentCanonicalLink(record, table) + ");"
		}
		// Redundant admission/event IDs find sources displaced by a uniqueness
		// conflict even when REPLACE does not run their DELETE triggers.
		trigger(prefix+"source_insert", source, "AFTER INSERT", cleanup(newSources)+"\n"+insert)
		trigger(prefix+"source_update", source, "AFTER UPDATE", cleanup("("+newSources+" OR "+oldSources+")")+"\n"+insert)
		trigger(prefix+"source_delete", source, "AFTER DELETE", cleanup(oldSources))
	}
	return objects
}

func incidentCanonicalLink(record bool, link string) string {
	source, alias := "events", "e"
	match := "e.sequence=" + link + ".event_sequence AND e.event_id=" + link + ".event_id"
	if record {
		source, alias = "records", "r"
		match = "r.kind=" + link + ".record_kind AND r.record_id=" + link + ".record_id AND r.version=" + link + ".record_version AND r.admission_event_id=" + link + ".admission_event_id"
	}
	return "EXISTS (SELECT 1 FROM " + source + " " + alias + " WHERE " + match + " AND (" + incidentLinkMatch(record, alias, link+".target_kind", link+".target_id") + "))"
}

// Migration is atomic with its caller's storage-version transaction. Sources
// are stable composite record identities or explicit event primary keys; an
// implicit records rowid must never become durable index identity.
func createIncidentLinkSchema(ctx context.Context, tx *sql.Tx) error {
	return createIncidentLinks(ctx, tx, 1)
}

func incidentLinkGrammar(statement string, version int) string {
	if version == 1 {
		statement = strings.ReplaceAll(statement, "_v2(", "_v1(")
		// v13 event extractors received NULL as their unused kind argument.
		// Preserve their exact SQL, not just equivalent extraction behavior.
		for _, source := range []string{"NEW.", "e.", ""} {
			statement = strings.ReplaceAll(statement, "0,"+source+"event_type,"+source+"payload", "0,NULL,"+source+"payload")
		}
	}
	return statement
}

func createIncidentLinks(ctx context.Context, tx *sql.Tx, version int) error {
	for _, object := range incidentLinkObjects {
		if object.kind == "trigger" {
			continue
		}
		if _, err := tx.ExecContext(ctx, incidentLinkGrammar(object.sql, version)); err != nil {
			return fmt.Errorf("create incident link schema: %w", err)
		}
	}
	for _, record := range []bool{false, true} {
		table, source, alias := "incident_event_links", "events", "e"
		identity := "e.sequence,e.event_id"
		if record {
			table, source, alias, identity = "incident_record_links", "records", "r", "r.kind,r.record_id,r.version,r.admission_event_id"
		}
		// The correlated extractor expands one source at a time, rather than
		// materializing the complete historical graph in the migration process.
		statement := "INSERT INTO " + table + " SELECT json_extract(link.value,'$.kind'),json_extract(link.value,'$.id')," + identity + " FROM " + source + " " + alias + " JOIN json_each(" + incidentLinkJSON(record, alias) + ") link"
		if _, err := tx.ExecContext(ctx, incidentLinkGrammar(statement, version)); err != nil {
			return fmt.Errorf("backfill incident links: %w", err)
		}
	}
	for _, object := range incidentLinkObjects {
		if object.kind != "trigger" {
			continue
		}
		if _, err := tx.ExecContext(ctx, incidentLinkGrammar(object.sql, version)); err != nil {
			return fmt.Errorf("guard incident link schema: %w", err)
		}
	}
	return nil
}

func validateIncidentLinkSchema(ctx context.Context, query storageQueryer) error {
	return validateIncidentLinkGrammar(ctx, query, 2)
}

func validateIncidentLinkGrammar(ctx context.Context, query storageQueryer, version int) error {
	want := make(map[string]incidentSchemaObject, len(incidentLinkChecks))
	args := make([]any, 0, len(incidentLinkChecks))
	maximumSQL := 0
	for _, object := range incidentLinkChecks {
		object.sql = incidentLinkGrammar(object.sql, version)
		want[object.name] = object
		args = append(args, object.name)
		maximumSQL = max(maximumSQL, len(object.sql))
	}
	// Compare compiled definitions, not mutable stored fingerprint metadata.
	// This fixed-size schema read belongs to the same snapshot as selection.
	// Extra link constraints can suppress INSERT OR IGNORE, and extra source
	// triggers can skip maintenance. Reject both rather than checking only the
	// expected names. LIMIT bounds even a damaged schema's returned inventory.
	boundedSQL := `substr(COALESCE(sql,''),1,` + fmt.Sprint(maximumSQL+1) + `)`
	statement := `SELECT type,name,tbl_name,` + boundedSQL + ` FROM main.sqlite_schema WHERE name IN (` + incidentMarks(len(args)) + `)
OR (name NOT LIKE 'sqlite_%' AND tbl_name IN ('incident_event_links','incident_record_links'))
OR (type='trigger' AND tbl_name IN ('events','records'))
UNION ALL SELECT 'temporary-'||type,name,tbl_name,` + boundedSQL + ` FROM temp.sqlite_schema
WHERE name IN ('events','records','incident_event_links','incident_record_links')
OR (type='trigger' AND tbl_name IN ('events','records','incident_event_links','incident_record_links')) LIMIT ` + fmt.Sprint(len(incidentLinkChecks)+1)
	rows, err := query.QueryContext(ctx, statement, args...)
	if err != nil {
		return fmt.Errorf("inspect incident link schema: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var object incidentSchemaObject
		if err := rows.Scan(&object.kind, &object.name, &object.table, &object.sql); err != nil {
			return err
		}
		expected, found := want[object.name]
		if !found || object.kind != expected.kind || object.table != expected.table || strings.TrimSpace(object.sql) != strings.TrimSpace(expected.sql) {
			return fmt.Errorf("incident link schema definition does not match %s", object.name)
		}
		delete(want, object.name)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(want) != 0 {
		return fmt.Errorf("incident link schema is incomplete")
	}
	return rows.Close()
}

// Guard definitions cannot prove that the index was maintained continuously.
// Verify its contents against both authoritative source tables in this read's
// snapshot, including histories unrelated to the selected incident. This adds
// one complete source scan and indexed link probes, not one query per source.
// The reader receives only aggregate counts. Extraction retains one source's
// document and distinct links at a time, never a complete historical graph.
func validateIncidentLinkContents(ctx context.Context, query storageQueryer) error {
	return validateIncidentLinkVersion(ctx, query, 2)
}

func validateIncidentLinkVersion(ctx context.Context, query storageQueryer, version int) error {
	var parts []string
	for _, record := range []bool{false, true} {
		table, source, alias := "incident_event_links", "events", "e"
		identity := "retained_link.event_sequence=e.sequence AND retained_link.event_id=e.event_id"
		if record {
			table, source, alias = "incident_record_links", "records", "r"
			identity = "retained_link.record_kind=r.kind AND retained_link.record_id=r.record_id AND retained_link.record_version=r.version AND retained_link.admission_event_id=r.admission_event_id"
		}
		canonical := "json_each(" + incidentLinkJSON(record, alias) + ") wanted"
		parts = append(parts, "SELECT '"+table+"',COUNT(*),COUNT(retained_link.target_id),(SELECT COUNT(*) FROM "+table+") FROM "+source+" "+alias+" JOIN "+canonical+" LEFT JOIN "+table+" retained_link ON retained_link.target_kind=json_extract(wanted.value,'$.kind') AND retained_link.target_id=json_extract(wanted.value,'$.id') AND "+identity)
	}
	rows, err := query.QueryContext(ctx, incidentLinkGrammar(strings.Join(parts, " UNION ALL "), version))
	if err != nil {
		return fmt.Errorf("verify incident link contents: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var table string
		var expected, matched, retained int64
		if err := rows.Scan(&table, &expected, &matched, &retained); err != nil {
			return fmt.Errorf("verify incident link contents: %w", err)
		}
		// Every canonical reference must have its exact unique index row. Equal
		// cardinality then excludes extras, including a forged row replacing a
		// missing row without changing the total count.
		if expected != matched || expected != retained {
			return fmt.Errorf("incident link contents do not match authoritative sources: %s", table)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("verify incident link contents: %w", err)
	}
	return rows.Close()
}

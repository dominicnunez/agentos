package ledger

// Migration seeds every existing freeze record or event, and the first new
// freeze mutation creates a row. An empty table therefore lets ordinary writes
// skip conflict work while preserving every freeze and REPLACE path.
const storageSchemaV11SQL = `CREATE TABLE freeze_changes (
organization_id TEXT PRIMARY KEY CHECK(organization_id<>''),
generation BLOB NOT NULL CHECK(typeof(generation)='blob' AND length(generation)=32),
rewrite_generation BLOB NOT NULL CHECK(typeof(rewrite_generation)='blob' AND length(rewrite_generation)=32));

INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
SELECT organization_id,randomblob(32),randomblob(32) FROM (
  SELECT record_id AS organization_id FROM records WHERE kind='organization_freeze' AND record_id<>''
  UNION
  SELECT organization_id FROM events WHERE event_type='FREEZE_SET' AND organization_id<>''
);

CREATE TRIGGER freeze_records_insert_change BEFORE INSERT ON records
WHEN NEW.kind='organization_freeze' OR
  (NEW.admission_event_id<>'' AND EXISTS(SELECT 1 FROM freeze_changes)) BEGIN
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  SELECT organization_id,randomblob(32),randomblob(32) FROM (
    SELECT NEW.record_id AS organization_id
    WHERE NEW.kind='organization_freeze' AND NEW.record_id<>'' AND EXISTS(
      SELECT 1 FROM records r WHERE r.kind=NEW.kind AND r.record_id=NEW.record_id AND r.version=NEW.version
    )
    UNION
    SELECT r.record_id FROM records r
    WHERE r.kind='organization_freeze' AND r.record_id<>'' AND NEW.admission_event_id<>'' AND
      r.admission_event_id<>'' AND r.admission_event_id=NEW.admission_event_id
  ) WHERE organization_id<>''
  ON CONFLICT(organization_id) DO UPDATE SET generation=randomblob(32),rewrite_generation=randomblob(32);
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  SELECT NEW.record_id,randomblob(32),randomblob(32)
  WHERE NEW.kind='organization_freeze' AND NEW.record_id<>''
  ON CONFLICT(organization_id) DO UPDATE SET
    generation=randomblob(32),
    rewrite_generation=CASE WHEN
      NEW.version=(SELECT COALESCE(MAX(version),0)+1 FROM records WHERE kind='organization_freeze' AND record_id=NEW.record_id)
    THEN freeze_changes.rewrite_generation ELSE randomblob(32) END;
END;

CREATE TRIGGER freeze_records_update_change BEFORE UPDATE ON records
WHEN OLD.kind='organization_freeze' OR NEW.kind='organization_freeze' OR
  EXISTS(SELECT 1 FROM freeze_changes) BEGIN
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  SELECT organization_id,randomblob(32),randomblob(32) FROM (
    SELECT OLD.record_id AS organization_id
    WHERE OLD.kind='organization_freeze' AND OLD.record_id<>''
    UNION
    SELECT NEW.record_id
    WHERE NEW.kind='organization_freeze' AND NEW.record_id<>''
    UNION
    SELECT r.record_id FROM records r
    WHERE r.kind='organization_freeze' AND r.record_id<>'' AND NEW.admission_event_id<>'' AND
      r.admission_event_id<>'' AND r.admission_event_id=NEW.admission_event_id AND
      NOT (r.kind=OLD.kind AND r.record_id=OLD.record_id AND r.version=OLD.version)
  ) WHERE organization_id<>''
  ON CONFLICT(organization_id) DO UPDATE SET generation=randomblob(32),rewrite_generation=randomblob(32);
END;

CREATE TRIGGER freeze_records_delete_change BEFORE DELETE ON records
WHEN OLD.kind='organization_freeze' AND OLD.record_id<>'' BEGIN
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  VALUES(OLD.record_id,randomblob(32),randomblob(32))
  ON CONFLICT(organization_id) DO UPDATE SET generation=randomblob(32),rewrite_generation=randomblob(32);
END;

CREATE TRIGGER freeze_events_insert_conflict BEFORE INSERT ON events
WHEN NEW.event_type='FREEZE_SET' OR EXISTS(SELECT 1 FROM freeze_changes) BEGIN
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  SELECT organization_id,randomblob(32),randomblob(32) FROM (
    SELECT e.organization_id FROM events e
    WHERE e.event_id=NEW.event_id AND e.event_type='FREEZE_SET' AND e.organization_id<>''
    UNION
    SELECT e.organization_id FROM events e
    WHERE e.sequence=NEW.sequence AND e.event_id<>NEW.event_id AND
      e.event_type='FREEZE_SET' AND e.organization_id<>''
    UNION
    SELECT r.record_id FROM records r
    WHERE r.kind='organization_freeze' AND r.record_id<>'' AND r.admission_event_id<>'' AND
      r.admission_event_id=NEW.event_id
    UNION
    SELECT r.record_id FROM events e
    JOIN records r ON r.admission_event_id=e.event_id AND r.admission_event_id<>''
    WHERE e.sequence=NEW.sequence AND e.event_id<>NEW.event_id AND
      r.kind='organization_freeze' AND r.record_id<>''
  ) WHERE organization_id<>''
  ON CONFLICT(organization_id) DO UPDATE SET generation=randomblob(32),rewrite_generation=randomblob(32);
END;

CREATE TRIGGER freeze_events_insert_change AFTER INSERT ON events
WHEN NEW.event_type='FREEZE_SET' AND NEW.organization_id<>'' BEGIN
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  VALUES(NEW.organization_id,randomblob(32),randomblob(32))
  ON CONFLICT(organization_id) DO UPDATE SET
    generation=randomblob(32),
    rewrite_generation=CASE WHEN NEW.sequence=(SELECT MAX(sequence) FROM events)
      THEN freeze_changes.rewrite_generation ELSE randomblob(32) END;
END;

CREATE TRIGGER freeze_events_update_change BEFORE UPDATE ON events
WHEN OLD.event_type='FREEZE_SET' OR NEW.event_type='FREEZE_SET' OR
  EXISTS(SELECT 1 FROM freeze_changes) BEGIN
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  SELECT organization_id,randomblob(32),randomblob(32) FROM (
    SELECT OLD.organization_id AS organization_id
    WHERE OLD.event_type='FREEZE_SET' AND OLD.organization_id<>''
    UNION
    SELECT NEW.organization_id
    WHERE NEW.event_type='FREEZE_SET' AND NEW.organization_id<>''
    UNION
    SELECT r.record_id FROM records r
    WHERE r.kind='organization_freeze' AND r.record_id<>'' AND
      r.admission_event_id<>'' AND r.admission_event_id=OLD.event_id
    UNION
    SELECT r.record_id FROM records r
    WHERE r.kind='organization_freeze' AND r.record_id<>'' AND NEW.event_id<>OLD.event_id AND
      r.admission_event_id<>'' AND r.admission_event_id=NEW.event_id
    UNION
    SELECT e.organization_id FROM events e
    WHERE e.event_id=NEW.event_id AND e.event_id<>OLD.event_id AND
      e.event_type='FREEZE_SET' AND e.organization_id<>''
    UNION
    SELECT e.organization_id FROM events e
    WHERE e.sequence=NEW.sequence AND e.event_id<>OLD.event_id AND e.event_id<>NEW.event_id AND
      e.event_type='FREEZE_SET' AND e.organization_id<>''
    UNION
    SELECT r.record_id FROM events e
    JOIN records r ON r.admission_event_id=e.event_id AND r.admission_event_id<>''
    WHERE e.sequence=NEW.sequence AND e.event_id<>OLD.event_id AND e.event_id<>NEW.event_id AND
      r.kind='organization_freeze' AND r.record_id<>''
  ) WHERE organization_id<>''
  ON CONFLICT(organization_id) DO UPDATE SET generation=randomblob(32),rewrite_generation=randomblob(32);
END;

CREATE TRIGGER freeze_events_delete_change BEFORE DELETE ON events
WHEN OLD.event_type='FREEZE_SET' OR EXISTS(SELECT 1 FROM freeze_changes) BEGIN
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  SELECT organization_id,randomblob(32),randomblob(32) FROM (
    SELECT OLD.organization_id AS organization_id
    WHERE OLD.event_type='FREEZE_SET' AND OLD.organization_id<>''
    UNION
    SELECT r.record_id FROM records r
    WHERE r.kind='organization_freeze' AND r.record_id<>'' AND r.admission_event_id<>'' AND
      r.admission_event_id=OLD.event_id
  ) WHERE organization_id<>''
  ON CONFLICT(organization_id) DO UPDATE SET generation=randomblob(32),rewrite_generation=randomblob(32);
END;`

var storageTriggersV11 = map[string]string{
	"freeze_events_delete_change":   "events",
	"freeze_events_insert_change":   "events",
	"freeze_events_insert_conflict": "events",
	"freeze_events_update_change":   "events",
	"freeze_records_delete_change":  "records",
	"freeze_records_insert_change":  "records",
	"freeze_records_update_change":  "records",
}

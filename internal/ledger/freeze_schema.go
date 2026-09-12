package ledger

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
WHEN NEW.kind='organization_freeze' OR NEW.admission_event_id<>'' BEGIN
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  SELECT NEW.record_id,randomblob(32),randomblob(32)
  WHERE NEW.kind='organization_freeze' AND NEW.record_id<>'' AND EXISTS(
    SELECT 1 FROM records r WHERE r.kind=NEW.kind AND r.record_id=NEW.record_id AND r.version=NEW.version
  )
  ON CONFLICT(organization_id) DO UPDATE SET generation=randomblob(32),rewrite_generation=randomblob(32);
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  SELECT DISTINCT r.record_id,randomblob(32),randomblob(32) FROM records r
  WHERE r.kind='organization_freeze' AND r.record_id<>'' AND NEW.admission_event_id<>'' AND
    r.admission_event_id<>'' AND r.admission_event_id=NEW.admission_event_id
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

CREATE TRIGGER freeze_records_update_change BEFORE UPDATE ON records BEGIN
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  SELECT OLD.record_id,randomblob(32),randomblob(32)
  WHERE OLD.kind='organization_freeze' AND OLD.record_id<>''
  ON CONFLICT(organization_id) DO UPDATE SET generation=randomblob(32),rewrite_generation=randomblob(32);
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  SELECT NEW.record_id,randomblob(32),randomblob(32)
  WHERE NEW.kind='organization_freeze' AND NEW.record_id<>''
  ON CONFLICT(organization_id) DO UPDATE SET generation=randomblob(32),rewrite_generation=randomblob(32);
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  SELECT DISTINCT r.record_id,randomblob(32),randomblob(32) FROM records r
  WHERE r.kind='organization_freeze' AND r.record_id<>'' AND NEW.admission_event_id<>'' AND
    r.admission_event_id<>'' AND r.admission_event_id=NEW.admission_event_id AND
    NOT (r.kind=OLD.kind AND r.record_id=OLD.record_id AND r.version=OLD.version)
  ON CONFLICT(organization_id) DO UPDATE SET generation=randomblob(32),rewrite_generation=randomblob(32);
END;

CREATE TRIGGER freeze_records_delete_change BEFORE DELETE ON records
WHEN OLD.kind='organization_freeze' AND OLD.record_id<>'' BEGIN
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  VALUES(OLD.record_id,randomblob(32),randomblob(32))
  ON CONFLICT(organization_id) DO UPDATE SET generation=randomblob(32),rewrite_generation=randomblob(32);
END;

CREATE TRIGGER freeze_events_insert_conflict BEFORE INSERT ON events BEGIN
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  SELECT e.organization_id,randomblob(32),randomblob(32) FROM events e
  WHERE e.event_id=NEW.event_id AND e.event_type='FREEZE_SET' AND e.organization_id<>''
  ON CONFLICT(organization_id) DO UPDATE SET generation=randomblob(32),rewrite_generation=randomblob(32);
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  SELECT e.organization_id,randomblob(32),randomblob(32) FROM events e
  WHERE e.sequence=NEW.sequence AND e.event_id<>NEW.event_id AND
    e.event_type='FREEZE_SET' AND e.organization_id<>''
  ON CONFLICT(organization_id) DO UPDATE SET generation=randomblob(32),rewrite_generation=randomblob(32);
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  SELECT DISTINCT r.record_id,randomblob(32),randomblob(32) FROM records r
  WHERE r.kind='organization_freeze' AND r.record_id<>'' AND r.admission_event_id<>'' AND
    r.admission_event_id=NEW.event_id
  ON CONFLICT(organization_id) DO UPDATE SET generation=randomblob(32),rewrite_generation=randomblob(32);
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  SELECT DISTINCT r.record_id,randomblob(32),randomblob(32) FROM events e
  JOIN records r ON r.admission_event_id=e.event_id AND r.admission_event_id<>''
  WHERE e.sequence=NEW.sequence AND e.event_id<>NEW.event_id AND
    r.kind='organization_freeze' AND r.record_id<>''
  ON CONFLICT(organization_id) DO UPDATE SET generation=randomblob(32),rewrite_generation=randomblob(32);
END;

CREATE TRIGGER freeze_events_insert_change AFTER INSERT ON events BEGIN
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  SELECT NEW.organization_id,randomblob(32),randomblob(32)
  WHERE NEW.event_type='FREEZE_SET' AND NEW.organization_id<>''
  ON CONFLICT(organization_id) DO UPDATE SET
    generation=randomblob(32),
    rewrite_generation=CASE WHEN NEW.sequence=(SELECT MAX(sequence) FROM events)
      THEN freeze_changes.rewrite_generation ELSE randomblob(32) END;
END;

CREATE TRIGGER freeze_events_update_change BEFORE UPDATE ON events BEGIN
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  SELECT OLD.organization_id,randomblob(32),randomblob(32)
  WHERE OLD.event_type='FREEZE_SET' AND OLD.organization_id<>''
  ON CONFLICT(organization_id) DO UPDATE SET generation=randomblob(32),rewrite_generation=randomblob(32);
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  SELECT NEW.organization_id,randomblob(32),randomblob(32)
  WHERE NEW.event_type='FREEZE_SET' AND NEW.organization_id<>''
  ON CONFLICT(organization_id) DO UPDATE SET generation=randomblob(32),rewrite_generation=randomblob(32);
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  SELECT DISTINCT r.record_id,randomblob(32),randomblob(32) FROM records r
  WHERE r.kind='organization_freeze' AND r.record_id<>'' AND
    r.admission_event_id<>'' AND r.admission_event_id=OLD.event_id
  ON CONFLICT(organization_id) DO UPDATE SET generation=randomblob(32),rewrite_generation=randomblob(32);
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  SELECT DISTINCT r.record_id,randomblob(32),randomblob(32) FROM records r
  WHERE r.kind='organization_freeze' AND r.record_id<>'' AND NEW.event_id<>OLD.event_id AND
    r.admission_event_id<>'' AND r.admission_event_id=NEW.event_id
  ON CONFLICT(organization_id) DO UPDATE SET generation=randomblob(32),rewrite_generation=randomblob(32);
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  SELECT e.organization_id,randomblob(32),randomblob(32) FROM events e
  WHERE e.event_id=NEW.event_id AND e.event_id<>OLD.event_id AND
    e.event_type='FREEZE_SET' AND e.organization_id<>''
  ON CONFLICT(organization_id) DO UPDATE SET generation=randomblob(32),rewrite_generation=randomblob(32);
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  SELECT e.organization_id,randomblob(32),randomblob(32) FROM events e
  WHERE e.sequence=NEW.sequence AND e.event_id<>OLD.event_id AND e.event_id<>NEW.event_id AND
    e.event_type='FREEZE_SET' AND e.organization_id<>''
  ON CONFLICT(organization_id) DO UPDATE SET generation=randomblob(32),rewrite_generation=randomblob(32);
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  SELECT DISTINCT r.record_id,randomblob(32),randomblob(32) FROM events e
  JOIN records r ON r.admission_event_id=e.event_id AND r.admission_event_id<>''
  WHERE e.sequence=NEW.sequence AND e.event_id<>OLD.event_id AND e.event_id<>NEW.event_id AND
    r.kind='organization_freeze' AND r.record_id<>''
  ON CONFLICT(organization_id) DO UPDATE SET generation=randomblob(32),rewrite_generation=randomblob(32);
END;

CREATE TRIGGER freeze_events_delete_change BEFORE DELETE ON events BEGIN
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  SELECT OLD.organization_id,randomblob(32),randomblob(32)
  WHERE OLD.event_type='FREEZE_SET' AND OLD.organization_id<>''
  ON CONFLICT(organization_id) DO UPDATE SET generation=randomblob(32),rewrite_generation=randomblob(32);
  INSERT INTO freeze_changes(organization_id,generation,rewrite_generation)
  SELECT DISTINCT r.record_id,randomblob(32),randomblob(32) FROM records r
  WHERE r.kind='organization_freeze' AND r.record_id<>'' AND r.admission_event_id<>'' AND
    r.admission_event_id=OLD.event_id
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

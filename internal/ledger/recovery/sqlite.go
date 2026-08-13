package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"modernc.org/sqlite"
)

var requiredColumns = map[string][]string{
	"consumed_approvals": {"approval_id", "effect_fingerprint", "consumed_at"},
	"events":             {"sequence", "event_id", "organization_id", "event_type", "source_actor_id", "source_execution_id", "recipient_scope", "recipient_id", "task_id", "authorization_refs", "artifact_refs", "payload", "correlation_id", "created_at", "schema_version"},
	"external_tasks":     {"organization_id", "task_id", "correlation_id"},
	"external_work":      {"organization_id", "request_id", "correlation_id", "intent_id"},
	"inbox":              {"recipient_scope", "recipient_id", "event_id", "organization_id", "task_id", "available_at", "observed_at", "observation_event_id"},
	"records":            {"kind", "record_id", "version", "body", "admission_event_id", "admission_fingerprint", "created_at"},
}
var requiredTables = []string{"consumed_approvals", "events", "external_tasks", "external_work", "inbox", "records"}

type Result struct {
	Path        string `json:"path"`
	SHA256      string `json:"sha256"`
	SizeBytes   int64  `json:"size_bytes"`
	EventCount  int64  `json:"event_count"`
	MaxSequence int64  `json:"max_sequence"`
}

type backuper interface {
	NewBackup(string) (*sqlite.Backup, error)
}

// Backup creates and verifies an online SQLite snapshot. Destination must not
// exist; publication uses a same-directory hard link so a concurrent creator
// cannot be overwritten between validation and publication.
func Backup(ctx context.Context, source, destination string) (Result, error) {
	return clone(ctx, source, destination)
}

// Restore verifies a backup and materializes it at a new path. It never
// replaces an existing database; the operator switches AGENTOS_DB only after
// stopping the runtime, leaving the prior database available for rollback.
func Restore(ctx context.Context, backup, destination string) (Result, error) {
	if _, err := Verify(ctx, backup); err != nil {
		return Result{}, fmt.Errorf("verify restore source: %w", err)
	}
	return clone(ctx, backup, destination)
}

// Verify checks SQLite integrity and the minimum Agent OS ledger schema, then
// returns a content checksum for an offline backup or restore candidate.
func Verify(ctx context.Context, path string) (result Result, finalErr error) {
	if ctx == nil {
		return Result{}, fmt.Errorf("context is required")
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	resolved, err := sourcePath(path)
	if err != nil {
		return Result{}, err
	}
	db, err := openReadOnlySQLite(ctx, resolved)
	if err != nil {
		return Result{}, fmt.Errorf("open recovery database read-only: %w", err)
	}
	defer func() {
		if db != nil {
			finalErr = errors.Join(finalErr, db.Close())
		}
	}()
	if err := verifyIntegrity(ctx, db); err != nil {
		return Result{}, err
	}

	var tableCount int
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(requiredTables)), ",")
	arguments := make([]any, len(requiredTables))
	for index, table := range requiredTables {
		arguments[index] = table
	}
	query := `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN (` + placeholders + `)`
	if err := db.QueryRowContext(ctx, query, arguments...).Scan(&tableCount); err != nil {
		return Result{}, fmt.Errorf("inspect Agent OS ledger schema: %w", err)
	}
	if tableCount != len(requiredColumns) {
		return Result{}, fmt.Errorf("database is not a complete Agent OS ledger")
	}
	for table, columns := range requiredColumns {
		if err := verifyColumns(ctx, db, table, columns); err != nil {
			return Result{}, err
		}
	}
	if err := verifyProjectionAdmissions(ctx, db); err != nil {
		return Result{}, err
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(MAX(sequence), 0) FROM events`).Scan(&result.EventCount, &result.MaxSequence); err != nil {
		return Result{}, fmt.Errorf("inspect Agent OS event ledger: %w", err)
	}
	if err := db.Close(); err != nil {
		return Result{}, fmt.Errorf("close verified database before checksum: %w", err)
	}
	db = nil
	result, err = fileResult(resolved, result.EventCount, result.MaxSequence)
	return result, err
}

type admittedProjectionEvent struct {
	event   events.Event
	payload events.ProjectionEventPayload
}

func verifyProjectionAdmissions(ctx context.Context, db *sql.DB) error {
	eventRows, err := db.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events ORDER BY sequence`)
	if err != nil {
		return fmt.Errorf("inspect projection admission events: %w", err)
	}
	defer func() { _ = eventRows.Close() }()
	admitted := map[string]admittedProjectionEvent{}
	for eventRows.Next() {
		var event events.Event
		var authorizationRefs, artifactRefs []byte
		var rawPayload any
		var createdAt string
		if err := eventRows.Scan(&event.EventID, &event.Sequence, &event.OrganizationID, &event.EventType, &event.SourceActorID, &event.SourceExecutionID, &event.RecipientScope, &event.RecipientID, &event.TaskID, &authorizationRefs, &artifactRefs, &rawPayload, &event.CorrelationID, &createdAt, &event.SchemaVersion); err != nil {
			_ = eventRows.Close()
			return fmt.Errorf("read projection admission event: %w", err)
		}
		event.Payload, err = sqliteBytes(rawPayload)
		if err != nil {
			_ = eventRows.Close()
			return fmt.Errorf("event %s payload: %w", event.EventID, err)
		}
		if json.Unmarshal(authorizationRefs, &event.AuthorizationRefs) != nil || json.Unmarshal(artifactRefs, &event.ArtifactRefs) != nil {
			_ = eventRows.Close()
			return fmt.Errorf("event %s has invalid reference arrays", event.EventID)
		}
		if createdAt != "" {
			parsed, err := time.Parse(time.RFC3339Nano, createdAt)
			if err != nil {
				_ = eventRows.Close()
				return fmt.Errorf("event %s has an invalid timestamp", event.EventID)
			}
			event.CreatedAt = parsed
		}
		payload, present, err := events.AdmittedProjection(event)
		if err != nil {
			_ = eventRows.Close()
			return fmt.Errorf("event %s: %w", event.EventID, err)
		}
		if !present {
			if events.RequiresProjectionAdmission(event.EventType, event.SourceActorID) {
				_ = eventRows.Close()
				return fmt.Errorf("event %s lacks required projection admission", event.EventID)
			}
			continue
		}
		if !events.ProjectionKindRequiresAdmission(payload.Projection.ProjectionKind) {
			_ = eventRows.Close()
			return fmt.Errorf("event %s carries unsupported projection kind %s", event.EventID, payload.Projection.ProjectionKind)
		}
		if err := events.ValidateProjectionEventBoundary(event, payload); err != nil {
			_ = eventRows.Close()
			return fmt.Errorf("event %s: %w", event.EventID, err)
		}
		if _, duplicate := admitted[event.EventID]; duplicate {
			_ = eventRows.Close()
			return fmt.Errorf("duplicate projection admission event %s", event.EventID)
		}
		admitted[event.EventID] = admittedProjectionEvent{event: event, payload: payload}
	}
	if err := eventRows.Err(); err != nil {
		_ = eventRows.Close()
		return fmt.Errorf("iterate projection admission events: %w", err)
	}
	if err := eventRows.Close(); err != nil {
		return fmt.Errorf("close projection admission events: %w", err)
	}

	recordRows, err := db.QueryContext(ctx, `SELECT kind,record_id,version,body,admission_event_id,admission_fingerprint FROM records ORDER BY kind,record_id,version`)
	if err != nil {
		return fmt.Errorf("inspect projection admission records: %w", err)
	}
	defer func() { _ = recordRows.Close() }()
	used := map[string]struct{}{}
	orderedAdmissions := make([]admittedProjectionEvent, 0, len(admitted))
	lastProjectionVersions := map[string]int{}
	lastProjectionSequences := map[string]int64{}
	lastTasks := map[core.ID]core.Task{}
	lastAgents := map[core.ID]core.Agent{}
	for recordRows.Next() {
		var kind, recordID, admissionEventID, admissionFingerprint string
		var version int
		var rawBody any
		if err := recordRows.Scan(&kind, &recordID, &version, &rawBody, &admissionEventID, &admissionFingerprint); err != nil {
			_ = recordRows.Close()
			return fmt.Errorf("read projection admission record: %w", err)
		}
		body, err := sqliteBytes(rawBody)
		if err != nil {
			_ = recordRows.Close()
			return fmt.Errorf("record %s/%s/%d body: %w", kind, recordID, version, err)
		}
		if !events.ProjectionKindRequiresAdmission(kind) {
			if admissionEventID != "" || admissionFingerprint != "" {
				_ = recordRows.Close()
				return fmt.Errorf("generic record %s/%s/%d carries projection authority", kind, recordID, version)
			}
			continue
		}
		versionKey := kind + "\x00" + recordID
		if version != lastProjectionVersions[versionKey]+1 {
			_ = recordRows.Close()
			return fmt.Errorf("projection record %s/%s version %d is not contiguous", kind, recordID, version)
		}
		lastProjectionVersions[versionKey] = version
		admission, found := admitted[admissionEventID]
		payload := admission.payload
		var record events.ProjectionRecord
		canonical, canonicalErr := json.Marshal(payload.Projection)
		if !found || canonicalErr != nil || !bytes.Equal(body, canonical) || decodeExactJSON(body, &record) != nil || record.ProjectionKind != kind || record.RecordID != recordID || record.Version != version || !reflect.DeepEqual(record, payload.Projection) || admissionEventID != payload.Admission.EventRef || admissionFingerprint != payload.Admission.Fingerprint {
			_ = recordRows.Close()
			return fmt.Errorf("projection record %s/%s/%d lacks exact event admission", kind, recordID, version)
		}
		if previousSequence := lastProjectionSequences[versionKey]; previousSequence != 0 && admission.event.Sequence <= previousSequence {
			_ = recordRows.Close()
			return fmt.Errorf("projection record %s/%s version %d precedes its prior admission event", kind, recordID, version)
		}
		lastProjectionSequences[versionKey] = admission.event.Sequence
		if kind == "task" {
			var task core.Task
			if decodeExactJSON(record.Value, &task) != nil || task.ID != core.ID(recordID) {
				_ = recordRows.Close()
				return fmt.Errorf("projection record %s/%s/%d contains an invalid Task", kind, recordID, version)
			}
			previous, previousFound := lastTasks[task.ID]
			var prior *core.Task
			if previousFound {
				prior = &previous
			}
			if err := events.ValidateTaskProjectionTransition(admission.event.EventType, version, prior, task); err != nil {
				_ = recordRows.Close()
				return fmt.Errorf("projection record %s/%s/%d: %w", kind, recordID, version, err)
			}
			lastTasks[task.ID] = task
		}
		if kind == "agent" {
			var agent core.Agent
			if decodeExactJSON(record.Value, &agent) != nil || agent.ID != core.ID(recordID) {
				_ = recordRows.Close()
				return fmt.Errorf("projection record %s/%s/%d contains an invalid Agent", kind, recordID, version)
			}
			previous, previousFound := lastAgents[agent.ID]
			var prior *core.Agent
			if previousFound {
				prior = &previous
			}
			if err := events.ValidateAgentProjectionTransition(admission.event.EventType, version, prior, agent); err != nil {
				_ = recordRows.Close()
				return fmt.Errorf("projection record %s/%s/%d: %w", kind, recordID, version, err)
			}
			lastAgents[agent.ID] = agent
		}
		if _, duplicate := used[admissionEventID]; duplicate {
			_ = recordRows.Close()
			return fmt.Errorf("projection admission event %s authorizes multiple records", admissionEventID)
		}
		used[admissionEventID] = struct{}{}
		orderedAdmissions = append(orderedAdmissions, admission)
	}
	if err := recordRows.Err(); err != nil {
		_ = recordRows.Close()
		return fmt.Errorf("iterate projection admission records: %w", err)
	}
	if err := recordRows.Close(); err != nil {
		return fmt.Errorf("close projection admission records: %w", err)
	}
	for eventID := range admitted {
		if _, found := used[eventID]; !found {
			return fmt.Errorf("projection admission event %s has no materialized record", eventID)
		}
	}
	return validateProjectionOrganizationBindings(orderedAdmissions)
}

func validateProjectionOrganizationBindings(admitted []admittedProjectionEvent) error {
	snapshot := core.DurableGraph{
		Organizations:     map[core.ID]core.DurableState[core.Organization]{},
		Missions:          map[core.ID]core.DurableState[core.Mission]{},
		Goals:             map[core.ID]core.DurableState[core.Goal]{},
		Teams:             map[core.ID]core.DurableState[core.Team]{},
		AgentBlueprints:   map[core.ID]core.DurableState[core.AgentBlueprint]{},
		ExecutionProfiles: map[core.ID]core.DurableState[core.ExecutionProfile]{},
		Agents:            map[core.ID]core.DurableState[core.Agent]{},
		Intents:           map[core.ID]core.DurableState[core.Intent]{},
		Works:             map[core.ID]core.DurableState[core.Work]{},
		Tasks:             map[core.ID]core.DurableState[core.Task]{},
	}
	directOrganizations := map[string]core.ID{}

	for _, admission := range admitted {
		event, record := admission.event, admission.payload.Projection
		var organizationID core.ID
		switch record.ProjectionKind {
		case "organization":
			var value core.Organization
			if decodeExactJSON(record.Value, &value) != nil || string(value.ID) != record.RecordID {
				return fmt.Errorf("event %s contains an invalid Organization projection", event.EventID)
			}
			organizationID = value.ID
			if err := setRecoveryProjection(snapshot.Organizations, record, value, false, nil); err != nil {
				return fmt.Errorf("event %s contains invalid Organization history: %w", event.EventID, err)
			}
		case "mission":
			var value core.Mission
			if decodeExactJSON(record.Value, &value) != nil || string(value.ID) != record.RecordID {
				return fmt.Errorf("event %s contains an invalid Mission projection", event.EventID)
			}
			organizationID = value.OrganizationID
			if err := setRecoveryProjection(snapshot.Missions, record, value, false, core.ValidMissionRevision); err != nil {
				return fmt.Errorf("event %s contains invalid Mission history: %w", event.EventID, err)
			}
		case "goal":
			var value core.Goal
			if decodeExactJSON(record.Value, &value) != nil || string(value.ID) != record.RecordID {
				return fmt.Errorf("event %s contains an invalid Goal projection", event.EventID)
			}
			organizationID = value.OrganizationID
			if err := setRecoveryProjection(snapshot.Goals, record, value, false, core.ValidGoalRevision); err != nil {
				return fmt.Errorf("event %s contains invalid Goal history: %w", event.EventID, err)
			}
		case "team":
			var value core.Team
			if decodeExactJSON(record.Value, &value) != nil || string(value.ID) != record.RecordID {
				return fmt.Errorf("event %s contains an invalid Team projection", event.EventID)
			}
			organizationID = value.OrganizationID
			if err := setRecoveryProjection(snapshot.Teams, record, value, false, nil); err != nil {
				return fmt.Errorf("event %s contains invalid Team history: %w", event.EventID, err)
			}
		case "agent_blueprint":
			var value core.AgentBlueprint
			if decodeExactJSON(record.Value, &value) != nil || string(value.ID) != record.RecordID {
				return fmt.Errorf("event %s contains an invalid Agent blueprint projection", event.EventID)
			}
			organizationID = value.OrganizationID
			if err := setRecoveryProjection(snapshot.AgentBlueprints, record, value, false, core.ValidAgentBlueprintRevision); err != nil {
				return fmt.Errorf("event %s contains invalid Agent blueprint history: %w", event.EventID, err)
			}
		case "execution_profile":
			var value core.ExecutionProfile
			if decodeExactJSON(record.Value, &value) != nil || string(value.ID) != record.RecordID {
				return fmt.Errorf("event %s contains an invalid execution profile projection", event.EventID)
			}
			organizationID = value.OrganizationID
			if err := setRecoveryProjection(snapshot.ExecutionProfiles, record, value, false, core.ValidExecutionProfileRevision); err != nil {
				return fmt.Errorf("event %s contains invalid execution profile history: %w", event.EventID, err)
			}
		case "agent":
			var value core.Agent
			if decodeExactJSON(record.Value, &value) != nil || string(value.ID) != record.RecordID {
				return fmt.Errorf("event %s contains an invalid Agent projection", event.EventID)
			}
			organizationID = value.OrganizationID
			if err := setRecoveryProjection(snapshot.Agents, record, value, false, core.ValidAgentRevision); err != nil {
				return fmt.Errorf("event %s contains invalid Agent history: %w", event.EventID, err)
			}
		case "intent":
			var value core.Intent
			if decodeExactJSON(record.Value, &value) != nil || string(value.ID) != record.RecordID {
				return fmt.Errorf("event %s contains an invalid Intent projection", event.EventID)
			}
			organizationID = value.OrganizationID
			if err := setRecoveryProjection(snapshot.Intents, record, value, true, nil); err != nil {
				return fmt.Errorf("event %s contains invalid Intent history: %w", event.EventID, err)
			}
		case "work":
			var value core.Work
			if decodeExactJSON(record.Value, &value) != nil || string(value.ID) != record.RecordID {
				return fmt.Errorf("event %s contains an invalid Work projection", event.EventID)
			}
			if err := setRecoveryProjection(snapshot.Works, record, value, true, core.ValidWorkRevision); err != nil {
				return fmt.Errorf("event %s contains invalid Work history: %w", event.EventID, err)
			}
			continue
		case "task":
			var value core.Task
			if decodeExactJSON(record.Value, &value) != nil || string(value.ID) != record.RecordID {
				return fmt.Errorf("event %s contains an invalid Task projection", event.EventID)
			}
			if err := setRecoveryProjection(snapshot.Tasks, record, value, true, core.ValidTaskRevision); err != nil {
				return fmt.Errorf("event %s contains invalid Task history: %w", event.EventID, err)
			}
			continue
		default:
			return fmt.Errorf("event %s contains unsupported projection kind %s", event.EventID, record.ProjectionKind)
		}
		if organizationID == "" || string(organizationID) != event.OrganizationID {
			return fmt.Errorf("event %s projection crosses its organization boundary", event.EventID)
		}
		directOrganizations[event.EventID] = organizationID
	}

	for eventID, organizationID := range directOrganizations {
		if _, found := snapshot.Organizations[organizationID]; !found {
			return fmt.Errorf("event %s projection references a missing Organization", eventID)
		}
	}
	if err := core.ValidateDurableGraph(snapshot); err != nil {
		return err
	}
	for _, admission := range admitted {
		event, record := admission.event, admission.payload.Projection
		var organizationID core.ID
		switch record.ProjectionKind {
		case "work":
			var value core.Work
			if decodeExactJSON(record.Value, &value) != nil {
				return fmt.Errorf("event %s contains an invalid Work projection", event.EventID)
			}
			organizationID = snapshot.Intents[value.IntentID].Value.OrganizationID
		case "task":
			var value core.Task
			if decodeExactJSON(record.Value, &value) != nil {
				return fmt.Errorf("event %s contains an invalid Task projection", event.EventID)
			}
			work, found := snapshot.Works[value.WorkID]
			if found {
				organizationID = snapshot.Intents[work.Value.IntentID].Value.OrganizationID
			}
		case "agent":
			var value core.Agent
			if decodeExactJSON(record.Value, &value) != nil {
				return fmt.Errorf("event %s contains an invalid Agent projection", event.EventID)
			}
			blueprint, found := snapshot.AgentBlueprints[value.BlueprintID]
			if !found || blueprint.Value.OrganizationID != value.OrganizationID || blueprint.Value.Version != value.BlueprintVersion {
				return fmt.Errorf("event %s Agent projection references an invalid blueprint", event.EventID)
			}
			profile, found := snapshot.ExecutionProfiles[value.ExecutionProfileID]
			if !found || profile.Value.OrganizationID != value.OrganizationID || profile.Value.Version != value.ExecutionProfileVersion {
				return fmt.Errorf("event %s Agent projection references an invalid execution profile", event.EventID)
			}
			organizationID = value.OrganizationID
		default:
			continue
		}
		if organizationID == "" || string(organizationID) != event.OrganizationID {
			return fmt.Errorf("event %s projection crosses its organization boundary", event.EventID)
		}
	}
	return nil
}

func setRecoveryProjection[T any](target map[core.ID]core.DurableState[T], record events.ProjectionRecord, value T, correlationStable bool, validRevision func(T, T) bool) error {
	return core.AdmitDurableRevision(target, core.ID(record.RecordID), record.Version, record.CorrelationID, value, correlationStable, validRevision)
}

func sqliteBytes(value any) ([]byte, error) {
	switch typed := value.(type) {
	case []byte:
		return typed, nil
	case string:
		return []byte(typed), nil
	default:
		return nil, fmt.Errorf("unsupported SQLite value %T", value)
	}
}

func decodeExactJSON(body []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("unexpected trailing JSON")
	}
	return nil
}

func verifyIntegrity(ctx context.Context, db *sql.DB) (finalErr error) {
	rows, err := db.QueryContext(ctx, `PRAGMA integrity_check`)
	if err != nil {
		return fmt.Errorf("check SQLite integrity: %w", err)
	}
	defer func() {
		finalErr = errors.Join(finalErr, rows.Close())
	}()
	integrityOK := false
	for rows.Next() {
		var finding string
		if err := rows.Scan(&finding); err != nil {
			return fmt.Errorf("read SQLite integrity result: %w", err)
		}
		if finding != "ok" {
			return fmt.Errorf("SQLite integrity check failed: %s", finding)
		}
		integrityOK = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate SQLite integrity results: %w", err)
	}
	if !integrityOK {
		return fmt.Errorf("SQLite integrity check returned no result")
	}
	return nil
}

func verifyColumns(ctx context.Context, db *sql.DB, table string, required []string) (finalErr error) {
	rows, err := db.QueryContext(ctx, `SELECT name FROM pragma_table_info(?)`, table)
	if err != nil {
		return fmt.Errorf("inspect Agent OS table %s: %w", table, err)
	}
	defer func() {
		finalErr = errors.Join(finalErr, rows.Close())
	}()
	found := make(map[string]struct{}, len(required))
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("read Agent OS table %s: %w", table, err)
		}
		found[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate Agent OS table %s: %w", table, err)
	}
	for _, column := range required {
		if _, ok := found[column]; !ok {
			return fmt.Errorf("agent OS table %s is missing column %s", table, column)
		}
	}
	return nil
}

func clone(ctx context.Context, source, destination string) (result Result, finalErr error) {
	if ctx == nil {
		return Result{}, fmt.Errorf("context is required")
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	resolvedSource, err := sourcePath(source)
	if err != nil {
		return Result{}, err
	}
	resolvedDestination, err := destinationPath(destination)
	if err != nil {
		return Result{}, err
	}
	if samePath(resolvedSource, resolvedDestination) {
		return Result{}, fmt.Errorf("source and destination must be different files")
	}

	temporary, err := os.CreateTemp(filepath.Dir(resolvedDestination), ".agentos-recovery-*")
	if err != nil {
		return Result{}, fmt.Errorf("create recovery staging file: %w", err)
	}
	temporaryPath := temporary.Name()
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return Result{}, fmt.Errorf("close recovery staging file: %w", err)
	}
	defer func() {
		if err := os.Remove(temporaryPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			finalErr = errors.Join(finalErr, fmt.Errorf("remove recovery staging file: %w", err))
		}
	}()

	db, err := openReadOnlySQLite(ctx, resolvedSource)
	if err != nil {
		return Result{}, fmt.Errorf("open backup source read-only: %w", err)
	}
	defer func() {
		if db != nil {
			finalErr = errors.Join(finalErr, db.Close())
		}
	}()
	connection, err := db.Conn(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("acquire backup source connection: %w", err)
	}
	if err := connection.Raw(func(driverConnection any) error {
		provider, ok := driverConnection.(backuper)
		if !ok {
			return fmt.Errorf("SQLite driver does not support online backup")
		}
		backup, err := provider.NewBackup(sqliteFileURI(temporaryPath, false))
		if err != nil {
			return err
		}
		for more := true; more; {
			if err := ctx.Err(); err != nil {
				return errors.Join(err, backup.Finish())
			}
			more, err = backup.Step(128)
			if err != nil {
				return errors.Join(err, backup.Finish())
			}
		}
		return backup.Finish()
	}); err != nil {
		_ = connection.Close()
		return Result{}, fmt.Errorf("create online SQLite backup: %w", err)
	}
	if err := connection.Close(); err != nil {
		return Result{}, fmt.Errorf("close backup source connection: %w", err)
	}
	if err := db.Close(); err != nil {
		return Result{}, fmt.Errorf("close backup source: %w", err)
	}
	db = nil

	result, err = Verify(ctx, temporaryPath)
	if err != nil {
		return Result{}, fmt.Errorf("verify recovery staging database: %w", err)
	}
	if err := os.Chmod(temporaryPath, 0o600); err != nil {
		return Result{}, fmt.Errorf("restrict recovery file permissions: %w", err)
	}
	if err := syncFile(temporaryPath); err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if err := requireNoSidecars(resolvedDestination); err != nil {
		return Result{}, err
	}
	if err := os.Link(temporaryPath, resolvedDestination); err != nil {
		return Result{}, fmt.Errorf("publish recovery file without overwrite: %w", err)
	}
	if err := requireNoSidecars(resolvedDestination); err != nil {
		return Result{}, errors.Join(err, os.Remove(resolvedDestination))
	}
	if err := syncDirectory(filepath.Dir(resolvedDestination)); err != nil {
		return Result{}, errors.Join(err, os.Remove(resolvedDestination))
	}
	result.Path = resolvedDestination
	return result, nil
}

func sourcePath(path string) (string, error) {
	if path == "" || path == ":memory:" {
		return "", fmt.Errorf("a file-backed SQLite database is required")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve source path: %w", err)
	}
	resolved := filepath.Clean(absolute)
	info, err := os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf("inspect source database: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("source database must be a regular file")
	}
	return resolved, nil
}

func destinationPath(path string) (string, error) {
	if path == "" || path == ":memory:" {
		return "", fmt.Errorf("a new file-backed destination is required")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve destination path: %w", err)
	}
	parent := filepath.Clean(filepath.Dir(absolute))
	info, err := os.Stat(parent)
	if err != nil {
		return "", fmt.Errorf("inspect destination directory: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("destination parent must be a directory")
	}
	resolved := filepath.Join(parent, filepath.Base(absolute))
	if _, err := os.Lstat(resolved); err == nil {
		return "", fmt.Errorf("destination already exists; recovery never overwrites files")
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("inspect destination: %w", err)
	}
	return resolved, nil
}

func requireNoSidecars(path string) error {
	for _, suffix := range []string{"-journal", "-shm", "-wal"} {
		sidecar := path + suffix
		if _, err := os.Lstat(sidecar); err == nil {
			return fmt.Errorf("destination SQLite sidecar already exists: %s", filepath.Base(sidecar))
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect destination SQLite sidecar: %w", err)
		}
	}
	return nil
}

func sqliteFileURI(path string, readOnly bool) string {
	slashPath := filepath.ToSlash(path)
	if runtime.GOOS == "windows" && !strings.HasPrefix(slashPath, "/") {
		slashPath = "/" + slashPath
	}
	uri := url.URL{Scheme: "file", Path: slashPath}
	if readOnly {
		query := uri.Query()
		query.Set("mode", "ro")
		uri.RawQuery = query.Encode()
	}
	return uri.String()
}

func openReadOnlySQLite(ctx context.Context, path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", sqliteFileURI(path, true))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, `PRAGMA query_only=ON`); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return db, nil
}

func samePath(left, right string) bool {
	if runtime.GOOS == "windows" {
		return strings.EqualFold(filepath.Clean(left), filepath.Clean(right))
	}
	return filepath.Clean(left) == filepath.Clean(right)
}

func fileResult(path string, eventCount, maxSequence int64) (Result, error) {
	file, err := os.Open(path)
	if err != nil {
		return Result{}, fmt.Errorf("open recovery file for checksum: %w", err)
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		_ = file.Close()
		return Result{}, fmt.Errorf("checksum recovery file: %w", err)
	}
	info, err := file.Stat()
	closeErr := file.Close()
	if err != nil {
		return Result{}, fmt.Errorf("inspect recovery file: %w", err)
	}
	if closeErr != nil {
		return Result{}, fmt.Errorf("close recovery file after checksum: %w", closeErr)
	}
	return Result{Path: path, SHA256: hex.EncodeToString(hash.Sum(nil)), SizeBytes: info.Size(), EventCount: eventCount, MaxSequence: maxSequence}, nil
}

func syncFile(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open recovery file for sync: %w", err)
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	return errors.Join(syncErr, closeErr)
}

func syncDirectory(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open recovery directory for sync: %w", err)
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	return errors.Join(syncErr, closeErr)
}

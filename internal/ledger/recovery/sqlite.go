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

	"github.com/dominicnunez/agentos/internal/boundaryjson"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	ledgerstore "github.com/dominicnunez/agentos/internal/ledger"
	"modernc.org/sqlite"
)

type Result struct {
	Path                string `json:"path"`
	SHA256              string `json:"sha256"`
	ChecksumScope       string `json:"checksum_scope"`
	EventChainSHA256    string `json:"event_chain_sha256,omitempty"`
	EventChainAlgorithm string `json:"event_chain_algorithm,omitempty"`
	SizeBytes           int64  `json:"size_bytes"`
	EventCount          int64  `json:"event_count"`
	MaxSequence         int64  `json:"max_sequence"`
	StorageVersion      int    `json:"storage_version"`
	EventSchemaVersion  int    `json:"event_schema_version"`
}

const (
	ChecksumOfflineDatabaseFile  = "OFFLINE_DATABASE_FILE"
	ChecksumOnlineBackupSnapshot = "ONLINE_BACKUP_SNAPSHOT"
)

type backuper interface {
	NewBackup(string) (*sqlite.Backup, error)
}

func backupSQLiteSnapshot(ctx context.Context, source *sql.DB, destination string) error {
	connection, err := source.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire SQLite backup connection: %w", err)
	}
	backupErr := connection.Raw(func(driverConnection any) error {
		provider, ok := driverConnection.(backuper)
		if !ok {
			return fmt.Errorf("SQLite driver does not support online backup")
		}
		backup, err := provider.NewBackup(sqliteFileURI(destination, false))
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
	})
	return errors.Join(backupErr, connection.Close())
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

	contract, err := ledgerstore.ValidateStorageContract(ctx, db)
	if err != nil {
		return Result{}, fmt.Errorf("verify Agent OS storage contract: %w", err)
	}
	result.StorageVersion = contract.StorageVersion
	result.EventSchemaVersion = contract.EventSchemaVersion
	if contract.EventSchemaVersion == events.SchemaVersion && contract.StorageVersion == ledgerstore.CurrentStorageVersion {
		if err := verifyProjectionAdmissions(ctx, db); err != nil {
			return Result{}, err
		}
		if err := ledgerstore.ValidateTaskCompletionAdmissions(ctx, db); err != nil {
			return Result{}, err
		}
		if err := ledgerstore.ValidateWorkCompletionAdmissions(ctx, db); err != nil {
			return Result{}, err
		}
		if err := ledgerstore.ValidateGoalAchievementAdmissions(ctx, db); err != nil {
			return Result{}, err
		}
		if err := ledgerstore.ValidateInferenceAdmissions(ctx, db); err != nil {
			return Result{}, err
		}
		if contract.StorageVersion >= ledgerstore.EventIntegrityStorageVersion {
			integrity, err := ledgerstore.ValidateEventIntegrity(ctx, db)
			if err != nil {
				return Result{}, fmt.Errorf("verify event integrity chain: %w", err)
			}
			result.EventChainSHA256 = integrity.SHA256
			result.EventChainAlgorithm = integrity.Algorithm
		}
	} else {
		if contract.StorageVersion >= ledgerstore.EventIntegrityStorageVersion {
			integrity, err := ledgerstore.ValidateEventIntegrity(ctx, db)
			if err != nil {
				return Result{}, fmt.Errorf("verify legacy event integrity chain: %w", err)
			}
			result.EventChainSHA256 = integrity.SHA256
			result.EventChainAlgorithm = integrity.Algorithm
		}
		if err := verifyLegacyAdmissionsAfterMigration(ctx, db); err != nil {
			return Result{}, err
		}
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(MAX(sequence), 0) FROM events`).Scan(&result.EventCount, &result.MaxSequence); err != nil {
		return Result{}, fmt.Errorf("inspect Agent OS event ledger: %w", err)
	}
	if err := db.Close(); err != nil {
		return Result{}, fmt.Errorf("close verified database before checksum: %w", err)
	}
	db = nil
	verified, err := fileResult(resolved, result.EventCount, result.MaxSequence)
	verified.StorageVersion = result.StorageVersion
	verified.EventSchemaVersion = result.EventSchemaVersion
	verified.EventChainSHA256 = result.EventChainSHA256
	verified.EventChainAlgorithm = result.EventChainAlgorithm
	return verified, err
}

// VerifyLive verifies one SQLite online-backup snapshot of a potentially live
// database. Its checksum identifies the snapshot file, including committed WAL
// state captured by SQLite, and is not presented as the live main-file hash or
// as a logical ledger identity.
func VerifyLive(ctx context.Context, path string) (Result, error) {
	return verifyLiveSnapshot(ctx, path, nil)
}

func verifyLiveSnapshot(ctx context.Context, path string, afterSnapshot func() error) (result Result, finalErr error) {
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
	temporary, err := os.CreateTemp("", "agentos-live-verification-*.db")
	if err != nil {
		return Result{}, fmt.Errorf("create live verification snapshot: %w", err)
	}
	temporaryPath := temporary.Name()
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return Result{}, fmt.Errorf("close live verification snapshot: %w", err)
	}
	defer func() {
		for _, candidate := range []string{temporaryPath, temporaryPath + "-journal", temporaryPath + "-shm", temporaryPath + "-wal"} {
			if removeErr := os.Remove(candidate); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				finalErr = errors.Join(finalErr, fmt.Errorf("remove live verification snapshot: %w", removeErr))
			}
		}
	}()

	db, err := openReadOnlySQLite(ctx, resolved)
	if err != nil {
		return Result{}, fmt.Errorf("open live verification source read-only: %w", err)
	}
	if err := backupSQLiteSnapshot(ctx, db, temporaryPath); err != nil {
		return Result{}, errors.Join(fmt.Errorf("create live SQLite verification snapshot: %w", err), db.Close())
	}
	if err := db.Close(); err != nil {
		return Result{}, fmt.Errorf("close live verification source: %w", err)
	}
	if afterSnapshot != nil {
		if err := afterSnapshot(); err != nil {
			return Result{}, fmt.Errorf("run post-snapshot verification hook: %w", err)
		}
	}
	result, err = Verify(ctx, temporaryPath)
	if err != nil {
		return Result{}, fmt.Errorf("verify live SQLite snapshot: %w", err)
	}
	result.Path = resolved
	result.ChecksumScope = ChecksumOnlineBackupSnapshot
	return result, nil
}

// verifyLegacyAdmissionsAfterMigration audits the exact migration result in an
// isolated snapshot. The source remains read-only and byte-for-byte unchanged;
// the migrated copy must satisfy every current admission check before the
// legacy backup can be reported as verified.
func verifyLegacyAdmissionsAfterMigration(ctx context.Context, source *sql.DB) (finalErr error) {
	temporary, err := os.CreateTemp("", "agentos-legacy-verification-*.db")
	if err != nil {
		return fmt.Errorf("create legacy verification snapshot: %w", err)
	}
	temporaryPath := temporary.Name()
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return fmt.Errorf("close legacy verification snapshot: %w", err)
	}
	defer func() {
		for _, path := range []string{temporaryPath, temporaryPath + "-journal", temporaryPath + "-shm", temporaryPath + "-wal"} {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				finalErr = errors.Join(finalErr, fmt.Errorf("remove legacy verification snapshot: %w", err))
			}
		}
	}()

	if err := backupSQLiteSnapshot(ctx, source, temporaryPath); err != nil {
		return fmt.Errorf("snapshot legacy storage for admission verification: %w", err)
	}

	migrated, err := ledgerstore.Open(temporaryPath)
	if err != nil {
		return fmt.Errorf("migrate legacy verification snapshot: %w", err)
	}
	if err := migrated.Close(); err != nil {
		return fmt.Errorf("close migrated legacy verification snapshot: %w", err)
	}
	if _, err := Verify(ctx, temporaryPath); err != nil {
		return fmt.Errorf("verify migrated legacy admissions: %w", err)
	}
	return nil
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
	stream := make([]events.Event, 0)
	eventIDs := map[string]struct{}{}
	sequences := map[int64]struct{}{}
	for eventRows.Next() {
		var event events.Event
		var authorizationRefs, artifactRefs []byte
		var rawPayload any
		var createdAt string
		if err := eventRows.Scan(&event.EventID, &event.Sequence, &event.OrganizationID, &event.EventType, &event.SourceActorID, &event.SourceExecutionID, &event.RecipientScope, &event.RecipientID, &event.TaskID, &authorizationRefs, &artifactRefs, &rawPayload, &event.CorrelationID, &createdAt, &event.SchemaVersion); err != nil {
			_ = eventRows.Close()
			return fmt.Errorf("read projection admission event: %w", err)
		}
		if event.EventID == "" || event.Sequence < 1 {
			_ = eventRows.Close()
			return fmt.Errorf("event stream contains an incomplete envelope")
		}
		if event.SchemaVersion != events.SchemaVersion {
			_ = eventRows.Close()
			return fmt.Errorf("event %s uses unsupported schema version %d", event.EventID, event.SchemaVersion)
		}
		if _, duplicate := eventIDs[event.EventID]; duplicate {
			_ = eventRows.Close()
			return fmt.Errorf("event stream contains duplicate event id %s", event.EventID)
		}
		if _, duplicate := sequences[event.Sequence]; duplicate {
			_ = eventRows.Close()
			return fmt.Errorf("event stream contains duplicate sequence %d at %s", event.Sequence, event.EventType)
		}
		eventIDs[event.EventID] = struct{}{}
		sequences[event.Sequence] = struct{}{}
		event.Payload, err = sqliteBytes(rawPayload)
		if err != nil {
			_ = eventRows.Close()
			return fmt.Errorf("event %s payload: %w", event.EventID, err)
		}
		if json.Unmarshal(authorizationRefs, &event.AuthorizationRefs) != nil || json.Unmarshal(artifactRefs, &event.ArtifactRefs) != nil {
			_ = eventRows.Close()
			return fmt.Errorf("event %s has invalid reference arrays", event.EventID)
		}
		parsed, err := time.Parse(time.RFC3339Nano, createdAt)
		if err != nil || parsed.IsZero() {
			_ = eventRows.Close()
			return fmt.Errorf("event %s has an invalid timestamp", event.EventID)
		}
		event.CreatedAt = parsed
		stream = append(stream, event)
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
	lastProjectionVersions := map[string]int{}
	lastProjectionSequences := map[string]int64{}
	authorityRecords := make([]events.AuthorityRecord, 0)
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
			if kind == "capability_lease" || kind == "organization_freeze" {
				if admissionEventID == "" || admissionFingerprint != "" {
					_ = recordRows.Close()
					return fmt.Errorf("authority record %s/%s/%d lacks its exact admission event", kind, recordID, version)
				}
				authorityRecords = append(authorityRecords, events.AuthorityRecord{Kind: kind, RecordID: recordID, Version: version, Body: append([]byte(nil), body...), AdmissionEventID: admissionEventID})
			} else if admissionEventID != "" || admissionFingerprint != "" {
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
		if _, duplicate := used[admissionEventID]; duplicate {
			_ = recordRows.Close()
			return fmt.Errorf("projection admission event %s authorizes multiple records", admissionEventID)
		}
		used[admissionEventID] = struct{}{}
	}
	if err := recordRows.Err(); err != nil {
		_ = recordRows.Close()
		return fmt.Errorf("iterate projection admission records: %w", err)
	}
	if err := recordRows.Close(); err != nil {
		return fmt.Errorf("close projection admission records: %w", err)
	}
	capabilityAdmissions, freezeAdmissions, err := events.ResolveAuthorityAdmissions(stream, authorityRecords)
	if err != nil {
		return fmt.Errorf("validate authority record admissions: %w", err)
	}
	if err := events.ValidateExecutionStops(stream, freezeAdmissions); err != nil {
		return err
	}
	if err := events.ValidateModelStops(stream, freezeAdmissions); err != nil {
		return err
	}
	if err := events.ValidateSecurityHoldOutcomes(stream, freezeAdmissions); err != nil {
		return err
	}
	for eventID := range admitted {
		if _, found := used[eventID]; !found {
			return fmt.Errorf("projection admission event %s has no materialized record", eventID)
		}
	}
	dispatchIndex, err := newRecoveryDispatchIndex(stream)
	if err != nil {
		return fmt.Errorf("index Agent dispatch recovery evidence: %w", err)
	}
	inbox, err := ledgerstore.ReadInboxObservations(ctx, db)
	if err != nil {
		return err
	}
	graph, err := events.ValidateProjectionHistory(stream, inbox, capabilityAdmissions, freezeAdmissions)
	if err != nil {
		return err
	}
	if err := core.ValidateDurableGraph(graph); err != nil {
		return err
	}
	return validateRecoveryAgentEvidence(stream, dispatchIndex)
}

type recoveryProjectionKey struct {
	organizationID string
	kind           string
	recordID       string
}

type recoveryDispatchIndex struct {
	eventsByID              map[string]events.Event
	nextProjectionByEventID map[string]events.Event
}

const maximumRecoveryDispatchEvidence = 6

func newRecoveryDispatchIndex(stream []events.Event) (*recoveryDispatchIndex, error) {
	index := &recoveryDispatchIndex{
		eventsByID:              make(map[string]events.Event, len(stream)),
		nextProjectionByEventID: make(map[string]events.Event),
	}
	lastProjection := make(map[recoveryProjectionKey]events.Event)
	var priorSequence int64
	for _, event := range stream {
		if event.Sequence <= priorSequence {
			return nil, fmt.Errorf("event stream is not in strict sequence order")
		}
		priorSequence = event.Sequence
		if _, duplicate := index.eventsByID[event.EventID]; duplicate {
			return nil, fmt.Errorf("event %s is duplicated", event.EventID)
		}
		index.eventsByID[event.EventID] = event
		payload, present, err := events.AdmittedProjection(event)
		if err != nil {
			return nil, err
		}
		if !present {
			continue
		}
		switch payload.Projection.ProjectionKind {
		case "agent", "agent_blueprint", "execution_profile":
			key := recoveryProjectionKey{
				organizationID: event.OrganizationID,
				kind:           payload.Projection.ProjectionKind,
				recordID:       payload.Projection.RecordID,
			}
			if prior, found := lastProjection[key]; found {
				index.nextProjectionByEventID[prior.EventID] = event
			}
			lastProjection[key] = event
		}
	}
	return index, nil
}

// boundedStreamForStart returns only the exact roster revisions named by one
// dispatch plus, when present, the first superseding revision before start.
// Existing Event Contract validation then applies unchanged to at most six
// events instead of repeatedly scanning the complete ledger.
func (index *recoveryDispatchIndex) boundedStreamForStart(start events.Event) ([]events.Event, error) {
	if index == nil {
		return nil, fmt.Errorf("agent dispatch recovery index is required")
	}
	payload, present, err := events.AdmittedProjection(start)
	if err != nil || !present {
		return nil, fmt.Errorf("execution start lacks its admitted Task projection")
	}
	var detail events.ExecutionStartDetail
	if decodeExactJSON(payload.Detail, &detail) != nil || detail.DispatchBinding == nil {
		return nil, fmt.Errorf("execution start lacks its dispatch binding")
	}
	binding := detail.DispatchBinding
	references := []string{binding.AgentEventRef, binding.BlueprintEventRef, binding.ExecutionProfileEventRef}
	bounded := make([]events.Event, 0, maximumRecoveryDispatchEvidence)
	for _, eventRef := range references {
		referenced, found := index.eventsByID[eventRef]
		if found {
			bounded = append(bounded, referenced)
		}
		if successor, superseded := index.nextProjectionByEventID[eventRef]; found && superseded &&
			successor.Sequence < start.Sequence {
			bounded = append(bounded, successor)
		}
	}
	if len(bounded) > maximumRecoveryDispatchEvidence {
		return nil, fmt.Errorf("bounded Agent dispatch recovery evidence exceeded %d events", maximumRecoveryDispatchEvidence)
	}
	return bounded, nil
}

type recoveryTaskKey struct {
	organizationID string
	correlationID  string
	taskID         string
}

type recoveryTaskRevision struct {
	task   core.Task
	record events.ProjectionRecord
}

type recoveryTaskStartKey struct {
	recoveryTaskKey
	version int
}

func validateRecoveryAgentEvidence(stream []events.Event, dispatchIndex *recoveryDispatchIndex) error {
	tasks := make(map[recoveryTaskKey]recoveryTaskRevision)
	starts := make(map[recoveryTaskStartKey][]events.Event)
	for _, evidence := range stream {
		payload, present, err := events.AdmittedProjection(evidence)
		if err != nil {
			return fmt.Errorf("event %s: inspect Agent evidence projection admission: %w", evidence.EventID, err)
		}
		if present && payload.Projection.ProjectionKind == "task" {
			var task core.Task
			if decodeExactJSON(payload.Projection.Value, &task) != nil {
				return fmt.Errorf("event %s has an invalid Agent evidence Task projection", evidence.EventID)
			}
			key := recoveryTaskKey{organizationID: evidence.OrganizationID, correlationID: evidence.CorrelationID, taskID: payload.Projection.RecordID}
			tasks[key] = recoveryTaskRevision{task: task, record: payload.Projection}
			if evidence.EventType == "EXECUTION_STARTED" {
				startKey := recoveryTaskStartKey{recoveryTaskKey: key, version: payload.Projection.Version}
				starts[startKey] = append(starts[startKey], evidence)
			}
		}
		if evidence.EventType != "EVIDENCE_PUBLISHED" {
			continue
		}
		key := recoveryTaskKey{organizationID: evidence.OrganizationID, correlationID: evidence.CorrelationID, taskID: evidence.TaskID}
		taskRevision, found := tasks[key]
		if !found {
			return fmt.Errorf("event %s lacks its Agent evidence Task admission", evidence.EventID)
		}
		startCandidates := starts[recoveryTaskStartKey{recoveryTaskKey: key, version: taskRevision.record.Version}]
		if len(startCandidates) != 1 {
			return fmt.Errorf("event %s has %d exact Agent execution starts", evidence.EventID, len(startCandidates))
		}
		start := startCandidates[0]
		bounded, err := dispatchIndex.boundedStreamForStart(start)
		if err != nil {
			return fmt.Errorf("event %s: index Agent dispatch admission: %w", evidence.EventID, err)
		}
		if err := events.ValidateAgentEvidencePublished(evidence, taskRevision.task, taskRevision.record.Version, start, bounded); err != nil {
			return fmt.Errorf("event %s: %w", evidence.EventID, err)
		}
	}
	return nil
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
	return boundaryjson.Unmarshal(body, target)
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
	if err := backupSQLiteSnapshot(ctx, db, temporaryPath); err != nil {
		return Result{}, fmt.Errorf("create online SQLite backup: %w", err)
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
	return Result{Path: path, SHA256: hex.EncodeToString(hash.Sum(nil)), ChecksumScope: ChecksumOfflineDatabaseFile, SizeBytes: info.Size(), EventCount: eventCount, MaxSequence: maxSequence}, nil
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

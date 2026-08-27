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
	"sort"
	"strings"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	ledgerstore "github.com/dominicnunez/agentos/internal/ledger"
	"modernc.org/sqlite"
)

type Result struct {
	Path                string `json:"path"`
	SHA256              string `json:"sha256"`
	EventChainSHA256    string `json:"event_chain_sha256,omitempty"`
	EventChainAlgorithm string `json:"event_chain_algorithm,omitempty"`
	SizeBytes           int64  `json:"size_bytes"`
	EventCount          int64  `json:"event_count"`
	MaxSequence         int64  `json:"max_sequence"`
	StorageVersion      int    `json:"storage_version"`
	EventSchemaVersion  int    `json:"event_schema_version"`
}

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
	if contract.EventSchemaVersion == events.SchemaVersion {
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
	if err := validateRecoveryIntentConfirmations(stream); err != nil {
		return err
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
	lastMissions := map[core.ID]core.Mission{}
	lastGoals := map[core.ID]core.Goal{}
	lastWorks := map[core.ID]core.Work{}
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
		var transitionErr error
		switch kind {
		case "mission":
			transitionErr = validateRecoveryLifecycle(record, admission.event.EventType, lastMissions, func(value core.Mission) core.ID { return value.ID }, events.ValidateMissionProjectionTransition)
		case "task":
			transitionErr = validateRecoveryLifecycle(record, admission.event.EventType, lastTasks, func(value core.Task) core.ID { return value.ID }, events.ValidateTaskProjectionTransition)
		case "agent":
			transitionErr = validateRecoveryLifecycle(record, admission.event.EventType, lastAgents, func(value core.Agent) core.ID { return value.ID }, events.ValidateAgentProjectionTransition)
		case "goal":
			transitionErr = validateRecoveryLifecycle(record, admission.event.EventType, lastGoals, func(value core.Goal) core.ID { return value.ID }, events.ValidateGoalProjectionTransition)
		case "work":
			transitionErr = validateRecoveryLifecycle(record, admission.event.EventType, lastWorks, func(value core.Work) core.ID { return value.ID }, events.ValidateWorkProjectionTransition)
		}
		if transitionErr != nil {
			_ = recordRows.Close()
			return fmt.Errorf("projection record %s/%s/%d: %w", kind, recordID, version, transitionErr)
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
	if _, _, err := events.ResolveAuthorityAdmissions(stream, authorityRecords); err != nil {
		return fmt.Errorf("validate authority record admissions: %w", err)
	}
	for eventID := range admitted {
		if _, found := used[eventID]; !found {
			return fmt.Errorf("projection admission event %s has no materialized record", eventID)
		}
	}
	if err := validateProjectionOrganizationBindings(orderedAdmissions, stream); err != nil {
		return err
	}
	return validateRecoveryAgentEvidence(stream)
}

func validateRecoveryAgentEvidence(stream []events.Event) error {
	for evidenceIndex, evidence := range stream {
		if evidence.EventType != "EVIDENCE_PUBLISHED" {
			continue
		}
		var task core.Task
		var taskRecord events.ProjectionRecord
		var taskVersion int
		for _, candidate := range stream[:evidenceIndex] {
			if candidate.OrganizationID != evidence.OrganizationID || candidate.CorrelationID != evidence.CorrelationID {
				continue
			}
			payload, present, err := events.AdmittedProjection(candidate)
			if err != nil {
				return fmt.Errorf("event %s: inspect Agent evidence Task admission: %w", evidence.EventID, err)
			}
			if !present || payload.Projection.ProjectionKind != "task" || payload.Projection.RecordID != evidence.TaskID {
				continue
			}
			var candidateTask core.Task
			if decodeExactJSON(payload.Projection.Value, &candidateTask) != nil {
				return fmt.Errorf("event %s has an invalid Agent evidence Task projection", evidence.EventID)
			}
			task = candidateTask
			taskRecord = payload.Projection
			taskVersion = payload.Projection.Version
		}
		var start events.Event
		for _, candidate := range stream[:evidenceIndex] {
			if candidate.EventType != "EXECUTION_STARTED" || candidate.OrganizationID != evidence.OrganizationID ||
				candidate.TaskID != evidence.TaskID || candidate.CorrelationID != evidence.CorrelationID {
				continue
			}
			payload, present, err := events.AdmittedProjection(candidate)
			if err != nil {
				return fmt.Errorf("event %s: inspect Agent evidence execution admission: %w", evidence.EventID, err)
			}
			if !present || !reflect.DeepEqual(payload.Projection, taskRecord) {
				continue
			}
			if start.EventID != "" {
				return fmt.Errorf("event %s has multiple exact Agent execution starts", evidence.EventID)
			}
			start = candidate
		}
		if err := events.ValidateAgentEvidencePublished(evidence, task, taskVersion, start, stream[:evidenceIndex+1]); err != nil {
			return fmt.Errorf("event %s: %w", evidence.EventID, err)
		}
	}
	return nil
}

func validateRecoveryIntentConfirmations(stream []events.Event) error {
	reviewEvidence := events.IndexReviewedIntentEvidence(stream)
	for _, event := range stream {
		if event.EventType != "INTAKE_ABANDONED" {
			continue
		}
		if err := events.ValidateIndexedIntakeAbandonment(reviewEvidence.At(event), event); err != nil {
			return fmt.Errorf("event %s: %w", event.EventID, err)
		}
	}
	replacements := make(map[core.ID]string)
	for _, event := range stream {
		if event.EventType != "INTENT_CONFIRMED" {
			continue
		}
		var confirmation events.IntentConfirmedPayload
		if decodeExactJSON(event.Payload, &confirmation) != nil {
			return fmt.Errorf("event %s contains an invalid intent confirmation", event.EventID)
		}
		if err := validateRecoveryReplacementConfirmation(stream, event, confirmation); err != nil {
			return fmt.Errorf("event %s: %w", event.EventID, err)
		}
		if predecessorID := core.ID(confirmation.ReplacesWorkID); predecessorID != "" {
			if correlationID, duplicate := replacements[predecessorID]; duplicate && correlationID != event.CorrelationID {
				return fmt.Errorf("event %s: failed Work has multiple reviewed replacements", event.EventID)
			}
			replacements[predecessorID] = event.CorrelationID
		}
		if confirmation.GoalID == "" {
			if err := events.ValidateIndexedReviewedIntentAdmission(reviewEvidence.At(event), event); err != nil {
				return fmt.Errorf("event %s: %w", event.EventID, err)
			}
			continue
		}
		goal, err := recoveryActiveGoalAtSequence(stream, core.ID(confirmation.GoalID), event.Sequence)
		if err != nil {
			return fmt.Errorf("event %s: %w", event.EventID, err)
		}
		if err := events.ValidateIndexedReviewedGoalIntentAdmission(reviewEvidence.At(event), event, goal); err != nil {
			return fmt.Errorf("event %s: %w", event.EventID, err)
		}
	}
	return nil
}

func validateProjectionOrganizationBindings(admitted []admittedProjectionEvent, stream []events.Event) error {
	sort.Slice(admitted, func(left, right int) bool {
		return admitted[left].event.Sequence < admitted[right].event.Sequence
	})
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
	reviewEvidence := events.IndexReviewedIntentEvidence(stream)
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
			if err := validateRecoveryOrganizationParent(value.OrganizationID, snapshot); err != nil {
				return fmt.Errorf("event %s Mission projection: %w", event.EventID, err)
			}
			if err := setRecoveryProjection(snapshot.Missions, record, value, false, core.ValidMissionRevision); err != nil {
				return fmt.Errorf("event %s contains invalid Mission history: %w", event.EventID, err)
			}
		case "goal":
			var value core.Goal
			if decodeExactJSON(record.Value, &value) != nil || string(value.ID) != record.RecordID {
				return fmt.Errorf("event %s contains an invalid Goal projection", event.EventID)
			}
			organizationID = value.OrganizationID
			if err := validateRecoveryOrganizationParent(value.OrganizationID, snapshot); err != nil {
				return fmt.Errorf("event %s Goal projection: %w", event.EventID, err)
			}
			mission, found := snapshot.Missions[value.MissionID]
			if !found || mission.Value.ID != value.MissionID || mission.Value.OrganizationID != value.OrganizationID {
				return fmt.Errorf("event %s Goal projection requires its durable same-organization Mission", event.EventID)
			}
			if err := setRecoveryProjection(snapshot.Goals, record, value, false, core.ValidGoalRevision); err != nil {
				return fmt.Errorf("event %s contains invalid Goal history: %w", event.EventID, err)
			}
		case "team":
			var value core.Team
			if decodeExactJSON(record.Value, &value) != nil || string(value.ID) != record.RecordID {
				return fmt.Errorf("event %s contains an invalid Team projection", event.EventID)
			}
			organizationID = value.OrganizationID
			if err := validateRecoveryTeamRoster(value, snapshot); err != nil {
				return fmt.Errorf("event %s contains an invalid Team roster: %w", event.EventID, err)
			}
			if err := setRecoveryProjection(snapshot.Teams, record, value, false, core.ValidTeamRevision); err != nil {
				return fmt.Errorf("event %s contains invalid Team history: %w", event.EventID, err)
			}
		case "agent_blueprint":
			var value core.AgentBlueprint
			if decodeExactJSON(record.Value, &value) != nil || string(value.ID) != record.RecordID || !core.ValidAgentBlueprint(value) {
				return fmt.Errorf("event %s contains an invalid Agent blueprint projection", event.EventID)
			}
			organizationID = value.OrganizationID
			if err := validateRecoveryOrganizationParent(value.OrganizationID, snapshot); err != nil {
				return fmt.Errorf("event %s Agent blueprint projection: %w", event.EventID, err)
			}
			if err := setRecoveryProjection(snapshot.AgentBlueprints, record, value, false, core.ValidAgentBlueprintRevision); err != nil {
				return fmt.Errorf("event %s contains invalid Agent blueprint history: %w", event.EventID, err)
			}
		case "execution_profile":
			var value core.ExecutionProfile
			if decodeExactJSON(record.Value, &value) != nil || string(value.ID) != record.RecordID || !core.ValidExecutionProfile(value) {
				return fmt.Errorf("event %s contains an invalid execution profile projection", event.EventID)
			}
			organizationID = value.OrganizationID
			if err := validateRecoveryOrganizationParent(value.OrganizationID, snapshot); err != nil {
				return fmt.Errorf("event %s execution profile projection: %w", event.EventID, err)
			}
			if err := setRecoveryProjection(snapshot.ExecutionProfiles, record, value, false, core.ValidExecutionProfileRevision); err != nil {
				return fmt.Errorf("event %s contains invalid execution profile history: %w", event.EventID, err)
			}
		case "agent":
			var value core.Agent
			if decodeExactJSON(record.Value, &value) != nil || string(value.ID) != record.RecordID {
				return fmt.Errorf("event %s contains an invalid Agent projection", event.EventID)
			}
			organizationID = value.OrganizationID
			if err := validateRecoveryOrganizationParent(value.OrganizationID, snapshot); err != nil {
				return fmt.Errorf("event %s Agent projection: %w", event.EventID, err)
			}
			blueprint, blueprintFound := snapshot.AgentBlueprints[value.BlueprintID]
			profile, profileFound := snapshot.ExecutionProfiles[value.ExecutionProfileID]
			if !blueprintFound || !profileFound || !core.ValidAgentConfigurationBinding(value, blueprint.Value, profile.Value) {
				return fmt.Errorf("event %s Agent projection references invalid pinned configuration at admission", event.EventID)
			}
			if err := setRecoveryProjection(snapshot.Agents, record, value, false, core.ValidAgentRevision); err != nil {
				return fmt.Errorf("event %s contains invalid Agent history: %w", event.EventID, err)
			}
		case "intent":
			var value core.Intent
			if decodeExactJSON(record.Value, &value) != nil || string(value.ID) != record.RecordID {
				return fmt.Errorf("event %s contains an invalid Intent projection", event.EventID)
			}
			organizationID = value.OrganizationID
			if err := validateRecoveryOrganizationParent(value.OrganizationID, snapshot); err != nil {
				return fmt.Errorf("event %s Intent projection: %w", event.EventID, err)
			}
			if err := setRecoveryProjection(snapshot.Intents, record, value, true, nil); err != nil {
				return fmt.Errorf("event %s contains invalid Intent history: %w", event.EventID, err)
			}
		case "work":
			var value core.Work
			if decodeExactJSON(record.Value, &value) != nil || string(value.ID) != record.RecordID {
				return fmt.Errorf("event %s contains an invalid Work projection", event.EventID)
			}
			if err := validateRecoveryWorkIntentBinding(event, record, value, snapshot, stream, reviewEvidence); err != nil {
				return fmt.Errorf("event %s contains an invalid Work binding: %w", event.EventID, err)
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
			if event.EventType == "EXECUTION_STARTED" && value.ExecutionKind == core.ExecutionAgent {
				if err := events.ValidateAgentDispatchStart(event, value, record.Version, stream); err != nil {
					return fmt.Errorf("event %s contains invalid Agent dispatch admission: %w", event.EventID, err)
				}
			}
			if err := validateRecoveryTaskWorkBinding(event, record, value, snapshot); err != nil {
				return fmt.Errorf("event %s contains an invalid Task binding: %w", event.EventID, err)
			}
			work := snapshot.Works[value.WorkID].Value
			intent := snapshot.Intents[work.IntentID].Value
			if err := core.ValidateTaskAssignment(value, intent.OrganizationID, snapshot); err != nil {
				return fmt.Errorf("event %s contains an invalid Task assignment: %w", event.EventID, err)
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
			blueprint, blueprintFound := snapshot.AgentBlueprints[value.BlueprintID]
			profile, profileFound := snapshot.ExecutionProfiles[value.ExecutionProfileID]
			if !blueprintFound || !profileFound || !core.ValidAgentConfigurationBinding(value, blueprint.Value, profile.Value) {
				return fmt.Errorf("event %s Agent projection references an invalid pinned configuration", event.EventID)
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

func validateRecoveryTeamRoster(team core.Team, snapshot core.DurableGraph) error {
	return core.ValidateTeamRoster(team, snapshot)
}

func validateRecoveryOrganizationParent(organizationID core.ID, snapshot core.DurableGraph) error {
	organization, found := snapshot.Organizations[organizationID]
	if !found || organization.Value.ID != organizationID {
		return fmt.Errorf("requires its durable parent Organization")
	}
	return nil
}

func validateRecoveryWorkIntentBinding(event events.Event, record events.ProjectionRecord, work core.Work, snapshot core.DurableGraph, stream []events.Event, reviewEvidence events.ReviewedIntentEvidenceIndex) error {
	intentState, found := snapshot.Intents[work.IntentID]
	if !found {
		return fmt.Errorf("work requires its durable Intent")
	}
	intent := intentState.Value
	if intentState.CorrelationID != record.CorrelationID || intent.ID != work.IntentID || intent.GoalID != work.GoalID || intent.ReplacesWorkID != work.ReplacesWorkID || intent.NormalizedObjective != work.Objective || string(intent.OrganizationID) != event.OrganizationID {
		return fmt.Errorf("work does not match its accepted Intent boundary")
	}
	if intentRequiresConfirmation(intent) {
		var confirmations []events.Event
		for _, candidate := range stream {
			if candidate.EventType == "INTENT_CONFIRMED" && candidate.CorrelationID == record.CorrelationID {
				confirmations = append(confirmations, candidate)
			}
		}
		if len(confirmations) != 1 || confirmations[0].Sequence >= event.Sequence {
			return fmt.Errorf("external Work requires one prior intent confirmation")
		}
		if err := events.ValidateIntentConfirmation(reviewEvidence.At(confirmations[0]), confirmations[0], intent); err != nil {
			return err
		}
		var confirmation events.IntentConfirmedPayload
		if decodeExactJSON(confirmations[0].Payload, &confirmation) != nil {
			return fmt.Errorf("replacement Work confirmation is invalid")
		}
		if err := validateRecoveryReplacementConfirmation(stream, confirmations[0], confirmation); err != nil {
			return err
		}
		if intent.GoalID != "" {
			goal, err := recoveryActiveGoalAtSequence(stream, intent.GoalID, confirmations[0].Sequence)
			if err != nil {
				return err
			}
			if err := events.ValidateIndexedReviewedGoalIntentAdmission(reviewEvidence.At(confirmations[0]), confirmations[0], goal); err != nil {
				return err
			}
		} else if err := events.ValidateIndexedReviewedIntentAdmission(reviewEvidence.At(confirmations[0]), confirmations[0]); err != nil {
			return err
		}
	}
	return nil
}

func intentRequiresConfirmation(intent core.Intent) bool {
	return intent.GoalID != "" || intent.ReplacesWorkID != "" || intent.SourceChannel == "HUMAN_DIRECT" || intent.SourceChannel == "A2A" || intent.SourcePrincipalKind == core.PrincipalHuman || intent.SourcePrincipalKind == core.PrincipalExternalAgent
}

func validateRecoveryReplacementConfirmation(stream []events.Event, confirmationEvent events.Event, confirmation events.IntentConfirmedPayload) error {
	predecessorID := core.ID(confirmation.ReplacesWorkID)
	if predecessorID == "" {
		return nil
	}
	var predecessor core.Work
	var predecessorCorrelation string
	for _, candidate := range stream {
		if candidate.Sequence >= confirmationEvent.Sequence {
			continue
		}
		payload, present, err := events.AdmittedProjection(candidate)
		if err != nil {
			return err
		}
		if !present || payload.Projection.ProjectionKind != "work" || payload.Projection.RecordID != string(predecessorID) {
			continue
		}
		if decodeExactJSON(payload.Projection.Value, &predecessor) != nil || predecessor.ID != predecessorID {
			return fmt.Errorf("reviewed replacement references an invalid predecessor Work admission")
		}
		predecessorCorrelation = payload.Projection.CorrelationID
	}
	if predecessor.ID != predecessorID || predecessor.Status != core.WorkFailed || predecessor.GoalID != core.ID(confirmation.GoalID) {
		return fmt.Errorf("reviewed replacement requires its prior failed Work with the same Goal binding at admission")
	}
	var predecessorIntent core.Intent
	for _, candidate := range stream {
		if candidate.Sequence >= confirmationEvent.Sequence {
			continue
		}
		payload, present, err := events.AdmittedProjection(candidate)
		if err != nil {
			return err
		}
		if !present || payload.Projection.ProjectionKind != "intent" || payload.Projection.RecordID != string(predecessor.IntentID) {
			continue
		}
		if decodeExactJSON(payload.Projection.Value, &predecessorIntent) != nil || predecessorIntent.ID != predecessor.IntentID {
			return fmt.Errorf("reviewed replacement references an invalid predecessor Intent admission")
		}
		if payload.Projection.CorrelationID != predecessorCorrelation {
			return fmt.Errorf("reviewed replacement predecessor crosses its correlation boundary")
		}
	}
	if predecessorIntent.ID != predecessor.IntentID || string(predecessorIntent.OrganizationID) != confirmationEvent.OrganizationID {
		return fmt.Errorf("reviewed replacement Work crosses its organization boundary")
	}
	return nil
}

func recoveryActiveGoalAtSequence(stream []events.Event, goalID core.ID, confirmationSequence int64) (core.Goal, error) {
	var goal core.Goal
	for _, candidate := range stream {
		if candidate.Sequence >= confirmationSequence {
			continue
		}
		payload, present, err := events.AdmittedProjection(candidate)
		if err != nil {
			return core.Goal{}, err
		}
		if !present || payload.Projection.ProjectionKind != "goal" || payload.Projection.RecordID != string(goalID) {
			continue
		}
		if decodeExactJSON(payload.Projection.Value, &goal) != nil || goal.ID != goalID {
			return core.Goal{}, fmt.Errorf("goal-bound intent confirmation references an invalid Goal admission")
		}
	}
	if goal.ID != goalID || goal.Status != core.GoalActive {
		return core.Goal{}, fmt.Errorf("goal-bound intent confirmation requires its active Goal at admission")
	}
	return goal, nil
}

func validateRecoveryTaskWorkBinding(event events.Event, record events.ProjectionRecord, task core.Task, snapshot core.DurableGraph) error {
	workState, found := snapshot.Works[task.WorkID]
	if !found || workState.CorrelationID != record.CorrelationID || workState.Value.ID != task.WorkID || workState.Value.Status != core.WorkActive {
		return fmt.Errorf("task requires its exact active Work on the same correlation boundary")
	}
	intentState, found := snapshot.Intents[workState.Value.IntentID]
	if !found || intentState.CorrelationID != record.CorrelationID || intentState.Value.ID != workState.Value.IntentID || string(intentState.Value.OrganizationID) != event.OrganizationID {
		return fmt.Errorf("task requires its exact Intent organization and correlation boundary")
	}
	return nil
}

func setRecoveryProjection[T any](target map[core.ID]core.DurableState[T], record events.ProjectionRecord, value T, correlationStable bool, validRevision func(T, T) bool) error {
	return core.AdmitDurableRevision(target, core.ID(record.RecordID), record.Version, record.CorrelationID, value, correlationStable, validRevision)
}

func validateRecoveryLifecycle[T any](record events.ProjectionRecord, eventType string, history map[core.ID]T, identity func(T) core.ID, validate func(string, int, *T, T) error) error {
	var value T
	if decodeExactJSON(record.Value, &value) != nil || identity(value) != core.ID(record.RecordID) {
		return fmt.Errorf("contains an invalid lifecycle value")
	}
	previous, found := history[identity(value)]
	var prior *T
	if found {
		prior = &previous
	}
	if err := validate(eventType, record.Version, prior, value); err != nil {
		return err
	}
	history[identity(value)] = value
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

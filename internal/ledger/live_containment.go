package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dominicnunez/agentos/internal/authority"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"modernc.org/sqlite"
)

type liveContainment struct {
	mu    sync.Mutex
	next  uint64
	calls map[uint64]liveCall
}
type liveCall struct {
	organization string
	cancel       context.CancelCauseFunc
}

type containmentGenerationKey struct{}
type containmentGeneration struct {
	organization string
	epoch        int64
}

// Bound how long admitted work can continue without a successful authority read.
const containmentObservationTimeout = time.Second

func containmentReadFailure(err error) error {
	if err == nil || errors.Is(err, core.ErrOrganizationFrozen) {
		return err
	}
	return errors.Join(core.ErrContainmentUnavailable, err)
}

// CheckInferenceContext rechecks committed containment at the adapter boundary.
// It does not claim atomicity with a remote provider or undo a dispatched call.
func (l *SQLite) CheckInferenceContext(ctx context.Context, organization string) (resultErr error) {
	defer func() {
		resultErr = containmentReadFailure(resultErr)
	}()
	if _, ok := ctx.Value(containmentGenerationKey{}).(containmentGeneration); !ok {
		return fmt.Errorf("inference containment generation is required")
	}
	return l.withContainmentSnapshot(ctx, func(tx *sql.Tx) error {
		return validatePreparationGeneration(ctx, tx, organization)
	})
}

// BeginInferenceContext registers before admission under the same lock used to
// commit and signal organization freezes. Releasing a freeze never revives an
// already-cancelled call. Reconciliation uses its own uncancelled context.
func (l *SQLite) BeginInferenceContext(ctx context.Context, organization string) (context.Context, func(), error) {
	return l.BeginExecutionContext(ctx, organization)
}

// BeginExecutionContext applies the same organization containment to task
// handlers, including handlers that do not invoke inference.
func (l *SQLite) BeginExecutionContext(ctx context.Context, organization string) (context.Context, func(), error) {
	if ctx == nil || organization == "" {
		return nil, nil, fmt.Errorf("live inference scope is required")
	}
	l.live.mu.Lock()
	defer l.live.mu.Unlock()
	epoch, frozen, err := l.containmentEpoch(ctx, organization)
	if err != nil {
		return nil, nil, containmentReadFailure(err)
	}
	generation := containmentGeneration{organization: organization, epoch: epoch}
	if prior, ok := ctx.Value(containmentGenerationKey{}).(containmentGeneration); ok {
		if prior.organization != organization {
			return nil, nil, fmt.Errorf("containment context crosses organizations")
		}
		generation = prior
		_, hold, err := l.containmentSince(ctx, organization, prior.epoch)
		if err != nil {
			return nil, nil, containmentReadFailure(err)
		}
		if hold != nil {
			return nil, nil, *hold
		}
	}
	if frozen {
		_, hold, err := l.containmentSince(ctx, organization, epoch-1)
		if err != nil {
			return nil, nil, containmentReadFailure(err)
		}
		if hold == nil {
			return nil, nil, containmentReadFailure(fmt.Errorf("frozen admission lost its authority identity"))
		}
		return nil, nil, *hold
	}
	epoch = generation.epoch
	callCtx, cancel := context.WithCancelCause(context.WithValue(ctx, containmentGenerationKey{}, generation))
	if l.live.calls == nil {
		l.live.calls = map[uint64]liveCall{}
	}
	l.live.next++
	id := l.live.next
	l.live.calls[id] = liveCall{organization, cancel}
	done := make(chan struct{})
	// The watchdog is independent of the poll: even a stalled snapshot cannot
	// leave the provider running indefinitely. Only a successful read renews it.
	watchdog := time.AfterFunc(containmentObservationTimeout, func() { cancel(core.ErrContainmentUnavailable) })
	go func() {
		defer close(done)
		defer watchdog.Stop()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-callCtx.Done():
				return
			case <-ticker.C:
				checkCtx, stop := context.WithTimeout(callCtx, 250*time.Millisecond)
				next, cause, checkErr := l.containmentSince(checkCtx, organization, epoch)
				stop()
				if checkErr != nil {
					// Brief contention is tolerated only within the watchdog's
					// bounded observation window; failed polls never renew it.
					if containmentReadContended(checkErr) && callCtx.Err() == nil {
						continue
					}
					cancel(core.ErrContainmentUnavailable)
					return
				}
				if cause != nil {
					l.live.mu.Lock()
					cancel(*cause)
					l.live.mu.Unlock()
					return
				}
				epoch = next
				watchdog.Reset(containmentObservationTimeout)
			}
		}
	}()
	return callCtx, func() {
		cancel(nil)
		<-done
		l.live.mu.Lock()
		delete(l.live.calls, id)
		l.live.mu.Unlock()
	}, nil
}

// containmentSince resolves every intervening authority revision in one
// snapshot, retaining the first freeze even when a later release is visible.
// A release alone is not evidence that this execution was held.
func (l *SQLite) containmentSince(ctx context.Context, organization string, after int64) (next int64, cause *core.SecurityHoldCause, resultErr error) {
	next = after
	resultErr = l.withContainmentSnapshot(ctx, func(tx *sql.Tx) error {
		var err error
		next, cause, err = containmentSinceTx(ctx, tx, organization, after)
		return err
	})
	return next, cause, resultErr
}

func (l *SQLite) withContainmentSnapshot(ctx context.Context, read func(*sql.Tx) error) (resultErr error) {
	conn, err := l.watchDB.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	// SQLite's busy handler may outlive a query cancellation. Bound each wait
	// well inside the observation window, then restore this connection's setting.
	var busyTimeout int
	if err := conn.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&busyTimeout); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "PRAGMA busy_timeout=50"); err != nil {
		return err
	}
	defer func() {
		_, err := conn.ExecContext(context.WithoutCancel(ctx), fmt.Sprintf("PRAGMA busy_timeout=%d", busyTimeout))
		resultErr = errors.Join(resultErr, err)
	}()
	// The entire snapshot retains cancellation, including transaction begin.
	// Open keeps private-memory authority alive across discarded connections.
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	return read(tx)
}

func containmentReadContended(err error) bool {
	var sqliteErr *sqlite.Error
	// Extended SQLite error codes retain primary BUSY (5) / LOCKED (6) in
	// their low byte. Retry only these transient lock failures or a deadline.
	return errors.Is(err, context.DeadlineExceeded) || errors.As(err, &sqliteErr) && (sqliteErr.Code()&255 == 5 || sqliteErr.Code()&255 == 6)
}

func containmentSinceTx(ctx context.Context, tx *sql.Tx, organization string, after int64) (int64, *core.SecurityHoldCause, error) {
	_, latest, found, err := latestAuthorityAdmission(ctx, tx, "organization_freeze", organization)
	if err != nil {
		return after, nil, err
	}
	if !found {
		if after != 0 {
			return after, nil, fmt.Errorf("containment authority disappeared")
		}
		return after, nil, nil
	}
	if latest.Sequence < after {
		return after, nil, fmt.Errorf("containment authority regressed")
	}
	rows, err := tx.QueryContext(ctx, `SELECT r.kind,r.record_id,r.version,r.body,r.admission_event_id
FROM records r JOIN events e ON e.event_id=r.admission_event_id
WHERE r.kind='organization_freeze' AND r.record_id=? AND e.sequence>? AND e.sequence<=?
ORDER BY e.sequence`, organization, after, latest.Sequence)
	if err != nil {
		return after, nil, err
	}
	var records []events.AuthorityRecord
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var record events.AuthorityRecord
		if err := rows.Scan(&record.Kind, &record.RecordID, &record.Version, &record.Body, &record.AdmissionEventID); err != nil {
			return after, nil, err
		}
		records = append(records, record)
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return after, nil, err
	}
	var cause *core.SecurityHoldCause
	for _, record := range records {
		_, admission, _, err := validateSelectedAuthorityAdmission(ctx, tx, record, true)
		if err != nil {
			return after, nil, err
		}
		var state authority.FreezeState
		if decodeExactJSONBytes(record.Body, &state) != nil || string(state.OrganizationID) != organization || admission.OrganizationID != organization {
			return after, nil, fmt.Errorf("invalid containment authority")
		}
		if state.Frozen && cause == nil {
			cause = &core.SecurityHoldCause{OrganizationID: core.ID(organization), EventRef: admission.EventID, Sequence: admission.Sequence}
		}
	}
	return latest.Sequence, cause, nil
}

// validatePreparationGeneration uses the writer snapshot, independent of the
// cancellation monitor's delivery latency. Context values survive derived
// timeouts and preserve the original preparation boundary for nested calls.
func validatePreparationGeneration(ctx context.Context, tx *sql.Tx, organization string) error {
	generation, ok := ctx.Value(containmentGenerationKey{}).(containmentGeneration)
	if !ok {
		return nil
	}
	if generation.organization != organization {
		return fmt.Errorf("containment preparation crosses organizations")
	}
	_, hold, err := containmentSinceTx(ctx, tx, organization, generation.epoch)
	if err != nil {
		return err
	}
	if hold != nil {
		return *hold
	}
	return nil
}

// containmentEpoch reads validated authority and its event sequence in one
// snapshot. Sequence changes latch cancellation rather than resuming old calls.
func (l *SQLite) containmentEpoch(ctx context.Context, organization string) (epoch int64, frozen bool, resultErr error) {
	resultErr = l.withContainmentSnapshot(ctx, func(tx *sql.Tx) error {
		record, event, found, err := latestAuthorityAdmission(ctx, tx, "organization_freeze", organization)
		if err != nil || !found {
			return err
		}
		var state authority.FreezeState
		if decodeExactJSONBytes(record.Body, &state) != nil || string(state.OrganizationID) != organization || event.OrganizationID != organization {
			return fmt.Errorf("invalid containment authority")
		}
		epoch, frozen = event.Sequence, state.Frozen
		return nil
	})
	return epoch, frozen, resultErr
}

func (l *SQLite) cancelOrganizationLocked(organization string, cause error) {
	for _, call := range l.live.calls {
		if call.organization == organization {
			call.cancel(cause)
		}
	}
}

// SuspendHeldExecution recovers a running revision from durable hold history.
// It does not depend on an outcome event surviving the interrupted process.
func (l *SQLite) SuspendHeldExecution(ctx context.Context, organization, taskID, correlation string, version int) (bool, error) {
	suspended := false
	err := l.withTx(ctx, func(tx *sql.Tx) error {
		record, task, found, err := latestProjectionRevision[core.Task](ctx, tx, "task", taskID)
		if err != nil {
			return err
		}
		if !found || record.Version != version || record.CorrelationID != correlation || task.Status != core.TaskRunning {
			return fmt.Errorf("running execution changed before hold recovery")
		}
		start, err := exactProjectionTransition(ctx, tx, "EXECUTION_STARTED", record)
		if err != nil {
			return err
		}
		if start.OrganizationID != organization {
			return fmt.Errorf("hold recovery crosses its execution organization")
		}
		executionID, err := events.ContainmentExecutionID(start)
		if err != nil {
			return err
		}
		var boundary int64
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MIN(sequence),0) FROM events WHERE organization_id=? AND task_id=? AND correlation_id=? AND source_execution_id=? AND event_type IN ('COMPLETION_REVIEW_REQUESTED','COMPLETION_VERIFIED') AND sequence>?`, organization, taskID, correlation, executionID, start.Sequence).Scan(&boundary); err != nil {
			return err
		}
		hold, err := executionIntervalHoldThrough(ctx, tx, events.TrustedDraft{OrganizationID: organization, TaskID: taskID, CorrelationID: correlation, SourceExecutionID: executionID}, boundary)
		if err != nil {
			return err
		}
		outcomeRef := ""
		if hold == nil {
			outcomes, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE organization_id=? AND task_id=? AND correlation_id=? AND source_execution_id=? AND event_type='TOOL_OUTCOME_RECORDED' AND sequence>? AND json_extract(payload,'$.error_class')='containment_unavailable' ORDER BY sequence`, organization, taskID, correlation, executionID, start.Sequence))
			if err != nil {
				return err
			}
			for _, event := range outcomes {
				if boundary != 0 && event.Sequence >= boundary {
					continue
				}
				var outcome core.ToolOutcome
				var evidence core.ExecutionInterruptionEvidence
				if event.SourceActorID != "runtime" || decodeExactJSONBytes(event.Payload, &outcome) != nil || !outcome.Valid() || outcome.ToolID != "runtime-containment" || outcome.Status != core.OutcomeFailed || outcome.PostconditionStatus != core.PostconditionNotChecked || outcome.Retryability != core.NotRetryable || decodeExactJSON(outcome.ObservedEffect, &evidence) != nil || !evidence.LocalExecutionStopped || evidence.ExternalEffectsStatus != "REQUIRES_RECONCILIATION" {
					return fmt.Errorf("invalid containment-unavailable recovery evidence")
				}
				outcomeRef = event.EventID
			}
			if outcomeRef == "" {
				return nil
			}
		}
		task.Status = core.TaskBlocked
		item, err := prepareProjection(events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: organization, EventType: "TASK_EXECUTION_SUSPENDED", SourceActorID: "runtime", TaskID: taskID, CorrelationID: correlation, Payload: map[string]any{"hold": hold, "outcome_event_ref": outcomeRef, "execution_start_ref": start.EventID, "reason": "containment interrupted execution; operator reconciliation required"}}, ProjectionKind: "task", RecordID: taskID, Version: version + 1, Value: task}, false, false)
		if err != nil {
			return err
		}
		if _, err := appendPreparedProjection(ctx, tx, item); err != nil {
			return err
		}
		suspended = true
		return nil
	})
	return suspended && err == nil, err
}

// validateContainedOutcome runs in the outcome writer transaction, so a
// committed freeze cannot race the check and successful result admission.
func validateContainedOutcome(ctx context.Context, tx *sql.Tx, draft events.TrustedDraft) error {
	var outcome core.ToolOutcome
	if decodeExactJSON(draft.Payload, &outcome) != nil || !outcome.Valid() {
		return fmt.Errorf("invalid containment outcome")
	}
	frozen, err := organizationFrozenAtSequence(ctx, tx, core.ID(draft.OrganizationID), 0)
	if err != nil {
		return err
	}
	historicalHold, err := executionIntervalHold(ctx, tx, draft)
	if err != nil {
		if frozen {
			return fmt.Errorf("%w: %w", core.ErrOrganizationFrozen, err)
		}
		return err
	}
	if historicalHold != nil && outcome.ErrorClass != "security_hold" {
		return *historicalHold
	}
	if frozen && outcome.ErrorClass != "security_hold" {
		return core.ErrOrganizationFrozen
	}
	if outcome.ErrorClass != "security_hold" {
		return nil
	}
	var evidence core.ExecutionInterruptionEvidence
	if outcome.ToolID != "runtime-containment" || outcome.Status != core.OutcomeFailed || outcome.PostconditionStatus != core.PostconditionNotChecked || outcome.Retryability != core.NotRetryable || len(outcome.ArtifactRefs) != 0 || decodeExactJSON(outcome.ObservedEffect, &evidence) != nil || evidence.Hold == nil || !evidence.LocalExecutionStopped || evidence.ExternalEffectsStatus != "REQUIRES_RECONCILIATION" {
		return fmt.Errorf("invalid security hold evidence")
	}
	hold := evidence.Hold
	if historicalHold == nil || *historicalHold != *hold {
		return fmt.Errorf("security hold is outside this execution interval")
	}
	if string(hold.OrganizationID) != draft.OrganizationID || hold.EventRef == "" || hold.Sequence <= 0 {
		return fmt.Errorf("invalid security hold identity")
	}
	var record events.AuthorityRecord
	if err := tx.QueryRowContext(ctx, `SELECT kind,record_id,version,body,admission_event_id FROM records WHERE kind='organization_freeze' AND record_id=? AND admission_event_id=?`, draft.OrganizationID, hold.EventRef).Scan(&record.Kind, &record.RecordID, &record.Version, &record.Body, &record.AdmissionEventID); err != nil {
		return fmt.Errorf("security hold lacks authority record: %w", err)
	}
	_, admission, _, err := validateSelectedAuthorityAdmission(ctx, tx, record, true)
	if err != nil {
		return err
	}
	var state authority.FreezeState
	if decodeExactJSONBytes(record.Body, &state) != nil || !state.Frozen || state.OrganizationID != hold.OrganizationID || admission.Sequence != hold.Sequence {
		return fmt.Errorf("security hold reference is not a committed freeze")
	}
	return nil
}

// validateExecutionPublication protects each subsequent publication in its
// writer transaction, including holds committed after outcome admission.
func validateExecutionPublication(ctx context.Context, tx *sql.Tx, draft events.TrustedDraft) error {
	if err := validatePreparationGeneration(ctx, tx, draft.OrganizationID); err != nil {
		return err
	}
	if draft.SourceExecutionID != "" {
		var boundary int64
		if draft.EventType == "COMPLETION_VERIFIED" {
			if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MIN(sequence),0) FROM events WHERE organization_id=? AND task_id=? AND correlation_id=? AND source_execution_id=? AND event_type='COMPLETION_REVIEW_REQUESTED'`, draft.OrganizationID, draft.TaskID, draft.CorrelationID, draft.SourceExecutionID).Scan(&boundary); err != nil {
				return err
			}
		}
		hold, err := executionIntervalHoldThrough(ctx, tx, draft, boundary)
		if err != nil {
			return err
		}
		if hold != nil {
			return *hold
		}
	}
	frozen, err := organizationFrozenAtSequence(ctx, tx, core.ID(draft.OrganizationID), 0)
	if err != nil {
		return err
	}
	if frozen {
		return core.ErrOrganizationFrozen
	}
	return nil
}

// Terminal task writes also prove the durable execution interval when callers
// do not carry a live context (for example human continuation or recovery).
func validateTerminalTaskContainment(ctx context.Context, tx *sql.Tx, item preparedProjection) error {
	if item.task == nil {
		return nil
	}
	var suspended bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records r JOIN events e ON e.event_id=r.admission_event_id WHERE r.kind='task' AND r.record_id=? AND r.version=(SELECT MAX(version) FROM records WHERE kind='task' AND record_id=?) AND e.event_type='TASK_EXECUTION_SUSPENDED')`, item.record.RecordID, item.record.RecordID).Scan(&suspended); err != nil {
		return err
	}
	if suspended {
		return fmt.Errorf("suspended execution requires security reconciliation: %w", core.ErrOrganizationFrozen)
	}
	requiresCheck := item.task.Status == core.TaskCompleted || item.task.Status == core.TaskFailed || item.eventDraft.EventType == "TASK_RECOVERED"
	if item.task.Status == core.TaskBlocked && item.eventDraft.EventType != "TASK_EXECUTION_SUSPENDED" {
		_, previous, found, err := latestProjectionRevision[core.Task](ctx, tx, "task", item.record.RecordID)
		if err != nil {
			return err
		}
		requiresCheck = found && previous.Status == core.TaskRunning
	}
	if !requiresCheck {
		return nil
	}
	draft := item.eventDraft
	_, _, found, err := latestAuthorityAdmission(ctx, tx, "organization_freeze", draft.OrganizationID)
	if err != nil || !found {
		return err
	}
	starts, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE organization_id=? AND task_id=? AND correlation_id=? AND event_type='EXECUTION_STARTED' ORDER BY sequence DESC LIMIT 1`, draft.OrganizationID, draft.TaskID, draft.CorrelationID))
	if err != nil {
		return err
	}
	if len(starts) != 0 {
		draft.SourceExecutionID, err = events.ContainmentExecutionID(starts[0])
		if err != nil {
			return err
		}
		if draft.EventType == "TASK_VERIFIED_COMPLETE" || draft.EventType == "COMPLETION_REJECTED" || draft.EventType == "TASK_BLOCKED" {
			var boundary int64
			err := tx.QueryRowContext(ctx, `SELECT COALESCE(MIN(sequence),0) FROM events WHERE organization_id=? AND task_id=? AND correlation_id=? AND source_execution_id=? AND event_type IN ('COMPLETION_REVIEW_REQUESTED','COMPLETION_VERIFIED') AND sequence>?`, draft.OrganizationID, draft.TaskID, draft.CorrelationID, draft.SourceExecutionID, starts[0].Sequence).Scan(&boundary)
			if err != nil {
				return err
			}
			if boundary != 0 {
				hold, err := executionIntervalHoldThrough(ctx, tx, draft, boundary)
				if err != nil {
					return err
				}
				if hold != nil {
					return *hold
				}
				if draft.EventType == "TASK_BLOCKED" {
					frozen, err := organizationFrozenAtSequence(ctx, tx, core.ID(draft.OrganizationID), 0)
					if err != nil {
						return err
					}
					if frozen {
						hold, err := executionIntervalHold(ctx, tx, draft)
						if err != nil {
							return err
						}
						if hold != nil {
							return *hold
						}
					}
				}
				// Independent review acts on an already-admitted candidate.
				// Current holds still block decisions; later released holds do
				// not invalidate the finished execution's candidate.
				draft.SourceExecutionID = ""
			}
		}
	}
	return validateExecutionPublication(ctx, tx, draft)
}

// executionIntervalHold retains the earliest freeze after this exact execution
// start, even if a release has since permitted a newer execution to run.
func executionIntervalHold(ctx context.Context, tx *sql.Tx, draft events.TrustedDraft) (*core.SecurityHoldCause, error) {
	return executionIntervalHoldThrough(ctx, tx, draft, 0)
}

func executionIntervalHoldThrough(ctx context.Context, tx *sql.Tx, draft events.TrustedDraft, endSequence int64) (*core.SecurityHoldCause, error) {
	record, admission, found, err := latestAuthorityAdmission(ctx, tx, "organization_freeze", draft.OrganizationID)
	if err != nil || !found {
		return nil, err
	}
	starts, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE organization_id=? AND task_id=? AND correlation_id=? AND (event_type='EXECUTION_STARTED' OR (event_type IN ('INTENT_NORMALIZATION_CONTEXT_MANIFESTED','PLANNING_CONTEXT_MANIFESTED') AND source_execution_id=?)) ORDER BY sequence`, draft.OrganizationID, draft.TaskID, draft.CorrelationID, draft.SourceExecutionID))
	if err != nil {
		return nil, err
	}
	var startSequence int64
	for _, start := range starts {
		executionID := start.SourceExecutionID
		if start.EventType == "EXECUTION_STARTED" {
			executionID, err = events.ContainmentExecutionID(start)
			if err != nil {
				return nil, err
			}
		}
		if executionID != draft.SourceExecutionID {
			continue
		}
		if startSequence != 0 {
			return nil, fmt.Errorf("execution has multiple start admissions")
		}
		startSequence = start.Sequence
	}
	if startSequence == 0 {
		if draft.EventType == "INTENT_NORMALIZATION_CONTEXT_MANIFESTED" || draft.EventType == "PLANNING_CONTEXT_MANIFESTED" {
			return nil, nil // This admission establishes the new interval.
		}
		return nil, fmt.Errorf("containment outcome lacks exact execution start")
	}
	var hold *core.SecurityHoldCause
	for found && admission.Sequence > startSequence {
		var state authority.FreezeState
		if decodeExactJSONBytes(record.Body, &state) != nil || string(state.OrganizationID) != draft.OrganizationID {
			return nil, fmt.Errorf("invalid execution containment authority")
		}
		if state.Frozen && (endSequence == 0 || admission.Sequence < endSequence) {
			hold = &core.SecurityHoldCause{OrganizationID: state.OrganizationID, EventRef: admission.EventID, Sequence: admission.Sequence}
		}
		record, admission, found, err = authorityAdmissionAtBoundary(ctx, tx, "organization_freeze", draft.OrganizationID, admission.Sequence)
		if err != nil {
			return nil, err
		}
	}
	return hold, nil
}

// Bind ordinary failure to its manifested interval inside the admitting writer.
// Callers cannot turn a held attempt into an ordinary finish by omitting scope.
func bindPlanningFailureContainment(ctx context.Context, tx *sql.Tx, draft *events.TrustedDraft) error {
	var detail struct {
		Code             string `json:"code"`
		Reason           string `json:"reason"`
		EvidenceEventRef string `json:"evidence_event_ref,omitempty"`
	}
	if draft.SourceActorID != "runtime" || decodeExactJSON(draft.Payload, &detail) != nil || detail.Code == "" || detail.Reason == "" {
		return fmt.Errorf("invalid planning failure boundary")
	}
	manifests, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE organization_id=? AND correlation_id=? AND event_type='PLANNING_CONTEXT_MANIFESTED' ORDER BY sequence`, draft.OrganizationID, draft.CorrelationID))
	if err != nil {
		return containmentReadFailure(err)
	}
	if len(manifests) != 0 {
		var matched bool
		for _, manifest := range manifests {
			if manifest.EventID != detail.EvidenceEventRef {
				continue
			}
			if manifest.SourceExecutionID == "" || manifest.TaskID == "" || draft.SourceExecutionID != "" && draft.SourceExecutionID != manifest.SourceExecutionID || draft.TaskID != "" && draft.TaskID != manifest.TaskID {
				return fmt.Errorf("planning failure crosses manifest identity")
			}
			draft.SourceExecutionID, draft.TaskID = manifest.SourceExecutionID, manifest.TaskID
			matched = true
		}
		if !matched {
			return fmt.Errorf("planning failure lacks its exact manifested attempt")
		}
	}
	return validateExecutionPublication(ctx, tx, *draft)
}

func validatePlanningRetry(ctx context.Context, tx *sql.Tx, draft events.TrustedDraft) error {
	var unresolved bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events m WHERE m.organization_id=? AND m.correlation_id=? AND m.event_type='PLANNING_CONTEXT_MANIFESTED' AND (m.source_execution_id=? OR NOT EXISTS(SELECT 1 FROM events n WHERE n.event_type='INFERENCE_NOT_SENT' AND n.organization_id=m.organization_id AND n.task_id=m.task_id AND n.correlation_id=m.correlation_id AND n.source_execution_id=m.source_execution_id AND n.sequence>m.sequence)))`, draft.OrganizationID, draft.CorrelationID, draft.SourceExecutionID).Scan(&unresolved); err != nil {
		return err
	}
	if unresolved {
		return core.ErrContainmentUnavailable
	}
	return nil
}

// CheckExecutionContainment lets recovery distinguish held model attempts from
// ordinary failures without retrying inference or relying on a live context.
func (l *SQLite) CheckExecutionContainment(ctx context.Context, organization, taskID, correlation, executionID string) (resultErr error) {
	defer func() { resultErr = containmentReadFailure(resultErr) }()
	if organization == "" || taskID == "" || correlation == "" || executionID == "" {
		return fmt.Errorf("complete execution identity is required")
	}
	return l.withTx(ctx, func(tx *sql.Tx) error {
		draft := events.TrustedDraft{OrganizationID: organization, TaskID: taskID, CorrelationID: correlation, SourceExecutionID: executionID}
		var notSent bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events n JOIN events m ON m.organization_id=n.organization_id AND m.task_id=n.task_id AND m.correlation_id=n.correlation_id AND m.source_execution_id=n.source_execution_id WHERE n.event_type='INFERENCE_NOT_SENT' AND m.event_type='PLANNING_CONTEXT_MANIFESTED' AND n.organization_id=? AND n.task_id=? AND n.correlation_id=? AND n.source_execution_id=? AND n.sequence>m.sequence)`, organization, taskID, correlation, executionID).Scan(&notSent); err != nil {
			return err
		}
		if notSent {
			draft.SourceExecutionID = ""
			return validateExecutionPublication(ctx, tx, draft)
		}
		var suspended bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events s JOIN events m ON m.event_id=json_extract(s.payload,'$.context_event_ref') WHERE s.event_type='PLANNING_CONTAINMENT_SUSPENDED' AND s.organization_id=? AND s.task_id=? AND s.correlation_id=? AND s.source_execution_id=? AND m.event_type='PLANNING_CONTEXT_MANIFESTED' AND m.organization_id=s.organization_id AND m.task_id=s.task_id AND m.correlation_id=s.correlation_id AND m.source_execution_id=s.source_execution_id AND s.sequence>m.sequence)`, organization, taskID, correlation, executionID).Scan(&suspended); err != nil {
			return err
		}
		if suspended {
			return core.ErrContainmentUnavailable
		}
		var finish int64
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MIN(f.sequence),0) FROM events f JOIN events m ON m.event_id=json_extract(f.payload,'$.evidence_event_ref') WHERE f.event_type='PLANNING_FAILED' AND f.organization_id=? AND f.correlation_id=? AND m.event_type='PLANNING_CONTEXT_MANIFESTED' AND m.organization_id=f.organization_id AND m.correlation_id=f.correlation_id AND m.task_id=? AND m.source_execution_id=? AND f.sequence>m.sequence`, organization, correlation, taskID, executionID).Scan(&finish); err != nil {
			return err
		}
		if finish != 0 {
			hold, err := executionIntervalHoldThrough(ctx, tx, draft, finish)
			if err != nil {
				return err
			}
			if hold != nil {
				return *hold
			}
			draft.SourceExecutionID = ""
		}
		if err := validateExecutionPublication(ctx, tx, draft); err != nil {
			return err
		}
		if finish == 0 {
			var unfinished bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events m WHERE m.event_type='PLANNING_CONTEXT_MANIFESTED' AND m.organization_id=? AND m.task_id=? AND m.correlation_id=? AND m.source_execution_id=? AND NOT EXISTS(SELECT 1 FROM events p WHERE p.event_type='PLAN_CREATED' AND p.organization_id=m.organization_id AND p.task_id=m.task_id AND p.correlation_id=m.correlation_id AND p.source_execution_id=m.source_execution_id AND p.sequence>m.sequence))`, organization, taskID, correlation, executionID).Scan(&unfinished); err != nil {
				return err
			}
			if unfinished {
				// A crash can lose the interruption marker. A manifest alone
				// proves admission, not an ordinary failure or a completed plan.
				return core.ErrContainmentUnavailable
			}
		}
		return nil
	})
}

// A new attempt ID cannot silently restart a held normalization of the same
// operator input. Check prior attempts atomically with the new manifest write.
func validateNormalizationRetry(ctx context.Context, tx *sql.Tx, draft events.TrustedDraft) error {
	var next events.IntentNormalizationContextPayload
	if err := decodeExactJSON(draft.Payload, &next); err != nil {
		return err
	}
	prior, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE organization_id=? AND task_id=? AND correlation_id=? AND event_type='INTENT_NORMALIZATION_CONTEXT_MANIFESTED' ORDER BY sequence`, draft.OrganizationID, draft.TaskID, draft.CorrelationID))
	if err != nil {
		return err
	}
	for _, event := range prior {
		var recorded events.IntentNormalizationContextPayload
		if err := decodeExactJSONBytes(event.Payload, &recorded); err != nil {
			return err
		}
		if recorded.SourceMessageID != next.SourceMessageID {
			continue
		}
		check := draft
		check.EventType = "INTENT_DRAFTED"
		check.SourceExecutionID = event.SourceExecutionID
		var notSent bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE organization_id=? AND task_id=? AND correlation_id=? AND source_execution_id=? AND event_type='INFERENCE_NOT_SENT' AND sequence>?)`, draft.OrganizationID, draft.TaskID, draft.CorrelationID, event.SourceExecutionID, event.Sequence).Scan(&notSent); err != nil {
			return err
		}
		if notSent {
			// The guard closed this invocation without dispatch. A fresh
			// attempt must still pass its own current containment admission.
			continue
		}
		var suspended bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE organization_id=? AND task_id=? AND correlation_id=? AND source_execution_id=? AND event_type='INTENT_NORMALIZATION_SUSPENDED' AND sequence>?)`, draft.OrganizationID, draft.TaskID, draft.CorrelationID, event.SourceExecutionID, event.Sequence).Scan(&suspended); err != nil {
			return err
		}
		if suspended {
			return core.ErrContainmentUnavailable
		}
		var finish int64
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MIN(sequence),0) FROM events WHERE organization_id=? AND task_id=? AND correlation_id=? AND source_execution_id=? AND event_type='INTENT_NORMALIZATION_FAILED' AND sequence>?`, draft.OrganizationID, draft.TaskID, draft.CorrelationID, event.SourceExecutionID, event.Sequence).Scan(&finish); err != nil {
			return err
		}
		hold, err := executionIntervalHoldThrough(ctx, tx, check, finish)
		if err != nil {
			return err
		}
		if hold != nil {
			return fmt.Errorf("normalization requires reconciliation or new input: %w", *hold)
		}
		if finish == 0 {
			// A crash can lose the suspension marker after dispatch. Absence of
			// a proven ordinary finish is not permission to repeat model work.
			return core.ErrContainmentUnavailable
		}
	}
	return nil
}

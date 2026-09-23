package recovery

import (
	"database/sql"
	"encoding/json"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
	"testing"
	"time"
)

func appendRecoveryPlan(t testing.TB, store *ledger.SQLite, correlationID string, intent core.Intent, task core.Task) {
	t.Helper()
	plan := core.Plan{ID: core.ID("plan-" + correlationID), IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, Version: 1, Tasks: []core.PlanTask{{Key: "bounded-task", Description: task.Description, ExecutionKind: task.ExecutionKind, ModelInferencePolicy: task.ModelInferencePolicy}}, CreatedAt: time.Now().UTC()}
	var err error
	plan.Fingerprint, err = core.FingerprintPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "PLAN_CREATED", SourceActorID: "runtime", TaskID: "task-" + correlationID, CorrelationID: correlationID, Payload: plan}); err != nil {
		t.Fatal(err)
	}
}

// insertPriorRecoveryConfirmation inserts valid review evidence before the
// abandonment so its failure proves the terminal-boundary rule itself.
func insertPriorRecoveryConfirmation(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := t.Context()
	var sequence int64
	var correlationID string
	if err := db.QueryRowContext(ctx, `SELECT sequence,correlation_id FROM events WHERE event_type='INTAKE_ABANDONED'`).Scan(&sequence, &correlationID); err != nil {
		t.Fatal(err)
	}
	draft := core.IntentDraft{ID: core.ID("intent-" + correlationID), OrganizationID: "org-1", Version: 1, Status: core.IntentStatusReadyForReview, Mode: core.IntentModeStandard, RequestedExecutionKind: core.ExecutionDeterministic, Objective: "Prepare bounded work.", Context: []core.IntentValue{}, Deliverables: []core.IntentValue{{Value: "result", Origin: "USER"}}, CompletionCriteria: []core.IntentValue{{Value: "verified result", Origin: "USER"}}, Constraints: []core.IntentValue{}, ResolvedDecisions: []core.IntentDecision{}, ConsequenceCandidates: []string{}, MissingUserInputs: []core.IntentValue{}, CreatedAt: time.Now().UTC()}
	var err error
	draft.Fingerprint, err = core.FingerprintIntentDraft(draft)
	if err != nil {
		t.Fatal(err)
	}
	draftBody, err := json.Marshal(events.IntentDraftedPayload{SourceMessageID: "message-1", Draft: draft, Reply: "Review bounded work."})
	if err != nil {
		t.Fatal(err)
	}
	confirmationBody, err := json.Marshal(events.IntentConfirmedPayload{IntentID: string(draft.ID), Version: 1, Fingerprint: draft.Fingerprint, ConfirmingActorID: "user-1", ConfirmingActorKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", MessageID: "confirmation-1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE events SET sequence=? WHERE event_type='INTAKE_ABANDONED'`, sequence+2); err != nil {
		t.Fatal(err)
	}
	for _, entry := range []struct {
		sequence         int64
		id, label, actor string
		body             []byte
	}{
		{sequence, "prior-draft", "INTENT_DRAFTED", "runtime", draftBody},
		{sequence + 1, "prior-confirmation", "INTENT_CONFIRMED", "user-1", confirmationBody},
	} {
		if _, err := db.ExecContext(ctx, `INSERT INTO events(sequence,event_id,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version)
SELECT ?,?,organization_id,?,?,'','','',task_id,'[]','[]',?,correlation_id,created_at,schema_version FROM events WHERE event_type='INTAKE_ABANDONED'`, entry.sequence, entry.id, entry.label, entry.actor, entry.body); err != nil {
			t.Fatal(err)
		}
	}
}

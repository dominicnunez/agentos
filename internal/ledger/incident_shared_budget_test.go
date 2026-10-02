package ledger

import (
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

func TestIncidentPrivateReferenceItems(t *testing.T) {
	for _, count := range []int{500, 550} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			store := sharedBudgetStore(t)
			appendPrivateInferenceGoal(t, store)
			sharedBudgetReservations(t, store, count, "model-stop", "intent-model-stop", "task-model-stop")
			baseline, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "goal", 256)
			if err != nil {
				t.Fatalf("healthy private writer baseline: %v", err)
			}
			sharedBudgetReservationCount(t, baseline, count)
			for i := range 3 {
				sharedBudgetNote(t, store, "org-1", "model-stop", i, false)
			}
			sharedBudgetFullOwner(t, store)
			// The exact selected reservation event and its accounting row are
			// distinct support, as are the3000 retained reference occurrences.
			// This lower bound does not reproduce the loader's implementation.
			lower := 2*count + 3000
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "goal", 256)
			if count == 550 {
				sharedBudgetRejection(t, snapshot, err, lower, events.MaximumIncidentEvidence)
				return
			}
			if err != nil {
				t.Fatalf("below-bound private reference control: %v", err)
			}
			sharedBudgetReservationCount(t, snapshot, count)
			refs := 0
			for _, event := range snapshot.DependencyEvents {
				if event.EventType == "AUDIT_NOTE" && event.CorrelationID == "model-stop" {
					refs += len(event.ArtifactRefs)
				}
			}
			if refs != 3000 || len(snapshot.Work.Events) != 1 {
				t.Fatalf("private refs=%d public events=%d want3000/1", refs, len(snapshot.Work.Events))
			}
		})
	}
}

func TestIncidentTransientReferenceItems(t *testing.T) {
	for _, count := range []int{500, 550} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			store := sharedBudgetStore(t)
			appendPrivateInferenceGoal(t, store)
			var notes []events.Event
			for i := range 3 {
				// These valid Unicode notes are conservative temporal candidates.
				// Foreign ownership/correlation keeps them outside the graph;
				// the Goal-backed start's strategy owner consumes its prefix.
				notes = append(notes, sharedBudgetNote(t, store, "foreign-org", "unrelated", i, true))
			}
			_, work := latestTestProjection[core.Work](t, t.Context(), store, "work", "work-1")
			_, intent := latestTestProjection[core.Intent](t, t.Context(), store, "intent", "intent-model-stop")
			full, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			_, eventRefs, contextRefs, err := events.ResolveStrategicContext("org-1", work, full, full[len(full)-1].Sequence+1)
			if err != nil {
				t.Fatal(err)
			}
			task := core.Task{ID: "task-model-stop", WorkID: work.ID, Description: "test", TaskContractVersion: "1", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, Status: core.TaskPending}
			if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "model-stop"}, ProjectionKind: "task", RecordID: string(task.ID), Version: 1, Value: task}); err != nil {
				t.Fatal(err)
			}
			plan := core.Plan{ID: "plan-model-stop", IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, Version: 1, StrategicEventRefs: eventRefs, StrategicContextRefs: contextRefs, Tasks: []core.PlanTask{{Key: "work", Description: task.Description, ExecutionKind: task.ExecutionKind, ModelInferencePolicy: task.ModelInferencePolicy}}, CreatedAt: time.Now().UTC()}
			plan.Fingerprint, err = core.FingerprintPlan(plan)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "PLAN_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "model-stop", Payload: plan}); err != nil {
				t.Fatal(err)
			}
			task.Status = core.TaskRunning
			start, _, err := store.AppendExecutionStart(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_STARTED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "model-stop", Payload: events.ExecutionStartDetail{StrategicEventRefs: eventRefs, StrategicContextRefs: contextRefs}}, ProjectionKind: "task", RecordID: string(task.ID), Version: 2, Value: task}, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			baseline, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "goal", 256)
			if err != nil {
				t.Fatalf("healthy temporal writer baseline: %v", err)
			}
			sharedBudgetTransientAbsent(t, baseline, notes)
			// No manifest is invented: the library owner supports an empty
			// auxiliary application context for these logical requests.
			sharedBudgetReservations(t, store, count, "model-stop", "intent-model-stop", "logical-shared-task")
			sharedBudgetFullOwner(t, store)
			full, err = store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if err := events.ValidateTaskExecutionStart(start, task, 2, work, intent, full); err != nil {
				t.Fatalf("full selected strategy owner rejected healthy temporal candidates: %v", err)
			}
			for _, note := range notes {
				var candidate bool
				if err := store.db.QueryRowContext(t.Context(), `SELECT `+incidentNegativeJSON+` FROM events WHERE event_id=?`, note.EventID).Scan(&candidate); err != nil {
					t.Fatal(err)
				}
				if !candidate || note.Sequence >= start.Sequence {
					t.Fatal("fixture lacks a prior conservative temporal candidate")
				}
			}
			lower := 2*count + 3000
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "goal", 256)
			if count == 550 {
				sharedBudgetRejection(t, snapshot, err, lower, events.MaximumIncidentEvidence)
				return
			}
			if err != nil {
				t.Fatalf("below-bound transient reference control: %v", err)
			}
			sharedBudgetReservationCount(t, snapshot, count)
			sharedBudgetTransientAbsent(t, snapshot, notes)
		})
	}
}

func TestIncidentEffectSharedSupportBytes(t *testing.T) {
	for _, target := range []int{15 << 20, (15 << 20) + (768 << 10)} {
		t.Run(fmt.Sprint(target), func(t *testing.T) {
			store := sharedBudgetStore(t)
			task := incidentTestExecution(t, store)
			lease := core.CapabilityLease{ID: "shared-budget-lease", ActorID: task.AssigneeID, ActorKind: core.PrincipalAgent, OriginTaskID: task.ID, Action: "send", Resource: "destination", Scope: "org-1"}
			if err := store.AppendRecord(t.Context(), "org-1", "CAPABILITY_GRANTED", "owner", string(task.ID), nil, nil, "capability_lease", string(lease.ID), 1, lease); err != nil {
				t.Fatal(err)
			}
			pending := approvedEffect(t, task, lease, "shared-budget-effect", "shared-budget-approval")
			pending.ReplayContext = map[string]string{"body": strings.Repeat("x", 350<<10)}
			var err error
			pending.EffectFingerprint, err = core.FingerprintEffect(pending)
			if err != nil {
				t.Fatal(err)
			}
			pending.Status, pending.CreatedAt = core.EffectPending, time.Now().UTC()
			if err := store.AppendRecord(t.Context(), "org-1", "EFFECT_OBLIGATION_TRANSITIONED", "", string(task.ID), pending.AuthorizationRefs, nil, "effect", string(pending.ID), 1, pending); err != nil {
				t.Fatal(err)
			}
			appendEffectApproval(t, store, pending, true)
			attempt := pending
			now := time.Now().UTC()
			attempt.Status, attempt.AttemptCount, attempt.LastAttemptAt = core.EffectAttempted, 1, &now
			trace, err := store.AuthorizeAndAppendEffectAttempt(t.Context(), pending, 2, attempt)
			if err != nil || !trace.Allowed {
				t.Fatalf("actual effect admission: %+v %v", trace, err)
			}
			sharedBudgetTwoPolicies(t, store)
			baseline, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256)
			if err != nil {
				t.Fatalf("healthy exact effect/policy baseline: %v", err)
			}
			sharedBudgetEffectCount(t, baseline)
			sharedBudgetPadPolicies(t, store, target)
			sharedBudgetFullOwner(t, store)
			var policyBytes, effectBytes int
			if err := store.db.QueryRowContext(t.Context(), `SELECT (SELECT SUM(length(CAST(body AS BLOB))) FROM inference_policies),(SELECT SUM(length(CAST(body AS BLOB))) FROM records WHERE kind='effect')`).Scan(&policyBytes, &effectBytes); err != nil {
				t.Fatal(err)
			}
			if policyBytes != 2*target || effectBytes >= 2<<20 {
				t.Fatalf("fixture policy/effect bytes=%d/%d", policyBytes, effectBytes)
			}
			lower := policyBytes + effectBytes
			over := target > 15<<20
			if (lower > events.MaximumIncidentEvidenceBytes) != over {
				t.Fatal("independent policy+effect-body lower bound does not discriminate")
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256)
			if over {
				sharedBudgetRejection(t, snapshot, err, lower, events.MaximumIncidentEvidenceBytes)
				return
			}
			if err != nil {
				t.Fatalf("below-bound effect support control: %v", err)
			}
			sharedBudgetEffectCount(t, snapshot)
		})
	}
}

func TestIncidentAdmissionSharedItems(t *testing.T) {
	store := sharedBudgetStore(t)
	appendPrivateInferenceGoal(t, store)
	policy := testInferencePolicy(time.Now().UTC())
	policy.OrganizationID, policy.Provider, policy.Model, policy.ExecutionProfileVersion = "org-1", "provider", "model", "v1"
	policy.MaxConcurrentRequests, policy.MaxTokensPerWindow = 2, 200000
	policy.Pricing.MaxCostNanoUSDPerWindow = 1000000000
	if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	appendPair := func(t *testing.T, id, correlation, task string) {
		t.Helper()
		request := testInferenceRequest(id)
		request.Scope.OrganizationID, request.Scope.CorrelationID, request.Scope.TaskID, request.Scope.IntentID = "org-1", correlation, task, "intent-model-stop"
		request.Scope.Purpose = inference.PurposeIntentNormalization
		request.Descriptor.Provider, request.Descriptor.Model, request.Descriptor.ExecutionProfileVersion = "provider", "model", "v1"
		reservation, err := store.ReserveInference(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.ReconcileInference(t.Context(), reservation, nil, inference.ReconciliationUncertain); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 1000 {
		appendPair(t, fmt.Sprintf("annotation-private-%04d", i), "model-stop", "logical-private-task")
	}
	sharedBudgetNote(t, store, "org-1", "model-stop", 0, false)
	public := 0
	for _, want := range []int{30, 50} {
		t.Run(fmt.Sprint(want), func(t *testing.T) {
			for public < want {
				appendPair(t, fmt.Sprintf("annotation-public-%04d", public), "goal", "logical-public-task")
				public++
			}
			sharedBudgetFullOwner(t, store)
			var privateEvents, privateRows, publicRows int
			if err := store.db.QueryRowContext(t.Context(), `SELECT
				(SELECT COUNT(*) FROM events WHERE correlation_id='model-stop' AND event_type IN ('INFERENCE_RESERVED','INFERENCE_RECONCILED')),
				(SELECT COUNT(*) FROM inference_reservations WHERE correlation_id='model-stop'),
				(SELECT COUNT(*) FROM inference_reservations WHERE correlation_id='goal')`).Scan(&privateEvents, &privateRows, &publicRows); err != nil {
				t.Fatal(err)
			}
			if privateEvents != 2000 || privateRows != 1000 || publicRows != want {
				t.Fatalf("private events/rows=%d/%d public rows=%d", privateEvents, privateRows, publicRows)
			}
			// Private event+row items,1000 reference occurrences, public
			// accounting rows, and distinct admission annotations are all
			// required support. Public event own items remain excluded.
			lower := privateEvents + privateRows + 1000 + publicRows + want
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "goal", 256)
			if want == 50 {
				sharedBudgetRejection(t, snapshot, err, lower, events.MaximumIncidentEvidence)
				return
			}
			if err != nil {
				t.Fatalf("below-bound admission control: %v", err)
			}
			if len(snapshot.Admissions) != want || len(snapshot.Work.Events) != 1+2*want {
				t.Fatalf("admissions/public=%d/%d want%d/%d", len(snapshot.Admissions), len(snapshot.Work.Events), want, 1+2*want)
			}
			sharedBudgetReservationCount(t, snapshot, 1000)
			refs, reconciled := 0, 0
			for _, event := range snapshot.DependencyEvents {
				if event.EventType == "INFERENCE_RECONCILED" {
					reconciled++
				}
				if event.EventType == "AUDIT_NOTE" && event.CorrelationID == "model-stop" {
					refs += len(event.ArtifactRefs)
				}
			}
			if refs != 1000 || reconciled != 1000 {
				t.Fatalf("private refs/reconciliations=%d/%d want1000/1000", refs, reconciled)
			}
		})
	}
}

func sharedBudgetStore(t *testing.T) *SQLite {
	store, err := Open(filepath.Join(t.TempDir(), "shared-budget.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func sharedBudgetReservations(t *testing.T, store *SQLite, count int, correlation, intent, task string) {
	t.Helper()
	policy := testInferencePolicy(time.Now().UTC())
	policy.OrganizationID, policy.Provider, policy.Model, policy.ExecutionProfileVersion = "org-1", "provider", "model", "v1"
	policy.MaxConcurrentRequests = count
	policy.MaxTokensPerWindow = 120*int64(count) + 100
	policy.Pricing.MaxCostNanoUSDPerWindow = 400000 * int64(count)
	if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	for i := range count {
		request := testInferenceRequest(fmt.Sprintf("shared-%04d", i))
		request.Scope.OrganizationID, request.Scope.CorrelationID, request.Scope.TaskID, request.Scope.IntentID = "org-1", correlation, task, intent
		request.Scope.Purpose = inference.PurposeIntentNormalization
		request.Descriptor.Provider, request.Descriptor.Model, request.Descriptor.ExecutionProfileVersion = "provider", "model", "v1"
		if _, err := store.ReserveInference(t.Context(), request); err != nil {
			t.Fatal(err)
		}
	}
	var rows int
	if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM inference_reservations WHERE correlation_id=?`, correlation).Scan(&rows); err != nil || rows != count {
		t.Fatalf("accounting rows=%d want%d: %v", rows, count, err)
	}
}

func sharedBudgetNote(t *testing.T, store *SQLite, organization, correlation string, note int, unicode bool) events.Event {
	t.Helper()
	refs := make([]string, 1000)
	for i := range refs {
		refs[i] = fmt.Sprintf("shared-artifact-%d-%04d", note, i)
	}
	text := "healthy retained evidence"
	if unicode {
		text = "healthy café evidence"
	}
	event, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: organization, CorrelationID: correlation, EventType: "AUDIT_NOTE", ArtifactRefs: refs, Payload: map[string]string{"text": text}})
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func sharedBudgetFullOwner(t *testing.T, store *SQLite) {
	t.Helper()
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatalf("full inference owner rejected healthy fixture: %v", err)
	}
	if _, err := ValidateEventIntegrity(t.Context(), store.db); err != nil {
		t.Fatal(err)
	}
	if err := validateIncidentLinkContents(t.Context(), store.db); err != nil {
		t.Fatal(err)
	}
}

func sharedBudgetReservationCount(t *testing.T, snapshot events.IncidentSnapshot, want int) {
	t.Helper()
	count := 0
	for _, event := range snapshot.DependencyEvents {
		if event.EventType == "INFERENCE_RESERVED" {
			count++
		}
	}
	if count != want {
		t.Fatalf("selected private reservations=%d want%d", count, want)
	}
}

func sharedBudgetTransientAbsent(t *testing.T, snapshot events.IncidentSnapshot, notes []events.Event) {
	t.Helper()
	for _, stream := range [][]events.Event{snapshot.Work.Events, snapshot.RelatedEvents, snapshot.DependencyEvents} {
		for _, event := range stream {
			for _, note := range notes {
				if event.EventID == note.EventID {
					t.Fatal("transient candidate unexpectedly became snapshot evidence")
				}
			}
		}
	}
}

func sharedBudgetRejection(t *testing.T, snapshot events.IncidentSnapshot, err error, lower, limit int) {
	t.Helper()
	if lower <= limit {
		t.Fatal("fixture lower bound does not exceed shared budget")
	}
	if err == nil {
		t.Fatalf("accepted shared support lower bound%d above%d", lower, limit)
	}
	if !strings.Contains(err.Error(), "limit") && !strings.Contains(err.Error(), "bound") {
		t.Fatalf("rejection was not a support bound: %v", err)
	}
	if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
		t.Fatal("overbudget read returned partial evidence")
	}
}

func sharedBudgetTwoPolicies(t *testing.T, store *SQLite) {
	t.Helper()
	policy := testInferencePolicy(time.Now().UTC())
	policy.OrganizationID = "org-1"
	policy.MaxConcurrentRequests, policy.MaxTokensPerWindow = 2, 1000
	policy.Pricing.MaxCostNanoUSDPerWindow = 2000000
	if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	request := testInferenceRequest("effect-first")
	request.Scope.OrganizationID, request.Scope.CorrelationID = "org-1", "stop-work"
	first, err := store.ReserveInference(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReconcileInference(t.Context(), first, nil, inference.ReconciliationUncertain); err != nil {
		t.Fatal(err)
	}
	policy.AuthorizedBy = "other-owner"
	policy.AuthorizedAt = policy.AuthorizedAt.Add(time.Second)
	if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	request.Scope.RequestID, request.Scope.ExecutionID = "effect-later", "effect-later"
	if _, err := store.ReserveInference(t.Context(), request); err != nil {
		t.Fatal(err)
	}
}

func sharedBudgetPadPolicies(t *testing.T, store *SQLite, target int) {
	t.Helper()
	rows, err := store.db.QueryContext(t.Context(), `SELECT policy_fingerprint,length(CAST(body AS BLOB)) FROM inference_policies ORDER BY policy_fingerprint`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	type stored struct {
		fingerprint string
		size        int
	}
	var policies []stored
	for rows.Next() {
		var policy stored
		if err := rows.Scan(&policy.fingerprint, &policy.size); err != nil {
			t.Fatal(err)
		}
		policies = append(policies, policy)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(policies) != 2 {
		t.Fatalf("policy count=%d want2", len(policies))
	}
	for _, policy := range policies {
		if policy.size >= target {
			t.Fatal("policy body already exceeds target")
		}
		if _, err := store.db.ExecContext(t.Context(), `UPDATE inference_policies SET body=CAST(body || ? AS BLOB) WHERE policy_fingerprint=?`, strings.Repeat(" ", target-policy.size), policy.fingerprint); err != nil {
			t.Fatal(err)
		}
	}
}

func sharedBudgetEffectCount(t *testing.T, snapshot events.IncidentSnapshot) {
	t.Helper()
	if len(snapshot.RelatedEvents) != 2 {
		t.Fatalf("selected effect revisions=%d want2", len(snapshot.RelatedEvents))
	}
	attempts, reservations := 0, 0
	for _, admission := range snapshot.Admissions {
		if admission.Kind == "EFFECT_ATTEMPT" {
			attempts++
		}
		if admission.Kind == "INFERENCE_RESERVATION" {
			reservations++
		}
	}
	if attempts != 1 || reservations != 2 {
		t.Fatalf("effect/reservation annotations=%d/%d want1/2", attempts, reservations)
	}
}

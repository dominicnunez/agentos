package ledger

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

func TestAuditIncidentDisplacedRecordOwnedIdentity(t *testing.T) {
	for _, selected := range []bool{false, true} {
		t.Run(map[bool]string{false: "unrelated-control", true: "selected-claim"}[selected], func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			appendTestMission(t, t.Context(), store, "org-audit", "mission-audit", time.Now().UTC())
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-audit", "mission-audit", 256); err != nil {
				t.Fatalf("valid history: %v", err)
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				var body []byte
				if err := tx.QueryRowContext(t.Context(), `SELECT body FROM records WHERE kind='mission' AND record_id='mission-audit'`).Scan(&body); err != nil {
					return err
				}
				var record events.ProjectionRecord
				if err := json.Unmarshal(body, &record); err != nil {
					return err
				}
				record.CorrelationID = "unrelated-correlation"
				if !selected {
					record.RecordID = "unrelated-mission"
					var value map[string]any
					if err := json.Unmarshal(record.Value, &value); err != nil {
						return err
					}
					value["id"] = "unrelated-mission"
					record.Value, err = json.Marshal(value)
					if err != nil {
						return err
					}
				}
				body, err := json.Marshal(record)
				if err != nil {
					return err
				}
				_, err = tx.ExecContext(t.Context(), `INSERT INTO records(kind,record_id,version,body,created_at) VALUES('mission','displaced-record',1,?,?)`, body, time.Now().UTC().Format(time.RFC3339Nano))
				return err
			}); err != nil {
				t.Fatal(err)
			}
			ownerErr := store.withTx(t.Context(), func(tx *sql.Tx) error {
				_, err := admittedProjectionRecordsBounded(t.Context(), tx, 4<<20, `WHERE r.kind='mission'`)
				return err
			})
			if ownerErr == nil {
				t.Fatal("full record owner accepted displaced owned identity")
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-audit", "mission-audit", 256)
			if !selected {
				if err != nil {
					t.Fatalf("unrelated retained identity affected incident: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("incident accepted displaced retained record claiming selected Mission in its body: public=%d private=%d fullOwnerError=%v", len(snapshot.Work.Events), len(snapshot.DependencyEvents), ownerErr)
			}
		})
	}
}

func TestAuditIncidentDispatchMalformedTemporalCandidate(t *testing.T) {
	for _, phase := range []string{"before-roster", "roster-to-start", "after-start"} {
		t.Run(phase, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			var candidate events.Event
			addCandidate := func() {
				var err error
				candidate, err = store.Append(t.Context(), events.TrustedDraft{OrganizationID: "unrelated-org", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "unrelated", Payload: map[string]string{"text": "ordinary independent note"}})
				if err != nil {
					t.Fatal(err)
				}
			}
			if phase == "before-roster" {
				addCandidate()
			}
			agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "selected", true)
			if phase == "roster-to-start" {
				addCandidate()
			}
			appendBenchmarkTaskInference(t, store, agent, config, "selected")
			if phase == "after-start" {
				addCandidate()
			}
			full, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			var start events.Event
			var task core.Task
			var work core.Work
			var intent core.Intent
			var version int
			for _, event := range full {
				payload, present, err := events.AdmittedProjection(event)
				if err != nil {
					t.Fatal(err)
				}
				if !present || event.CorrelationID != "selected" {
					continue
				}
				switch payload.Projection.ProjectionKind {
				case "work":
					if err := json.Unmarshal(payload.Projection.Value, &work); err != nil {
						t.Fatal(err)
					}
				case "intent":
					if err := json.Unmarshal(payload.Projection.Value, &intent); err != nil {
						t.Fatal(err)
					}
				case "task":
					if event.EventType == "EXECUTION_STARTED" {
						start = event
						version = payload.Projection.Version
						if err := json.Unmarshal(payload.Projection.Value, &task); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			if err := events.ValidateTaskExecutionStart(start, task, version, work, intent, full); err != nil {
				t.Fatalf("healthy direct start owner: %v", err)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256); err != nil {
				t.Fatalf("healthy public: %v", err)
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, []byte(`{"projection":{"projection_kind":"mission","record_id":"unrelated-mission"}}`), candidate.EventID); err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
					return err
				}
				return rebuildEventIntegrity(t.Context(), tx)
			}); err != nil {
				t.Fatal(err)
			}
			full, err = store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			ownerErr := events.ValidateTaskExecutionStart(start, task, version, work, intent, full)
			if phase != "roster-to-start" {
				if ownerErr != nil {
					t.Fatalf("direct owner changed out-of-window meaning: %v", ownerErr)
				}
				if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256); err != nil {
					t.Fatalf("out-of-window malformed history poisoned incident: %v", err)
				}
				return
			}
			if ownerErr == nil {
				t.Fatal("direct full start owner ignored malformed temporal candidate")
			}
			if snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256); err == nil {
				t.Fatalf("incident omitted direct owner's applicable malformed roster/start temporal candidate: public=%d private=%d fullOwnerError=%v", len(snapshot.Work.Events), len(snapshot.DependencyEvents), ownerErr)
			}
		})
	}
}

func TestAuditIncidentIndependentAdmissionEventIdentity(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	now := time.Now().UTC()
	appendTestMission(t, t.Context(), store, "org-audit", "mission-audit", now)
	mission := core.Mission{ID: "unrelated-mission", OrganizationID: "org-audit", Statement: "independent direction", Status: core.MissionActive, CreatedAt: now}
	incoming, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-audit", EventType: "MISSION_CREATED", SourceActorID: "runtime", CorrelationID: "unrelated-mission"}, ProjectionKind: "mission", RecordID: "unrelated-mission", Version: 1, Value: mission})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-audit", "mission-audit", 256); err != nil {
		t.Fatal(err)
	}
	var target string
	if err := store.db.QueryRowContext(t.Context(), `SELECT event_id FROM events WHERE event_type='MISSION_CREATED' AND correlation_id='mission-audit'`).Scan(&target); err != nil {
		t.Fatal(err)
	}
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=CAST(json_set(payload,'$.admission.event_ref',?) AS BLOB) WHERE event_id=?`, target, incoming.EventID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	}); err != nil {
		t.Fatal(err)
	}
	full, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	_, ownerErr := events.ValidateProjectionHistory(full, nil, nil, nil)
	if ownerErr == nil {
		t.Fatal("full owner accepted mismatched admission event identity")
	}
	if snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-audit", "mission-audit", 256); err == nil {
		t.Fatalf("incident omitted independent admission.event_ref claim against selected global event: public=%d private=%d fullOwnerError=%v", len(snapshot.Work.Events), len(snapshot.DependencyEvents), ownerErr)
	}
}

func TestAuditIncidentStoredPolicyMetadata(t *testing.T) {
	for _, mode := range []string{"healthy", "activation-time", "zero-time", "empty-time", "active-negative", "active-large", "oversized-time", "oversized-active"} {
		for _, version := range []int{1, inference.ConnectionPolicyVersion} {
			t.Run(fmt.Sprintf("v%d/%s", version, mode), func(t *testing.T) {
				store, err := Open(":memory:")
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = store.Close() }()
				now := time.Now().UTC()
				policy := testInferencePolicy(now)
				policy.Version = version
				if version == inference.ConnectionPolicyVersion {
					policy.Version = inference.ConnectionPolicyVersion
					policy.ConnectionID = "audit-connection"
					policy.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, MaxCostNanoUSDPerWindow: 1000000, MaxConcurrentRequests: 2}
				}
				if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
					t.Fatal(err)
				}
				request := testInferenceRequest("audit-policy-call")
				request.ConnectionID = policy.ConnectionID
				manifest := events.PlanningContextPayload{ConnectionID: policy.ConnectionID, IntentID: request.Scope.IntentID, Provider: request.Descriptor.Provider, Model: request.Descriptor.Model, ExecutionProfileVersion: request.Descriptor.ExecutionProfileVersion}
				if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: policy.OrganizationID, EventType: "PLANNING_CONTEXT_MANIFESTED", SourceActorID: "runtime", SourceExecutionID: request.Scope.ExecutionID, TaskID: request.Scope.TaskID, CorrelationID: request.Scope.CorrelationID, Payload: manifest}); err != nil {
					t.Fatal(err)
				}
				if _, err := store.ReserveInference(t.Context(), request); err != nil {
					t.Fatal(err)
				}
				if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
					t.Fatalf("healthy full owner: %v", err)
				}
				if _, err := store.VerifiedIncidentEvents(t.Context(), policy.OrganizationID, request.Scope.CorrelationID, 256); err != nil {
					t.Fatalf("healthy public: %v", err)
				}
				if mode == "healthy" {
					return
				}
				column, value := "activated_at", any("invalid")
				switch mode {
				case "zero-time":
					value = time.Time{}.Format(time.RFC3339Nano)
				case "empty-time":
					value = ""
				case "active-negative":
					column, value = "active", -1
				case "active-large":
					column, value = "active", 2
				case "oversized-time":
					value = strings.Repeat("\x00", 33<<20)
				case "oversized-active":
					column, value = "active", strings.Repeat("x", 33<<20)
				}
				if _, err := store.db.ExecContext(t.Context(), `UPDATE inference_policies SET `+column+`=?`, value); err != nil {
					t.Fatal(err)
				}
				ownerErr := store.ValidateInferenceAdmissions(t.Context())
				if ownerErr == nil {
					t.Fatal("full owner accepted damaged policy metadata")
				}
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), policy.OrganizationID, request.Scope.CorrelationID, 256)
				if err == nil {
					t.Fatal("incident ignored selected policy metadata")
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("invalid metadata returned partial incident evidence")
				}
				if strings.HasPrefix(mode, "oversized-") && !strings.Contains(err.Error(), "support limit") {
					t.Fatalf("oversized stored metadata was decoded before byte preflight: %v", err)
				}
			})
		}
	}
}

func TestAuditIncidentUnusedJudgmentKnowledgeIdentity(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	appendTaskProjectionParents(t, t.Context(), store, "org-1", "setup", "work-1")
	appendFactualInferenceKnowledge(t, store, "fact", "Verified fact", "A bounded observation.")
	baseline, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "knowledge-fact", 256)
	if err != nil {
		t.Fatalf("healthy public: %v", err)
	}
	_, err = store.Append(t.Context(), events.TrustedDraft{OrganizationID: "foreign-org", EventType: "HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED", SourceActorID: "human-1", TaskID: "foreign-task", CorrelationID: "unconsumed", Payload: events.KnowledgeJudgmentPayload{KnowledgeID: "fact", CandidateVersion: 1, Decision: events.KnowledgeJudgmentValidated, Statement: "Unconsumed independent statement", CapabilityCheckEventID: "unconsumed-capability-check", SourcePrincipalID: "human-1", SourcePrincipalKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", ArtifactRefs: []string{}}})
	if err != nil {
		t.Fatalf("raw owner admission: %v", err)
	}
	full, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	leases, freezes, err := events.ResolveAuthorityAdmissions(full, append(append([]events.AuthorityRecord{}, baseline.FreezeRecords...), baseline.AuthorityRecords...))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := events.ValidateProjectionHistory(full, nil, leases, freezes); err != nil {
		t.Fatalf("full owner changed meaning of unconsumed judgment: %v", err)
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "knowledge-fact", 256); err != nil {
		t.Fatalf("unconsumed raw judgment's KnowledgeID poisoned incident: %v", err)
	}
}

func TestAuditIncidentIndependentLeaseRecordIdentity(t *testing.T) {
	for _, selected := range []bool{false, true} {
		t.Run(map[bool]string{false: "unrelated-control", true: "selected-claim"}[selected], func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			appendTaskProjectionParents(t, t.Context(), store, "org-1", "setup", "work-1")
			appendFactualInferenceKnowledge(t, store, "fact", "Verified fact", "A bounded observation.")
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "knowledge-fact", 256); err != nil {
				t.Fatalf("healthy public: %v", err)
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				var body []byte
				if err := tx.QueryRowContext(t.Context(), `SELECT body FROM records WHERE kind='capability_lease' AND record_id='lease-fact'`).Scan(&body); err != nil {
					return err
				}
				if !selected {
					var value core.CapabilityLease
					if err := json.Unmarshal(body, &value); err != nil {
						return err
					}
					value.ID = "unrelated-lease"
					var err error
					body, err = json.Marshal(value)
					if err != nil {
						return err
					}
				}
				_, err := tx.ExecContext(t.Context(), `INSERT INTO records(kind,record_id,version,body,created_at) VALUES('capability_lease','displaced-lease',1,?,?)`, body, time.Now().UTC().Format(time.RFC3339Nano))
				return err
			}); err != nil {
				t.Fatal(err)
			}
			full, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			rows, err := store.db.QueryContext(t.Context(), `SELECT kind,record_id,version,body,admission_event_id FROM records WHERE kind IN ('capability_lease','organization_freeze') ORDER BY kind,record_id,version`)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = rows.Close() }()
			var records []events.AuthorityRecord
			for rows.Next() {
				var record events.AuthorityRecord
				if err := rows.Scan(&record.Kind, &record.RecordID, &record.Version, &record.Body, &record.AdmissionEventID); err != nil {
					t.Fatal(err)
				}
				records = append(records, record)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			_, _, ownerErr := events.ResolveAuthorityAdmissions(full, records)
			if ownerErr == nil {
				t.Fatal("full owner accepted unmatched lease record")
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "knowledge-fact", 256)
			if !selected {
				if err != nil {
					t.Fatalf("unrelated lease affected incident: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("incident omitted selected lease ID in retained record body: public=%d private=%d fullOwnerError=%v", len(snapshot.Work.Events), len(snapshot.DependencyEvents), ownerErr)
			}
		})
	}
}

func TestAuditIncidentIndependentProjectionValueIdentity(t *testing.T) {
	for _, source := range []string{"event", "record", "both"} {
		t.Run(source, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			now := time.Now().UTC()
			appendTestMission(t, t.Context(), store, "org-audit", "mission-audit", now)
			mission := core.Mission{ID: "unrelated-mission", OrganizationID: "org-audit", Statement: "independent direction", Status: core.MissionActive, CreatedAt: now}
			incoming, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-audit", EventType: "MISSION_CREATED", SourceActorID: "runtime", CorrelationID: "unrelated-mission"}, ProjectionKind: "mission", RecordID: "unrelated-mission", Version: 1, Value: mission})
			if err != nil {
				t.Fatal(err)
			}
			full, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := events.ValidateProjectionHistory(full, nil, nil, nil); err != nil {
				t.Fatalf("healthy full owner: %v", err)
			}
			before, err := store.VerifiedIncidentEvents(t.Context(), "org-audit", "mission-audit", 256)
			if err != nil {
				t.Fatalf("healthy incident: %v", err)
			}
			for _, event := range before.DependencyEvents {
				if event.EventID == incoming.EventID {
					t.Fatal("healthy unrelated Mission selected")
				}
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				event, found, err := eventByID(t.Context(), tx, incoming.EventID)
				if err != nil || !found {
					return err
				}
				projection, _, err := events.AdmittedProjection(event)
				if err != nil {
					return err
				}
				mission.ID = "mission-audit"
				projection.Projection.Value, err = json.Marshal(mission)
				if err != nil {
					return err
				}
				sealed, err := events.SealProjectionEvent(event, projection.Projection, projection.Detail)
				if err != nil {
					return err
				}
				eventBody, err := json.Marshal(sealed)
				if err != nil {
					return err
				}
				recordBody, err := json.Marshal(projection.Projection)
				if err != nil {
					return err
				}
				if source != "record" {
					if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, eventBody, event.EventID); err != nil {
						return err
					}
				}
				if source != "event" {
					if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=?,admission_fingerprint=? WHERE admission_event_id=?`, recordBody, sealed.Admission.Fingerprint, event.EventID); err != nil {
						return err
					}
				}
				if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
					return err
				}
				return rebuildEventIntegrity(t.Context(), tx)
			}); err != nil {
				t.Fatal(err)
			}
			var ownerErr error
			if source == "record" {
				ownerErr = store.withTx(t.Context(), func(tx *sql.Tx) error {
					_, err := admittedProjectionRecordsBounded(t.Context(), tx, 4<<20, `WHERE r.kind='mission'`)
					return err
				})
			} else {
				full, err = store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				_, ownerErr = events.ValidateProjectionHistory(full, nil, nil, nil)
			}
			if ownerErr == nil {
				t.Fatal("full owner accepted mismatched independent value identity")
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-audit", "mission-audit", 256)
			if err == nil {
				t.Fatalf("incident omitted independent retained Value.ID claim: public=%d private=%d fullOwnerError=%v", len(snapshot.Work.Events), len(snapshot.DependencyEvents), ownerErr)
			}
		})
	}
}

func TestAuditIncidentDuplicateForeignReservationIdentity(t *testing.T) {
	parallelIncidentTest(t)
	for _, mode := range []string{"unrelated-control", "plain-selected", "duplicate-selected"} {
		t.Run(mode, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "selected", true)
			request := appendBenchmarkTaskInference(t, store, agent, config, "selected")
			policy := testInferencePolicy(time.Now().UTC())
			policy.OrganizationID, policy.Provider, policy.Model, policy.ExecutionProfileVersion = "org-1", "provider", "model", config.ProfileVersion
			policy.Mode, policy.Pricing = inference.Local, nil
			if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			if _, err := store.ReserveInference(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
				t.Fatalf("healthy full owner: %v", err)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256); err != nil {
				t.Fatalf("healthy public: %v", err)
			}
			var reservation string
			if err := store.db.QueryRowContext(t.Context(), `SELECT reservation_id FROM inference_reservations`).Scan(&reservation); err != nil {
				t.Fatal(err)
			}
			selected, err := json.Marshal(reservation)
			if err != nil {
				t.Fatal(err)
			}
			body := `{"reservation_id":"unrelated-reservation"}`
			if mode == "plain-selected" {
				body = `{"reservation_id":` + string(selected) + `}`
			}
			if mode == "duplicate-selected" {
				body = `{"reservation_id":"unrelated-reservation","reservation_id":` + string(selected) + `}`
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				event, err := appendEvent(t.Context(), tx, events.TrustedDraft{OrganizationID: "foreign-org", CorrelationID: "foreign-correlation", EventType: "INFERENCE_RECONCILED", SourceActorID: "runtime", SourceExecutionID: "foreign-execution", TaskID: "foreign-task", Payload: map[string]string{"reservation_id": "unrelated-reservation"}})
				if err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, []byte(body), event.EventID); err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
					return err
				}
				return rebuildEventIntegrity(t.Context(), tx)
			}); err != nil {
				t.Fatal(err)
			}
			ownerErr := store.ValidateInferenceAdmissions(t.Context())
			if ownerErr == nil {
				t.Fatal("full inference owner accepted unmatched foreign reconciliation")
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256)
			if mode == "unrelated-control" {
				if err != nil {
					t.Fatalf("unrelated identity affected selected incident: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("incident omitted foreign retained claim against globally keyed selected reservation: public=%d dependencies=%d fullOwnerError=%v", len(snapshot.Work.Events), len(snapshot.DependencyEvents), ownerErr)
			}
		})
	}
}

func TestAuditIncidentStrategyMalformedTemporalCandidate(t *testing.T) {
	for _, phase := range []string{"before-strategy", "before-strategy-casefold", "after-start"} {
		t.Run(phase, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			var candidate events.Event
			add := func() {
				candidate, err = store.Append(t.Context(), events.TrustedDraft{OrganizationID: "foreign-org", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "unrelated", Payload: map[string]string{"note": "healthy unrelated input"}})
				if err != nil {
					t.Fatal(err)
				}
			}
			if phase == "before-strategy" || phase == "before-strategy-casefold" {
				add()
			}
			appendPrivateInferenceGoal(t, store)
			_, work := latestTestProjection[core.Work](t, t.Context(), store, "work", core.ID("work-1"))
			_, intent := latestTestProjection[core.Intent](t, t.Context(), store, "intent", core.ID("intent-model-stop"))
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
			if phase == "after-start" {
				add()
			}
			full, err = store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if err := events.ValidateTaskExecutionStart(start, task, 2, work, intent, full); err != nil {
				t.Fatalf("healthy selected owner: %v", err)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "model-stop", 256); err != nil {
				t.Fatalf("healthy public: %v", err)
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				body := []byte(`{"projection":{"projection_kind":"mission","record_id":"unrelated-mission"}}`)
				if phase == "before-strategy-casefold" {
					body = []byte(`{"Projection":{"projection_kind":"mission","record_id":"unrelated-mission"}}`)
				}
				if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, candidate.EventID); err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
					return err
				}
				return rebuildEventIntegrity(t.Context(), tx)
			}); err != nil {
				t.Fatal(err)
			}
			full, err = store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			ownerErr := events.ValidateTaskExecutionStart(start, task, 2, work, intent, full)
			snapshot, publicErr := store.VerifiedIncidentEvents(t.Context(), "org-1", "model-stop", 256)
			if phase == "after-start" {
				if ownerErr != nil || publicErr != nil {
					t.Fatalf("after-start control changed meaning owner=%v public=%v", ownerErr, publicErr)
				}
				return
			}
			if ownerErr == nil {
				t.Fatal("selected strategic start owner accepted malformed prior candidate")
			}
			if publicErr == nil {
				t.Fatalf("public omitted selected-operation strategic candidate: public=%d private=%d selectedOwnerError=%v", len(snapshot.Work.Events), len(snapshot.DependencyEvents), ownerErr)
			}
		})
	}
}

func TestAuditIncidentKnowledgeMalformedTemporalCandidate(t *testing.T) {
	for _, phase := range []string{"sameorg-before-roster", "sameorg-invalid-target-before-start", "foreign-before-roster", "sameorg-after-start-emptyrefs", "sameorg-after-start-usedrefs", "sameorg-after-reservation"} {
		t.Run(phase, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			var candidate events.Event
			add := func() {
				org := "org-1"
				if phase == "foreign-before-roster" {
					org = "foreign-org"
				}
				candidate, err = store.Append(t.Context(), events.TrustedDraft{OrganizationID: org, EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "unrelated", Payload: map[string]string{"note": "healthy unrelated input"}})
				if err != nil {
					t.Fatal(err)
				}
			}
			if phase == "sameorg-before-roster" || phase == "foreign-before-roster" {
				add()
			}
			agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "selected", true)
			if phase == "sameorg-invalid-target-before-start" {
				appendFactualInferenceKnowledge(t, store, "unrelated-fact", "Revenue", "Sales increased three percent.")
				stream, err := store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				for _, event := range stream {
					if event.EventType == "KNOWLEDGE_ACTIVATED" {
						candidate = event
					}
				}
			}
			if phase == "sameorg-after-start-usedrefs" {
				appendFactualInferenceKnowledge(t, store, "selected-fact", "Bounded verification", "The bounded rehearsal restored three records.")
			}
			request := appendBenchmarkTaskInference(t, store, agent, config, "selected")
			if phase == "sameorg-after-start-emptyrefs" || phase == "sameorg-after-start-usedrefs" {
				add()
			}
			policy := testInferencePolicy(time.Now().UTC())
			policy.OrganizationID, policy.Provider, policy.Model, policy.ExecutionProfileVersion = "org-1", "provider", "model", config.ProfileVersion
			policy.Mode, policy.Pricing = inference.Local, nil
			if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			if _, err := store.ReserveInference(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if phase == "sameorg-after-reservation" {
				add()
			}
			before, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256)
			if err != nil {
				t.Fatalf("healthy public: %v", err)
			}
			full, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			var start, reservation events.Event
			for _, event := range full {
				if event.EventType == "EXECUTION_STARTED" {
					start = event
				}
				if event.EventType == "INFERENCE_RESERVED" {
					reservation = event
				}
			}
			_, task := latestTestProjection[core.Task](t, t.Context(), store, "task", core.ID("task-selected"))
			_, work := latestTestProjection[core.Work](t, t.Context(), store, "work", core.ID("work-selected"))
			_, intent := latestTestProjection[core.Intent](t, t.Context(), store, "intent", core.ID("intent-selected"))
			_, blueprint := latestTestProjection[core.AgentBlueprint](t, t.Context(), store, "agent_blueprint", config.BlueprintID)
			_, profile := latestTestProjection[core.ExecutionProfile](t, t.Context(), store, "execution_profile", config.ProfileID)
			binding := events.WorkCompletionBinding{OrganizationID: "org-1", CorrelationID: "selected", Work: work, WorkVersion: 1, Intent: intent, InboxObservations: before.InboxObservations, AgentBlueprints: map[core.ID]core.AgentBlueprint{config.BlueprintID: blueprint}, ExecutionProfiles: map[core.ID]core.ExecutionProfile{config.ProfileID: profile}}
			if _, err := events.ValidateAgentExecutionManifest(binding, task, reservation.SourceExecutionID, start, reservation.Sequence, full); err != nil {
				t.Fatalf("healthy exact selected manifest owner: %v", err)
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				if phase == "sameorg-invalid-target-before-start" {
					projection, _, err := events.AdmittedProjection(candidate)
					if err != nil {
						return err
					}
					var record core.KnowledgeRecord
					if err := json.Unmarshal(projection.Projection.Value, &record); err != nil {
						return err
					}
					record.Status = core.KnowledgeCandidate
					projection.Projection.Value, err = json.Marshal(record)
					if err != nil {
						return err
					}
					sealed, err := events.SealProjectionEvent(candidate, projection.Projection, projection.Detail)
					if err != nil {
						return err
					}
					body, err := json.Marshal(sealed)
					if err != nil {
						return err
					}
					recordBody, err := json.Marshal(projection.Projection)
					if err != nil {
						return err
					}
					if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, candidate.EventID); err != nil {
						return err
					}
					if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=?,admission_fingerprint=? WHERE admission_event_id=?`, recordBody, sealed.Admission.Fingerprint, candidate.EventID); err != nil {
						return err
					}
					if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
						return err
					}
					return rebuildEventIntegrity(t.Context(), tx)
				}
				if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, []byte(`{"projection":{"projection_kind":"knowledge","record_id":"unrelated-knowledge"}}`), candidate.EventID); err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
					return err
				}
				return rebuildEventIntegrity(t.Context(), tx)
			}); err != nil {
				t.Fatal(err)
			}
			full, err = store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			_, ownerErr := events.ValidateAgentExecutionManifest(binding, task, reservation.SourceExecutionID, start, reservation.Sequence, full)
			snapshot, publicErr := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256)
			if phase != "sameorg-before-roster" && phase != "sameorg-after-start-usedrefs" && phase != "sameorg-invalid-target-before-start" {
				if ownerErr != nil || publicErr != nil {
					t.Fatalf("excluded temporal source changed selected consumer meaning: owner=%v public=%v", ownerErr, publicErr)
				}
				return
			}
			if ownerErr == nil {
				t.Fatal("exact selected manifest owner accepted malformed prior Knowledge candidate")
			}
			if publicErr == nil {
				t.Fatalf("public omitted selected-operation Knowledge candidate: public=%d private=%d selectedOwnerError=%v", len(snapshot.Work.Events), len(snapshot.DependencyEvents), ownerErr)
			}
		})
	}
}

func TestAuditIncidentLegacyKnowledgeMalformedTemporalCandidate(t *testing.T) {
	parallelIncidentTest(t)
	for _, version := range []string{"v1", "v2", "v3", "v4", "bare-v4"} {
		for _, selected := range []bool{false, true} {
			scope := "foreign-control"
			if selected {
				scope = "sameorg-before-roster"
			}
			t.Run(version+"/"+scope, func(t *testing.T) {
				consumed := version != "bare-v4"
				contractVersion := version
				if !consumed {
					contractVersion = "v4"
				}
				store, err := Open(":memory:")
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = store.Close() }()
				org := "foreign-org"
				if selected {
					org = "org-1"
				}
				candidate, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: org, EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "unrelated", Payload: map[string]string{"note": "healthy unrelated input"}})
				if err != nil {
					t.Fatal(err)
				}
				appendLegacyContextParents(t, store)
				blueprint, agent, config := appendLegacyContextAgent(t, store)
				task := appendPendingAgentExecutionTask(t, t.Context(), store, "legacy-context", "task-legacy-context", agent, config)
				_, work := latestTestProjection[core.Work](t, t.Context(), store, "work", task.WorkID)
				_, intent := latestTestProjection[core.Intent](t, t.Context(), store, "intent", work.IntentID)
				_, profile := latestTestProjection[core.ExecutionProfile](t, t.Context(), store, "execution_profile", config.ProfileID)
				plan := core.Plan{ID: "plan-legacy-context", IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, Version: 1, Tasks: []core.PlanTask{{Key: "root", Description: task.Description, ExecutionKind: task.ExecutionKind, ModelInferencePolicy: task.ModelInferencePolicy}}, CreatedAt: time.Now().UTC()}
				plan.Fingerprint, err = core.FingerprintPlan(plan)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "PLAN_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "legacy-context", Payload: plan}); err != nil {
					t.Fatal(err)
				}
				task.Status = core.TaskRunning
				inputContext := core.AgentExecutionInputContext{Blueprint: blueprint, Task: task}
				var manifest core.ExecutionContextManifest
				start, _, err := store.AppendExecutionStart(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_STARTED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "legacy-context"}, ProjectionKind: "task", RecordID: string(task.ID), Version: 2, Value: task}, []events.InboxRoute{{Scope: events.RecipientTask, ID: string(task.ID)}, {Scope: events.RecipientAgent, ID: string(agent.ID)}}, func(selection events.ExecutionStartSelection) (core.ExecutionContextManifest, error) {
					manifest = testAgentStartManifest(task, selection)
					manifest.Provider, manifest.Model, manifest.PromptVersion = "fake", "fake-model/v1", "v1"
					binding, err := core.BindCurrentAgentExecutionInput("org-1", manifest.ExecutionID, inputContext)
					if err != nil {
						return core.ExecutionContextManifest{}, err
					}
					body, err := binding.Request().Canonical()
					manifest.ExecutionInputSHA256 = core.FingerprintExecutionInput(string(body))
					return manifest, err
				})
				if err != nil {
					t.Fatal(err)
				}
				manifest.ContextBuilderVersion = contractVersion
				var input string
				if contractVersion == "v4" {
					binding, err := core.BindAgentExecutionInput("org-1", manifest.ExecutionID, inputContext)
					if err != nil {
						t.Fatal(err)
					}
					body, err := binding.Request().Canonical()
					if err != nil {
						t.Fatal(err)
					}
					input = string(body)
				} else {
					_, input, err = core.MaterializeAgentExecutionInput(inputContext)
					if err != nil {
						t.Fatal(err)
					}
				}
				manifest.ExecutionInputSHA256 = core.FingerprintExecutionInput(input)
				rewriteLegacyContext(t, store, &manifest, core.KnowledgeScopeOrganization, "org-1", "")
				if consumed {
					appendLegacyContextCompletion(t, store, task, manifest, input)
				}
				before, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "legacy-context", 256)
				if err != nil {
					t.Fatalf("healthy public historical completion: %v", err)
				}
				full, err := store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				var use int64
				if !consumed {
					use = full[len(full)-1].Sequence + 1
				}
				for _, event := range full {
					if event.EventType == "TOOL_OUTCOME_RECORDED" {
						use = event.Sequence
					}
				}
				binding := events.WorkCompletionBinding{OrganizationID: "org-1", CorrelationID: "legacy-context", Work: work, WorkVersion: 1, Intent: intent, InboxObservations: before.InboxObservations, AgentBlueprints: map[core.ID]core.AgentBlueprint{config.BlueprintID: blueprint}, ExecutionProfiles: map[core.ID]core.ExecutionProfile{config.ProfileID: profile}}
				if _, err := events.ValidateAgentExecutionManifest(binding, task, string(manifest.ExecutionID), start, use, full); err != nil {
					t.Fatalf("healthy exact legacy selected consumer: %v", err)
				}
				if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
					if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, []byte(`{"projection":{"projection_kind":"knowledge","record_id":"unrelated-knowledge"}}`), candidate.EventID); err != nil {
						return err
					}
					if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
						return err
					}
					return rebuildEventIntegrity(t.Context(), tx)
				}); err != nil {
					t.Fatal(err)
				}
				full, err = store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				_, ownerErr := events.ValidateAgentExecutionManifest(binding, task, string(manifest.ExecutionID), start, use, full)
				snapshot, publicErr := store.VerifiedIncidentEvents(t.Context(), "org-1", "legacy-context", 256)
				if !consumed {
					// Calling the hypothetical v4 consumer would reject this source,
					// but no admitted reservation or completion consumes this manifest.
					if publicErr != nil {
						t.Fatalf("bare legacy manifest activated unused Knowledge validation: %v", publicErr)
					}
					return
				}
				if contractVersion == "v1" {
					if ownerErr != nil || publicErr != nil {
						t.Fatalf("v1 unused Knowledge changed selected completion owner=%v public=%v", ownerErr, publicErr)
					}
					return
				}
				if !selected {
					if ownerErr != nil || publicErr != nil {
						t.Fatalf("foreign legacy exclusion changed selected meaning owner=%v public=%v", ownerErr, publicErr)
					}
					return
				}
				if ownerErr == nil {
					t.Fatal("exact selected legacy manifest consumer accepted malformed prior Knowledge candidate")
				}
				if publicErr == nil {
					t.Fatalf("public omitted consumed legacy Knowledge candidate: public=%d private=%d selectedOwnerError=%v", len(snapshot.Work.Events), len(snapshot.DependencyEvents), ownerErr)
				}
			})
		}
	}
}

func TestAuditIncidentKnowledgeValidUnusedTemporalSource(t *testing.T) {
	for _, phase := range []string{"low-relevance-active", "noncandidate"} {
		t.Run(phase, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "selected", true)
			if phase == "low-relevance-active" {
				appendFactualInferenceKnowledge(t, store, "unrelated-fact", "Revenue", "Sales increased three percent.")
			} else {
				evidence, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "unrelated", Payload: map[string]string{"note": "Independent revenue observation"}})
				if err != nil {
					t.Fatal(err)
				}
				record := core.KnowledgeRecord{KnowledgeID: "unrelated-candidate", OrganizationID: "org-1", Version: 1, Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: "org-1", Status: core.KnowledgeCandidate, Title: "Revenue", Content: "Sales increased three percent.", Basis: core.KnowledgeBasisExternalEvidence, ProvenanceEventRefs: []string{evidence.EventID}, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated}
				if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: "knowledge-" + string(record.KnowledgeID)}, ProjectionKind: "knowledge", RecordID: string(record.KnowledgeID), Version: 1, Value: record}); err != nil {
					t.Fatal(err)
				}
			}
			request := appendBenchmarkTaskInference(t, store, agent, config, "selected")
			policy := testInferencePolicy(time.Now().UTC())
			policy.OrganizationID, policy.Provider, policy.Model, policy.ExecutionProfileVersion = "org-1", "provider", "model", config.ProfileVersion
			policy.Mode, policy.Pricing = inference.Local, nil
			if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			if _, err := store.ReserveInference(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
				t.Fatalf("healthy full consumer owner: %v", err)
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256)
			if err != nil {
				t.Fatalf("valid unused Knowledge changed selected operation: %v", err)
			}
			for _, event := range snapshot.Work.Events {
				if event.EventType == "EXECUTION_CONTEXT_MANIFESTED" {
					var manifest core.ExecutionContextManifest
					if err := json.Unmarshal(event.Payload, &manifest); err != nil {
						t.Fatal(err)
					}
					if len(manifest.KnowledgeRefs) != 0 {
						t.Fatal("valid unused Knowledge was selected by runtime relevance")
					}
				}
			}
		})
	}
}

func TestAuditIncidentKnowledgeTerminalUseCandidate(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	appendLegacyContextParents(t, store)
	blueprint, agent, config := appendLegacyContextAgent(t, store)
	fact := appendFactualInferenceKnowledge(t, store, "selected-fact", "Bounded verification", "The bounded rehearsal restored three records.")
	task := appendPendingAgentExecutionTask(t, t.Context(), store, "legacy-context", "task-legacy-context", agent, config)
	_, work := latestTestProjection[core.Work](t, t.Context(), store, "work", task.WorkID)
	_, intent := latestTestProjection[core.Intent](t, t.Context(), store, "intent", work.IntentID)
	_, profile := latestTestProjection[core.ExecutionProfile](t, t.Context(), store, "execution_profile", config.ProfileID)
	plan := core.Plan{ID: "plan-legacy-context", IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, Version: 1, Tasks: []core.PlanTask{{Key: "root", Description: task.Description, ExecutionKind: task.ExecutionKind, ModelInferencePolicy: task.ModelInferencePolicy}}, CreatedAt: time.Now().UTC()}
	plan.Fingerprint, err = core.FingerprintPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "PLAN_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "legacy-context", Payload: plan}); err != nil {
		t.Fatal(err)
	}
	task.Status = core.TaskRunning
	inputContext := core.AgentExecutionInputContext{Blueprint: blueprint, Task: task, Knowledge: []core.KnowledgeRecord{fact}}
	var manifest core.ExecutionContextManifest
	_, _, err = store.AppendExecutionStart(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_STARTED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "legacy-context"}, ProjectionKind: "task", RecordID: string(task.ID), Version: 2, Value: task}, []events.InboxRoute{{Scope: events.RecipientTask, ID: string(task.ID)}, {Scope: events.RecipientAgent, ID: string(agent.ID)}}, func(selection events.ExecutionStartSelection) (core.ExecutionContextManifest, error) {
		manifest = testAgentStartManifest(task, selection)
		manifest.Provider, manifest.Model, manifest.PromptVersion = "fake", "fake-model/v1", "v1"
		binding, err := core.BindCurrentAgentExecutionInput("org-1", manifest.ExecutionID, inputContext)
		if err != nil {
			return core.ExecutionContextManifest{}, err
		}
		body, err := binding.Request().Canonical()
		manifest.ExecutionInputSHA256 = core.FingerprintExecutionInput(string(body))
		return manifest, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.KnowledgeRefs) != 1 {
		t.Fatal("healthy writer lacks selected factual reference")
	}
	modelInput, err := core.BindCurrentAgentExecutionInput("org-1", manifest.ExecutionID, inputContext)
	if err != nil {
		t.Fatal(err)
	}
	body, err := modelInput.Request().Canonical()
	if err != nil {
		t.Fatal(err)
	}
	appendLegacyContextCompletion(t, store, task, manifest, string(body))
	full, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	var outcome events.Event
	for _, event := range full {
		if event.EventType == "TOOL_OUTCOME_RECORDED" {
			outcome = event
		}
	}
	candidate := events.Event{EventID: "terminal-temporal-candidate", OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "unrelated", CreatedAt: outcome.CreatedAt, SchemaVersion: events.SchemaVersion, Payload: json.RawMessage(`{"note":"healthy unrelated source between outcome and terminal"}`)}
	insertCandidateForTest(t, store, candidate, outcome.Sequence+1)
	before, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "legacy-context", 256)
	if err != nil {
		t.Fatalf("healthy public terminal-use control: %v", err)
	}
	full, err = store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	_, terminalTask := latestTestProjection[core.Task](t, t.Context(), store, "task", task.ID)
	var terminal events.Event
	for _, event := range full {
		if event.EventType == "TASK_VERIFIED_COMPLETE" {
			terminal = event
		}
	}
	binding := events.WorkCompletionBinding{OrganizationID: "org-1", CorrelationID: "legacy-context", Work: work, WorkVersion: 1, Intent: intent, InboxObservations: before.InboxObservations, AgentBlueprints: map[core.ID]core.AgentBlueprint{config.BlueprintID: blueprint}, ExecutionProfiles: map[core.ID]core.ExecutionProfile{config.ProfileID: profile}}
	if _, err := events.NewCompletionEvidenceValidator(full).ValidateTask(binding, events.WorkCompletionTaskBinding{Task: terminalTask, Version: 3, CorrelationID: "legacy-context"}, terminal); err != nil {
		t.Fatalf("healthy exact selected Task terminal owner: %v", err)
	}
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, []byte(`{"projection":{"projection_kind":"knowledge","record_id":"unrelated-knowledge"}}`), candidate.EventID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	}); err != nil {
		t.Fatal(err)
	}
	full, err = store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	_, ownerErr := events.NewCompletionEvidenceValidator(full).ValidateTask(binding, events.WorkCompletionTaskBinding{Task: terminalTask, Version: 3, CorrelationID: "legacy-context"}, terminal)
	if ownerErr == nil {
		t.Fatal("selected terminal owner accepted malformed prior at-use source")
	}
	snapshot, publicErr := store.VerifiedIncidentEvents(t.Context(), "org-1", "legacy-context", 256)
	if publicErr == nil {
		t.Fatalf("public omitted selected Task terminal at-use source: public=%d private=%d exactOwnerError=%v", len(snapshot.Work.Events), len(snapshot.DependencyEvents), ownerErr)
	}
}

func TestAuditIncidentKnowledgeAggregateUseCandidates(t *testing.T) {
	parallelIncidentTest(t)
	for _, phase := range []string{"work-evidence", "work-terminal", "goal-use", "after-goal-evaluation", "after-goal"} {
		t.Run(phase, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			now := time.Now().UTC()
			mission := core.Mission{ID: "mission-audit", OrganizationID: "org-1", Statement: "bounded direction", Status: core.MissionActive, CreatedAt: now}
			goal := core.Goal{ID: "goal-audit", OrganizationID: "org-1", MissionID: mission.ID, Objective: "bounded work", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "The result is independently verified.", Origin: "DEFAULT"}}, Status: core.GoalActive, CreatedAt: now}
			for _, draft := range []events.ProjectionDraft{
				{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "organization", RecordID: "org-1", Version: 1, Value: core.Organization{ID: "org-1", Name: "Audit", PolicyVersion: "v1", CreatedAt: now}},
				{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "MISSION_CREATED", SourceActorID: "runtime", CorrelationID: "mission-audit"}, ProjectionKind: "mission", RecordID: string(mission.ID), Version: 1, Value: mission},
				{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "GOAL_CREATED", SourceActorID: "runtime", CorrelationID: "goal-audit"}, ProjectionKind: "goal", RecordID: string(goal.ID), Version: 1, Value: goal},
			} {
				if _, err := store.AppendProjection(t.Context(), draft); err != nil {
					t.Fatal(err)
				}
			}
			blueprint, agent, config := appendLegacyContextAgent(t, store)
			fact := appendFactualInferenceKnowledge(t, store, "selected-fact", "Bounded verification", "The bounded rehearsal restored three records.")
			reviewed := appendReviewedGoalIntent(t, t.Context(), store, "org-1", "legacy-context", "intent-legacy-context", string(goal.ID), "bounded work", core.ExecutionAgent, now)
			confirmation := events.IntentConfirmedPayload{IntentID: string(reviewed.ID), GoalID: string(goal.ID), Version: 1, Fingerprint: reviewed.Fingerprint, ConfirmingActorID: "user-1", ConfirmingActorKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", MessageID: "audit-confirm"}
			if _, err := store.AppendIntentConfirmation(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CONFIRMED", SourceActorID: "user-1", TaskID: "task-legacy-context", CorrelationID: "legacy-context", Payload: confirmation}, goal.ID, ""); err != nil {
				t.Fatal(err)
			}
			intent := core.Intent{ID: reviewed.ID, OrganizationID: "org-1", GoalID: goal.ID, OriginalInstruction: "bounded work under goal-audit", NormalizedObjective: reviewed.Objective, Context: reviewed.Context, Deliverables: reviewed.Deliverables, CompletionCriteria: reviewed.CompletionCriteria, AcceptedFingerprint: reviewed.Fingerprint, SourcePrincipalID: "user-1", SourcePrincipalKind: core.PrincipalHuman, SourceChannel: "HUMAN_DIRECT", SourceMessageID: "source-legacy-context", CreatedAt: now}
			work := core.Work{ID: "work-1", IntentID: intent.ID, GoalID: goal.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
			for _, draft := range []events.ProjectionDraft{
				{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: "legacy-context"}, ProjectionKind: "intent", RecordID: string(intent.ID), Version: 1, Value: intent},
				{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: "legacy-context"}, ProjectionKind: "work", RecordID: string(work.ID), Version: 1, Value: work},
			} {
				if _, err := store.AppendProjection(t.Context(), draft); err != nil {
					t.Fatal(err)
				}
			}
			full, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			strategy, eventRefs, contextRefs, err := events.ResolveStrategicContext("org-1", work, full, full[len(full)-1].Sequence+1)
			if err != nil {
				t.Fatal(err)
			}
			plan := core.Plan{ID: "plan-legacy-context", IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, Version: 1, StrategicEventRefs: eventRefs, StrategicContextRefs: contextRefs, Tasks: []core.PlanTask{{Key: "root", Description: "bounded Agent work", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed}}, CreatedAt: now}
			plan.Fingerprint, err = core.FingerprintPlan(plan)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "PLAN_CREATED", SourceActorID: "runtime", TaskID: "task-legacy-context", CorrelationID: "legacy-context", Payload: plan}); err != nil {
				t.Fatal(err)
			}
			brief, err := core.AgentTaskExecutionBrief(intent, plan.Tasks[0], plan.Fingerprint)
			if err != nil {
				t.Fatal(err)
			}
			task := core.Task{ID: "task-legacy-context", WorkID: work.ID, Description: "bounded Agent work", ExecutionBrief: brief, AcceptanceCriteria: intent.CompletionCriteria, ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: &config, TaskContractVersion: "1", Status: core.TaskPending}
			if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "legacy-context"}, ProjectionKind: "task", RecordID: string(task.ID), Version: 1, Value: task}); err != nil {
				t.Fatal(err)
			}
			task.Status = core.TaskRunning
			inputContext := core.AgentExecutionInputContext{Blueprint: blueprint, Task: task, Strategy: strategy, Knowledge: []core.KnowledgeRecord{fact}}
			var manifest core.ExecutionContextManifest
			_, _, err = store.AppendExecutionStart(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_STARTED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "legacy-context", Payload: events.ExecutionStartDetail{StrategicEventRefs: eventRefs, StrategicContextRefs: contextRefs}}, ProjectionKind: "task", RecordID: string(task.ID), Version: 2, Value: task}, []events.InboxRoute{{Scope: events.RecipientTask, ID: string(task.ID)}, {Scope: events.RecipientAgent, ID: string(agent.ID)}}, func(selection events.ExecutionStartSelection) (core.ExecutionContextManifest, error) {
				manifest = testAgentStartManifest(task, selection)
				manifest.Provider, manifest.Model, manifest.PromptVersion = "fake", "fake-model/v1", "v1"
				manifest.AdditionalContextRefs = contextRefs
				manifest.EventRefs = append(append([]string{}, eventRefs...), manifest.EventRefs...)
				binding, err := core.BindCurrentAgentExecutionInput("org-1", manifest.ExecutionID, inputContext)
				if err != nil {
					return core.ExecutionContextManifest{}, err
				}
				body, err := binding.Request().Canonical()
				manifest.ExecutionInputSHA256 = core.FingerprintExecutionInput(string(body))
				return manifest, err
			})
			if err != nil {
				t.Fatal(err)
			}
			input, err := core.BindCurrentAgentExecutionInput("org-1", manifest.ExecutionID, inputContext)
			if err != nil {
				t.Fatal(err)
			}
			body, err := input.Request().Canonical()
			if err != nil {
				t.Fatal(err)
			}
			appendLegacyContextCompletion(t, store, task, manifest, string(body))
			full, err = store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			var verification, terminal events.Event
			for _, event := range full {
				if event.EventType == "COMPLETION_VERIFIED" {
					verification = event
				}
				if event.EventType == "TASK_VERIFIED_COMPLETE" {
					terminal = event
				}
			}
			evidence := events.WorkCompletionEvidencePayload{WorkID: work.ID, WorkVersion: 2, GoalID: goal.ID, IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, PlanID: plan.ID, PlanVersion: 1, Criteria: intent.CompletionCriteria, Tasks: []events.WorkCompletionTaskEvidencePayload{{TaskID: task.ID, TaskVersion: 3, VerificationEventRef: verification.EventID, CompletionEventRef: terminal.EventID}}, CreatedAt: time.Now().UTC()}
			evidence.Fingerprint, err = evidence.ExpectedFingerprint()
			if err != nil {
				t.Fatal(err)
			}
			evidenceEvent, err := store.AppendWorkCompletionEvidence(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_COMPLETION_EVALUATED", SourceActorID: "runtime", CorrelationID: "legacy-context", Payload: evidence})
			if err != nil {
				t.Fatal(err)
			}
			work.Status = core.WorkCompleted
			workTerminal, err := store.AppendWorkCompletion(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_COMPLETED", SourceActorID: "runtime", CorrelationID: "legacy-context", Payload: events.WorkCompletionTransitionPayload{EvidenceEventRef: evidenceEvent.EventID, Fingerprint: evidence.Fingerprint}}, ProjectionKind: "work", RecordID: string(work.ID), Version: 2, Value: work})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.AppendGoalProgress(t.Context(), "org-1", goal.ID); err != nil {
				t.Fatal(err)
			}
			full, err = store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			var goalUse events.Event
			for _, event := range full {
				if event.EventType == "GOAL_PROGRESS_EVALUATED" {
					goalUse = event
				}
			}
			insertion := evidenceEvent.Sequence
			if phase == "work-terminal" {
				insertion = workTerminal.Sequence
			}
			if phase == "goal-use" {
				insertion = goalUse.Sequence
			}
			if phase == "after-goal-evaluation" {
				insertion = goalUse.Sequence + 1
			}
			if phase == "after-goal" {
				insertion = full[len(full)-1].Sequence + 1
			}
			candidate := events.Event{EventID: "aggregate-temporal-candidate", OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "unrelated", CreatedAt: time.Now().UTC(), SchemaVersion: events.SchemaVersion, Payload: json.RawMessage(`{"note":"healthy independent source"}`)}
			insertCandidateForTest(t, store, candidate, insertion)
			correlation := "legacy-context"
			if phase == "goal-use" || phase == "after-goal-evaluation" || phase == "after-goal" {
				correlation = "goal-audit"
			}
			before, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
			if err != nil {
				t.Fatalf("healthy selected aggregate public: %v", err)
			}
			full, err = store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			leases, freezes, err := store.KnowledgeAuthorityAdmissions(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			graph, err := events.ValidateProjectionHistory(full, before.InboxObservations, leases, freezes)
			if err != nil {
				t.Fatalf("healthy owner graph: %v", err)
			}
			if err := events.ValidateProjectionCompletions(graph, full, before.InboxObservations); err != nil {
				t.Fatalf("healthy exact aggregate owners: %v", err)
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, []byte(`{"projection":{"projection_kind":"knowledge","record_id":"unrelated-knowledge"}}`), candidate.EventID); err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
					return err
				}
				return rebuildEventIntegrity(t.Context(), tx)
			}); err != nil {
				t.Fatal(err)
			}
			full, err = store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			ownerErr := events.ValidateWorkCompletions(graph, full, nil, before.InboxObservations)
			if phase == "goal-use" || phase == "after-goal-evaluation" || phase == "after-goal" {
				ownerErr = events.ValidateGoalCompletions(graph, full, nil, before.InboxObservations)
			}
			snapshot, publicErr := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
			if phase == "after-goal-evaluation" || phase == "after-goal" {
				if ownerErr != nil || publicErr != nil {
					t.Fatalf("after-use source changed selected meaning owner=%v public=%v", ownerErr, publicErr)
				}
				return
			}
			if ownerErr == nil {
				t.Fatal("selected aggregate owners accepted malformed at-use source")
			}
			if publicErr == nil {
				t.Fatalf("public omitted selected aggregate at-use source: public=%d private=%d exactOwnerError=%v", len(snapshot.Work.Events), len(snapshot.DependencyEvents), ownerErr)
			}
		})
	}
}

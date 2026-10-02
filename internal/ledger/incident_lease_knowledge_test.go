package ledger

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentIncomingKnowledgeLease(t *testing.T) {
	parallelIncidentTest(t)
	for _, mode := range []string{"both", "payload", "envelope", "event-only", "record-only", "event-only-kind", "event-only-method", "event-only-principal", "event-only-missing-gates", "record-only-missing-gates", "event-counterpart", "record-counterpart", "event-only-duplicate-gates", "event-only-duplicate-container", "event-only-duplicate-refs", "record-only-kind", "record-only-status", "same-envelope", "same-envelope-record-only", "duplicate-lease", "before-selected", "after-revoke", "unconsumed", "unrelated", "consequential-only"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "knowledge-lease.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			var check events.Event
			if mode == "before-selected" {
				check = appendIncidentLeaseKnowledge(t, store, "org-2", "foreign-fact", true)
			}
			correlation := appendDerivedIncidentChain(t, store, 1)
			if mode != "before-selected" {
				check = appendIncidentLeaseKnowledge(t, store, "org-2", "foreign-fact", mode != "unconsumed")
			}
			if mode == "after-revoke" {
				var lease core.CapabilityLease
				var body []byte
				if err := store.db.QueryRowContext(t.Context(), `SELECT body FROM records WHERE kind='capability_lease' AND record_id='lease-derived-0'`).Scan(&body); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(body, &lease); err != nil {
					t.Fatal(err)
				}
				now := time.Now().UTC()
				lease.RevokedAt = &now
				if err := store.AppendRecord(t.Context(), "org-1", "CAPABILITY_REVOKED", "runtime", string(lease.OriginTaskID), nil, nil, "capability_lease", string(lease.ID), 2, lease); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "event-counterpart" || mode == "record-counterpart" {
				var trace core.AuthorizationTrace
				if err := json.Unmarshal(check.Payload, &trace); err != nil {
					t.Fatal(err)
				}
				check, err = store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-2", EventType: "CAPABILITY_CHECKED", SourceActorID: "validator", TaskID: string(trace.TaskID), CorrelationID: "independent-check", AuthorizationRefs: []string{string(trace.LeaseID)}, Payload: trace})
				if err != nil {
					t.Fatal(err)
				}
			}
			leases, freezes, err := store.KnowledgeAuthorityAdmissions(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			stream, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := events.ValidateProjectionHistory(stream, nil, leases, freezes); err != nil {
				t.Fatalf("owning writers: %v", err)
			}
			if _, err := admittedProjectionRecordsBounded(t.Context(), store.db, 2<<20, `WHERE r.kind='knowledge' AND r.record_id='foreign-fact'`); err != nil {
				t.Fatalf("healthy retained record owner: %v", err)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256); err != nil {
				t.Fatalf("healthy unrelated consumer: %v", err)
			}
			if mode == "consequential-only" {
				changeKnowledgePayloadRef(t, store, check, "consequential", []core.CapabilityDecision{{Allowed: true, LeaseID: "lease-derived-0", Action: "observe", Resource: "foreign-fact", Scope: "org-2"}})
			} else if mode == "duplicate-lease" {
				duplicateKnowledgeDocument(t, store, check.EventID, nil, "lease_id", []byte(`"lease-derived-0"`), false)
			} else if mode != "unrelated" {
				if mode != "envelope" {
					changeKnowledgePayloadRef(t, store, check, "lease_id", "lease-derived-0")
				}
				if mode != "payload" {
					refs, _ := json.Marshal([]string{"lease-derived-0"})
					if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET authorization_refs=? WHERE event_id=?`, refs, check.EventID); err != nil {
						t.Fatal(err)
					}
				}
			}
			if mode == "same-envelope" || mode == "same-envelope-record-only" {
				if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET organization_id='org-1' WHERE event_id=?`, check.EventID); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "event-only-duplicate-gates" || mode == "event-only-duplicate-container" || mode == "event-only-duplicate-refs" || mode == "event-only-missing-gates" {
				var activationID string
				if err := store.db.QueryRowContext(t.Context(), `SELECT admission_event_id FROM records WHERE kind='knowledge' AND record_id='foreign-fact' AND version=2`).Scan(&activationID); err != nil {
					t.Fatal(err)
				}
				if mode == "event-only-duplicate-gates" {
					duplicateKnowledgeDocument(t, store, activationID, []string{"projection", "value"}, "validation_method", []byte(`"UNVALIDATED"`), true)
					duplicateKnowledgeDocument(t, store, activationID, []string{"projection", "value"}, "validated_by_kind", []byte(`"RUNTIME"`), true)
				}
				if mode == "event-only-duplicate-container" {
					duplicateKnowledgeDocument(t, store, activationID, nil, "projection", []byte(`{"projection_kind":"unrelated","value":{}}`), true)
				}
				if mode == "event-only-duplicate-refs" {
					duplicateKnowledgeDocument(t, store, activationID, []string{"projection", "value"}, "validation_refs", []byte(`[]`), true)
				}
			}
			if mode == "event-only-missing-gates" || mode == "record-only-missing-gates" {
				if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET payload=CAST(json_remove(payload,'$.projection.value.validation_method','$.projection.value.validated_by_kind') AS BLOB) WHERE event_id IN (SELECT admission_event_id FROM records WHERE kind='knowledge' AND record_id='foreign-fact' AND version=2)`); err != nil {
					t.Fatal(err)
				}
				if _, err := store.db.ExecContext(t.Context(), `UPDATE records SET body=CAST(json_remove(body,'$.value.validation_method','$.value.validated_by_kind') AS BLOB) WHERE kind='knowledge' AND record_id='foreign-fact' AND version=2`); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				if mode == "event-counterpart" || mode == "record-counterpart" {
					refs, err := json.Marshal([]string{check.EventID})
					if err != nil {
						return err
					}
					if mode == "event-counterpart" {
						if _, err := tx.ExecContext(t.Context(), `UPDATE events SET event_type='AUDIT_NOTE',payload=CAST(json_set(payload,'$.projection.projection_kind','unrelated','$.projection.value.validation_refs',json(?)) AS BLOB) WHERE event_id IN (SELECT admission_event_id FROM records WHERE kind='knowledge' AND record_id='foreign-fact' AND version=2)`, string(refs)); err != nil {
							return err
						}
					} else {
						if _, err := tx.ExecContext(t.Context(), `UPDATE records SET kind='authorization_trace',body=CAST(json_set(body,'$.projection_kind','unrelated','$.value.validation_refs',json(?)) AS BLOB) WHERE kind='knowledge' AND record_id='foreign-fact' AND version=2`, string(refs)); err != nil {
							return err
						}
					}
				}
				if mode == "event-only" || mode == "event-only-kind" || mode == "event-only-method" || mode == "event-only-principal" || mode == "event-only-duplicate-gates" || mode == "event-only-duplicate-container" || mode == "event-only-duplicate-refs" || mode == "event-only-missing-gates" {
					if mode == "event-only-kind" || mode == "event-only-method" || mode == "event-only-principal" {
						path, value := "$.projection.projection_kind", "unrelated"
						if mode == "event-only-method" {
							path, value = "$.projection.value.validation_method", "UNVALIDATED"
						}
						if mode == "event-only-principal" {
							path, value = "$.projection.value.validated_by_kind", "RUNTIME"
						}
						if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=CAST(json_set(payload,?,?) AS BLOB) WHERE event_id IN (SELECT admission_event_id FROM records WHERE kind='knowledge' AND record_id='foreign-fact' AND version=2)`, path, value); err != nil {
							return err
						}
					}
					if _, err := tx.ExecContext(t.Context(), `DELETE FROM records WHERE kind='knowledge' AND record_id='foreign-fact'`); err != nil {
						return err
					}
				}
				if mode == "record-only" || mode == "same-envelope-record-only" || mode == "record-only-kind" || mode == "record-only-status" || mode == "record-only-missing-gates" {
					if mode == "record-only-kind" || mode == "record-only-status" {
						path, value := "$.projection_kind", "unrelated"
						if mode == "record-only-status" {
							path, value = "$.value.status", "CANDIDATE"
						}
						if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=CAST(json_set(body,?,?) AS BLOB) WHERE kind='knowledge' AND record_id='foreign-fact' AND version=2`, path, value); err != nil {
							return err
						}
					}
					if _, err := tx.ExecContext(t.Context(), `DELETE FROM events WHERE event_id IN (SELECT admission_event_id FROM records WHERE kind='knowledge' AND record_id='foreign-fact' AND version=2)`); err != nil {
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
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			stream, err = store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			_, fullErr := events.ValidateProjectionHistory(stream, nil, leases, freezes)
			invalid := mode != "unconsumed" && mode != "unrelated" && mode != "consequential-only"
			if mode == "record-counterpart" {
				if fullErr != nil {
					t.Fatalf("healthy event channel was changed by record-only corruption: %v", fullErr)
				}
				if _, err := admittedProjectionRecordsBounded(t.Context(), store.db, 2<<20, `WHERE r.record_id='foreign-fact'`); err == nil {
					t.Fatal("retained record owner accepted displaced Knowledge authority")
				}
			}
			if mode == "record-only" || mode == "same-envelope-record-only" || mode == "record-only-kind" || mode == "record-only-status" || mode == "record-only-missing-gates" {
				if _, err := admittedProjectionRecordsBounded(t.Context(), store.db, 2<<20, `WHERE r.kind='knowledge' AND r.record_id='foreign-fact'`); err == nil {
					t.Fatal("retained record owner accepted orphan or malformed governed Knowledge")
				}
			}
			if mode != "record-only" && mode != "same-envelope-record-only" && mode != "record-only-kind" && mode != "record-only-status" && mode != "record-only-missing-gates" && mode != "record-counterpart" && (fullErr != nil) != invalid {
				t.Fatalf("full recovery applicability: %v", fullErr)
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
			if invalid {
				if err == nil {
					t.Fatal("incident omitted consumed foreign Knowledge claim against selected capability lease")
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("invalid lease consumer returned partial snapshot")
				}
			} else {
				if err != nil {
					t.Fatalf("unrelated or unused capability check poisoned incident: %v", err)
				}
				for _, event := range snapshot.DependencyEvents {
					if event.EventID == check.EventID {
						t.Fatal("unrelated or unused capability check selected")
					}
				}
			}
		})
	}
}

func TestIncidentSharedKnowledgeLease(t *testing.T) {
	parallelIncidentTest(t)
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	consumed := appendIncidentLeaseKnowledge(t, store, "org-1", "shared-fact", true)
	var trace core.AuthorizationTrace
	if err := json.Unmarshal(consumed.Payload, &trace); err != nil {
		t.Fatal(err)
	}
	selected, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "CAPABILITY_CHECKED", SourceActorID: string(trace.ActorID), TaskID: string(trace.TaskID), CorrelationID: "selected-authority", AuthorizationRefs: []string{string(trace.LeaseID)}, Payload: trace})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	leases, freezes, err := store.KnowledgeAuthorityAdmissions(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := events.ValidateProjectionHistory(stream, nil, leases, freezes); err != nil {
		t.Fatalf("valid governed shared-lease consumer: %v", err)
	}
	snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", selected.CorrelationID, 256)
	if err != nil {
		t.Fatal(err)
	}
	selectedLease := false
	for _, record := range snapshot.AuthorityRecords {
		selectedLease = selectedLease || record.RecordID == string(trace.LeaseID)
	}
	if !selectedLease {
		t.Fatal("selected incident did not retain shared capability authority")
	}
	for _, event := range snapshot.DependencyEvents {
		if event.EventID == consumed.EventID || event.EventType == "KNOWLEDGE_ACTIVATED" {
			t.Fatal("valid same-organization governed use expanded an independent incident")
		}
	}
}

func TestIncidentLeaseConsumerLookup(t *testing.T) {
	parallelIncidentTest(t)
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	consumed := appendIncidentLeaseKnowledge(t, store, "org-1", "shared-fact", true)
	var trace core.AuthorizationTrace
	if err := json.Unmarshal(consumed.Payload, &trace); err != nil {
		t.Fatal(err)
	}
	for n := 1; n < 64; n++ {
		appendIncidentLeaseKnowledge(t, store, fmt.Sprintf("other-org-%d", n), fmt.Sprintf("other-fact-%d", n), true)
	}
	if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "CAPABILITY_CHECKED", SourceActorID: string(trace.ActorID), TaskID: string(trace.TaskID), CorrelationID: "selected-authority", AuthorizationRefs: []string{string(trace.LeaseID)}, Payload: trace}); err != nil {
		t.Fatal(err)
	}
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	leases, freezes, err := store.KnowledgeAuthorityAdmissions(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := events.ValidateProjectionHistory(stream, nil, leases, freezes); err != nil {
		t.Fatalf("real consumer history: %v", err)
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected-authority", 256); err != nil {
		t.Fatalf("complete public incident read: %v", err)
	}
	rows, err := store.db.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+incidentLeaseConsumerSQL(1), "org-1", string(trace.LeaseID))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	lookups := 0
	var repeated []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(detail, "check_event") {
			continue
		}
		if strings.Contains(detail, "SEARCH check_event ") && strings.Contains(detail, "event_id=?") {
			lookups++
		} else {
			repeated = append(repeated, detail)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	// Each owned event/record branch must probe the globally unique check ID.
	// An event-type scan before reference expansion repeats JSON traversal for
	// every capability check in unrelated governed supporting histories.
	if lookups != 2 || len(repeated) != 0 {
		t.Fatalf("capability check lookup repeats supporting history: global ID probes=%d repeated=%v", lookups, repeated)
	}
}

func TestIncidentOpaqueLeaseConsumer(t *testing.T) {
	parallelIncidentTest(t)
	path := filepath.Join(t.TempDir(), "opaque-lease-consumer.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	correlation := appendDerivedIncidentChain(t, store, 1)
	appendIncidentLeaseKnowledge(t, store, "org-2", "foreign-fact", true)
	var selectedCheck string
	if err := store.db.QueryRowContext(t.Context(), `SELECT event_id FROM events WHERE event_type='CAPABILITY_CHECKED' AND organization_id='org-1'`).Scan(&selectedCheck); err != nil {
		t.Fatal(err)
	}
	records, err := store.Records(t.Context(), "knowledge", "foreign-fact")
	if err != nil || len(records) != 2 {
		t.Fatalf("writer Knowledge source: records=%d error=%v", len(records), err)
	}
	var note events.ProjectionRecord
	if err := json.Unmarshal(records[1], &note); err != nil {
		t.Fatal(err)
	}
	var quoted core.KnowledgeRecord
	if err := json.Unmarshal(note.Value, &quoted); err != nil {
		t.Fatal(err)
	}
	quoted.ValidationRefs = []string{selectedCheck}
	note.Value, err = json.Marshal(quoted)
	if err != nil {
		t.Fatal(err)
	}
	// The public generic writer stores this quoted projection-shaped note with
	// no admission metadata. Its content does not become Knowledge authority.
	if err := store.AppendRecord(t.Context(), "org-2", "AUDIT_NOTE", "runtime", "", nil, nil, "authorization_trace", "quoted-knowledge", 1, note); err != nil {
		t.Fatal(err)
	}
	var admission, fingerprint string
	if err := store.db.QueryRowContext(t.Context(), `SELECT admission_event_id,admission_fingerprint FROM records WHERE kind='authorization_trace' AND record_id='quoted-knowledge'`).Scan(&admission, &fingerprint); err != nil {
		t.Fatal(err)
	}
	if admission != "" || fingerprint != "" {
		t.Fatal("generic writer attached projection authority to the quoted note")
	}
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	leases, freezes, err := store.KnowledgeAuthorityAdmissions(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := events.ValidateProjectionHistory(stream, nil, leases, freezes); err != nil {
		t.Fatalf("full event owner promoted opaque note: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
	if err != nil {
		t.Fatalf("opaque generic record was promoted to a governed lease consumer: %v", err)
	}
	for _, event := range snapshot.DependencyEvents {
		if event.OrganizationID != "org-1" {
			t.Fatal("opaque foreign source entered selected dependencies")
		}
	}
}

func TestIncidentKnowledgeLeaseObservation(t *testing.T) {
	parallelIncidentTest(t)
	for _, field := range []string{"provenance", "occurrence"} {
		t.Run(field, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			correlation := appendDerivedIncidentChain(t, store, 1)
			if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-2", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "foreign-setup"}, ProjectionKind: "organization", RecordID: "org-2", Version: 1, Value: core.Organization{ID: "org-2", Name: "Other organization", PolicyVersion: "v1", CreatedAt: time.Now().UTC()}}); err != nil {
				t.Fatal(err)
			}
			check, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-2", EventType: "CAPABILITY_CHECKED", SourceActorID: "validator", TaskID: "observation-task", CorrelationID: "ordinary-observation", AuthorizationRefs: []string{"lease-derived-0"}, Payload: core.AuthorizationTrace{Allowed: true, LeaseID: "lease-derived-0", ActorID: "validator", ActorKind: core.PrincipalHuman, TaskID: "observation-task", Action: "knowledge.validate", Resource: "ordinary-fact", Scope: "org-2"}})
			if err != nil {
				t.Fatal(err)
			}
			candidate := core.KnowledgeRecord{KnowledgeID: "ordinary-fact", OrganizationID: "org-2", Version: 1, Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: "org-2", Status: core.KnowledgeCandidate, Title: "Observed capability decision", Content: "The recorded decision is evidence of an observation.", Basis: core.KnowledgeBasisExternalEvidence, ProvenanceEventRefs: []string{check.EventID}, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated}
			if field == "occurrence" {
				evidence, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-2", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "independent-provenance", Payload: map[string]string{"note": "Independent recorded observation."}})
				if err != nil {
					t.Fatal(err)
				}
				candidate.ProvenanceEventRefs, candidate.OccurrenceEventRefs = []string{evidence.EventID}, []string{check.EventID}
				candidate.CreatedAt = time.Now().UTC()
			}
			if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-2", EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: "knowledge-ordinary-fact"}, ProjectionKind: "knowledge", RecordID: "ordinary-fact", Version: 1, Value: candidate}); err != nil {
				t.Fatal(err)
			}
			stream, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			leases, freezes, err := store.KnowledgeAuthorityAdmissions(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := events.ValidateProjectionHistory(stream, nil, leases, freezes); err != nil {
				t.Fatalf("ordinary check citation became validation authority: %v", err)
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
			if err != nil {
				t.Fatalf("ordinary %s citation poisoned selected lease: %v", field, err)
			}
			for _, event := range snapshot.DependencyEvents {
				if event.EventID == check.EventID {
					t.Fatal("ordinary check citation entered selected lease dependencies")
				}
			}
		})
	}
}

func BenchmarkIncidentLeaseConsumerRead(b *testing.B) {
	for _, workload := range []struct {
		name                        string
		reads, consumers, unrelated int
	}{
		{"small", 1, 1, 0}, {"many-reads", 16, 1, 0}, {"many-consumers", 16, 64, 0}, {"unrelated-history", 16, 64, 256},
	} {
		b.Run(workload.name, func(b *testing.B) {
			store, err := Open(":memory:")
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = store.Close() })
			consumed := appendIncidentLeaseKnowledge(b, store, "org-1", "shared-fact", true)
			var trace core.AuthorizationTrace
			if err := json.Unmarshal(consumed.Payload, &trace); err != nil {
				b.Fatal(err)
			}
			for n := 1; n < workload.consumers; n++ {
				appendIncidentLeaseKnowledge(b, store, fmt.Sprintf("other-org-%d", n), fmt.Sprintf("other-fact-%d", n), true)
			}
			for range workload.unrelated {
				if _, err := store.Append(b.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "unrelated", Payload: map[string]string{"note": "Independent history."}}); err != nil {
					b.Fatal(err)
				}
			}
			for n := range workload.reads {
				if _, err := store.Append(b.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "CAPABILITY_CHECKED", SourceActorID: string(trace.ActorID), TaskID: string(trace.TaskID), CorrelationID: fmt.Sprintf("selected-authority-%d", n), AuthorizationRefs: []string{string(trace.LeaseID)}, Payload: trace}); err != nil {
					b.Fatal(err)
				}
			}
			stream, err := store.Events(b.Context(), "")
			if err != nil {
				b.Fatal(err)
			}
			leases, freezes, err := store.KnowledgeAuthorityAdmissions(b.Context())
			if err != nil {
				b.Fatal(err)
			}
			if _, err := events.ValidateProjectionHistory(stream, nil, leases, freezes); err != nil {
				b.Fatalf("writer history: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				for n := range workload.reads {
					if _, err := store.VerifiedIncidentEvents(b.Context(), "org-1", fmt.Sprintf("selected-authority-%d", n), 256); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ReportMetric(float64(workload.reads), "reads/op")
		})
	}
}

// Build governed history through the same durable writers used by live admission.
// Unique Tasks, correlations and evidence ensure only the claimed lease connects it.
func appendIncidentLeaseKnowledge(t testing.TB, store *SQLite, organization, id string, consumed bool) events.Event {
	t.Helper()
	ctx := t.Context()
	_, err := store.AppendProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: organization, EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup-" + organization}, ProjectionKind: "organization", RecordID: organization, Version: 1, Value: core.Organization{ID: core.ID(organization), Name: "Organization", PolicyVersion: "v1", CreatedAt: time.Now().UTC()}})
	if err != nil {
		t.Fatal(err)
	}
	artifact, task := "artifact-"+id, "task-"+id
	evidence, err := store.Append(ctx, events.TrustedDraft{OrganizationID: organization, EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "evidence-" + id, ArtifactRefs: []string{artifact}, Payload: map[string]string{"summary": "Recorded observation."}})
	if err != nil {
		t.Fatal(err)
	}
	candidate := core.KnowledgeRecord{KnowledgeID: core.ID(id), OrganizationID: core.ID(organization), Version: 1, Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: core.ID(organization), Status: core.KnowledgeCandidate, Title: "Observation", Content: "Preserve the observation.", Basis: core.KnowledgeBasisExternalEvidence, ProvenanceEventRefs: []string{evidence.EventID}, EvidenceArtifactRefs: []string{artifact}, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated}
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: organization, EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: "knowledge-" + id, ArtifactRefs: []string{artifact}}, ProjectionKind: "knowledge", RecordID: id, Version: 1, Value: candidate}); err != nil {
		t.Fatal(err)
	}
	lease := core.CapabilityLease{ID: core.ID("lease-" + id), ActorID: "validator", ActorKind: core.PrincipalHuman, OriginTaskID: core.ID(task), Action: "knowledge.validate", Resource: id, Scope: organization}
	if err := store.AppendRecord(ctx, organization, "CAPABILITY_GRANTED", "runtime", task, nil, nil, "capability_lease", string(lease.ID), 1, lease); err != nil {
		t.Fatal(err)
	}
	trace := core.AuthorizationTrace{Allowed: true, LeaseID: lease.ID, ActorID: lease.ActorID, ActorKind: lease.ActorKind, TaskID: lease.OriginTaskID, Action: lease.Action, Resource: lease.Resource, Scope: lease.Scope}
	check, err := store.Append(ctx, events.TrustedDraft{OrganizationID: organization, EventType: "CAPABILITY_CHECKED", SourceActorID: "validator", TaskID: task, CorrelationID: "check-" + id, AuthorizationRefs: []string{string(lease.ID)}, Payload: trace})
	if err != nil {
		t.Fatal(err)
	}
	if !consumed {
		return check
	}
	judgment, err := store.Append(ctx, events.TrustedDraft{OrganizationID: organization, EventType: "HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED", SourceActorID: "validator", TaskID: task, CorrelationID: "judgment-" + id, ArtifactRefs: []string{artifact}, Payload: events.KnowledgeJudgmentPayload{KnowledgeID: candidate.KnowledgeID, CandidateVersion: 1, Decision: events.KnowledgeJudgmentValidated, ContextUse: core.KnowledgeFactualReference, Statement: "The observation is a factual reference.", CapabilityCheckEventID: check.EventID, SourcePrincipalID: "validator", SourcePrincipalKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", ArtifactRefs: []string{artifact}}})
	if err != nil {
		t.Fatal(err)
	}
	active := candidate
	previous, verifiedAt := 1, time.Now().UTC()
	active.Version, active.Status, active.ContextUse = 2, core.KnowledgeActive, core.KnowledgeFactualReference
	active.SupersedesVersion, active.LastVerifiedAt = &previous, &verifiedAt
	active.ValidationMethod, active.ValidatedBy, active.ValidatedByKind = core.KnowledgeValidationHuman, "validator", core.PrincipalHuman
	active.ValidationRefs = []string{check.EventID, judgment.EventID}
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: organization, EventType: "KNOWLEDGE_ACTIVATED", SourceActorID: "runtime", CorrelationID: "knowledge-" + id, ArtifactRefs: []string{artifact}}, ProjectionKind: "knowledge", RecordID: id, Version: 2, Value: active}); err != nil {
		t.Fatal(err)
	}
	return check
}

package ledger

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

// SQLite evaluates both operands of AND when it produces a Boolean value.
// Inside the container CASE, that would traverse an unrelated typed source's
// owner tree even when identity membership is false. An error-producing JSON
// expression provides an evaluation sentinel without a wall-clock threshold.
func TestProjectionCandidateMembershipLazyEvaluation(t *testing.T) {
	store := projectionScopeFixture(t)
	var eventID string
	if err := store.db.QueryRowContext(t.Context(), `SELECT admission_event_id FROM records WHERE kind='team'`).Scan(&eventID); err != nil {
		t.Fatal(err)
	}
	for _, sample := range []struct {
		name, prefix, container, membership, query, owner string
		selected                                          string
	}{
		{"scope", "", projectionEventContainerBytes("e.payload"), `(e.organization_id=?1 OR ` + projectionSourceIdentityBytes("e.payload", "?1") + `)`, projectionScopeSQL(), projectionScopeClaims("event"), "org-1"},
		{"identity", `WITH selected AS MATERIALIZED (SELECT json_extract(value,'$.id') AS identity FROM json_each(?1)) `, projectionIdentityEventContainerBytes("e.payload"), projectionSelectedSourceBytes("e.payload"), projectionIdentitySQL(), projectionIdentityClaims("event"), `[{"kind":"mission","id":"selected-mission"},{"kind":"event","id":"selected-event"}]`},
	} {
		t.Run(sample.name, func(t *testing.T) {
			var container, member bool
			if err := store.db.QueryRowContext(t.Context(), sample.prefix+`SELECT `+sample.container+`,`+sample.membership+` FROM events e WHERE e.event_id=?2`, sample.selected, eventID).Scan(&container, &member); err != nil {
				t.Fatal(err)
			}
			if !container || member {
				t.Fatalf("actual foreign writer source does not discriminate container from membership: container=%v member=%v", container, member)
			}
			// Keep the complete production query/control flow. Replace only its
			// expensive event predicate, and select the actual foreign source in
			// a derived table; all record predicates remain unchanged.
			if strings.Count(sample.query, sample.owner) != 1 {
				t.Fatal("production query does not have exactly one event-owner predicate")
			}
			query := strings.Replace(sample.query, sample.owner, `json_extract('{','$.owner')`, 1)
			query = strings.Replace(query, "FROM events e", "FROM (SELECT * FROM events WHERE event_id=?2) e", 1)
			var conflict bool
			if err := store.db.QueryRowContext(t.Context(), query, sample.selected, eventID).Scan(&conflict); err != nil || conflict {
				t.Fatalf("production false membership did not skip the expensive-branch sentinel: conflict=%v err=%v", conflict, err)
			}
		})
	}
}

// The oracle deliberately has no byte gate: retained scalar ownership is the
// contract, and candidate rejection must preserve its result for the same raw
// source, metadata and selected keys. Valid ordinary foreign sources must also
// be excluded, so an always-true candidate cannot satisfy the property.
func TestProjectionSourceGateDifferential(t *testing.T) {
	t.Parallel()
	store := projectionScopeFixture(t) // Actual writer/full-owner/public baseline.
	var eventID string
	if err := store.db.QueryRowContext(t.Context(), `SELECT admission_event_id FROM records WHERE kind='team'`).Scan(&eventID); err != nil {
		t.Fatal(err)
	}

	// Clone one healthy writer seed before any damage. Fresh writer fixtures
	// would change event identities, timestamps and sealed source bytes.
	var paths [2]string
	for worker := range paths {
		paths[worker] = filepath.Join(t.TempDir(), "worker.db")
		if _, err := store.db.ExecContext(t.Context(), `VACUUM INTO ?`, paths[worker]); err != nil {
			t.Fatal(err)
		}
	}
	type decisions struct {
		scope, identity, containers int
		channels                    [2][2][2]int
	}
	var results [2]decisions
	run := func(t *testing.T, store *SQLite, first, last int) decisions {
		keys := map[incidentKey]bool{{kind: "event", id: "selected-event"}: true, {kind: "capability_lease", id: "selected-lease"}: true}
		identities := []map[string]string{{"kind": "event", "id": "selected-event"}, {"kind": "capability_lease", "id": "selected-lease"}}
		for _, kind := range projectionScopeKinds {
			for _, prefix := range []string{"selected-", "snowman-☃-", "control-\n-", "nul-\x00-", "quote-\"-"} {
				id := prefix + kind
				keys[incidentKey{kind: kind, id: id}] = true
				identities = append(identities, map[string]string{"kind": kind, "id": id})
			}
		}
		selectedJSON, err := json.Marshal(identities)
		if err != nil {
			t.Fatal(err)
		}
		scopeOracle := `SELECT EXISTS(SELECT 1 FROM events e WHERE ` + projectionScopeClaims("event") + `)
			OR EXISTS(SELECT 1 FROM records r LEFT JOIN events e ON e.event_id=r.admission_event_id WHERE
			(r.kind IN (` + incidentProjectionKindsSQL + `) OR r.admission_event_id<>'' OR r.admission_fingerprint<>'') AND ` + projectionScopeClaims("record") + `)`
		identityOracle := `WITH selected AS MATERIALIZED (SELECT json_extract(value,'$.kind') AS kind,json_extract(value,'$.id') AS identity FROM json_each(?1))
			SELECT EXISTS(SELECT 1 FROM events e WHERE ` + projectionIdentityClaims("event") + `)
			OR EXISTS(SELECT 1 FROM records r LEFT JOIN events e ON e.event_id=r.admission_event_id WHERE
			(r.kind IN (` + incidentProjectionKindsSQL + `,'capability_lease') OR r.admission_event_id<>'' OR r.admission_fingerprint<>'') AND ` + projectionIdentityClaims("record") + `)`
		// Retain unfiltered source applicability but freeze the prior complete
		// occurrence/owner relation; production node changes must not change both
		// sides of the differential comparison.
		scopeOracle = projectionPriorValueSQL(t, scopeOracle)
		identityOracle = projectionPriorValueSQL(t, identityOracle)
		scopeConflicts, identityConflicts := 0, 0
		var channelDecisions [2][2][2]int // channel, guard, accept/conflict
		caseIndex, checked := 0, 0
		assigned := func() bool { return caseIndex > first && caseIndex <= last }
		check := func(name string, payload, body []byte, kind, recordID, envelope, label, backing string, expected ...bool) {
			t.Helper()
			if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET payload=?,organization_id=?,event_type=? WHERE event_id=?`, payload, envelope, label, eventID); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.ExecContext(t.Context(), `UPDATE records SET body=?,kind=?,record_id=?,admission_event_id=? WHERE admission_fingerprint='source-gate' OR admission_event_id=?`, body, kind, recordID, backing, eventID); err != nil {
				t.Fatal(err)
			}
			// A stable test-only locator survives physical key/backing corruption.
			if _, err := store.db.ExecContext(t.Context(), `UPDATE records SET admission_fingerprint='source-gate' WHERE kind=? AND record_id=?`, kind, recordID); err != nil {
				t.Fatal(err)
			}
			caseIndex++
			if !assigned() {
				return
			}
			checked++
			tx, err := store.db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			var scope, identity bool
			if err := tx.QueryRowContext(t.Context(), scopeOracle, "org-1").Scan(&scope); err != nil {
				t.Fatalf("%s unfiltered scope: %v", name, err)
			}
			if err := tx.QueryRowContext(t.Context(), identityOracle, selectedJSON).Scan(&identity); err != nil {
				t.Fatalf("%s unfiltered identity: %v", name, err)
			}
			if scope {
				scopeConflicts++
			}
			if identity {
				identityConflicts++
			}
			if len(expected) == 2 && (scope != expected[0] || identity != expected[1]) {
				t.Fatalf("%s unfiltered oracle lost the expected source claim: scope=%v identity=%v want %v", name, scope, identity, expected)
			}
			for channel, prefix := range []string{"event/", "record/"} {
				if strings.HasPrefix(name, prefix) {
					for guard, conflict := range []bool{scope, identity} {
						decision := 0
						if conflict {
							decision = 1
						}
						channelDecisions[channel][guard][decision]++
					}
				}
			}
			for _, proof := range []struct {
				name, conflict string
				want           bool
				err            error
			}{
				{"scope", "incident projection organization claim conflicts with its retained source", scope, validateIncidentProjectionScope(t.Context(), tx, "org-1")},
				{"identity", "incident retained source claims a conflicting selected identity", identity, validateIncidentProjectionIDs(t.Context(), tx, keys)},
			} {
				if proof.err != nil && proof.err.Error() != proof.conflict {
					t.Fatalf("%s %s SQL failure: %v", name, proof.name, proof.err)
				}
				if (proof.err != nil) != proof.want {
					t.Fatalf("%s %s gated decision differs from unfiltered scalar guard: want conflict=%v err=%v payload=%q body=%q", name, proof.name, proof.want, proof.err, payload, body)
				}
			}
		}
		for _, channel := range []string{"event", "record"} {
			for _, kind := range projectionScopeKinds {
				for variant := 0; variant < 16; variant++ {
					ownID := "foreign-" + kind
					field := "id"
					if kind == "knowledge" {
						field = "knowledge_id"
					}
					value := map[string]any{field: ownID, "organization_id": "org-2", "routing": map[string]string{"organization_id": "org-2"}}
					if kind == "organization" {
						value[field] = "org-2"
					}
					doc := map[string]any{"projection_kind": kind, "record_id": ownID, "value": value}
					switch variant % 5 {
					case 0:
						value[field] = "selected-" + kind
					case 1:
						value[field] = "snowman-☃-" + kind
					case 2:
						value[field] = "control-\n-" + kind
					case 3:
						value[field] = "nul-\x00-" + kind
					case 4:
						value[field] = "quote-\"-" + kind
					}
					if variant >= 5 && variant < 10 {
						value["organization_id"] = "org-1"
						value["routing"] = map[string]string{"organization_id": "org-1"}
						if kind == "organization" {
							value[field] = "org-1"
						}
					}
					if variant >= 10 {
						delete(doc, "projection_kind")
					}
					if variant == 15 {
						value[field] = ownID
						if kind == "organization" {
							value[field] = "org-2"
						}
					}
					raw, err := json.Marshal(doc)
					if err != nil {
						t.Fatal(err)
					}
					if variant == 4 {
						raw = []byte(strings.ReplaceAll(string(raw), `"id"`, `"i\u0064"`))
					}
					if variant == 7 {
						raw = []byte(strings.ReplaceAll(string(raw), `"org-1"`, `"\u006frg-1"`))
					}
					if variant == 8 {
						raw = []byte(strings.ReplaceAll(string(raw), `"value":`, `"value":{"id":"ordinary"},"value":`))
					}
					if variant == 9 {
						raw = []byte(strings.ReplaceAll(string(raw), `"projection_kind":`, `"projection_kind":"work","projection_kind":`))
					}
					payload, body := []byte(`{"note":"ordinary"}`), []byte(`{"note":"ordinary"}`)
					physical, key, envelope, label, backing := kind, "team-2", "org-2", "TEAM_CREATED", eventID
					labels := events.ProjectionLifecycleEventTypes(kind)
					if len(labels) != 0 {
						label = labels[0]
					}
					if channel == "event" {
						payload = append(append([]byte(`{"projection":`), raw...), []byte(`,"admission":{"event_ref":"ordinary-event"}}`)...)
					} else {
						body = raw
						if variant >= 10 {
							backing = "missing-source"
						}
					}
					check(fmt.Sprintf("%s/%s/%d", channel, kind, variant), payload, body, physical, key, envelope, label, backing)
				}
			}
		}
		for _, sample := range []struct {
			name, payload, body, kind, key, envelope, label, backing string
			scope, identity                                          bool
		}{
			{"damaged-Organization-key-orphan", `{"note":"safe"}`, `{"projection_kind":"organization","record_id":"org-2","value":{"id":"org-2"}}`, "damaged-kind", "org-1", "org-2", "AUDIT_NOTE", "missing-source", true, false},
			{"backing-envelope-independent", `{"note":"safe"}`, `{"projection_kind":"team","record_id":"foreign-team","value":{"id":"foreign-team","organization_id":"org-2"}}`, "damaged-kind", "foreign-key", "org-1", "AUDIT_NOTE", eventID, true, false},
			{"event-physical-owner", `{"projection":{"record_id":"foreign","value":{"id":"selected-team","organization_id":"org-1"}}}`, `{"note":"safe"}`, "team", "team-2", "org-2", "AUDIT_NOTE", eventID, true, true},
			{"record-lifecycle-owner", `{"note":"safe"}`, `{"record_id":"foreign","value":{"id":"selected-team","organization_id":"org-1"}}`, "damaged-kind", "foreign-key", "org-2", "TEAM_CREATED", eventID, true, true},
			{"lease-displaced", `{"note":"safe"}`, `{"id":"selected-lease"}`, "capability_lease", "foreign-key", "org-2", "CAPABILITY_GRANTED", "missing-source", false, true},
			{"lease-escaped", `{"i\u0064":"selected-lease"}`, `{"note":"safe"}`, "capability_lease", "foreign-key", "org-2", "CAPABILITY_GRANTED", eventID, false, true},
			{"admission-container-duplicate", `{"admission":{"event_ref":"ordinary"},"admission":{"event_ref":"selected-event"}}`, `{"note":"safe"}`, "team", "team-2", "org-2", "AUDIT_NOTE", eventID, false, true},
			{"admission-escaped", `{"ad\u006dission":{"event_ref":"\u0073elected-event"}}`, `{"note":"safe"}`, "team", "team-2", "org-2", "AUDIT_NOTE", eventID, false, true},
			{"projection-container-duplicate", `{"projection":{"projection_kind":"work"},"projection":{"projection_kind":"mission","record_id":"foreign","value":{"id":"selected-mission"}}}`, `{"note":"safe"}`, "team", "team-2", "org-2", "AUDIT_NOTE", eventID, false, true},
			{"opaque-nested-selected", `{"note":{"projection":{"value":{"id":"selected-mission","organization_id":"org-1"}}}}`, `{"note":{"id":"selected-lease"}}`, "authorization_trace", "foreign-key", "org-2", "AUDIT_NOTE", "missing-source", false, false},
		} {
			check(sample.name, []byte(sample.payload), []byte(sample.body), sample.kind, sample.key, sample.envelope, sample.label, sample.backing, sample.scope, sample.identity)
		}
		// Container eligibility is independent of selected-identity bytes. These
		// ordinary sources contain those bytes and use the selected envelope, so
		// the former byte gates alone cannot exclude them. Projection, admission,
		// and each independent lease-owner channel have positive conflict seeds.
		containerExclusions := 0
		for _, sample := range []struct {
			name, payload, kind, envelope, label, backing      string
			scope, identity, scopeContainer, identityContainer bool
			selectedBytes                                      bool
		}{
			{"ordinary-selected-envelope-and-identities", `{"note":"org-1 selected-mission selected-event selected-lease"}`, "authorization_trace", "org-1", "AUDIT_NOTE", eventID, false, false, false, false, true},
			{"ordinary-nested-id-selected-envelope", `{"manifest":{"id":"selected-lease"},"note":"org-1"}`, "authorization_trace", "org-1", "AUDIT_NOTE", eventID, false, false, false, false, true},
			{"unowned-root-id", `{"id":"selected-lease","note":"org-1"}`, "authorization_trace", "org-1", "AUDIT_NOTE", eventID, false, false, false, false, true},
			{"projection-only", `{"projection":{"projection_kind":"mission","record_id":"foreign","value":{"id":"selected-mission","organization_id":"org-1"}}}`, "authorization_trace", "org-2", "AUDIT_NOTE", eventID, true, true, true, true, true},
			{"admission-only", `{"admission":{"event_ref":"selected-event"}}`, "authorization_trace", "org-1", "AUDIT_NOTE", eventID, false, true, false, true, true},
			{"lease-label-only", `{"id":"selected-lease"}`, "authorization_trace", "org-1", "CAPABILITY_GRANTED", eventID, false, true, false, true, true},
			{"lease-physical-only", `{"id":"selected-lease"}`, "capability_lease", "org-1", "AUDIT_NOTE", eventID, false, true, false, true, true},
			{"lease-label-missing-counterpart", `{"id":"selected-lease"}`, "authorization_trace", "org-1", "CAPABILITY_REVOKED", "missing-source", false, false, false, true, true},
			{"escaped-projection", `{"projec\u0074ion":{"projection_kind":"mission","record_id":"foreign","value":{"id":"selected-mission","organization_id":"org-1"}}}`, "authorization_trace", "org-2", "AUDIT_NOTE", eventID, true, true, true, true, true},
			{"escaped-admission", `{"ad\u006dission":{"event_ref":"selected-event"}}`, "authorization_trace", "org-1", "AUDIT_NOTE", eventID, false, true, true, true, true},
			{"escaped-lease-label", `{"i\u0064":"selected-lease"}`, "authorization_trace", "org-1", "CAPABILITY_GRANTED", eventID, false, true, true, true, true},
			{"unknown-noncanonical-containers", `{"Projection":{"value":{"id":"selected-mission","organization_id":"org-1"}},"Admission":{"event_ref":"selected-event"},"ID":"selected-lease"}`, "authorization_trace", "org-1", "AUDIT_NOTE", eventID, false, false, false, false, true},
			{"nested-projection-overselects", `{"note":{"projection":{"value":{"id":"selected-mission","organization_id":"org-1"}}}}`, "authorization_trace", "org-1", "AUDIT_NOTE", eventID, false, false, true, true, true},
			{"nested-id-physical-overselects", `{"note":{"id":"selected-lease"}}`, "capability_lease", "org-1", "AUDIT_NOTE", eventID, false, false, false, true, true},
			{"ordinary-escape-overselects", `{"note":"org-1 selected-mission \u0061"}`, "authorization_trace", "org-1", "AUDIT_NOTE", eventID, false, false, true, true, true},
		} {
			check("event/container/"+sample.name, []byte(sample.payload), []byte(`{"note":"safe"}`), sample.kind, "foreign-key", sample.envelope, sample.label, sample.backing, sample.scope, sample.identity)
			if !assigned() {
				continue
			}
			query := `WITH selected AS MATERIALIZED (SELECT json_extract(value,'$.id') AS identity FROM json_each(?1))
				SELECT ` + projectionEventContainerBytes("e.payload") + `,` + projectionIdentityEventContainerBytes("e.payload") + `,` + projectionSelectedSourceBytes("e.payload") + ` FROM events e WHERE e.event_id=?2`
			var scopeContainer, identityContainer, selectedBytes bool
			if err := store.db.QueryRowContext(t.Context(), query, selectedJSON, eventID).Scan(&scopeContainer, &identityContainer, &selectedBytes); err != nil {
				t.Fatalf("%s container decision: %v", sample.name, err)
			}
			if scopeContainer != sample.scopeContainer || identityContainer != sample.identityContainer || selectedBytes != sample.selectedBytes {
				t.Fatalf("%s container/identity byte decisions=%v/%v/%v want=%v/%v/%v", sample.name, scopeContainer, identityContainer, selectedBytes, sample.scopeContainer, sample.identityContainer, sample.selectedBytes)
			}
			if !scopeContainer && !identityContainer && selectedBytes {
				containerExclusions++
			}
		}
		random := rand.New(rand.NewSource(2174154025621))
		seeds := [][]byte{
			[]byte(`{"projection":{"projection_kind":"team","record_id":"foreign","value":{"id":"selected-team","organization_id":"org-1"}}}`),
			[]byte(`{"projection":{"projection_kind":"knowledge","record_id":"foreign","value":{"knowledge_id":"selected-knowledge","organization_id":"\u006frg-1"}}}`),
			[]byte(`{"admission":{"event_ref":"selected-event"}}`), []byte(`{"id":"selected-lease"}`),
			[]byte(`{"text":"ordinary foreign café"}`), []byte(`{"text":"\ud800"}`), []byte(`{} {}`),
			[]byte(`{"projection":{"projection_kind":"team","record_id":"foreign","value":{"id":"snowman-\u2603-team"}}}`),
			append(append([]byte(`{"projection":{"projection_kind":"team","record_id":"foreign","value":{"id":"selected-team","organization_id":"org-1"}},"note":"`), 0xff, 0xc0, 0x80), []byte(`"}`)...),
		}
		for _, raw := range [][]byte{append([]byte{0}, seeds[0]...), append(append([]byte(nil), seeds[0]...), 0), append(append(append([]byte(nil), seeds[0]...), 0), []byte(`trailing`)...)} {
			check("explicit-raw-NUL", raw, raw, "team", "team-2", "org-2", "TEAM_CREATED", eventID)
		}
		for sample := 0; sample < 200; sample++ {
			raw := append([]byte(nil), seeds[sample%len(seeds)]...)
			if sample < 128 {
				raw = append(raw, byte(sample))
			} else {
				for range 1 + random.Intn(3) {
					raw[random.Intn(len(raw))] = byte(random.Intn(256))
				}
			}
			check(fmt.Sprintf("raw-mutation-%d", sample), raw, raw, "team", "team-2", "org-2", "TEAM_CREATED", eventID)
		}
		for sample := 0; sample < 100; sample++ {
			raw, err := json.Marshal(map[string]any{"text": fmt.Sprintf("ordinary foreign source %d", sample), "value": negativeASCIIObject(random, 0)})
			if err != nil {
				t.Fatal(err)
			}
			// Marshal may emit escapes in generated text; these are conservatively
			// admitted. Plain generated foreign strings must be excluded.
			plain, _ := json.Marshal(map[string]string{"text": fmt.Sprintf("ordinary foreign source %d", sample)})
			if caseIndex >= first && caseIndex < last {
				var scope, identity bool
				query := `WITH selected AS MATERIALIZED (SELECT json_extract(value,'$.id') AS identity FROM json_each(?1)) SELECT ` + projectionSourceIdentityBytes("?2", "'org-1'") + `,` + projectionSelectedSourceBytes("?2")
				if err := store.db.QueryRowContext(t.Context(), query, selectedJSON, plain).Scan(&scope, &identity); err != nil {
					t.Fatal(err)
				}
				if scope || identity {
					t.Fatal("plain unrelated foreign source was not excluded")
				}
			}
			check(fmt.Sprintf("ordinary-%d", sample), raw, raw, "authorization_trace", "foreign-key", "org-2", "AUDIT_NOTE", "missing-source")
		}

		if caseIndex != 744 || checked != last-first {
			t.Fatalf("differential corpus coverage: generated=%d checked=%d want=744/%d", caseIndex, checked, last-first)
		}
		return decisions{scopeConflicts, identityConflicts, containerExclusions, channelDecisions}
	}
	// The root holds no permit while these children acquire their permits.
	// Each worker replays every original three-statement mutation in order;
	// only its contiguous half performs the oracle and validator assertions.
	// Independent identical RNG streams preserve the original generated bytes.
	t.Run("workers", func(t *testing.T) {
		for worker := range paths {
			t.Run(fmt.Sprint(worker), func(t *testing.T) {
				t.Parallel()
				incidentTestSlots <- struct{}{}
				t.Cleanup(func() { <-incidentTestSlots })
				copy, err := Open(paths[worker])
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = copy.Close() })
				results[worker] = run(t, copy, worker*372, (worker+1)*372)
			})
		}
	})
	scopeConflicts, identityConflicts, containerExclusions := 0, 0, 0
	var channelDecisions [2][2][2]int
	for _, result := range results {
		scopeConflicts += result.scope
		identityConflicts += result.identity
		containerExclusions += result.containers
		for channel := range channelDecisions {
			for guard := range channelDecisions[channel] {
				for decision := range channelDecisions[channel][guard] {
					channelDecisions[channel][guard][decision] += result.channels[channel][guard][decision]
				}
			}
		}
	}
	if containerExclusions < 4 {
		t.Fatalf("container-specific exclusion corpus is vacuous: %d", containerExclusions)
	}
	if scopeConflicts == 0 || identityConflicts == 0 {
		t.Fatalf("differential corpus has no true conflicts: scope=%d identity=%d", scopeConflicts, identityConflicts)
	}
	for channel, guards := range channelDecisions {
		for guard, decisions := range guards {
			if decisions[0] == 0 || decisions[1] == 0 {
				t.Fatalf("source-specific corpus is vacuous: channel=%d guard=%d accepts/conflicts=%v", channel, guard, decisions)
			}
		}
	}
}

func TestIncidentProjectionScopeDamagedOrganizationPhysicalKey(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprint(missing), func(t *testing.T) {
			store := projectionScopeFixture(t)
			backing := "admission_event_id"
			if missing {
				backing = "'missing-source'"
			}
			if _, err := store.db.ExecContext(t.Context(), `UPDATE records SET kind='damaged-kind',record_id='org-1',admission_event_id=`+backing+` WHERE kind='organization' AND record_id='org-2'`); err != nil {
				t.Fatal(err)
			}
			tx, err := store.db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := admittedProjectionRecordsBounded(t.Context(), tx, 2<<20, `WHERE r.kind='damaged-kind' AND r.record_id='org-1'`); err == nil {
				t.Fatal("exact record owner accepted damaged Organization physical-key claim")
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "incident-org-1", 256)
			if err == nil {
				t.Fatal("public scope guard omitted physical Organization key after kind corruption")
			}
			if !strings.Contains(err.Error(), "incident projection organization claim conflicts with its retained source") {
				t.Fatalf("public rejected a different boundary from the retained organization claim: %v", err)
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("scope conflict returned partial evidence")
			}
		})
	}
}

// Match TestIncidentPublicEvidenceLimit's successful bound workload exactly:
// 4,000 source notes, 16 admitted Knowledge nodes, the Organization, and 255
// additional public notes. Setup/full-owner checks are outside measured polls.
func BenchmarkIncidentPublicEvidenceBoundSourceGates(b *testing.B) {
	store, err := Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })
	if _, err := store.AppendProjection(b.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "organization", RecordID: "org-1", Version: 1, Value: core.Organization{ID: "org-1", Name: "Evidence", PolicyVersion: "v1", CreatedAt: time.Now().UTC()}}); err != nil {
		b.Fatal(err)
	}
	var previous, correlation string
	for node := range 16 {
		refs := make([]string, 0, 251)
		if previous != "" {
			refs = append(refs, previous)
		}
		for source := range 250 {
			evidence, err := store.Append(b.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", TaskID: fmt.Sprintf("observation-task-%d", node*250+source), SourceExecutionID: fmt.Sprintf("observation-execution-%d", node*250+source), CorrelationID: "observations", Payload: map[string]int{"observation": node*250 + source}})
			if err != nil {
				b.Fatal(err)
			}
			refs = append(refs, evidence.EventID)
		}
		id := fmt.Sprintf("bounded-%d", node)
		correlation = "knowledge-" + id
		value := core.KnowledgeRecord{KnowledgeID: core.ID(id), OrganizationID: "org-1", Version: 1, Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: "org-1", Status: core.KnowledgeCandidate, Title: "Recorded observations", Content: "Retain the source observations", Basis: core.KnowledgeBasisExternalEvidence, ProvenanceEventRefs: refs, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated}
		event, err := store.AppendProjection(b.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: correlation}, ProjectionKind: "knowledge", RecordID: id, Version: 1, Value: value})
		if err != nil {
			b.Fatal(err)
		}
		previous = event.EventID
	}
	for range 255 {
		if _, err := store.Append(b.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: correlation, Payload: map[string]string{"summary": "Public observation"}}); err != nil {
			b.Fatal(err)
		}
	}
	stream, err := store.Events(b.Context(), "")
	if err != nil {
		b.Fatal(err)
	}
	if len(stream) != 4272 {
		b.Fatalf("bound workload source count=%d", len(stream))
	}
	if _, err := events.ValidateProjectionHistory(stream, nil, nil, nil); err != nil {
		b.Fatalf("actual writer full-owner baseline: %v", err)
	}
	read := func() {
		snapshot, err := store.VerifiedIncidentEvents(b.Context(), "org-1", correlation, 256)
		if err != nil {
			b.Fatal(err)
		}
		if len(snapshot.Work.Events) != 256 || len(snapshot.DependencyEvents) != 4016 {
			b.Fatalf("bound workload public/private=%d/%d", len(snapshot.Work.Events), len(snapshot.DependencyEvents))
		}
		if err := events.ValidateIncidentBounds(snapshot); err != nil {
			b.Fatal(err)
		}
	}
	read()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		read()
	}
}

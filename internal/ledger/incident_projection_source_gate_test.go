package ledger

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

// The oracle deliberately has no byte gate: retained scalar ownership is the
// contract, and candidate rejection must preserve its result for the same raw
// source, metadata and selected keys. Valid ordinary foreign sources must also
// be excluded, so an always-true candidate cannot satisfy the property.
func TestProjectionSourceGateDifferential(t *testing.T) {
	store := projectionScopeFixture(t) // Actual writer/full-owner/public baseline.
	var eventID string
	if err := store.db.QueryRowContext(t.Context(), `SELECT admission_event_id FROM records WHERE kind='team'`).Scan(&eventID); err != nil {
		t.Fatal(err)
	}
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
	scopeConflicts, identityConflicts := 0, 0
	var channelDecisions [2][2][2]int // channel, guard, accept/conflict
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
		tx, err := store.db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
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
				physical, key, envelope, label, backing := "team", "team-2", "org-2", "TEAM_CREATED", eventID
				labels := events.ProjectionLifecycleEventTypes(kind)
				if len(labels) != 0 {
					label = labels[0]
				}
				if channel == "event" {
					payload = append(append([]byte(`{"projection":`), raw...), []byte(`,"admission":{"event_ref":"ordinary-event"}}`)...)
					physical = kind
				} else {
					body = raw
					physical = kind
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
		var scope, identity bool
		query := `WITH selected AS MATERIALIZED (SELECT json_extract(value,'$.id') AS identity FROM json_each(?1)) SELECT ` + projectionSourceIdentityBytes("?2", "'org-1'") + `,` + projectionSelectedSourceBytes("?2")
		if err := store.db.QueryRowContext(t.Context(), query, selectedJSON, plain).Scan(&scope, &identity); err != nil {
			t.Fatal(err)
		}
		if scope || identity {
			t.Fatal("plain unrelated foreign source was not excluded")
		}
		check(fmt.Sprintf("ordinary-%d", sample), raw, raw, "authorization_trace", "foreign-key", "org-2", "AUDIT_NOTE", "missing-source")
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

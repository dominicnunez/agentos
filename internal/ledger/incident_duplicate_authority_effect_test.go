package ledger

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentDuplicateCapabilityClaim(t *testing.T) {
	s, e := Open(":memory:")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = s.Close() })
	appendTaskProjectionParents(t, t.Context(), s, "org-1", "setup", "work-1")
	appendFactualInferenceKnowledge(t, s, "fact", "Verified fact", "A bounded observation.")
	var b []byte
	if err := s.db.QueryRowContext(t.Context(), `SELECT body FROM records WHERE kind='capability_lease' AND record_id='lease-fact'`).Scan(&b); err != nil {
		t.Fatal(err)
	}
	var l core.CapabilityLease
	if err := json.Unmarshal(b, &l); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	l.RevokedAt = &now
	if e := s.AppendRecord(t.Context(), "org-1", "CAPABILITY_REVOKED", "runtime", string(l.OriginTaskID), nil, nil, "capability_lease", string(l.ID), 2, l); e != nil {
		t.Fatal(e)
	}
	baseline, e := s.VerifiedIncidentEvents(t.Context(), "org-1", "knowledge-fact", 256)
	if e != nil {
		t.Fatal(e)
	}
	var id string
	if err := s.db.QueryRowContext(t.Context(), `SELECT event_id FROM events WHERE event_type='CAPABILITY_REVOKED'`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	execDuplicateClaim(t, s, `UPDATE events SET organization_id='other',payload=CAST(json_set(payload,'$.id','decoy') AS BLOB) WHERE event_id=?`, id)
	target, _ := json.Marshal(string(l.ID))
	duplicateKnowledgeDocument(t, s, id, nil, "id", target, false)
	execDuplicateClaim(t, s, `DELETE FROM records WHERE kind='capability_lease' AND version=2`)
	if e := s.withTx(t.Context(), func(tx *sql.Tx) error {
		if _, e := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); e != nil {
			return e
		}
		return rebuildEventIntegrity(t.Context(), tx)
	}); e != nil {
		t.Fatal(e)
	}
	stream, e := s.Events(t.Context(), "")
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e := events.ResolveAuthorityAdmissions(stream, baseline.AuthorityRecords); e == nil {
		t.Fatal("full authority accepted malformed duplicate revocation")
	}
	if snapshot, e := s.VerifiedIncidentEvents(t.Context(), "org-1", "knowledge-fact", 256); e == nil {
		t.Fatal("incident omitted duplicate foreign revocation claim")
	} else if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
		t.Fatal("invalid authority returned partial snapshot")
	}
}
func TestIncidentDuplicateEffectTaskClaim(t *testing.T) {
	s, e := Open(":memory:")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = s.Close() })
	seedIncidentEffects(t, s, 2)
	baseline, e := s.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256)
	if e != nil || len(baseline.RelatedEvents) != 4 {
		t.Fatal(e)
	}
	task := baseline.RelatedEvents[0].TaskID
	rows, e := s.db.QueryContext(t.Context(), `SELECT event_id FROM events WHERE event_type='EFFECT_OBLIGATION_TRANSITIONED' AND json_extract(payload,'$.effect_obligation_id')='effect-001'`)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected two effect revisions, got %d", len(ids))
	}
	target, _ := json.Marshal(task)
	for _, id := range ids {
		execDuplicateClaim(t, s, `UPDATE events SET organization_id='other',correlation_id='other',task_id='decoy',payload=CAST(json_set(payload,'$.organization_id','other','$.task_id','decoy') AS BLOB) WHERE event_id=?`, id)
		duplicateKnowledgeDocument(t, s, id, nil, "task_id", target, false)
	}
	execDuplicateClaim(t, s, `UPDATE records SET body=CAST(json_set(body,'$.organization_id','other','$.task_id','decoy') AS BLOB) WHERE kind='effect' AND record_id='effect-001'`)
	// Matching raw record contains the same hidden Task claim.
	type rec struct {
		v int
		b []byte
	}
	var rs []rec
	r, e := s.db.QueryContext(t.Context(), `SELECT version,body FROM records WHERE kind='effect' AND record_id='effect-001'`)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { _ = r.Close() }()
	for r.Next() {
		var x rec
		if err := r.Scan(&x.v, &x.b); err != nil {
			t.Fatal(err)
		}
		rs = append(rs, x)
	}
	if err := r.Err(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if len(rs) != 2 {
		t.Fatalf("expected two record revisions, got %d", len(rs))
	}
	for _, x := range rs {
		changed := append(append(append([]byte(nil), x.b[:len(x.b)-1]...), []byte(`,"task_id":`)...), append(target, '}')...)
		if _, e := core.DecodeEffectObligation(changed); e == nil {
			t.Fatal("effect owner accepted duplicate Task")
		}
		execDuplicateClaim(t, s, `UPDATE records SET body=? WHERE kind='effect' AND record_id='effect-001' AND version=?`, changed, x.v)
	}
	if e := s.withTx(t.Context(), func(tx *sql.Tx) error {
		if _, e := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); e != nil {
			return e
		}
		return rebuildEventIntegrity(t.Context(), tx)
	}); e != nil {
		t.Fatal(e)
	}
	if snapshot, e := s.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256); e == nil {
		t.Fatal("incident omitted duplicate foreign effect Task claim")
	} else if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
		t.Fatal("invalid effect returned partial snapshot")
	}
}

func execDuplicateClaim(t *testing.T, store *SQLite, query string, args ...any) {
	t.Helper()
	if _, err := store.db.ExecContext(t.Context(), query, args...); err != nil {
		t.Fatal(err)
	}
}

func TestIncidentDuplicateEffectIdentity(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	seedIncidentEffects(t, store, 2)
	// Move one complete history outside the selected Task. The second payload
	// identity below then makes it claim the selected globally keyed effect.
	execDuplicateClaim(t, store, `UPDATE events SET organization_id='other',correlation_id='other',task_id='other-task',payload=CAST(json_set(payload,'$.organization_id','other','$.task_id','other-task') AS BLOB) WHERE event_type='EFFECT_OBLIGATION_TRANSITIONED' AND json_extract(payload,'$.effect_obligation_id')='effect-001'`)
	execDuplicateClaim(t, store, `UPDATE records SET body=CAST(json_set(body,'$.organization_id','other','$.task_id','other-task') AS BLOB) WHERE kind='effect' AND record_id='effect-001'`)
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	}); err != nil {
		t.Fatal(err)
	}
	baseline, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256)
	if err != nil || len(baseline.RelatedEvents) != 2 {
		t.Fatalf("independent effect baseline: events=%d err=%v", len(baseline.RelatedEvents), err)
	}
	var incoming string
	if err := store.db.QueryRowContext(t.Context(), `SELECT event_id FROM events WHERE event_type='EFFECT_OBLIGATION_TRANSITIONED' AND json_extract(payload,'$.effect_obligation_id')='effect-001' ORDER BY sequence DESC LIMIT 1`).Scan(&incoming); err != nil {
		t.Fatal(err)
	}
	var selected struct {
		ID string `json:"effect_obligation_id"`
	}
	if err := json.Unmarshal(baseline.RelatedEvents[0].Payload, &selected); err != nil || selected.ID == "" {
		t.Fatalf("selected effect identity: %v", err)
	}
	target, err := json.Marshal(selected.ID)
	if err != nil {
		t.Fatal(err)
	}
	duplicateKnowledgeDocument(t, store, incoming, nil, "effect_obligation_id", target, false)
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256)
	if err == nil {
		t.Fatal("incident omitted later duplicate selected effect identity")
	}
	if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
		t.Fatal("duplicate effect identity returned partial evidence")
	}
}

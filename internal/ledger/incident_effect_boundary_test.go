package ledger

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentCountsStoredBytes(t *testing.T) {
	for _, value := range []string{strings.Repeat("界", 750000), "actor\x00" + strings.Repeat("x", 2<<20)} {
		store, err := Open(":memory:")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", CorrelationID: "byte-bound", SourceActorID: value, EventType: "AUDIT_NOTE", Payload: map[string]bool{"test": true}}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "byte-bound", 256); err == nil {
			t.Fatal("incident counted characters or stopped at NUL instead of stored bytes")
		}
	}
}

func TestIncidentEffectEnvelope(t *testing.T) {
	for _, mutation := range []string{"none", "authorization_refs", "artifact_refs", "source_actor_id", "correlation_id", "empty-receipt", "duplicate-receipt"} {
		t.Run(mutation, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			attempt := incidentEffectFixture(t, store)
			confirmed := attempt
			confirmed.Status = core.EffectConfirmed
			confirmed.ConfirmationEvidenceRefs = []string{"receipt"}
			confirmed.ReconciliationEvidenceRefs = []string{"receipt", "destination-check"}
			now := time.Now().UTC()
			confirmed.ReconciledAt = &now
			refs := []string{"receipt", "destination-check"}
			if mutation == "empty-receipt" || mutation == "duplicate-receipt" {
				confirmed.ReconciliationEvidenceRefs, confirmed.ReconciledAt = nil, nil
				refs = []string{""}
				if mutation == "duplicate-receipt" {
					refs = []string{"receipt", "receipt"}
				}
				confirmed.ConfirmationEvidenceRefs = refs
			}
			if err := store.AppendRecord(t.Context(), "org-1", "EFFECT_OBLIGATION_TRANSITIONED", "", string(attempt.TaskID), attempt.AuthorizationRefs, refs, "effect", string(attempt.ID), 3, confirmed); err != nil {
				t.Fatal(err)
			}
			if mutation != "none" && mutation != "empty-receipt" && mutation != "duplicate-receipt" {
				value := "wrong"
				if mutation == "authorization_refs" || mutation == "artifact_refs" {
					value = `["wrong"]`
				}
				if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
					if _, err := tx.ExecContext(t.Context(), `UPDATE events SET `+mutation+`=? WHERE event_type='EFFECT_OBLIGATION_TRANSITIONED'`, value); err != nil {
						return err
					}
					if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
						return err
					}
					return rebuildEventIntegrity(t.Context(), tx)
				}); err != nil {
					t.Fatal(err)
				}
			}
			_, err = store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256)
			if mutation == "none" && err != nil {
				t.Fatal(err)
			}
			if mutation != "none" && err == nil {
				t.Fatal("accepted effect metadata inconsistent with its record")
			}
		})
	}
}

func TestIncidentRejectsPrematureEffectEvidence(t *testing.T) {
	for _, field := range []string{"confirmation", "reconciliation"} {
		t.Run(field, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			attempt := incidentEffectFixture(t, store)
			if field == "confirmation" {
				attempt.ConfirmationEvidenceRefs = []string{"premature-receipt"}
			} else {
				attempt.ReconciliationEvidenceRefs = []string{"premature-receipt"}
				now := time.Now().UTC()
				attempt.ReconciledAt = &now
			}
			body, err := json.Marshal(attempt)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=? WHERE kind='effect'`, body); err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=?,artifact_refs='["premature-receipt"]' WHERE event_type='EFFECT_OBLIGATION_TRANSITIONED'`, body); err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
					return err
				}
				return rebuildEventIntegrity(t.Context(), tx)
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256); err == nil {
				t.Fatal("unfinished effect accepted terminal evidence")
			}
		})
	}
}

func TestIncidentIgnoresOtherTenantJSON(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	incidentEffectFixture(t, store)
	before, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256)
	if err != nil {
		t.Fatal(err)
	}
	other, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "other-org", EventType: "AUDIT_NOTE", Payload: map[string]string{"other": "private"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET event_type='EFFECT_OBLIGATION_TRANSITIONED',payload=CAST('{' AS BLOB) WHERE event_id=?`, other.EventID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	}); err != nil {
		t.Fatal(err)
	}
	after, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before.RelatedEvents, after.RelatedEvents) || !reflect.DeepEqual(before.Admissions, after.Admissions) {
		t.Fatal("other tenant's malformed payload changed selected evidence")
	}
}

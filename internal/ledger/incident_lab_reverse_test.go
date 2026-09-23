package ledger

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentOwnedExecutionContracts(t *testing.T) {
	for _, token := range strings.Split(incidentExecutionTypes, ",") {
		kind := strings.Trim(strings.TrimSpace(token), "'")
		if !incidentOwnedContract(kind) {
			t.Errorf("execution contract omitted: %s", kind)
		}
	}
	for _, kind := range []string{"", "EXECUTION", "AUDIT_NOTE"} {
		if incidentOwnedContract(kind) {
			t.Errorf("unowned contract accepted: %q", kind)
		}
	}
}

func TestIncidentRejectsMovedExperiment(t *testing.T) {
	for _, missing := range []string{"", "event", "record"} {
		t.Run("missing-"+missing, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "lab.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			appendTaskProjectionParents(t, t.Context(), store, "org-1", "incident", "work-1")
			experiment := core.Experiment{ID: "experiment-1", OrganizationID: "org-1", WorkID: "work-1", Objective: "test task", SandboxRef: "sandbox", CapabilityProfileRef: "profile", Budget: core.ExperimentBudget{MaxExecutions: 1, MaxUsageUnits: 1, MaxWallTimeSeconds: 1, AllowedInferencePools: []string{"test"}}, Status: core.ExperimentRunning, TrustLabel: core.ExperimentTrustUnverified, StartedAt: time.Now().UTC()}
			if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "LAB_EXPERIMENT_STARTED", SourceActorID: "runtime", CorrelationID: "incident"}, ProjectionKind: "lab_experiment", RecordID: string(experiment.ID), Version: 1, Value: experiment}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "incident", 256); err != nil {
				t.Fatalf("valid baseline: %v", err)
			}
			MoveIncidentProjectionForTest(t, store, "lab_experiment", missing)
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "incident", 256); err == nil {
				t.Fatal("accepted experiment moved away from its Work")
			}
		})
	}
}

// MoveIncidentProjectionForTest preserves seals and integrity while changing a retained binding.
func MoveIncidentProjectionForTest(t *testing.T, store *SQLite, kind, missing string) {
	t.Helper()
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		var body []byte
		var id string
		if err := tx.QueryRowContext(t.Context(), `SELECT body,admission_event_id FROM records WHERE kind=?`, kind).Scan(&body, &id); err != nil {
			return err
		}
		var record events.ProjectionRecord
		if err := json.Unmarshal(body, &record); err != nil {
			return err
		}
		event, _, err := eventByID(t.Context(), tx, id)
		if err != nil {
			return err
		}
		record.CorrelationID = "moved"
		event.CorrelationID = "moved"
		sealed, err := events.SealProjectionEvent(event, record, nil)
		if err != nil {
			return err
		}
		payload, err := json.Marshal(sealed)
		if err != nil {
			return err
		}
		body, err = json.Marshal(record)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(t.Context(), `UPDATE events SET correlation_id=?,payload=? WHERE event_id=?`, event.CorrelationID, payload, id); err != nil {
			return err
		}
		if _, err = tx.ExecContext(t.Context(), `UPDATE records SET body=?,admission_fingerprint=? WHERE admission_event_id=?`, body, sealed.Admission.Fingerprint, id); err != nil {
			return err
		}
		if missing == "event" {
			if _, err = tx.ExecContext(t.Context(), `DELETE FROM events WHERE event_id=?`, id); err != nil {
				return err
			}
		} else if missing == "record" {
			if _, err = tx.ExecContext(t.Context(), `DELETE FROM records WHERE admission_event_id=?`, id); err != nil {
				return err
			}
		}
		if _, err = tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	}); err != nil {
		t.Fatal(err)
	}

}

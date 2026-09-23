package lab_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/lab"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/replay"
)

func TestIncidentLabReproduction(t *testing.T) {
	for _, missing := range []string{"", "lab_experiment", "work"} {
		t.Run("missing-"+missing, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "lab.db")
			store, err := ledger.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			gateway := events.NewGateway(store)
			runtime := app.New(gateway)
			experiment, err := runtime.SubmitExperiment(t.Context(), app.Submit{RequestID: "experiment", OrganizationID: "org-1", Statement: "echo candidate", Kind: core.ExecutionDeterministic}, experimentSpec())
			if err != nil {
				t.Fatal(err)
			}
			reproduction, err := runtime.Submit(t.Context(), app.Submit{RequestID: "reproduction", OrganizationID: "org-1", Statement: "echo independent", Kind: core.ExecutionDeterministic})
			if err != nil {
				t.Fatal(err)
			}
			_, err = lab.New(gateway).Nominate(t.Context(), lab.Nomination{OrganizationID: "org-1", ExperimentID: experiment.Experiment.ID, TargetKind: core.PromotionTargetKnowledge, TargetRef: "candidate-1", Summary: "independently reproduced", ReproductionEvidenceRefs: []string{eventOfType(t, reproduction.Events, "WORK_COMPLETED").EventID}})
			if err != nil {
				t.Fatal(err)
			}
			if err = store.Close(); err != nil {
				t.Fatal(err)
			}
			if missing != "" {
				db, err := sql.Open("sqlite", path)
				if err != nil {
					t.Fatal(err)
				}
				id := string(experiment.Experiment.ID)
				if missing == "work" {
					id = string(reproduction.Work.ID)
				}
				_, err = db.ExecContext(t.Context(), `DELETE FROM records WHERE kind=? AND record_id=?`, missing, id)
				_ = db.Close()
				if err != nil {
					t.Fatal(err)
				}
			}
			store, err = ledger.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", experiment.Events[0].CorrelationID, 256)
			if missing != "" {
				if err == nil {
					t.Fatal("incident accepted missing Lab dependency backing")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, event := range snapshot.DependencyEvents {
				if event.EventID == eventOfType(t, reproduction.Events, "WORK_COMPLETED").EventID {
					found = true
				}
			}
			if !found {
				t.Fatal("independent reproduction absent from private evidence")
			}
			if _, err := replay.ProjectIncident(snapshot, experiment.Events[0].CorrelationID); err != nil {
				t.Fatal(err)
			}
		})
	}
}

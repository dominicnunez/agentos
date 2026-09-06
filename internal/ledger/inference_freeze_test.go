package ledger

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/authority"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/inference"
)

func appendInferenceFreeze(t *testing.T, store *SQLite, organization string, version int, frozen bool) {
	t.Helper()
	state := authority.FreezeState{OrganizationID: core.ID(organization), Frozen: frozen, Reason: "security hold", UpdatedAt: store.nowUTC()}
	if err := store.AppendRecord(t.Context(), organization, "FREEZE_SET", "user-1", "task-1", nil, nil, "organization_freeze", organization, version, state); err != nil {
		t.Fatal(err)
	}
}

func TestInferenceRejectsFrozenOrganization(t *testing.T) {
	now := time.Date(2026, 9, 6, 3, 0, 0, 0, time.UTC)
	for _, purpose := range []inference.Purpose{inference.PurposeIntentNormalization, inference.PurposePlanning, inference.PurposeTaskExecution} {
		t.Run(string(purpose), func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			store.now = func() time.Time { return now }
			if err := store.ActivateInferencePolicy(t.Context(), testInferencePolicy(now)); err != nil {
				t.Fatal(err)
			}
			appendInferenceFreeze(t, store, "organization-1", 1, true)
			request := testInferenceRequest("after-freeze")
			request.Scope.Purpose = purpose
			request.Scope.IntentID = "intent-1"
			if _, err := store.ReserveInference(t.Context(), request); err == nil || !strings.Contains(err.Error(), "frozen") {
				t.Fatalf("frozen inference admission: %v", err)
			}
			stream, err := store.Events(t.Context(), "work-1")
			if err != nil || len(stream) != 0 {
				t.Fatalf("denial wrote events: %v", err)
			}
			store.now = func() time.Time { return now.Add(time.Minute) }
			appendInferenceFreeze(t, store, "organization-1", 2, false)
			if _, err := store.ReserveInference(t.Context(), request); err != nil {
				t.Fatalf("release did not permit first admission: %v", err)
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestInferenceFreezeIsTenantScopedAndPreservesReconciliation(t *testing.T) {
	now := time.Date(2026, 9, 6, 3, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.now = func() time.Time { return now }
	if err := store.ActivateInferencePolicy(t.Context(), testInferencePolicy(now)); err != nil {
		t.Fatal(err)
	}
	appendInferenceFreeze(t, store, "other-organization", 1, true)
	reservation, err := store.ReserveInference(t.Context(), testInferenceRequest("before-freeze"))
	if err != nil {
		t.Fatalf("other tenant blocked admission: %v", err)
	}
	appendInferenceFreeze(t, store, "organization-1", 1, true)
	usage := testInferenceUsage()
	if _, err := store.ReconcileInference(t.Context(), reservation, &usage, inference.ReconciliationCompleted); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReserveInference(t.Context(), testInferenceRequest("after-restart")); err == nil || !strings.Contains(err.Error(), "frozen") {
		t.Fatalf("restart lost freeze: %v", err)
	}
}

func TestInferenceRecoveryRejectsReservationInsideFreeze(t *testing.T) {
	now := time.Date(2026, 9, 6, 3, 0, 0, 0, time.UTC)
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.now = func() time.Time { return now }
	if err := store.ActivateInferencePolicy(t.Context(), testInferencePolicy(now)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReserveInference(t.Context(), testInferenceRequest("before-freeze")); err != nil {
		t.Fatal(err)
	}
	appendInferenceFreeze(t, store, "organization-1", 1, true)
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Simulate a historical admission bypass by placing the genuine freeze
	// contract before the genuine reservation. No wall-clock inference is used.
	if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET sequence=sequence+100 WHERE event_type='INFERENCE_RESERVED'`); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err == nil || !strings.Contains(err.Error(), "frozen") {
		t.Fatalf("invalid freeze ordering passed recovery: %v", err)
	}
}

func TestInferenceRejectsMalformedFreezeAuthority(t *testing.T) {
	now := time.Date(2026, 9, 6, 3, 0, 0, 0, time.UTC)
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.now = func() time.Time { return now }
	if err := store.ActivateInferencePolicy(t.Context(), testInferencePolicy(now)); err != nil {
		t.Fatal(err)
	}
	appendInferenceFreeze(t, store, "organization-1", 1, false)
	if _, err := store.db.ExecContext(t.Context(), `UPDATE records SET body='{}' WHERE kind='organization_freeze'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReserveInference(t.Context(), testInferenceRequest("malformed")); err == nil {
		t.Fatal("malformed authority admitted inference")
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err == nil {
		t.Fatal("malformed authority passed recovery")
	}
}

func TestInferenceFreezeAndReservationShareTransactionOrder(t *testing.T) {
	for range 8 {
		now := time.Date(2026, 9, 6, 3, 0, 0, 0, time.UTC)
		store, err := Open(":memory:")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		store.now = func() time.Time { return now }
		if err := store.ActivateInferencePolicy(t.Context(), testInferencePolicy(now)); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		frozen := make(chan error, 1)
		reserved := make(chan error, 1)
		go func() {
			<-start
			state := authority.FreezeState{OrganizationID: "organization-1", Frozen: true, UpdatedAt: now}
			frozen <- store.AppendRecord(t.Context(), "organization-1", "FREEZE_SET", "user-1", "task-1", nil, nil, "organization_freeze", "organization-1", 1, state)
		}()
		go func() {
			<-start
			_, err := store.ReserveInference(t.Context(), testInferenceRequest("concurrent"))
			reserved <- err
		}()
		close(start)
		if err := <-frozen; err != nil {
			t.Fatal(err)
		}
		if err := <-reserved; err != nil && !strings.Contains(err.Error(), "frozen") {
			t.Fatal(err)
		}
		// Either reservation commits first or freeze prevents it. The historical
		// validator rejects the forbidden outcome in either goroutine ordering.
		if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
			t.Fatal(err)
		}
		if _, err := store.ReserveInference(t.Context(), testInferenceRequest("later")); err == nil || !strings.Contains(err.Error(), "frozen") {
			t.Fatalf("post-race admission: %v", err)
		}
	}
}

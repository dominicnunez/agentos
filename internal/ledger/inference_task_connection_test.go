package ledger

import "testing"

func TestTaskInferenceConnectionMustMatchManifest(t *testing.T) {
	store, request := setupAdmittedTaskInference(t)
	tx, err := store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := validateInferenceKnowledge(t.Context(), tx, request); err != nil {
		t.Fatalf("original request rejected: %v", err)
	}
	request.ConnectionID = "other-account"
	if err := validateInferenceKnowledge(t.Context(), tx, request); err == nil {
		t.Fatal("same-model account substituted for manifested connection")
	}
}

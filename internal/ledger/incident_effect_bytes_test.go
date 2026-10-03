package ledger

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestIncidentEffectRecordBytes(t *testing.T) {
	for _, mutation := range []string{"none", "trailing-space", "escaped-key"} {
		t.Run(mutation, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "effects.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			attempt := incidentEffectFixture(t, store)
			var original []byte
			if err := store.db.QueryRowContext(t.Context(), `SELECT body FROM records WHERE kind='effect' AND record_id=? AND version=1`, attempt.ID).Scan(&original); err != nil {
				t.Fatal(err)
			}
			changed := append([]byte(nil), original...)
			switch mutation {
			case "trailing-space":
				changed = append(changed, ' ')
			case "escaped-key":
				changed = bytes.Replace(changed, []byte(`"effect_obligation_id"`), []byte(`"\u0065ffect_obligation_id"`), 1)
			}
			if mutation != "none" && bytes.Equal(changed, original) {
				t.Fatal("record mutation had no effect")
			}
			if _, err := store.db.ExecContext(t.Context(), `UPDATE records SET body=? WHERE kind='effect' AND record_id=? AND version=1`, changed, attempt.ID); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			_, err = store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256)
			if mutation == "none" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "effect record differs from its ordered event") {
				t.Fatalf("altered effect record bytes: %v", err)
			}
		})
	}
}

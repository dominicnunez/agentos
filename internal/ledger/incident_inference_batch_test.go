package ledger

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/inference"
)

func TestIncidentDistinctInferenceGrowth(t *testing.T) {
	for _, count := range []int{1, 40} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "inference.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			now := time.Now().UTC()
			store.now = func() time.Time { return now }
			for i := range count {
				policy := testInferencePolicy(now)
				policy.AuthorizedBy = fmt.Sprintf("owner-%03d", i)
				policy.AuthorizedAt = now.Add(-time.Hour + time.Duration(i)*time.Second)
				policy.MaxTokensPerWindow = 100000
				policy.Pricing.MaxCostNanoUSDPerWindow = 100000000
				if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
					t.Fatal(err)
				}
				reservation, err := store.ReserveInference(t.Context(), testInferenceRequest(fmt.Sprint(i)))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := store.ReconcileInference(t.Context(), reservation, nil, inference.ReconciliationUncertain); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "organization-1", "work-1", 256)
			elapsed := time.Since(start)
			if err != nil || len(snapshot.Admissions) != count {
				t.Fatalf("distinct inference snapshot admissions=%d err=%v", len(snapshot.Admissions), err)
			}
			t.Logf("complete reopened read with %d distinct reservations and policies: %s", count, elapsed)
		})
	}
}

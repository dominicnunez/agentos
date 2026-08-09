package inference

import (
	"testing"
	"time"
)

func TestSelectPreservesReserve(t *testing.T) {
	m := Manager{Pools: []Pool{{ID: "a", Available: true, AllowedModels: []string{"m"}, Snapshot: UsageSnapshot{Source: "official_api", ObservedAt: time.Now(), Confidence: 1, Remaining: 10}, ContinuityReserve: 5}}}
	if _, err := m.Select(Request{EstimatedUsage: 6}); err == nil {
		t.Fatal("reserve spent")
	}
	if _, err := m.Select(Request{EstimatedUsage: 5}); err != nil {
		t.Fatal(err)
	}
}

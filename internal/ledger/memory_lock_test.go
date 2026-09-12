package ledger

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestMemoryContainmentSnapshotBoundsExclusiveLockWait(t *testing.T) {
	t.Run("snapshot", func(t *testing.T) { testMemoryExclusiveLockWait(t, false) })
	t.Run("writer", func(t *testing.T) { testMemoryExclusiveLockWait(t, true) })
}

func testMemoryExclusiveLockWait(t *testing.T, write bool) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	appendInferenceFreeze(t, store, "organization-1", 1, false)
	keeper, err := store.memoryKeepalive.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = keeper.ExecContext(context.Background(), "ROLLBACK"); _ = keeper.Close() }()
	if _, err := keeper.ExecContext(t.Context(), "BEGIN EXCLUSIVE"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		if write {
			done <- store.withTx(ctx, func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, "DELETE FROM records")
				return err
			})
			return
		}
		_, _, err := store.containmentSince(ctx, "organization-1", 0)
		done <- err
	}()
	limit := 500 * time.Millisecond
	if write {
		limit = 6 * time.Second
	} // Existing writer busy bound is five seconds.
	bounded := false
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("exclusive lock allowed a snapshot")
		}
		bounded = true
	case <-time.After(limit):
	}
	// Release even on failure so the regression reports a bounded assertion
	// instead of hanging the package on the driver's uncancellable retry.
	if _, err := keeper.ExecContext(t.Context(), "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	if !bounded {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("snapshot remained stuck after lock release")
		}
		t.Fatal("memory snapshot ignored its deadline until the lock was released")
	}
	if epoch, hold, err := store.containmentSince(t.Context(), "organization-1", 0); err != nil || epoch <= 0 || hold != nil {
		t.Fatalf("authority changed after lock release: epoch=%d hold=%v err=%v", epoch, hold, err)
	}
}

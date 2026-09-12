package ledger

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestRejectPrivateWatchDatabase(t *testing.T) {
	for _, path := range []string{"", "file::memory:"} {
		t.Run(path, func(t *testing.T) {
			store, err := Open(path)
			if store != nil {
				_ = store.Close()
				t.Fatal("opened a ledger whose observer would read a different database")
			}
			if err == nil || !strings.Contains(err.Error(), "open authority reader") {
				t.Fatalf("unexpected connection-private database result: %v", err)
			}
		})
	}
}

func TestWatchWhileWriterValidates(t *testing.T) {
	for _, storage := range []string{"memory", "file"} {
		t.Run(storage, func(t *testing.T) {
			path := ":memory:"
			if storage == "file" {
				path = filepath.Join(t.TempDir(), "ledger.db")
			}
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			appendInferenceFreeze(t, store, "organization-1", 1, false)
			call, finish, err := store.BeginExecutionContext(t.Context(), "organization-1")
			if err != nil {
				t.Fatal(err)
			}
			defer finish()
			entered, unblock := make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			go func() {
				done <- store.withTx(t.Context(), func(tx *sql.Tx) error {
					var count int
					if err := tx.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM records").Scan(&count); err != nil {
						return err
					}
					close(entered)
					<-unblock
					return nil
				})
			}()
			released := false
			defer func() {
				if !released {
					close(unblock)
					<-done
				}
			}()
			select {
			case <-entered:
			case err := <-done:
				released = true
				t.Fatalf("writer validation failed: %v", err)
			}
			// The writer is validating its snapshot, not changing authority.
			// Committed authority remains readable on another connection.
			check, stop := context.WithTimeout(call, 250*time.Millisecond)
			if err := store.CheckInferenceContext(check, "organization-1"); err != nil {
				stop()
				t.Fatalf("writer blocked the dispatch authority check: %v", err)
			}
			nested, finishNested, err := store.BeginExecutionContext(check, "organization-1")
			if err != nil {
				stop()
				t.Fatalf("writer blocked execution admission: %v", err)
			}
			if nested.Err() != nil {
				t.Errorf("new execution was already stopped: %v", nested.Err())
			}
			finishNested()
			stop()
			select {
			case <-call.Done():
				t.Fatal("writer validation prevented observation of committed authority")
			case <-time.After(containmentObservationTimeout + 200*time.Millisecond):
			}
			close(unblock)
			released = true
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			appendInferenceFreeze(t, store, "organization-1", 2, true)
			select {
			case <-call.Done():
			case <-time.After(containmentObservationTimeout):
				t.Fatal("committed freeze did not cancel live execution")
			}
			if !errors.Is(context.Cause(call), core.ErrOrganizationFrozen) {
				t.Fatalf("lost committed hold cause: %v", context.Cause(call))
			}
		})
	}
}

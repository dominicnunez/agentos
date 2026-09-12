package ledger

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCancelledQueriesReleaseAuthorityLocks(t *testing.T) {
	for _, storage := range []string{"memory", "file"} {
		t.Run(storage, func(t *testing.T) {
			path := ":memory:"
			if storage == "file" {
				path = filepath.Join(t.TempDir(), "authority.db")
			}
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			appendInferenceFreeze(t, store, "organization-1", 1, false)
			observer := store.memoryKeepalive
			if observer == nil {
				observer, err = sql.Open("sqlite", path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = observer.Close() })
			}
			conn, err := observer.Conn(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			if _, err := conn.ExecContext(t.Context(), "PRAGMA busy_timeout=100"); err != nil {
				t.Fatal(err)
			}
			// A wide result exercises cancellation while the driver constructs
			// Rows, after SQLite has already acquired the authority read lock.
			// Older drivers could discard those Rows without finalizing them.
			query := "SELECT " + strings.Repeat("body,", 999) + "body FROM records"
			cancelled := 0
			for i := 0; i < 500; i++ {
				ctx, cancel := context.WithTimeout(t.Context(), time.Duration(100+i%20*100)*time.Microsecond)
				queryErr := func() error {
					rows, err := store.db.QueryContext(ctx, query)
					if err != nil {
						return err
					}
					defer func() { _ = rows.Close() }()
					return rows.Err()
				}()
				cancel()
				if errors.Is(queryErr, context.DeadlineExceeded) {
					cancelled++
				} else if queryErr != nil {
					t.Fatalf("query %d: %v", i, queryErr)
				}
				// Use another connection so an orphaned statement cannot hide
				// behind the reader's own lock. There is no active query now.
				if _, err := conn.ExecContext(t.Context(), "BEGIN EXCLUSIVE"); err != nil {
					t.Fatalf("query %d retained an authority lock after returning %v: %v", i, queryErr, err)
				}
				if _, err := conn.ExecContext(t.Context(), "ROLLBACK"); err != nil {
					t.Fatal(err)
				}
			}
			if cancelled == 0 {
				t.Fatal("did not exercise query cancellation")
			}
			appendInferenceFreeze(t, store, "organization-1", 2, true)
			if epoch, hold, err := store.containmentSince(t.Context(), "organization-1", 0); err != nil || hold == nil || hold.Sequence != epoch {
				t.Fatalf("authority did not remain writable and observable: epoch=%d hold=%v err=%v", epoch, hold, err)
			}
		})
	}
}

//go:build linux

package fileguard

import (
	"path/filepath"
	"testing"
)

func TestProcessLockRejectsConcurrentOwnerAndReleases(t *testing.T) {
	path := filepath.Join(t.TempDir(), "writer.lock")
	first, err := AcquireProcessLock(path, 0o700)
	if err != nil {
		t.Fatal(err)
	}
	if second, err := AcquireProcessLock(path, 0o700); err == nil {
		_ = second.Close()
		t.Fatal("concurrent process lock owner was accepted")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	third, err := AcquireProcessLock(path, 0o700)
	if err != nil {
		t.Fatalf("released process lock could not be reacquired: %v", err)
	}
	if err := third.Close(); err != nil {
		t.Fatal(err)
	}
}

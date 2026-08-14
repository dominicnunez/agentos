package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestReadUnchangedBoundedFileRequiresTheOriginalBoundedSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input")
	want := []byte("bounded input")
	if err := os.WriteFile(path, want, 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := readUnchangedBoundedFile(path, before, int64(len(want)), "input")
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("body=%q err=%v", got, err)
	}
	if _, err := readUnchangedBoundedFile(path, before, int64(len(want)-1), "input"); err == nil {
		t.Fatal("oversized snapshot was accepted")
	}

	replacement := filepath.Join(filepath.Dir(path), "replacement")
	if err := os.WriteFile(replacement, want, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	if _, err := readUnchangedBoundedFile(path, before, int64(len(want)), "input"); err == nil {
		t.Fatal("replacement file was accepted")
	}

	before, err = os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := readUnchangedBoundedFile(path, before, int64(len(want)), "input"); err == nil {
		t.Fatal("file changed during the snapshot window was accepted")
	}
}

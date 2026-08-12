package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestReadArtifactUploadRejectsLinksAndReadsBoundedRegularFiles(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "evidence.txt")
	want := []byte("bounded evidence\n")
	if err := os.WriteFile(path, want, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := readArtifactUpload(path)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("artifact=%q err=%v", got, err)
	}
	link := filepath.Join(directory, "evidence-link.txt")
	if err := os.Symlink(path, link); err == nil {
		if _, err := readArtifactUpload(link); err == nil {
			t.Fatal("artifact symlink was accepted")
		}
	}
}

package artifacts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStorageFailureDoesNotExposeFilesystemDiagnostics(t *testing.T) {
	root := filepath.Join(t.TempDir(), "PRIVATE_STORAGE_CANARY")
	if err := os.WriteFile(root, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	evidence, created, err := (Store{Root: root}).Put("org", "task", "human", Upload{Role: "report", Name: "report.txt", Data: []byte("report content")})
	if err == nil {
		t.Fatal("unusable storage accepted artifact")
	}
	if strings.Contains(err.Error(), "PRIVATE_STORAGE_CANARY") || strings.Contains(err.Error(), root) {
		t.Fatalf("storage error exposed private path: %s", err)
	}
	if created || evidence.Ref != "" {
		t.Fatal("storage failure returned admitted artifact evidence")
	}
}

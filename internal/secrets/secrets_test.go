package secrets

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestCredentialDirectoryResolvesOnlyBoundedNamedRegularFiles(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "openai-api-key"), []byte("secret-value\n"), 0o400); err != nil {
		t.Fatal(err)
	}
	source := CredentialDirectory{Path: directory}
	value, err := source.Resolve(context.Background(), "openai-api-key")
	if err != nil || value != "secret-value" {
		t.Fatalf("value=%q err=%v", value, err)
	}
	for _, ref := range []Ref{"", "../escape", "nested/value"} {
		if _, err := source.Resolve(context.Background(), ref); err == nil {
			t.Fatalf("invalid reference %q was accepted", ref)
		}
	}
	if err := os.Symlink(filepath.Join(directory, "openai-api-key"), filepath.Join(directory, "link")); err == nil {
		if _, err := source.Resolve(context.Background(), "link"); err == nil {
			t.Fatal("symlink credential was accepted")
		}
	}
}

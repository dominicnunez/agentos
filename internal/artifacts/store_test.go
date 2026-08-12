package artifacts

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestStoreValidatesContentAndUsesContentAddressedPrivateFiles(t *testing.T) {
	store := Store{Root: t.TempDir()}
	upload := Upload{Role: "supporting-note", Name: "note.txt", MediaType: "text/plain", Data: []byte("verified note\n")}
	evidence, created, err := store.Put("org-1", "task-1", "local-uid-1000", upload)
	if err != nil || !created || evidence.Trust != "UNTRUSTED_USER_ARTIFACT" {
		t.Fatalf("evidence=%+v created=%t err=%v", evidence, created, err)
	}
	path := store.Root + string(os.PathSeparator) + evidence.SHA256[:2] + string(os.PathSeparator) + evidence.SHA256
	body, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(body, upload.Data) {
		t.Fatalf("stored body=%q err=%v", body, err)
	}
	second, created, err := store.Put("org-1", "task-1", "local-uid-1000", upload)
	if err != nil || created || second.Ref != evidence.Ref {
		t.Fatalf("idempotent put=%+v created=%t err=%v", second, created, err)
	}
	bad := upload
	bad.MediaType = "application/pdf"
	if _, _, err := store.Put("org-1", "task-1", "local-uid-1000", bad); err == nil {
		t.Fatal("media type spoofing was accepted")
	}
	bad = upload
	bad.Name = "report\x1b[31m.txt"
	if _, _, err := store.Put("org-1", "task-1", "local-uid-1000", bad); err == nil {
		t.Fatal("terminal control characters were accepted in artifact metadata")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(store.Root, "missing"), path); err == nil {
		if _, _, err := store.Put("org-1", "task-1", "local-uid-1000", upload); err == nil {
			t.Fatal("symlink at an existing content address was accepted")
		}
	}
}

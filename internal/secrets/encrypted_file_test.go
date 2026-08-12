package secrets

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestSealedFileRoundTripAndAuthentication(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	path := filepath.Join(t.TempDir(), "provider", "codex-auth.enc")
	plaintext := []byte(`{"access_token":"secret"}`)
	if err := SealFile(path, "codex-auth-v1", key, plaintext); err != nil {
		t.Fatal(err)
	}
	opened, err := OpenSealedFile(path, "codex-auth-v1", key)
	if err != nil || !bytes.Equal(opened, plaintext) {
		t.Fatalf("opened=%q err=%v", opened, err)
	}
	wrong := bytes.Repeat([]byte{8}, 32)
	if _, err := OpenSealedFile(path, "codex-auth-v1", wrong); err == nil {
		t.Fatal("wrong key authenticated the sealed credential")
	}
	if _, err := OpenSealedFile(path, "different-purpose", key); err == nil {
		t.Fatal("wrong purpose authenticated the sealed credential")
	}
}

package fileguard

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestWriteAtomicallyPreservesModesAndRejectsSymlinkedParents(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "private", "configuration")
	path := filepath.Join(directory, "state")
	if err := WriteAtomically(path, []byte("first"), 0o600, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := WriteAtomically(path, []byte("second"), 0o640, 0o700); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil || string(body) != "second" {
		t.Fatalf("body=%q err=%v", body, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o640 {
		t.Fatalf("file mode=%#o", info.Mode().Perm())
	}
	directoryInfo, err := os.Stat(directory)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && directoryInfo.Mode().Perm() != 0o700 {
		t.Fatalf("directory mode=%#o", directoryInfo.Mode().Perm())
	}
	if err := WriteAtomically(path, nil, os.ModeSymlink|0o600, 0o700); err == nil {
		t.Fatal("non-permission file mode was accepted")
	}

	target := filepath.Join(t.TempDir(), "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "redirect")
	if err := os.Symlink(target, link); err == nil {
		if err := WriteAtomically(filepath.Join(link, "state"), []byte("unsafe"), 0o600, 0o700); err == nil {
			t.Fatal("symlinked parent was accepted")
		}
	}
}

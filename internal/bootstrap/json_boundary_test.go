package bootstrap

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfigFileRejectsAmbiguityAndContentPastReadLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	for _, body := range []string{
		`{"version":1,"version":2}`,
		`{"VERSION":1}`,
		`{}` + strings.Repeat(" ", 1<<20) + `{}`,
	} {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		var value struct {
			Version int `json:"version"`
		}
		if decodeFile(path, "config", &value) == nil {
			t.Fatal("ambiguous or oversized configuration accepted")
		}
	}
}

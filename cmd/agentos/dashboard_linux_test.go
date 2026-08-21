//go:build linux

package main

import (
	"os"
	"strings"
	"testing"
)

func TestWriteDashboardBootstrapKeepsCredentialPrivateAndOffPath(t *testing.T) {
	const token = "private-bootstrap-token"
	path, err := writeDashboardBootstrap("127.0.0.1:41000", token)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(path) }()
	if strings.Contains(path, token) {
		t.Fatal("dashboard bootstrap credential appeared in its file path")
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("dashboard bootstrap mode=%v", info.Mode())
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	if !strings.Contains(text, "#bootstrap="+token) || !strings.Contains(text, "Content-Security-Policy") || !strings.Contains(text, "sha256-") {
		t.Fatal("dashboard bootstrap lacks its credential redirect or hash-bound policy")
	}
}

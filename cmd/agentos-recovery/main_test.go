package main

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/ledger/recovery"
)

func TestRunRequiresKnownCommand(t *testing.T) {
	for _, args := range [][]string{nil, {"unknown"}, {"verify", "extra"}} {
		if err := run(context.Background(), args, &bytes.Buffer{}); err == nil {
			t.Fatalf("run(%v) succeeded", args)
		}
	}
}

func TestRunPrintsVersionWithoutOpeningDatabase(t *testing.T) {
	for _, args := range [][]string{{"--version"}, {"version"}} {
		var output bytes.Buffer
		if err := run(context.Background(), args, &output); err != nil {
			t.Fatal(err)
		}
		if output.String() != version+"\n" {
			t.Fatalf("run(%v) output=%q", args, output.String())
		}
	}
}

func TestRunVerifyReturnsStructuredResult(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source.db")
	store, err := ledger.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := run(context.Background(), []string{"verify", "--database", source}, &output); err != nil {
		t.Fatal(err)
	}
	var result recovery.Result
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Path != source || result.SHA256 == "" || result.SizeBytes == 0 {
		t.Fatalf("result=%+v", result)
	}
}

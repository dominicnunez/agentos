package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/ledger"
)

type diagnosticFailureModel struct{ failingExecutionModel }

func (m *diagnosticFailureModel) Complete(context.Context, string) (execution.ModelResponse, error) {
	m.calls++
	return execution.ModelResponse{}, errors.New("Authorization: Bearer synthetic-private-canary")
}

func TestProviderDiagnosticsStayOutOfDurableWorkAcrossRestart(t *testing.T) {
	ctx := t.Context()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	store, err := ledger.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	model := &diagnosticFailureModel{}
	service := NewWithModel(events.NewGateway(store), model)
	result, runErr := service.Submit(ctx, Submit{RequestID: "private-fault", OrganizationID: "org-1", Statement: "bounded work", Kind: core.ExecutionAgent})
	if runErr == nil || result.Task.Status != core.TaskFailed || result.Work.Status != "FAILED" {
		t.Fatal("provider failure did not terminalize work")
	}
	if strings.Contains(fmt.Sprint(runErr), "synthetic-private-canary") {
		t.Fatal("returned error exposes private diagnostic")
	}
	verify := func(current *ledger.SQLite) {
		t.Helper()
		stream, err := current.Events(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, event := range stream {
			if strings.Contains(string(event.Payload), "synthetic-private-canary") {
				t.Fatal("durable event exposes private diagnostic")
			}
			if event.EventType == "TOOL_OUTCOME_RECORDED" {
				var outcome core.ToolOutcome
				if err := json.Unmarshal(event.Payload, &outcome); err != nil {
					t.Fatal(err)
				}
				if outcome.ErrorClass != "provider_failure" || outcome.Status != core.OutcomeFailed {
					t.Fatal("durable outcome lost safe failure category")
				}
				found = true
			}
		}
		if !found {
			t.Fatal("test did not reach durable outcome admission")
		}
	}
	verify(store)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = ledger.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	restarted := NewWithModel(events.NewGateway(store), model)
	if _, err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	verify(store)
	if model.calls != 1 {
		t.Fatal("recovery repeated the failed provider request")
	}
}

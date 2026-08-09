package app

import (
	"context"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
	"testing"
)

func TestVerticalSlice(t *testing.T) {
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	s := New(events.NewGateway(l))
	r, err := s.Submit(context.Background(), Submit{RequestID: "r1", OrganizationID: "o1", Statement: "echo hello", Kind: core.ExecutionDeterministic})
	if err != nil {
		t.Fatal(err)
	}
	if !r.Completion.Complete || r.Task.Status != core.TaskCompleted || len(r.Events) != 9 {
		t.Fatalf("unexpected result: %#v", r)
	}
}
func TestAgentExecutionUsesFakeAdapter(t *testing.T) {
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	r, err := New(events.NewGateway(l)).Submit(context.Background(), Submit{RequestID: "r2", OrganizationID: "o1", Statement: "summarize", Kind: core.ExecutionAgent})
	if err != nil {
		t.Fatal(err)
	}
	if r.Outcome.ObservedEffect != "fake-model: summarize" {
		t.Fatalf("effect=%q", r.Outcome.ObservedEffect)
	}
}

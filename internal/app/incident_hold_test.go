package app

import (
	"context"
	"encoding/json"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/planning"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type heldIncidentPlanner struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	calls   atomic.Int32
}

func (p *heldIncidentPlanner) Descriptor() (planning.Descriptor, bool) {
	return planning.Descriptor{PromptVersion: "v1", Provider: "test", Model: "test", ExecutionProfileVersion: "v1"}, true
}

func (p *heldIncidentPlanner) Build(ctx context.Context, _ planning.Input, _ core.ExecutionKind) (planning.Result, error) {
	p.calls.Add(1)
	close(p.started)
	<-p.release
	return planning.Result{Usage: &events.InferenceUsageRecordedPayload{Source: "test", Provider: "test", Model: "test", InputTokens: 1, OutputTokens: 1, TotalTokens: 2}}, context.Cause(ctx)
}

func TestIncidentReplayIncludesOwnerHold(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	planner := &heldIncidentPlanner{started: make(chan struct{}), release: make(chan struct{})}
	runtime := NewWithModelAndPlanner(events.NewGateway(store), execution.FakeModel{}, planner)
	t.Cleanup(func() {
		planner.once.Do(func() { close(planner.release) })
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runtime.WaitForStops(ctx); err != nil {
			t.Error(err)
		}
		_ = store.Close()
	})
	finished := make(chan error, 1)
	go func() {
		_, err := runtime.Submit(t.Context(), Submit{RequestID: "incident-hold", OrganizationID: "org-1", Statement: "private incident objective", Kind: core.ExecutionAgent})
		finished <- err
	}()
	select {
	case <-planner.started:
	case <-time.After(5 * time.Second):
		t.Fatal("planner did not start")
	}
	hold := setAppTestFreeze(t, t.Context(), store, "org-1", 1, true)
	release := setAppTestFreeze(t, t.Context(), store, "org-1", 2, false)
	select {
	case err := <-finished:
		if err == nil {
			t.Fatal("stopped planning succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stopped planning did not release caller")
	}
	before, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	read := func() string {
		t.Helper()
		report, found, err := runtime.IncidentReplay(t.Context(), "org-1", "incident-hold")
		if err != nil || !found {
			t.Fatalf("incident found=%t err=%v", found, err)
		}
		encoded, err := json.Marshal(report)
		if err != nil {
			t.Fatal(err)
		}
		return string(encoded)
	}
	response := read()
	for _, ref := range []string{hold.EventRef, release.EventRef} {
		if !strings.Contains(response, ref) {
			t.Fatalf("incident omitted exact hold/release reference %s", ref)
		}
	}
	for _, evidence := range []string{"MODEL_STOP_REQUESTED", "MODEL_STOP_UNCERTAIN", `"local_state":"UNCERTAIN"`} {
		if !strings.Contains(response, evidence) {
			t.Fatalf("incident omitted pending stop evidence %s", evidence)
		}
	}
	if read() != response {
		t.Fatal("unchanged incident snapshot produced a different report")
	}
	for _, private := range []string{"private incident objective", "private hold reason", "private release reason", `"sequence":`, `"ledger_sha256":`} {
		if strings.Contains(response, private) {
			t.Fatalf("incident leaked %q", private)
		}
	}
	after, err := store.Events(t.Context(), "")
	if err != nil || len(after) != len(before) || planner.calls.Load() != 1 {
		t.Fatalf("inspection performed work: before=%d after=%d calls=%d err=%v", len(before), len(after), planner.calls.Load(), err)
	}
	planner.once.Do(func() { close(planner.release) })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := runtime.WaitForStops(ctx); err != nil {
		t.Fatal(err)
	}
	late := read()
	for _, evidence := range []string{"MODEL_STOP_UNCERTAIN", "MODEL_STOP_CONFIRMED", "INFERENCE_USAGE_RECORDED", `"local_state":"RETURNED"`, `"usage_ref":"evt-`} {
		if !strings.Contains(late, evidence) {
			t.Fatalf("late incident omitted %s", evidence)
		}
	}
	if strings.Contains(late, "PLAN_CREATED") || strings.Contains(late, "TASK_RESUMED") || planner.calls.Load() != 1 {
		t.Fatal("inspection or hold release resumed stopped work")
	}
}

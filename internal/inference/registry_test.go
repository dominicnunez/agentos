package inference

import (
	"errors"
	"reflect"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
)

func TestConnectionRegistryIsolatesAccountsWithIdenticalModels(t *testing.T) {
	store := &guardStore{}
	response := execution.ModelResponse{Usage: events.InferenceUsageRecordedPayload{ConnectionID: "provider-spoofed-account", Source: "provider", Provider: "provider", Model: "model", InputTokens: 1, OutputTokens: 1, TotalTokens: 2}}
	first, second := &guardModel{response: response}, &guardModel{response: response}
	connections := []Connection{{ID: "second", Adapter: second}, {ID: "first", Adapter: first}}
	registry, err := NewConnectionRegistry(store, connections)
	if err != nil {
		t.Fatal(err)
	}
	connections[0] = Connection{ID: "injected", Adapter: first}
	ids := registry.Connections()
	if !reflect.DeepEqual(ids, []string{"first", "second"}) {
		t.Fatalf("connections=%v", ids)
	}
	ids[0] = "injected"
	for _, id := range []string{"", "provider", "model", "absent", "injected", "https://endpoint"} {
		if _, err := registry.Adapter(id); err == nil {
			t.Fatalf("unconfigured alias %q selected a connection", id)
		}
	}
	adapter, err := registry.Adapter("second")
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Complete(guardedContext(t), "prompt")
	if err != nil {
		t.Fatal(err)
	}
	if result.Usage.ConnectionID != "second" || first.called || !second.called || store.reservation.Request.ConnectionID != "second" {
		t.Fatal("same-model accounts crossed connection identity")
	}
	first.called, second.called = false, false
	store.reserveErr = errors.New("denied")
	if _, err := adapter.Complete(guardedContext(t), "prompt"); err == nil || first.called || second.called {
		t.Fatal("denied connection invoked an adapter or fell back")
	}
}

func TestConnectionRegistryRejectsAmbiguousConfiguration(t *testing.T) {
	for _, connections := range [][]Connection{
		nil,
		{{ID: "same", Adapter: &guardModel{}}, {ID: "same", Adapter: &guardModel{}}},
		{{ID: "missing-adapter"}},
		{{ID: "bad id", Adapter: &guardModel{}}},
	} {
		if _, err := NewConnectionRegistry(&guardStore{}, connections); err == nil {
			t.Fatal("invalid registry accepted")
		}
	}
}

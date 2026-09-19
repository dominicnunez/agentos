package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/modelinput"
)

type shutdownModel struct {
	execution.FakeModel
	started, cancelled, release chan struct{}
	once                        sync.Once
}

func (m *shutdownModel) finish() { m.once.Do(func() { close(m.release) }) }

func (m *shutdownModel) CompleteRequest(ctx context.Context, request modelinput.Request) (execution.ModelResponse, error) {
	close(m.started)
	select {
	case <-ctx.Done():
		close(m.cancelled)
		<-m.release
	case <-m.release:
	}
	return m.FakeModel.CompleteRequest(ctx, request)
}

func TestShutdownStopsWorkBeforeHTTPDrain(t *testing.T) {
	for _, trigger := range []string{"runtime-cancelled", "listener-failed"} {
		t.Run(trigger, func(t *testing.T) {
			store, err := ledger.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			gateway := events.NewGateway(store)
			model := &shutdownModel{started: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{})}
			service := app.NewWithModel(gateway, model)
			listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := newHTTPServer("", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, submitErr := service.Submit(r.Context(), app.Submit{RequestID: "shutdown-request", OrganizationID: "org", Statement: "prepare a note", Kind: core.ExecutionAgent})
				if submitErr != nil {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				w.WriteHeader(http.StatusOK)
			}), nil)
			otherListener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
			if err != nil {
				_ = listener.Close()
				t.Fatal(err)
			}
			otherServer := newHTTPServer("", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			}), nil)
			bindings := []serverBinding{{server: otherServer, listener: otherListener}, {server: server, listener: listener}}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { defer close(done); done <- serveAll(ctx, bindings, service.StopExecutions) }()
			client := &http.Client{Timeout: 5 * time.Second}
			requestDone := make(chan int, 1)
			go func() {
				defer close(requestDone)
				request, requestErr := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+listener.Addr().String(), nil)
				if requestErr != nil {
					requestDone <- 0
					return
				}
				response, requestErr := client.Do(request) //nolint:gosec // Loopback listener owned by this test.
				if requestErr != nil {
					requestDone <- 0
					return
				}
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				requestDone <- response.StatusCode
			}()
			t.Cleanup(func() {
				cancel()
				model.finish()
				_ = server.Close()
				_ = otherServer.Close()
				client.CloseIdleConnections()
				service.StopExecutions()
				stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer stopCancel()
				if err := service.WaitForStops(stopCtx); err != nil {
					t.Error(err)
				}
				select {
				case <-done:
				case <-stopCtx.Done():
					t.Error("server cleanup did not finish")
				}
				select {
				case <-requestDone:
				case <-stopCtx.Done():
					t.Error("request cleanup did not finish")
				}
			})
			select {
			case <-model.started:
			case <-time.After(5 * time.Second):
				t.Fatal("HTTP request did not start task execution")
			}
			if trigger == "runtime-cancelled" {
				cancel()
			} else if err := otherListener.Close(); err != nil {
				t.Fatal(err)
			}
			// This deadline is shorter than the server's ten-second drain. The
			// model stays blocked until cleanup, so handler return cannot mask
			// cancellation being deferred until after HTTP shutdown.
			select {
			case <-model.cancelled:
			case <-time.After(2 * time.Second):
				t.Fatal("HTTP drain began without cancelling active task work")
			}
			select {
			case status := <-requestDone:
				if status != http.StatusServiceUnavailable {
					t.Fatalf("interrupted HTTP request status=%d", status)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("HTTP request did not drain independently of its model")
			}
			select {
			case err := <-done:
				if trigger == "runtime-cancelled" && err != nil {
					t.Fatalf("runtime shutdown: %v", err)
				}
				if trigger == "listener-failed" && !errors.Is(err, net.ErrClosed) {
					t.Fatalf("listener failure was lost: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("server did not finish its HTTP drain")
			}
			stream, err := gateway.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			counts := map[string]int{}
			for _, event := range stream {
				counts[event.EventType]++
			}
			if counts["EXECUTION_STOP_REQUESTED"] != 1 || counts["EXECUTION_STOP_UNCERTAIN"] != 1 || counts["EXECUTION_STOP_CONFIRMED"] != 0 || counts["RESULT_PUBLISHED"] != 0 {
				t.Fatalf("shutdown did not preserve uncertain stop before handler return: %v", counts)
			}
		})
	}
}

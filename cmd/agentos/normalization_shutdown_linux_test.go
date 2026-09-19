//go:build linux

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/gateway"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/intake"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/modelinput"
)

type normalizationShutdownContextKey struct{}

type shutdownNormalizationAdapter struct {
	started, cancelled chan struct{}
	release            chan struct{}
	once               sync.Once
	releaseOnce        sync.Once
	preservedBaseValue bool
	stopCause          error
}

func (*shutdownNormalizationAdapter) Name() string { return "shutdown-normalization/test-model" }

func (*shutdownNormalizationAdapter) Descriptor() execution.ModelDescriptor {
	return execution.ModelDescriptor{Provider: "shutdown-normalization", Model: "test-model", ExecutionProfileVersion: "test-profile"}
}

func (m *shutdownNormalizationAdapter) Complete(ctx context.Context, _ string) (execution.ModelResponse, error) {
	return m.waitForShutdown(ctx)
}

func (m *shutdownNormalizationAdapter) CompleteRequest(ctx context.Context, _ modelinput.Request) (execution.ModelResponse, error) {
	return m.waitForShutdown(ctx)
}

func (m *shutdownNormalizationAdapter) waitForShutdown(ctx context.Context) (execution.ModelResponse, error) {
	m.once.Do(func() { close(m.started) })
	m.preservedBaseValue = ctx.Value(normalizationShutdownContextKey{}) == "base-context-value"
	<-ctx.Done()
	m.stopCause = context.Cause(ctx)
	close(m.cancelled)
	<-m.release
	return execution.ModelResponse{}, context.Cause(ctx)
}

func (m *shutdownNormalizationAdapter) finish() {
	m.releaseOnce.Do(func() { close(m.release) })
}

func TestShutdownCancelsActiveHTTPNormalizationBeforeDrain(t *testing.T) {
	for _, trigger := range []string{"runtime-cancelled", "listener-failed"} {
		t.Run(trigger, func(t *testing.T) {
			uid, gid := syscall.Geteuid(), syscall.Getegid()
			store, err := ledger.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			now := time.Now().UTC()
			if err := store.ActivateInferencePolicy(t.Context(), inference.Policy{
				Version: inference.PolicyVersion, OrganizationID: "org-1", Provider: "shutdown-normalization", Model: "test-model",
				ExecutionProfileVersion: "test-profile", Mode: inference.Local,
				MaxInputTokensPerRequest: 262_144, MaxOutputTokensPerRequest: 262_144, MaxTokensPerWindow: 1_000_000,
				ContinuityReserveTokens: 100_000, WindowDurationSeconds: 3600, MaxConcurrentRequests: 1, MaxAttemptsPerRequest: 1,
				AuthorizedBy: "local-uid-" + strconv.Itoa(uid), AuthorizedAt: now.Add(-time.Minute), AuthorizationExpiresAt: now.Add(time.Hour),
			}); err != nil {
				t.Fatal(err)
			}
			adapter := &shutdownNormalizationAdapter{started: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{})}
			guarded, err := inference.NewGuardedAdapter(store, adapter)
			if err != nil {
				t.Fatal(err)
			}
			normalizer, err := intake.NewModelNormalizer(intakeModel{adapter: guarded})
			if err != nil {
				t.Fatal(err)
			}
			eventGateway := events.NewGateway(store)
			runtime := app.New(eventGateway)
			operator := intake.NewWithNormalizer(runtime, normalizer)
			human, err := gateway.NewHuman(operator, gateway.LocalHuman{
				UID: uid, ID: core.ID("local-uid-" + strconv.Itoa(uid)), OrganizationID: "org-1",
			})
			if err != nil {
				t.Fatal(err)
			}
			userMux := http.NewServeMux()
			userMux.Handle("/v1/user/", human)
			userServer := newHTTPServer("", userMux, nil)
			userServer.ConnContext = localConnContext
			userServer.BaseContext = func(net.Listener) context.Context {
				return context.WithValue(context.Background(), normalizationShutdownContextKey{}, "base-context-value")
			}
			runtimeDir := filepath.Join(t.TempDir(), "runtime")
			if err := os.Mkdir(runtimeDir, 0o700); err != nil {
				t.Fatal(err)
			}
			socketPath := filepath.Join(runtimeDir, "user.sock")
			userListener, err := listenLocalHuman(t.Context(), socketPath, uid, gid)
			if err != nil {
				t.Fatal(err)
			}
			siblingListener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
			if err != nil {
				_ = userListener.Close()
				t.Fatal(err)
			}
			siblingServer := newHTTPServer("", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			}), nil)
			ctx, cancel := context.WithCancel(t.Context())
			serveDone := make(chan error, 1)
			go func() {
				defer close(serveDone)
				serveDone <- serveAll(ctx, []serverBinding{{server: siblingServer, listener: siblingListener}, {server: userServer, listener: userListener}}, runtime.StopExecutions)
			}()
			transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
			}}
			client := &http.Client{Transport: transport, Timeout: 15 * time.Second}
			requestDone := make(chan struct {
				status int
				err    error
			}, 1)
			go func() {
				defer close(requestDone)
				body := bytes.NewBufferString(`{"conversation_id":"shutdown-normalization","message_id":"message-1","text":"Draft a release note"}`)
				request, requestErr := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://agentos.local/v1/user/messages", body)
				if requestErr != nil {
					requestDone <- struct {
						status int
						err    error
					}{err: requestErr}
					return
				}
				request.Header.Set("Content-Type", "application/json")
				response, requestErr := client.Do(request)
				if requestErr != nil {
					requestDone <- struct {
						status int
						err    error
					}{err: requestErr}
					return
				}
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				requestDone <- struct {
					status int
					err    error
				}{status: response.StatusCode}
			}()
			t.Cleanup(func() {
				adapter.finish()
				cancel()
				_ = userServer.Close()
				_ = siblingServer.Close()
				client.CloseIdleConnections()
				select {
				case <-serveDone:
				case <-time.After(12 * time.Second):
					t.Error("server cleanup did not finish")
				}
				select {
				case <-requestDone:
				case <-time.After(2 * time.Second):
					t.Error("request cleanup did not finish")
				}
				cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancelCleanup()
				if err := runtime.WaitForStops(cleanupCtx); err != nil {
					t.Error(err)
				}
			})
			select {
			case <-adapter.started:
			case <-time.After(5 * time.Second):
				t.Fatal("normalization did not reach the provider adapter")
			}
			if trigger == "runtime-cancelled" {
				cancel()
			} else if err := siblingListener.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-adapter.cancelled:
			case <-time.After(2 * time.Second):
				t.Fatal("HTTP shutdown did not cancel active normalization before drain")
			}
			if !adapter.preservedBaseValue {
				t.Fatal("shutdown context discarded the server base context")
			}
			if !errors.Is(adapter.stopCause, core.ErrExecutionStopped) {
				t.Fatalf("runtime shutdown lost its cause: %v", adapter.stopCause)
			}
			select {
			case result := <-requestDone:
				if result.err != nil || result.status != http.StatusServiceUnavailable {
					t.Fatalf("normalization response status=%d err=%v", result.status, result.err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("cancelled normalization request waited for the blocked provider")
			}
			stopping, err := eventGateway.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			stopCounts := map[string]int{}
			for _, event := range stopping {
				stopCounts[event.EventType]++
			}
			if stopCounts["MODEL_STOP_REQUESTED"] != 1 || stopCounts["MODEL_STOP_UNCERTAIN"] != 1 || stopCounts["MODEL_STOP_CONFIRMED"] != 0 {
				t.Fatalf("normalization stop evidence before provider return=%v", stopCounts)
			}
			waitingCtx, cancelWait := context.WithTimeout(t.Context(), 50*time.Millisecond)
			err = runtime.WaitForStops(waitingCtx)
			cancelWait()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("normalization falsely drained an unreturned provider: %v", err)
			}
			adapter.finish()
			stopCtx, cancelStop := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancelStop()
			if err := runtime.WaitForStops(stopCtx); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-serveDone:
				if trigger == "runtime-cancelled" && err != nil {
					t.Fatalf("runtime shutdown: %v", err)
				}
				if trigger == "listener-failed" && !errors.Is(err, net.ErrClosed) {
					t.Fatalf("listener failure was lost: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("servers did not finish draining cancelled normalization")
			}
			stream, err := eventGateway.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			counts := map[string]int{}
			for _, event := range stream {
				counts[event.EventType]++
			}
			if counts["INFERENCE_RESERVED"] != 1 || counts["INFERENCE_RECONCILED"] != 1 || counts["INTENT_NORMALIZATION_FAILED"] != 0 || counts["INTENT_DRAFTED"] != 0 || counts["MODEL_STOP_CONFIRMED"] != 1 {
				t.Fatalf("shutdown normalization accounting=%v", counts)
			}
		})
	}
}

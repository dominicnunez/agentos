package execution

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/codex-sdk-go/login/auth"
)

// Exercise public construction, authentication, wire cancellation, process
// retirement and a fresh request without calling a provider or using real tokens.
func TestCodexSubscriptionRecoversProcess(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	credentialsPath := filepath.Join(t.TempDir(), "credentials.json")
	if err := auth.SaveCredentials(credentialsPath, auth.Credentials{
		AccessToken: "synthetic-token", RefreshToken: "synthetic-refresh",
		AccountID: "synthetic-account", ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	adapter, err := NewCodexSubscription(ctx, CodexSubscriptionConfig{
		BinaryPath: binary, CredentialsPath: credentialsPath, Model: "gpt-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := adapter.Close(); err != nil {
			t.Error(err)
		}
	})
	oldRoot := adapter.isolatedDir
	turnCtx, cancelTurn := context.WithCancel(ctx)
	defer cancelTurn()
	turnDone := make(chan error, 1)
	go func() {
		_, err := adapter.Complete(turnCtx, "hang-for-stop")
		turnDone <- err
	}()
	joined := false
	t.Cleanup(func() {
		cancelTurn()
		if !joined {
			_ = adapter.Close()
			select {
			case <-turnDone:
			case <-time.After(5 * time.Second):
				t.Error("canceled turn did not return during cleanup")
			}
		}
	})
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		_, err := os.Stat(filepath.Join(oldRoot, "turn-received"))
		if err == nil {
			break
		}
		if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		select {
		case err := <-turnDone:
			joined = true
			t.Fatalf("turn returned before the peer received it: %v", err)
		case <-ctx.Done():
			t.Fatal("peer did not receive turn before timeout")
		case <-tick.C:
		}
	}
	cancelTurn()
	select {
	case err = <-turnDone:
		joined = true
	case <-ctx.Done():
		t.Fatal("canceled turn did not return")
	}
	stop, found := StopOutcome(err)
	if !errors.Is(err, context.Canceled) || !found || !stop.LocalProcessStopAttempted || !stop.LocalProcessStopped || stop.RemoteStatus != RemoteStopUncertain || WasRequestNotSent(err) {
		t.Fatalf("stop evidence=%+v found=%v error=%v", stop, found, err)
	}
	if adapter.isolatedDir != "" && adapter.isolatedDir != oldRoot {
		t.Fatal("canceled request started a replacement process")
	}
	if _, err := os.Stat(oldRoot); !os.IsNotExist(err) {
		t.Fatalf("retired process root remains: %v", err)
	}
	response, err := adapter.Complete(ctx, "fresh-request")
	if err != nil || response.Text != "answer" || response.Usage.InputTokens != 3 || response.Usage.OutputTokens != 2 {
		t.Fatalf("independent request failed: response=%+v error=%v", response, err)
	}
	newRoot := adapter.isolatedDir
	if newRoot == "" || newRoot == oldRoot {
		t.Fatal("independent request did not get a fresh process root")
	}
	if _, err := os.Stat(filepath.Join(newRoot, "turn-received")); !os.IsNotExist(err) {
		t.Fatalf("interrupted turn was replayed in the replacement process: %v", err)
	}
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(newRoot); !os.IsNotExist(err) {
		t.Fatalf("replacement process root remains after close: %v", err)
	}
}

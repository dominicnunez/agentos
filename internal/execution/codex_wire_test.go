package execution

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/codex-sdk-go/appserver/protocol"
	"github.com/dominicnunez/codex-sdk-go/appserver/transport"
)

func wireTestObserver(t *testing.T) *codexTurnObserver {
	t.Helper()
	_, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	return &codexTurnObserver{threadID: "thread-1", done: make(chan struct{}), cancel: cancel}
}

func TestCodexWireObservesRerouteBeforeSDKWorkers(t *testing.T) {
	source, writer := io.Pipe()
	wire := newCodexWireReader(source)
	o := wireTestObserver(t)
	if err := wire.attach(o); err != nil {
		t.Fatal(err)
	}
	tr := transport.NewStdioTransport(wire, io.Discard)
	t.Cleanup(func() { _ = writer.Close(); _ = tr.Close() })
	client := protocol.NewClient(tr)
	entered, release, completed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	client.AddNotificationListener("model/rerouted", func(context.Context, protocol.Notification) {
		close(entered)
		<-release
	})
	client.AddNotificationListener("turn/completed", func(context.Context, protocol.Notification) { close(completed) })
	if _, err := io.WriteString(writer, "{\"method\":\"model/rerouted\",\"params\":{}}\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("reroute worker not started")
	}
	if _, err := io.WriteString(writer, "{\"method\":\"turn/completed\",\"params\":{\"threadId\":\"thread-1\",\"turn\":{\"id\":\"turn-1\",\"status\":\"completed\",\"items\":[]}}}\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("completion worker not started")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.err == nil || o.completed != nil {
		t.Fatal("wire reroute failed to invalidate completion")
	}
}

func TestCodexWireRejectsUnobservedMethods(t *testing.T) {
	for _, method := range []string{"item/reasoning/summaryPartAdded", "turn/plan/updated", "item/fileChange/patchUpdated", "future/unknown"} {
		t.Run(method, func(t *testing.T) {
			wire := newCodexWireReader(io.NopCloser(strings.NewReader("{\"method\":\"" + method + "\",\"params\":{}}\n")))
			o := wireTestObserver(t)
			if err := wire.attach(o); err != nil {
				t.Fatal(err)
			}
			if _, err := wire.Read(make([]byte, 1024)); err != nil {
				t.Fatal(err)
			}
			if o.err == nil {
				t.Fatal("unrecognized method bypassed observer")
			}
		})
	}
}

func TestCodexWireBoundsAllFrames(t *testing.T) {
	for _, tc := range []struct {
		name, frame string
		repetitions int
	}{
		{"count", "{\"id\":1,\"result\":{}}\n", 16385},
		{"aggregate", "{\"id\":1,\"result\":\"" + strings.Repeat("a", 256<<10) + "\"}\n", 17},
		{"individual", "{\"id\":1,\"result\":\"" + strings.Repeat("a", 512<<10) + "\"}\n", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wire := newCodexWireReader(io.NopCloser(strings.NewReader(strings.Repeat(tc.frame, tc.repetitions))))
			o := wireTestObserver(t)
			if err := wire.attach(o); err != nil {
				t.Fatal(err)
			}
			buffer := make([]byte, codexMaximumStreamBytes+1)
			var err error
			for i := 0; i < tc.repetitions; i++ {
				if _, err = wire.Read(buffer); err != nil {
					break
				}
			}
			if err == nil || o.err == nil {
				t.Fatal("frame limits did not fail closed")
			}
		})
	}
}

func TestCodexWireRejectsAmbiguousEnvelopeBeforeSDK(t *testing.T) {
	for _, frame := range []string{
		`{"method":"turn/completed","method":"model/rerouted","params":{}}`,
		`{"Method":"model/rerouted","params":{}}`,
		`{"jsonrpc":"1.0","method":"model/rerouted","params":{}}`,
		`{"method":"model/rerouted","result":{}}`,
		`{"id":null,"method":"model/rerouted","params":{}}`,
		`{"id":true,"result":{}}`,
		`{"id":1,"result":{},"error":{}}`,
		`null`,
	} {
		wire := newCodexWireReader(io.NopCloser(strings.NewReader(frame + "\n")))
		if n, err := wire.Read(make([]byte, 1024)); n != 0 || err == nil {
			t.Fatalf("ambiguous frame forwarded: %q", frame)
		}
	}
}

func TestCodexWireRejectsDisguisedEvidence(t *testing.T) {
	for _, frame := range []string{
		`{"id":1,"method":"model/rerouted","params":{}}`,
		`{"id":2,"method":"item/commandExecution/requestApproval","params":{}}`,
		`{"method":"turn/completed","params":{"threadId":"foreign","ThreadId":"thread-1","turn":{"id":"turn-1","status":"completed","items":[]}}}`,
		`{"method":"turn/completed","params":{"threadId":"thread-1","turn":{"id":"turn-1","ID":"other","status":"completed","items":[]}}}`,
	} {
		wire := newCodexWireReader(io.NopCloser(strings.NewReader(frame + "\n")))
		o := wireTestObserver(t)
		if err := wire.attach(o); err != nil {
			t.Fatal(err)
		}
		if n, err := wire.Read(make([]byte, 1024)); n != 0 || err == nil || o.err == nil {
			t.Fatalf("disguised evidence forwarded: %s", frame)
		}
	}
}

func TestCodexWireAllowsBoundedAuthenticationRequests(t *testing.T) {
	frame := `{"id":"refresh-1","method":"account/chatgptAuthTokens/refresh","params":{"reason":"unauthorized"}}` + "\n"
	wire := newCodexWireReader(io.NopCloser(strings.NewReader(frame)))
	o := wireTestObserver(t)
	if err := wire.attach(o); err != nil {
		t.Fatal(err)
	}
	if n, err := wire.Read(make([]byte, 1024)); n != len(frame) || err != nil || o.err != nil {
		t.Fatal("auth request did not reach SDK")
	}
}

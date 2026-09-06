package execution

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"

	"github.com/dominicnunez/agentos/internal/boundaryjson"
	"github.com/dominicnunez/codex-sdk-go/appserver/protocol"
)

// codexWireReader validates each entire frame and observes notifications before
// exposing any of its bytes to the SDK. SDK worker scheduling therefore cannot
// move an earlier reroute (or budget violation) after completion evidence.
// The SDK remains responsible for request matching and approval dispatch.
type codexWireReader struct {
	source        io.ReadCloser
	reader        *bufio.Reader
	pending       []byte
	mu            sync.Mutex
	observer      *codexTurnObserver
	terminal      error
	bytes, frames int
}

func newCodexWireReader(source io.ReadCloser) *codexWireReader {
	return &codexWireReader{source: source, reader: bufio.NewReaderSize(source, codexMaximumStreamBytes+1)}
}

func (r *codexWireReader) attach(observer *codexTurnObserver) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.terminal != nil {
		return r.terminal
	}
	if r.observer != nil {
		return errors.New("codex wire observer already attached")
	}
	r.observer = observer
	r.bytes, r.frames = 0, 0
	return nil
}

func (r *codexWireReader) detach(observer *codexTurnObserver) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.observer == observer {
		r.observer = nil
	}
}

func (r *codexWireReader) reject() error {
	r.terminal = errors.New("codex inbound stream is invalid or exceeds limits")
	if r.observer != nil {
		r.observer.mu.Lock()
		r.observer.fail("codex inbound stream is invalid or exceeds limits")
		r.observer.mu.Unlock()
	}
	return r.terminal
}

func (r *codexWireReader) inspect(frame []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.terminal != nil {
		return r.terminal
	}
	if len(frame) > codexMaximumStreamBytes || boundaryjson.Validate(frame) != nil {
		return r.reject()
	}
	// Closed envelope keys prevent aliases from changing whether the frame is a
	// notification, response, or request. Payloads retain protocol-specific schemas.
	var fields map[string]json.RawMessage
	if json.Unmarshal(frame, &fields) != nil || fields == nil {
		return r.reject()
	}
	for name := range fields {
		switch name {
		case "jsonrpc", "id", "method", "params", "result", "error":
		default:
			return r.reject()
		}
	}
	if version, ok := fields["jsonrpc"]; ok && string(version) != `"2.0"` {
		return r.reject()
	}
	if r.observer != nil {
		r.bytes += len(frame)
		r.frames++
		if r.bytes > 4<<20 || r.frames > 16384 {
			return r.reject()
		}
	}
	methodJSON, hasMethod := fields["method"]
	_, hasID := fields["id"]
	if hasID {
		var id protocol.RequestID
		if string(fields["id"]) == "null" || json.Unmarshal(fields["id"], &id) != nil {
			return r.reject()
		}
	}
	if hasMethod {
		var method string
		if json.Unmarshal(methodJSON, &method) != nil || method == "" {
			return r.reject()
		}
		if _, exists := fields["result"]; exists {
			return r.reject()
		}
		if _, exists := fields["error"]; exists {
			return r.reject()
		}
		if !hasID && r.observer != nil {
			if !exactCodexWireIdentity(fields["params"]) {
				return r.reject()
			}
			r.observer.observe(protocol.Notification{Method: method, Params: fields["params"]})
		} else if hasID && r.observer != nil && method != "account/chatgptAuthTokens/refresh" {
			return r.reject()
		}
	} else if !hasID {
		return r.reject()
	} else {
		_, result := fields["result"]
		_, failure := fields["error"]
		if result == failure {
			return r.reject()
		}
	}
	return nil
}

// The SDK accepts case-insensitive struct fields. Do not let a later alias
// override the exact identity/type fields used to scope model evidence.
func exactCodexWireIdentity(data json.RawMessage) bool {
	var value any
	if json.Unmarshal(data, &value) != nil {
		return false
	}
	return exactCodexWireValue(value)
}

func exactCodexWireValue(value any) bool {
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			for _, name := range []string{"threadId", "turnId", "id", "model", "modelProvider", "thread", "turn", "type", "status"} {
				if key != name && strings.EqualFold(key, name) {
					return false
				}
			}
			if !exactCodexWireValue(child) {
				return false
			}
		}
	case []any:
		for _, child := range v {
			if !exactCodexWireValue(child) {
				return false
			}
		}
	}
	return true
}

func (r *codexWireReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(r.pending) == 0 {
		frame, err := r.reader.ReadSlice('\n')
		if err != nil {
			r.mu.Lock()
			failure := r.reject()
			r.mu.Unlock()
			return 0, failure
		}
		if err := r.inspect(frame); err != nil {
			return 0, err
		}
		r.pending = frame
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}

func (r *codexWireReader) Close() error { return r.source.Close() }

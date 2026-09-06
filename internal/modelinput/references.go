package modelinput

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
)

var ErrReference = errors.New("source reference is not available in this invocation")

// Binding is constructed from runtime-selected input. It owns its source map;
// model output and payload metadata cannot add entries. Handles are scoped
// content identifiers, not secrets, capabilities, or proof of source truth.
type Binding struct {
	scope      string
	request    Request
	references map[string]Source
}

// Bind creates deterministic opaque references so exact ledger replay can
// reconstruct the same mapping. The caller supplies a runtime-owned invocation
// scope, never an identifier extracted from model output.
func Bind(scope string, request Request) (*Binding, error) {
	if !validReference(scope) {
		return nil, ErrInvalid
	}
	if len(request.Messages) > MaximumMessages {
		return nil, ErrLimit
	}
	request.Messages = append([]Message(nil), request.Messages...)
	for _, message := range request.Messages {
		if message.Source.Handle != "" {
			return nil, ErrInvalid
		}
	}
	fingerprint, err := request.Fingerprint()
	if err != nil {
		return nil, err
	}
	binding := &Binding{scope: scope, request: request, references: make(map[string]Source, len(request.Messages))}
	for i := range binding.request.Messages {
		message := &binding.request.Messages[i]
		digest := sha256.Sum256([]byte("agentos-source-v1\x00" + scope + "\x00" + fingerprint + "\x00" + strconv.Itoa(i)))
		handle := "src_" + hex.EncodeToString(digest[:])
		binding.references[handle] = message.Source
		message.Source.Handle = handle
	}
	if _, err := binding.request.Canonical(); err != nil {
		return nil, err
	}
	return binding, nil
}

func (b *Binding) Request() Request {
	if b == nil {
		return Request{}
	}
	request := b.request
	request.Messages = append([]Message(nil), request.Messages...)
	return request
}

// Resolve performs membership lookup only. It never parses or reconstructs a
// reference from a model-provided ID, URI, content string, or metadata object.
func (b *Binding) Resolve(scope, handle string) (Source, error) {
	if b == nil || scope != b.scope || !validHandle(handle) {
		return Source{}, ErrReference
	}
	source, ok := b.references[handle]
	if !ok {
		return Source{}, ErrReference
	}
	return source, nil
}

func validHandle(handle string) bool {
	if len(handle) != 68 || handle[:4] != "src_" {
		return false
	}
	for _, c := range handle[4:] {
		if c < '0' || c > '9' && c < 'a' || c > 'f' {
			return false
		}
	}
	return true
}

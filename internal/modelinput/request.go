// Package modelinput preserves instruction roles and runtime-selected source
// evidence independently of provider transport. It grants no execution authority.
package modelinput

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"
)

const Version = "model-input-v1"
const MaximumBytes = 256 << 10
const MaximumMessages = 256

type Role string

const (
	System    Role = "system"
	User      Role = "user"
	Data      Role = "low_privilege_data"
	Assistant Role = "assistant"
)

type SourceKind string

const (
	RuntimeContract     SourceKind = "runtime_contract"
	AgentBlueprint      SourceKind = "agent_blueprint"
	OperatorMessage     SourceKind = "operator_message"
	TaskContext         SourceKind = "task_context"
	StrategyContext     SourceKind = "strategy_context"
	KnowledgeContext    SourceKind = "knowledge_context"
	CoordinationContext SourceKind = "coordination_context"
	PriorModelOutput    SourceKind = "prior_model_output"
)

// Source is supplied by runtime materialization, never promoted from model
// output. Reference identifies the selected revision; Digest binds its content.
// Provider serialization may omit the reference, but the manifested fingerprint
// always includes it. A source reference is evidence, not an authority token.
type Source struct {
	Kind      SourceKind `json:"kind"`
	Reference string     `json:"reference"`
	Digest    string     `json:"digest"`
	Handle    string     `json:"handle,omitempty"`
}

type Message struct {
	Role   Role   `json:"role"`
	Text   string `json:"text"`
	Source Source `json:"source"`
}

type Request struct {
	Version  string    `json:"version"`
	Messages []Message `json:"messages"`
}

var ErrInvalid = errors.New("invalid structured model input")
var ErrLimit = errors.New("structured model input exceeds limits")

func TextDigest(text string) string {
	digest := sha256.Sum256([]byte(text))
	return hex.EncodeToString(digest[:])
}

// Validate rejects invalid privilege/source combinations before any provider
// call. Runtime policy and blueprint instructions are the only system sources;
// ordinary task, knowledge, coordination and user content remain data.
func (r Request) Validate() error {
	if r.Version != Version || len(r.Messages) == 0 {
		return ErrInvalid
	}
	if len(r.Messages) > MaximumMessages {
		return ErrLimit
	}
	bytes := 0
	for _, message := range r.Messages {
		if len(message.Text) > MaximumBytes || len(message.Source.Reference) > 256 {
			return ErrLimit
		}
		if !utf8.ValidString(message.Text) || message.Text == "" || !validReference(message.Source.Reference) || message.Source.Digest != TextDigest(message.Text) {
			return ErrInvalid
		}
		if message.Source.Handle != "" && !validHandle(message.Source.Handle) {
			return ErrInvalid
		}
		switch message.Source.Kind {
		case RuntimeContract, AgentBlueprint:
			if message.Role != System {
				return ErrInvalid
			}
		case OperatorMessage, TaskContext:
			if message.Role != User {
				return ErrInvalid
			}
		case StrategyContext, KnowledgeContext, CoordinationContext:
			if message.Role != Data {
				return ErrInvalid
			}
		case PriorModelOutput:
			if message.Role != Assistant {
				return ErrInvalid
			}
		default:
			return ErrInvalid
		}
		bytes += len(message.Text) + len(message.Source.Reference)
		if bytes > MaximumBytes {
			return ErrLimit
		}
	}
	return nil
}

func validReference(reference string) bool {
	return len(reference) > 0 && len(reference) <= 256 && utf8.ValidString(reference) && strings.IndexFunc(reference, func(r rune) bool { return r <= 32 || r == 127 }) == -1
}

// Canonical includes both text and its role/source binding. Its bytes are for
// durable fingerprinting, not a fallback prompt that flattens roles into text.
func (r Request) Canonical() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	body, err := json.Marshal(r)
	if err != nil {
		return nil, ErrInvalid
	}
	if len(body) > MaximumBytes {
		return nil, ErrLimit
	}
	return body, nil
}

func (r Request) Fingerprint() (string, error) {
	body, err := r.Canonical()
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:]), nil
}

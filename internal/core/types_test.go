package core

import (
	"testing"
	"time"
)

func TestTaskReady(t *testing.T) {
	a := Task{ID: "a", Status: TaskCompleted}
	b := Task{ID: "b", DependsOn: []ID{"a"}}
	if !b.Ready(map[ID]Task{"a": a}) {
		t.Fatal("expected ready")
	}
	a.Status = TaskFailed
	if b.Ready(map[ID]Task{"a": a}) {
		t.Fatal("expected blocked")
	}
}

func TestValidIntentSourceIdentity(t *testing.T) {
	for _, test := range []struct {
		name    string
		id      ID
		kind    PrincipalKind
		channel string
		valid   bool
	}{
		{name: "runtime", id: "runtime", kind: PrincipalRuntime, channel: "INTERNAL", valid: true},
		{name: "user", id: "user-1", kind: PrincipalHuman, channel: "HUMAN_DIRECT", valid: true},
		{name: "external Agent", id: "agent-1", kind: PrincipalExternalAgent, channel: "A2A", valid: true},
		{name: "missing identity", kind: PrincipalExternalAgent, channel: "A2A"},
		{name: "user over A2A", id: "user-1", kind: PrincipalHuman, channel: "A2A"},
		{name: "Agent over user channel", id: "agent-1", kind: PrincipalExternalAgent, channel: "HUMAN_DIRECT"},
		{name: "runtime over A2A", id: "runtime", kind: PrincipalRuntime, channel: "A2A"},
		{name: "unknown channel", id: "user-1", kind: PrincipalHuman, channel: "WEB"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := ValidIntentSourceIdentity(test.id, test.kind, test.channel); got != test.valid {
				t.Fatalf("valid=%t want=%t", got, test.valid)
			}
		})
	}
}

func TestToolOutcomeValidUsesClosedContractVocabulary(t *testing.T) {
	now := time.Unix(10, 0).UTC()
	valid := ToolOutcome{
		ToolInvocationID: "invocation-1", ToolID: "bounded/test", Status: OutcomeSucceeded,
		PostconditionStatus: PostconditionVerified, Retryability: NotRetryable,
		StartedAt: now, FinishedAt: now.Add(time.Second),
	}
	if !valid.Valid() {
		t.Fatal("valid outcome was rejected")
	}
	for _, status := range []ToolOutcomeStatus{OutcomeSucceeded, OutcomeFailed, OutcomePartial} {
		candidate := valid
		candidate.Status = status
		if !candidate.Valid() {
			t.Fatalf("supported status %q was rejected", status)
		}
	}
	for _, retryability := range []Retryability{Retryable, NotRetryable, RetryAfterChange} {
		candidate := valid
		candidate.Retryability = retryability
		if !candidate.Valid() {
			t.Fatalf("supported retryability %q was rejected", retryability)
		}
	}
	for name, mutate := range map[string]func(*ToolOutcome){
		"missing invocation": func(outcome *ToolOutcome) { outcome.ToolInvocationID = "" },
		"missing tool":       func(outcome *ToolOutcome) { outcome.ToolID = "" },
		"unknown status":     func(outcome *ToolOutcome) { outcome.Status = "UNKNOWN" },
		"unknown check":      func(outcome *ToolOutcome) { outcome.PostconditionStatus = "UNKNOWN" },
		"unknown retry":      func(outcome *ToolOutcome) { outcome.Retryability = "UNKNOWN" },
		"missing start":      func(outcome *ToolOutcome) { outcome.StartedAt = time.Time{} },
		"missing finish":     func(outcome *ToolOutcome) { outcome.FinishedAt = time.Time{} },
		"reversed time":      func(outcome *ToolOutcome) { outcome.FinishedAt = now.Add(-time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			if candidate.Valid() {
				t.Fatal("invalid outcome was accepted")
			}
		})
	}
}

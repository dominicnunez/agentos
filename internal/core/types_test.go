package core

import "testing"

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

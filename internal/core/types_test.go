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

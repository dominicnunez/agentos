package core

import (
	"strings"
	"testing"
)

func TestValidateTaskDAGRejectsCycles(t *testing.T) {
	tasks := map[ID]Task{
		"first":  {ID: "first", DependsOn: []ID{"second"}},
		"second": {ID: "second", DependsOn: []ID{"first"}},
	}
	if err := ValidateTaskDAG(tasks); err == nil || !strings.Contains(err.Error(), "dependency cycle") {
		t.Fatalf("cyclic Task DAG error=%v", err)
	}
}

func TestValidateTaskDAGAcceptsAcyclicDependencies(t *testing.T) {
	tasks := map[ID]Task{
		"first":  {ID: "first"},
		"second": {ID: "second", DependsOn: []ID{"first"}},
	}
	if err := ValidateTaskDAG(tasks); err != nil {
		t.Fatalf("valid Task DAG rejected: %v", err)
	}
}

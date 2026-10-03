package events

import (
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestProjectionCompletionsChecksGraphAndEvidence(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*core.DurableGraph)
		want   string
	}{
		{"active graph", func(*core.DurableGraph) {}, ""},
		{"missing Task dependency", func(graph *core.DurableGraph) {
			task := graph.Tasks["task-1"]
			task.Value.DependsOn = []core.ID{"missing-task"}
			graph.Tasks["task-1"] = task
		}, "invalid dependency"},
		{"Work completion lacks transition", func(graph *core.DurableGraph) {
			work := graph.Works["work-1"]
			work.Value.Status = core.WorkCompleted
			graph.Works["work-1"] = work
		}, "lacks an authoritative transition"},
		{"Goal achievement lacks transition", func(graph *core.DurableGraph) {
			now := time.Now().UTC()
			graph.Missions["mission-1"] = core.DurableState[core.Mission]{Version: 1, CorrelationID: "strategy", Value: core.Mission{ID: "mission-1", OrganizationID: "org-1", Statement: "useful work", Status: core.MissionActive, CreatedAt: now}}
			graph.Goals["goal-1"] = core.DurableState[core.Goal]{Version: 2, CorrelationID: "strategy", Value: core.Goal{ID: "goal-1", OrganizationID: "org-1", MissionID: "mission-1", Objective: "verified output", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "verified output", Origin: "USER"}}, Status: core.GoalAchieved, CreatedAt: now}}
		}, "authoritative GOAL_ACHIEVED transition is missing"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stream := historyTestEvents(t, core.WorkActive, "")
			graph, err := ValidateProjectionHistory(stream, nil, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(&graph)
			err = ValidateProjectionCompletions(graph, stream, nil)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v; want %q", err, tc.want)
			}
		})
	}
}

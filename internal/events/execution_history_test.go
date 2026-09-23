package events

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func executionHistoryFixture(t *testing.T, extra int) ([]Event, Event, core.Task, core.Work, core.Intent) {
	t.Helper()
	now := time.Unix(10, 0).UTC()
	mission := core.Mission{ID: "mission-1", OrganizationID: "org-1", Statement: "direction", Status: core.MissionActive, CreatedAt: now}
	goal := core.Goal{ID: "goal-1", OrganizationID: "org-1", MissionID: mission.ID, Objective: "outcome", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "evidence", Origin: "USER"}}, Status: core.GoalActive, CreatedAt: now}
	intent := core.Intent{ID: "intent-run-1", OrganizationID: "org-1", AcceptedFingerprint: "accepted", CreatedAt: now}
	work := core.Work{ID: "work-1", IntentID: intent.ID, GoalID: goal.ID, Objective: "echo hello", Status: core.WorkActive, CreatedAt: now}
	refs := []string{"event-1", "event-2"}
	versions := []core.VersionedRef{{ID: "mission/mission-1", Version: "1", MaterializationState: core.MaterializedFull}, {ID: "goal/goal-1", Version: "1", MaterializationState: core.MaterializedFull}}
	plan := core.Plan{ID: "plan-run-1", IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, Version: 1, StrategicEventRefs: refs, StrategicContextRefs: versions, Tasks: []core.PlanTask{{Key: "root", Description: "echo hello", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden}}, CreatedAt: now}
	plan.Fingerprint, _ = core.FingerprintPlan(plan)
	body, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	stream := []Event{strategicProjectionEvent(t, 1, "MISSION_CREATED", "mission", mission.ID, 1, mission), strategicProjectionEvent(t, 2, "GOAL_CREATED", "goal", goal.ID, 1, goal), {EventID: "plan-event", Sequence: 3, OrganizationID: "org-1", EventType: "PLAN_CREATED", SourceActorID: "runtime", TaskID: "task-run-1", Payload: body, CorrelationID: "run-1", CreatedAt: now, SchemaVersion: SchemaVersion}}
	for i := 0; i < extra; i++ {
		other := mission
		other.ID = core.ID(fmt.Sprintf("other-%d", i))
		stream = append(stream, strategicProjectionEvent(t, int64(4+i), "MISSION_CREATED", "mission", other.ID, 1, other))
	}
	task := core.Task{ID: "task-run-1", WorkID: work.ID, Description: "echo hello", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, Status: core.TaskRunning}
	start := nonAgentStrategicExecutionStartEvent(t, int64(4+extra), task, refs, versions)
	return append(stream, start), start, task, work, intent
}

func TestExecutionHistoryReusesSnapshot(t *testing.T) {
	allocations := func(extra int) float64 {
		stream, start, task, work, intent := executionHistoryFixture(t, extra)
		history := newExecutionHistory(stream)
		if err := history.validate(start, task, 2, work, intent); err != nil {
			t.Fatal(err)
		}
		return testing.AllocsPerRun(3, func() {
			if err := history.validate(start, task, 2, work, intent); err != nil {
				t.Fatal(err)
			}
		})
	}
	small, large := allocations(8), allocations(256)
	t.Logf("reused start allocations: 8 unrelated projections=%.0f, 256=%.0f", small, large)
	if large > small*2 {
		t.Fatalf("reused execution validation allocations grow with unrelated history: small=%.0f large=%.0f", small, large)
	}
}

func TestExecutionStartOneOffSparseHistory(t *testing.T) {
	allocations := func(extra int) float64 {
		stream, start, task, work, intent := executionHistoryFixture(t, extra)
		work.GoalID = ""
		var plan core.Plan
		if err := json.Unmarshal(stream[2].Payload, &plan); err != nil {
			t.Fatal(err)
		}
		plan.StrategicEventRefs, plan.StrategicContextRefs = nil, nil
		plan.Fingerprint, _ = core.FingerprintPlan(plan)
		stream[2].Payload, _ = json.Marshal(plan)
		start = nonAgentStrategicExecutionStartEvent(t, start.Sequence, task, nil, nil)
		stream[len(stream)-1] = start
		return testing.AllocsPerRun(3, func() {
			if err := ValidateTaskExecutionStart(start, task, 2, work, intent, stream); err != nil {
				t.Fatal(err)
			}
		})
	}
	small, large := allocations(8), allocations(256)
	t.Logf("one-off ad hoc start allocations: 8 unrelated projections=%.0f, 256=%.0f", small, large)
	if large > small*2 {
		t.Fatalf("one-off sparse validation unnecessarily decodes unrelated projections: small=%.0f large=%.0f", small, large)
	}
}

func TestExecutionHistoryReusesLargePlan(t *testing.T) {
	allocations := func(count int) float64 {
		stream, _, task, work, intent := executionHistoryFixture(t, 0)
		stream = stream[:3]
		var plan core.Plan
		if err := json.Unmarshal(stream[2].Payload, &plan); err != nil {
			t.Fatal(err)
		}
		plan.Tasks = nil
		starts := make([]Event, count)
		tasks := make([]core.Task, count)
		for i := range count {
			key := fmt.Sprintf("task-%d", i)
			plan.Tasks = append(plan.Tasks, core.PlanTask{Key: key, Description: "echo hello", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden})
			tasks[i] = task
			tasks[i].ID = core.ID(key)
			starts[i] = nonAgentStrategicExecutionStartEvent(t, int64(i+4), tasks[i], plan.StrategicEventRefs, plan.StrategicContextRefs)
			// Event IDs participate in projection sealing.
			payload, _, err := AdmittedProjection(starts[i])
			if err != nil {
				t.Fatal(err)
			}
			starts[i].EventID = fmt.Sprintf("start-%d", i)
			sealed, err := SealProjectionEvent(starts[i], payload.Projection, payload.Detail)
			if err != nil {
				t.Fatal(err)
			}
			starts[i].Payload, _ = json.Marshal(sealed)
		}
		plan.Fingerprint, _ = core.FingerprintPlan(plan)
		stream[2].Payload, _ = json.Marshal(plan)
		stream = append(stream, starts...)
		history := newExecutionHistory(stream)
		if err := history.validate(starts[0], tasks[0], 2, work, intent); err != nil {
			t.Fatal(err)
		}
		return testing.AllocsPerRun(2, func() {
			for i, start := range starts {
				if err := history.validate(start, tasks[i], 2, work, intent); err != nil {
					t.Fatal(err)
				}
			}
		}) / float64(count)
	}
	small, large := allocations(8), allocations(128)
	t.Logf("allocations per start: 8-task Plan=%.0f, 128-task Plan=%.0f", small, large)
	if large > small*2 {
		t.Fatalf("per-start allocations grow with already validated Plan size: small=%.0f large=%.0f", small, large)
	}
}

func TestExecutionHistoryPlanEvidence(t *testing.T) {
	for _, mutation := range []string{"none", "duplicate", "foreign-duplicate", "future", "malformed", "changed-intent"} {
		t.Run(mutation, func(t *testing.T) {
			stream, start, task, work, intent := executionHistoryFixture(t, 0)
			switch mutation {
			case "duplicate", "foreign-duplicate":
				other := stream[2]
				other.EventID = "other-plan"
				other.Sequence = start.Sequence + 1
				if mutation == "foreign-duplicate" {
					other.OrganizationID = "other"
				}
				stream = append(stream, other)
			case "future":
				stream[2].Sequence = start.Sequence + 1
			case "malformed":
				stream[2].Payload = []byte(`{`)
			}
			history := newExecutionHistory(stream)
			if mutation == "changed-intent" {
				if err := history.validate(start, task, 2, work, intent); err != nil {
					t.Fatal(err)
				}
				intent.AcceptedFingerprint = "changed"
			}
			err := history.validate(start, task, 2, work, intent)
			if (err == nil) != (mutation == "none") {
				t.Fatalf("mutation=%s error=%v", mutation, err)
			}
		})
	}
}

func TestExecutionHistoryRetryNegativeEvidence(t *testing.T) {
	for _, mutation := range []string{"none", "reserved", "usage", "duplicate-context", "duplicate-proof"} {
		t.Run(mutation, func(t *testing.T) {
			stream, _, _, work, intent := executionHistoryFixture(t, 0)
			planEvent := stream[2]
			proof := modelStopHistoryFixture(t)
			proof = append(proof[:2], proof[4])
			for i := range proof {
				proof[i].OrganizationID = "org-1"
				proof[i].CorrelationID = "run-1"
				proof[i].TaskID = "task-run-1"
				proof[i].SourceExecutionID = "planning-plan-run-1-attempt-1"
				proof[i].Sequence = int64(i + 3)
			}
			context := PlanningContextPayload{PlanID: "plan-run-1", IntentID: string(intent.ID), IntentFingerprint: intent.AcceptedFingerprint, PromptVersion: "v1", Provider: "provider", Model: "model", ExecutionProfileVersion: "v1", InputEventRefs: []string{"input"}}
			proof[0].Payload, _ = json.Marshal(context)
			proof[2].Payload, _ = json.Marshal(ModelStopResult{StopRequestRef: "request", LocalState: "NOT_STARTED"})
			second := proof[0]
			second.EventID = "second"
			second.Sequence = 6
			second.SourceExecutionID = "planning-plan-run-1-attempt-2"
			planEvent.Sequence = 7
			planEvent.SourceExecutionID = second.SourceExecutionID
			stream = append(stream[:2], proof...)
			stream = append(stream, second, planEvent)
			switch mutation {
			case "reserved", "usage":
				negative := proof[0]
				negative.EventID = "negative"
				negative.Sequence = 8
				negative.EventType = "INFERENCE_RESERVED"
				if mutation == "usage" {
					negative.EventType = "INFERENCE_USAGE_RECORDED"
				}
				negative.Payload = []byte(`{}`)
				stream = append(stream, negative)
			case "duplicate-context", "duplicate-proof":
				duplicate := proof[0]
				if mutation == "duplicate-proof" {
					duplicate = proof[2]
				}
				duplicate.CorrelationID = "other"
				duplicate.Sequence = 8
				stream = append(stream, duplicate)
			}
			_, _, fullErr := resolvePlan("org-1", "run-1", work, intent, stream)
			_, _, indexedErr := newExecutionHistory(stream).resolvePlan("org-1", "run-1", work, intent)
			want := mutation == "none"
			if (fullErr == nil) != want || (indexedErr == nil) != want {
				t.Fatalf("want=%t full=%v indexed=%v", want, fullErr, indexedErr)
			}
		})
	}
}

func TestExecutionHistoryDispatchNegativeEvidence(t *testing.T) {
	for _, mutation := range []string{"none", "duplicate", "superseded", "foreign-superseded", "malformed-between", "malformed-after"} {
		t.Run(mutation, func(t *testing.T) {
			agent := core.Agent{ID: "agent-1", OrganizationID: "org-1", Status: "ACTIVE", BlueprintID: "blueprint", BlueprintVersion: "1", ExecutionProfileID: "profile", ExecutionProfileVersion: "1", RuntimeAdapter: "runtime"}
			first := strategicProjectionEvent(t, 1, "AGENT_CREATED", "agent", "agent-1", 1, agent)
			start := Event{OrganizationID: "org-1", Sequence: 4}
			stream := []Event{first}
			switch mutation {
			case "duplicate":
				stream = append(stream, first)
			case "superseded", "foreign-superseded":
				next := strategicProjectionEvent(t, 2, "AGENT_CONFIGURATION_UPDATED", "agent", "agent-1", 2, agent)
				if mutation == "foreign-superseded" {
					payload, _, err := AdmittedProjection(next)
					if err != nil {
						t.Fatal(err)
					}
					agent.OrganizationID = "other"
					payload.Projection.Value, _ = json.Marshal(agent)
					next.OrganizationID = "other"
					sealed, err := SealProjectionEvent(next, payload.Projection, payload.Detail)
					if err != nil {
						t.Fatal(err)
					}
					next.Payload, _ = json.Marshal(sealed)
				}
				stream = append(stream, next)
			case "malformed-between", "malformed-after":
				sequence := int64(2)
				if mutation == "malformed-after" {
					sequence = 5
				}
				stream = append(stream, Event{EventID: "bad", Sequence: sequence, EventType: "TASK_CREATED", Payload: []byte(`{`)})
			}
			_, fullErr := dispatchProjectionRevision(start, first.EventID, "agent", "agent-1", 1, stream)
			history := newExecutionHistory(stream)
			scoped := history.dispatchStream(start, &AgentDispatchBinding{AgentID: "agent-1", AgentEventRef: first.EventID})
			_, indexedErr := dispatchProjectionRevision(start, first.EventID, "agent", "agent-1", 1, scoped)
			want := mutation == "none" || mutation == "malformed-after"
			if (fullErr == nil) != want || (indexedErr == nil) != want {
				t.Fatalf("want=%t full=%v indexed=%v", want, fullErr, indexedErr)
			}
		})
	}
}

func TestExecutionHistoryStrategyEvidence(t *testing.T) {
	for _, mutation := range []string{"none", "malformed-before", "malformed-after", "invalid-earlier", "later-revision", "foreign-revision"} {
		t.Run(mutation, func(t *testing.T) {
			stream, _, _, work, _ := executionHistoryFixture(t, 0)
			stream = stream[:3]
			before := int64(6)
			switch mutation {
			case "malformed-before", "malformed-after":
				sequence := int64(4)
				if mutation == "malformed-after" {
					sequence = 7
				}
				stream = append(stream, Event{EventID: "bad", Sequence: sequence, EventType: "TASK_CREATED", Payload: []byte(`{`)})
			case "invalid-earlier", "later-revision", "foreign-revision":
				_, goal, err := exactGoalProjection("org-1", stream[1])
				if err != nil {
					t.Fatal(err)
				}
				if mutation == "invalid-earlier" {
					bad := goal
					bad.Objective = ""
					stream = append(stream, strategicProjectionEvent(t, 4, "GOAL_REFINED", "goal", goal.ID, 2, bad))
				}
				if mutation == "foreign-revision" {
					goal.OrganizationID = "other"
				}
				goal.Objective = "updated"
				next := strategicProjectionEvent(t, 5, "GOAL_REFINED", "goal", goal.ID, 3, goal)
				if mutation == "foreign-revision" {
					payload, _, err := AdmittedProjection(next)
					if err != nil {
						t.Fatal(err)
					}
					next.OrganizationID = "other"
					sealed, err := SealProjectionEvent(next, payload.Projection, payload.Detail)
					if err != nil {
						t.Fatal(err)
					}
					next.Payload, _ = json.Marshal(sealed)
				}
				stream = append(stream, next)
			}
			history := newExecutionHistory(stream)
			full, refs, versions, fullErr := ResolveStrategicContext("org-1", work, stream, before)
			indexed, selectedRefs, selectedVersions, indexedErr := ResolveStrategicContext("org-1", work, history.strategyStream("org-1", work, before, []string{"event-1", "event-2"}), before)
			want := mutation != "malformed-before" && mutation != "invalid-earlier"
			if (fullErr == nil) != want || (indexedErr == nil) != want {
				t.Fatalf("want=%t full=%v indexed=%v", want, fullErr, indexedErr)
			}
			if want && (!reflect.DeepEqual(full, indexed) || !reflect.DeepEqual(refs, selectedRefs) || !reflect.DeepEqual(versions, selectedVersions)) {
				t.Fatal("indexed strategy differs from full retained history")
			}
		})
	}
}

func TestExecutionHistoryHumanInput(t *testing.T) {
	for _, mode := range []string{"OPERATOR_HUMAN_INPUT", "STRUCTURED_HUMAN_COMPLETION"} {
		for _, mutation := range []string{"none", "missing", "future", "organization", "task", "correlation", "malformed"} {
			t.Run(mode+"/"+mutation, func(t *testing.T) {
				stream, _, task, work, intent := executionHistoryFixture(t, 0)
				stream = stream[:3]
				task.ExecutionKind = core.ExecutionHuman
				var plan core.Plan
				if err := json.Unmarshal(stream[2].Payload, &plan); err != nil {
					t.Fatal(err)
				}
				plan.Tasks[0].ExecutionKind = core.ExecutionHuman
				plan.Fingerprint, _ = core.FingerprintPlan(plan)
				stream[2].Payload, _ = json.Marshal(plan)
				input := Event{EventID: "input", Sequence: 4, OrganizationID: "org-1", TaskID: string(task.ID), CorrelationID: "run-1", SourceActorID: "owner", SchemaVersion: SchemaVersion, CreatedAt: time.Unix(4, 0).UTC()}
				if mode == "OPERATOR_HUMAN_INPUT" {
					input.EventType = "HUMAN_INPUT_RECEIVED"
					input.Payload, _ = json.Marshal(OperatorInputReceivedPayload{MessageID: "message", Text: "done", SourcePrincipalID: "owner", SourcePrincipalKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT"})
				} else {
					input.EventType = "HUMAN_TASK_COMPLETION_SUBMITTED"
					input.Payload, _ = json.Marshal(HumanTaskCompletionSubmittedPayload{MessageID: "message", Fields: map[string]string{"answer": "done"}, SourcePrincipalID: "owner", SourceChannel: "HUMAN_DIRECT"})
				}
				start := nonAgentStrategicExecutionStartEvent(t, 6, task, plan.StrategicEventRefs, plan.StrategicContextRefs)
				payload, _, err := AdmittedProjection(start)
				if err != nil {
					t.Fatal(err)
				}
				detail, _ := json.Marshal(ExecutionStartDetail{Mode: mode, InputEventRef: input.EventID, StrategicEventRefs: plan.StrategicEventRefs, StrategicContextRefs: plan.StrategicContextRefs})
				sealed, err := SealProjectionEvent(start, payload.Projection, detail)
				if err != nil {
					t.Fatal(err)
				}
				start.Payload, _ = json.Marshal(sealed)
				switch mutation {
				case "future":
					input.Sequence = 7
				case "organization":
					input.OrganizationID = "other"
				case "task":
					input.TaskID = "other"
				case "correlation":
					input.CorrelationID = "other"
				case "malformed":
					input.Payload = []byte(`{}`)
				}
				if mutation != "missing" {
					stream = append(stream, input)
				}
				stream = append(stream, start)
				if err := newExecutionHistory(stream).validate(start, task, 2, work, intent); (err == nil) != (mutation == "none") {
					t.Fatalf("mutation=%s err=%v", mutation, err)
				}
			})
		}
	}
}

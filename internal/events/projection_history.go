package events

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"

	"github.com/dominicnunez/agentos/internal/boundaryjson"
	"github.com/dominicnunez/agentos/internal/core"
)

// ValidateProjectionHistory validates projection admission and temporal history
// against its prior durable graph. The caller must supply complete supporting
// history, inbox observations and admitted authority records. It does not verify
// ledger integrity, stored projection coverage or final Work/Goal completion
// admissions; those remain required at the caller's durable read boundary.
// It returns the admitted graph only after every selected event is valid.
func ValidateProjectionHistory(stream []Event, inboxObservations map[string]InboxObservationBinding, leaseAdmissions []CapabilityLeaseAdmission, freezeAdmissions []OrganizationFreezeAdmission) (core.DurableGraph, error) {
	if err := ValidateModelStops(stream, freezeAdmissions); err != nil {
		return core.DurableGraph{}, err
	}
	if err := ValidateExecutionStops(stream, freezeAdmissions); err != nil {
		return core.DurableGraph{}, err
	}
	if err := ValidateSecurityHoldOutcomes(stream, freezeAdmissions); err != nil {
		return core.DurableGraph{}, err
	}
	eventIDs := make(map[string]struct{}, len(stream))
	eventIndex := make(map[string]Event, len(stream))
	sequences := make(map[int64]struct{}, len(stream))
	ordered := append([]Event(nil), stream...)
	sort.Slice(ordered, func(left, right int) bool { return ordered[left].Sequence < ordered[right].Sequence })
	reviewEvidence := IndexReviewedIntentEvidence(ordered)
	startHistory := newExecutionHistory(ordered)
	tasks := make(map[core.ID]core.DurableState[core.Task])
	agents := make(map[core.ID]core.DurableState[core.Agent])
	missions := make(map[core.ID]core.DurableState[core.Mission])
	goals := make(map[core.ID]core.DurableState[core.Goal])
	works := make(map[core.ID]core.DurableState[core.Work])
	experiments := make(map[core.ID]core.DurableState[core.Experiment])
	knowledgeAdmissions := NewKnowledgeAdmissionValidator(ordered)
	knowledgeAdmissions.UseCapabilityLeaseAdmissions(leaseAdmissions)
	knowledgeAdmissions.UseOrganizationFreezeAdmissions(freezeAdmissions)
	graph := core.DurableGraph{
		Organizations: map[core.ID]core.DurableState[core.Organization]{}, Missions: map[core.ID]core.DurableState[core.Mission]{},
		Goals: map[core.ID]core.DurableState[core.Goal]{}, Teams: map[core.ID]core.DurableState[core.Team]{},
		AgentBlueprints: map[core.ID]core.DurableState[core.AgentBlueprint]{}, ExecutionProfiles: map[core.ID]core.DurableState[core.ExecutionProfile]{},
		Agents: map[core.ID]core.DurableState[core.Agent]{}, Intents: map[core.ID]core.DurableState[core.Intent]{},
		Works: map[core.ID]core.DurableState[core.Work]{}, Tasks: map[core.ID]core.DurableState[core.Task]{},
		Experiments: map[core.ID]core.DurableState[core.Experiment]{}, PromotionCandidates: map[core.ID]core.DurableState[core.PromotionCandidate]{},
		Knowledge: map[core.ID]core.DurableState[core.KnowledgeRecord]{},
	}
	confirmations := make(map[string][]Event)
	for _, event := range ordered {
		if event.EventType == "INTENT_CONFIRMED" {
			confirmations[event.CorrelationID] = append(confirmations[event.CorrelationID], event)
		}
	}
	replacementConfirmations := make(map[core.ID]string)
	teamRecords := make(map[string][][]byte)
	blueprintRevisions := make(map[core.ID]map[string]core.AgentBlueprint)
	profileRevisions := make(map[core.ID]map[string]core.ExecutionProfile)
	executionStarts := make(map[string]Event)
	for _, event := range ordered {
		if event.EventID == "" || event.Sequence < 1 || event.CreatedAt.IsZero() {
			return core.DurableGraph{}, fmt.Errorf("event stream contains an incomplete envelope")
		}
		if event.SchemaVersion != SchemaVersion {
			return core.DurableGraph{}, fmt.Errorf("event %s uses unsupported schema version %d", event.EventID, event.SchemaVersion)
		}
		if _, duplicate := eventIDs[event.EventID]; duplicate {
			return core.DurableGraph{}, fmt.Errorf("event stream contains duplicate event id %s", event.EventID)
		}
		if _, duplicate := sequences[event.Sequence]; duplicate {
			return core.DurableGraph{}, fmt.Errorf("event stream contains duplicate sequence %d at %s", event.Sequence, event.EventType)
		}
		eventIDs[event.EventID] = struct{}{}
		eventIndex[event.EventID] = event
		sequences[event.Sequence] = struct{}{}
		payload, present, err := AdmittedProjection(event)
		if err != nil {
			return core.DurableGraph{}, fmt.Errorf("event %s: %w", event.EventID, err)
		}
		if !present {
			if event.EventType == "EVIDENCE_PUBLISHED" {
				task := graph.Tasks[core.ID(event.TaskID)]
				start := executionStarts[fmt.Sprintf("execution-%s-v%d", task.Value.ID, task.Version)]
				if err := validateAgentEvidenceBinding(event, task.Value, task.Version, start); err != nil {
					return core.DurableGraph{}, fmt.Errorf("event %s: %w", event.EventID, err)
				}
			}
			if event.EventType == "INTAKE_ABANDONED" {
				if err := ValidateIndexedIntakeAbandonment(reviewEvidence.At(event), event); err != nil {
					return core.DurableGraph{}, fmt.Errorf("event %s: %w", event.EventID, err)
				}
			}
			if event.EventType == "INTENT_CONFIRMED" {
				var confirmation IntentConfirmedPayload
				if boundaryjson.Unmarshal(event.Payload, &confirmation) != nil {
					return core.DurableGraph{}, fmt.Errorf("event %s contains an invalid intent confirmation", event.EventID)
				}
				if confirmation.GoalID == "" {
					if err := ValidateIndexedReviewedIntentAdmission(reviewEvidence.At(event), event); err != nil {
						return core.DurableGraph{}, fmt.Errorf("event %s: %w", event.EventID, err)
					}
				} else {
					goal, found := graph.Goals[core.ID(confirmation.GoalID)]
					if !found || goal.Value.ID != core.ID(confirmation.GoalID) || goal.Value.Status != core.GoalActive {
						return core.DurableGraph{}, fmt.Errorf("event %s Goal-bound intent confirmation lacks its active Goal", event.EventID)
					}
					if err := ValidateIndexedReviewedGoalIntentAdmission(reviewEvidence.At(event), event, goal.Value); err != nil {
						return core.DurableGraph{}, fmt.Errorf("event %s: %w", event.EventID, err)
					}
				}
				if err := validateProjectionReplacementConfirmation(event, confirmation, graph, replacementConfirmations); err != nil {
					return core.DurableGraph{}, fmt.Errorf("event %s: %w", event.EventID, err)
				}
				continue
			}
			if RequiresProjectionAdmission(event.EventType, event.SourceActorID) {
				return core.DurableGraph{}, fmt.Errorf("event %s uses a projection lifecycle event without typed admission", event.EventID)
			}
			continue
		}
		if err := ValidateProjectionEventBoundary(event, payload); err != nil {
			return core.DurableGraph{}, fmt.Errorf("event %s: %w", event.EventID, err)
		}
		record := payload.Projection
		switch record.ProjectionKind {
		case "organization":
			var value core.Organization
			if boundaryjson.Unmarshal(record.Value, &value) != nil || value.ID != core.ID(record.RecordID) || string(value.ID) != event.OrganizationID {
				err = fmt.Errorf("contains an invalid Organization projection")
			} else {
				err = core.AdmitDurableRevision(graph.Organizations, value.ID, record.Version, record.CorrelationID, value, false, nil)
			}
		case "mission":
			var value core.Mission
			if boundaryjson.Unmarshal(record.Value, &value) != nil {
				err = fmt.Errorf("contains an invalid Mission projection")
			} else {
				err = validateOrganizedProjectionLifecycle(value, event, record, graph, missions, "Mission", func(value core.Mission) core.ID { return value.ID }, func(value core.Mission) core.ID { return value.OrganizationID }, ValidateMissionProjectionTransition)
			}
			if err == nil {
				err = core.AdmitDurableRevision(graph.Missions, value.ID, record.Version, record.CorrelationID, value, false, core.ValidMissionRevision)
			}
		case "goal":
			var value core.Goal
			if boundaryjson.Unmarshal(record.Value, &value) != nil {
				err = fmt.Errorf("contains an invalid Goal projection")
			} else {
				err = validateOrganizedProjectionLifecycle(value, event, record, graph, goals, "Goal", func(value core.Goal) core.ID { return value.ID }, func(value core.Goal) core.ID { return value.OrganizationID }, ValidateGoalProjectionTransition)
			}
			if err == nil {
				mission, found := graph.Missions[value.MissionID]
				if !found || mission.Value.ID != value.MissionID || mission.Value.OrganizationID != value.OrganizationID {
					err = fmt.Errorf("goal requires its durable same-organization Mission")
				}
			}
			if err == nil {
				err = core.AdmitDurableRevision(graph.Goals, value.ID, record.Version, record.CorrelationID, value, false, core.ValidGoalRevision)
			}
		case "agent_blueprint":
			err = admitVersionedOrganizedProjection(record, event, graph, graph.AgentBlueprints, blueprintRevisions, "Agent blueprint", func(value core.AgentBlueprint) core.ID { return value.ID }, func(value core.AgentBlueprint) core.ID { return value.OrganizationID }, func(value core.AgentBlueprint) string { return value.Version }, core.ValidAgentBlueprint, core.ValidAgentBlueprintRevision)
		case "execution_profile":
			err = admitVersionedOrganizedProjection(record, event, graph, graph.ExecutionProfiles, profileRevisions, "execution profile", func(value core.ExecutionProfile) core.ID { return value.ID }, func(value core.ExecutionProfile) core.ID { return value.OrganizationID }, func(value core.ExecutionProfile) string { return value.Version }, core.ValidExecutionProfile, core.ValidExecutionProfileRevision)
		case "agent":
			var value core.Agent
			if boundaryjson.Unmarshal(record.Value, &value) != nil {
				err = fmt.Errorf("contains an invalid Agent projection")
			} else {
				err = validateProjectionEventLifecycle(event, record, "Agent", agents, func(value core.Agent) core.ID { return value.ID }, false, ValidateAgentProjectionTransition)
			}
			if err == nil {
				err = validateProjectionOrganizationAtAdmission(value.OrganizationID, event, graph)
			}
			if err == nil {
				blueprint, blueprintFound := graph.AgentBlueprints[value.BlueprintID]
				profile, profileFound := graph.ExecutionProfiles[value.ExecutionProfileID]
				if !blueprintFound || !profileFound || !core.ValidAgentConfigurationBinding(value, blueprint.Value, profile.Value) {
					err = fmt.Errorf("agent references invalid pinned configuration at admission")
				}
			}
			if err == nil {
				err = core.AdmitDurableRevision(graph.Agents, value.ID, record.Version, record.CorrelationID, value, false, core.ValidAgentRevision)
			}
		case "team":
			var value core.Team
			if boundaryjson.Unmarshal(record.Value, &value) != nil || value.ID != core.ID(record.RecordID) {
				err = fmt.Errorf("contains an invalid Team projection")
			} else {
				err = validateProjectionTeamAtAdmission(value, event, graph)
			}
			if err == nil {
				err = core.AdmitDurableRevision(graph.Teams, value.ID, record.Version, record.CorrelationID, value, false, core.ValidTeamRevision)
			}
			if err == nil {
				body, marshalErr := json.Marshal(record)
				if marshalErr != nil {
					err = marshalErr
				} else {
					teamRecords[event.OrganizationID] = append(teamRecords[event.OrganizationID], body)
				}
			}
		case "intent":
			var value core.Intent
			if boundaryjson.Unmarshal(record.Value, &value) != nil || value.ID != core.ID(record.RecordID) {
				err = fmt.Errorf("contains an invalid Intent projection")
			} else {
				err = validateProjectionOrganizationAtAdmission(value.OrganizationID, event, graph)
			}
			if err == nil {
				err = core.AdmitDurableRevision(graph.Intents, value.ID, record.Version, record.CorrelationID, value, true, nil)
			}
		case "work":
			err = admitLifecycleProjection(event, record, "Work", works, graph.Works, func(value core.Work) core.ID { return value.ID }, ValidateWorkProjectionTransition, func(value core.Work) error {
				return validateProjectionWorkAtAdmission(value, event, record, graph, confirmations, reviewEvidence)
			}, core.ValidWorkRevision)
		case "task":
			var value core.Task
			if boundaryjson.Unmarshal(record.Value, &value) != nil {
				err = fmt.Errorf("contains an invalid Task projection")
			} else {
				err = validateProjectionEventLifecycle(event, record, "Task", tasks, func(value core.Task) core.ID { return value.ID }, true, ValidateTaskProjectionTransition)
			}
			if err == nil {
				err = validateProjectionTaskAtAdmission(value, event, record, graph, startHistory)
			}
			if err == nil && event.EventType == "TASK_VERIFIED_COMPLETE" {
				err = validateTaskCompletionAtAdmission(value, event, record, graph, ordered, teamRecords[event.OrganizationID], inboxObservations, blueprintRevisions, profileRevisions, startHistory)
			}
			if err == nil {
				err = core.AdmitDurableRevision(graph.Tasks, value.ID, record.Version, record.CorrelationID, value, true, core.ValidTaskRevision)
			}
			if err == nil && event.EventType == "EXECUTION_STARTED" {
				// The full dispatch was validated above. Evidence checks reuse
				// that exact revision without rescanning configuration history.
				executionStarts[fmt.Sprintf("execution-%s-v%d", value.ID, record.Version)] = event
			}
		case "lab_experiment":
			err = admitLifecycleProjection(event, record, "Lab experiment", experiments, graph.Experiments, func(value core.Experiment) core.ID { return value.ID }, ValidateExperimentProjectionTransition, func(value core.Experiment) error {
				return validateLabExperimentAtAdmission(value, event, record, graph, ordered)
			}, core.ValidExperimentRevision)
		case "lab_promotion_candidate":
			var value core.PromotionCandidate
			if boundaryjson.Unmarshal(record.Value, &value) != nil || ValidatePromotionCandidateProjectionTarget(event.EventType, record.Version, value) != nil {
				err = fmt.Errorf("contains an invalid Lab promotion-candidate projection")
			} else {
				err = validateLabPromotionCandidateAtAdmission(value, event, record, graph, ordered)
			}
			if err == nil {
				err = core.AdmitDurableRevision(graph.PromotionCandidates, value.ID, record.Version, record.CorrelationID, value, true, nil)
			}
		case "knowledge":
			var value core.KnowledgeRecord
			if boundaryjson.Unmarshal(record.Value, &value) != nil || value.KnowledgeID != core.ID(record.RecordID) ||
				!core.ValidKnowledgeRecord(value) || value.Version != record.Version {
				err = fmt.Errorf("contains an invalid knowledge projection")
			} else {
				err = knowledgeAdmissions.Validate(value, event, record, graph)
			}
			if err == nil {
				err = core.AdmitDurableRevision(graph.Knowledge, value.KnowledgeID, record.Version, record.CorrelationID, value, true, core.ValidKnowledgeRevision)
			}
		default:
			err = fmt.Errorf("contains unsupported projection kind %s", record.ProjectionKind)
		}
		if err != nil {
			return core.DurableGraph{}, fmt.Errorf("event %s: %w", event.EventID, err)
		}
	}
	return graph, nil
}

func validateProjectionOrganizationAtAdmission(organizationID core.ID, event Event, graph core.DurableGraph) error {
	organization, found := graph.Organizations[organizationID]
	if organizationID == "" || !found || organization.Value.ID != organizationID || event.OrganizationID != string(organizationID) {
		return fmt.Errorf("requires its durable parent Organization at admission")
	}
	return nil
}

func validateProjectionTeamAtAdmission(team core.Team, event Event, graph core.DurableGraph) error {
	if err := validateProjectionOrganizationAtAdmission(team.OrganizationID, event, graph); err != nil {
		return err
	}
	return core.ValidateTeamRoster(team, graph)
}

func validateProjectionWorkAtAdmission(work core.Work, event Event, record ProjectionRecord, graph core.DurableGraph, confirmations map[string][]Event, reviewEvidence ReviewedIntentEvidenceIndex) error {
	intent, found := graph.Intents[work.IntentID]
	if !found || intent.CorrelationID != record.CorrelationID || intent.Value.ID != work.IntentID || intent.Value.GoalID != work.GoalID || intent.Value.NormalizedObjective != work.Objective || string(intent.Value.OrganizationID) != event.OrganizationID {
		return fmt.Errorf("work requires its exact prior Intent on the same organization and correlation boundary")
	}
	if intent.Value.ReplacesWorkID != work.ReplacesWorkID {
		return fmt.Errorf("work does not match its accepted Intent replacement lineage")
	}
	if IntentRequiresConfirmation(intent.Value) {
		matching := confirmations[record.CorrelationID]
		if len(matching) != 1 || matching[0].Sequence >= event.Sequence {
			return fmt.Errorf("external Work requires one prior reviewed intent confirmation")
		}
		if err := ValidateIntentConfirmation(reviewEvidence.At(matching[0]), matching[0], intent.Value); err != nil {
			return err
		}
	}
	if predecessorID := work.ReplacesWorkID; predecessorID != "" {
		predecessor, found := graph.Works[predecessorID]
		if !found || predecessor.Value.Status != core.WorkFailed || predecessor.Value.GoalID != work.GoalID {
			return fmt.Errorf("replacement Work requires its prior failed Work with the same Goal binding")
		}
		predecessorIntent, found := graph.Intents[predecessor.Value.IntentID]
		if !found || predecessorIntent.Value.OrganizationID != intent.Value.OrganizationID {
			return fmt.Errorf("replacement Work crosses its organization boundary")
		}
		for existingID, existing := range graph.Works {
			if existingID != work.ID && existing.Value.ReplacesWorkID == predecessorID {
				return fmt.Errorf("failed Work already has a replacement")
			}
		}
	}
	return nil
}

func validateProjectionReplacementConfirmation(event Event, confirmation IntentConfirmedPayload, graph core.DurableGraph, replacements map[core.ID]string) error {
	predecessorID := core.ID(confirmation.ReplacesWorkID)
	if predecessorID == "" {
		return nil
	}
	predecessor, found := graph.Works[predecessorID]
	if !found || predecessor.Value.Status != core.WorkFailed || predecessor.Value.GoalID != core.ID(confirmation.GoalID) {
		return fmt.Errorf("reviewed replacement requires a prior failed Work with the same Goal binding")
	}
	predecessorIntent, found := graph.Intents[predecessor.Value.IntentID]
	if !found || string(predecessorIntent.Value.OrganizationID) != event.OrganizationID {
		return fmt.Errorf("reviewed replacement Work crosses its organization boundary")
	}
	if correlationID, duplicate := replacements[predecessorID]; duplicate && correlationID != event.CorrelationID {
		return fmt.Errorf("failed Work has multiple reviewed replacements")
	}
	replacements[predecessorID] = event.CorrelationID
	return nil
}

func IntentRequiresConfirmation(intent core.Intent) bool {
	return intent.GoalID != "" || intent.ReplacesWorkID != "" || intent.SourceChannel == "HUMAN_DIRECT" || intent.SourceChannel == "A2A" || intent.SourcePrincipalKind == core.PrincipalHuman || intent.SourcePrincipalKind == core.PrincipalExternalAgent
}

func validateProjectionTaskAtAdmission(task core.Task, event Event, record ProjectionRecord, graph core.DurableGraph, startHistory *executionHistory) error {
	work, found := graph.Works[task.WorkID]
	if !found || work.CorrelationID != record.CorrelationID || work.Value.ID != task.WorkID || work.Value.Status != core.WorkActive {
		return fmt.Errorf("task requires its exact active Work on the same correlation boundary")
	}
	intent, found := graph.Intents[work.Value.IntentID]
	if !found || intent.CorrelationID != record.CorrelationID || intent.Value.ID != work.Value.IntentID || string(intent.Value.OrganizationID) != event.OrganizationID {
		return fmt.Errorf("task requires its exact Intent organization and correlation boundary")
	}
	if err := core.ValidateTaskAssignment(task, intent.Value.OrganizationID, graph); err != nil {
		return err
	}
	if event.EventType == "EXECUTION_STARTED" {
		return startHistory.validate(event, task, record.Version, work.Value, intent.Value)
	}
	return nil
}

func validateOrganizedProjectionLifecycle[T any](value T, event Event, record ProjectionRecord, graph core.DurableGraph, history map[core.ID]core.DurableState[T], kind string, identity func(T) core.ID, organization func(T) core.ID, validate func(string, int, *T, T) error) error {
	if err := validateProjectionEventLifecycle(event, record, kind, history, identity, false, validate); err != nil {
		return err
	}
	return validateProjectionOrganizationAtAdmission(organization(value), event, graph)
}

func admitVersionedOrganizedProjection[T any](record ProjectionRecord, event Event, graph core.DurableGraph, target map[core.ID]core.DurableState[T], revisions map[core.ID]map[string]T, kind string, identity func(T) core.ID, organization func(T) core.ID, semanticVersion func(T) string, valid func(T) bool, validRevision func(T, T) bool) error {
	var value T
	if boundaryjson.Unmarshal(record.Value, &value) != nil || identity(value) != core.ID(record.RecordID) || !valid(value) {
		return fmt.Errorf("contains an invalid %s projection", kind)
	}
	if err := validateProjectionOrganizationAtAdmission(organization(value), event, graph); err != nil {
		return err
	}
	if err := core.AdmitDurableRevision(target, identity(value), record.Version, record.CorrelationID, value, false, validRevision); err != nil {
		return err
	}
	if revisions[identity(value)] == nil {
		revisions[identity(value)] = make(map[string]T)
	}
	revisions[identity(value)][semanticVersion(value)] = value
	return nil
}

func validateLabExperimentAtAdmission(experiment core.Experiment, event Event, record ProjectionRecord, graph core.DurableGraph, stream []Event) error {
	if experiment.ID != core.ID(record.RecordID) || experiment.OrganizationID != core.ID(event.OrganizationID) || record.CorrelationID != event.CorrelationID {
		return fmt.Errorf("lab experiment crosses its durable identity boundary")
	}
	work, found := graph.Works[experiment.WorkID]
	if !found || work.CorrelationID != event.CorrelationID {
		return fmt.Errorf("lab experiment requires its exact bounded Work")
	}
	intent, found := graph.Intents[work.Value.IntentID]
	if !found || intent.Value.OrganizationID != experiment.OrganizationID || experiment.Objective != work.Value.Objective {
		return fmt.Errorf("lab experiment crosses its Work organization or objective")
	}
	if intent.Value.ReplacesWorkID != "" || work.Value.ReplacesWorkID != "" {
		return fmt.Errorf("lab Work cannot carry production replacement lineage")
	}
	switch experiment.Status {
	case core.ExperimentRunning:
		if work.Value.Status != core.WorkActive {
			return fmt.Errorf("running Lab experiment requires active Work")
		}
	case core.ExperimentCompleted:
		if !core.ValidTerminalExperimentWorkStatus(experiment, work.Value) || len(experiment.ResultEventRefs) != 1 {
			return fmt.Errorf("completed Lab experiment requires one exact completed Work transition")
		}
		completion, found := priorEventByID(stream, event.Sequence, experiment.ResultEventRefs[0])
		if !found || completion.OrganizationID != event.OrganizationID || completion.CorrelationID != event.CorrelationID || completion.EventType != "WORK_COMPLETED" {
			return fmt.Errorf("completed Lab experiment result is not its exact Work completion")
		}
		payload, admitted, projectionErr := AdmittedProjection(completion)
		var detail WorkCompletionTransitionPayload
		if projectionErr != nil || !admitted || payload.Projection.ProjectionKind != "work" || payload.Projection.RecordID != string(experiment.WorkID) || boundaryjson.Unmarshal(payload.Detail, &detail) != nil || detail.EvidenceEventRef == "" {
			return fmt.Errorf("completed Lab experiment result lacks evidence-backed Work admission")
		}
		evidence, found := priorEventByID(stream, completion.Sequence, detail.EvidenceEventRef)
		if !found || evidence.EventType != "WORK_COMPLETION_EVALUATED" || evidence.OrganizationID != event.OrganizationID || evidence.CorrelationID != event.CorrelationID || !slices.Equal(experiment.ArtifactRefs, evidence.ArtifactRefs) {
			return fmt.Errorf("completed Lab experiment artifacts do not match Work evidence")
		}
	case core.ExperimentFailed:
		if !core.ValidTerminalExperimentWorkStatus(experiment, work.Value) {
			return fmt.Errorf("failed Lab experiment conflicts with its Work outcome")
		}
	}
	return nil
}

func validateLabPromotionCandidateAtAdmission(candidate core.PromotionCandidate, event Event, record ProjectionRecord, graph core.DurableGraph, stream []Event) error {
	if candidate.ID != core.ID(record.RecordID) || candidate.OrganizationID != core.ID(event.OrganizationID) || record.CorrelationID != event.CorrelationID {
		return fmt.Errorf("lab promotion candidate crosses its durable identity boundary")
	}
	experiment, found := graph.Experiments[candidate.ExperimentID]
	if !found || experiment.Value.Status != core.ExperimentCompleted || experiment.Value.OrganizationID != candidate.OrganizationID ||
		experiment.Version != candidate.ExperimentVersion || experiment.CorrelationID != event.CorrelationID ||
		!slices.Equal(experiment.Value.ResultEventRefs, candidate.ExperimentResultEventRefs) || candidate.CreatedAt.Before(*experiment.Value.FinishedAt) {
		return fmt.Errorf("lab promotion candidate lacks its exact completed experiment")
	}
	work := graph.Works[experiment.Value.WorkID]
	intent := graph.Intents[work.Value.IntentID]
	if candidate.NominatedBy != intent.Value.SourcePrincipalID {
		return fmt.Errorf("lab promotion candidate does not preserve its commissioning actor")
	}
	for _, ref := range candidate.ReproductionEvidenceRefs {
		reproduction, found := priorEventByID(stream, event.Sequence, ref)
		if !found || reproduction.OrganizationID != event.OrganizationID || reproduction.CorrelationID == event.CorrelationID || reproduction.EventType != "WORK_COMPLETED" {
			return fmt.Errorf("lab promotion candidate lacks independent same-organization reproduction evidence")
		}
		payload, admitted, err := AdmittedProjection(reproduction)
		if err != nil || !admitted || payload.Projection.ProjectionKind != "work" || payload.Projection.RecordID == string(experiment.Value.WorkID) {
			return fmt.Errorf("lab promotion reproduction is not a distinct admitted Work completion")
		}
	}
	return nil
}

func priorEventByID(stream []Event, beforeSequence int64, eventID string) (Event, bool) {
	for _, event := range stream {
		if event.EventID == eventID && event.Sequence < beforeSequence {
			return event, true
		}
	}
	return Event{}, false
}

func validateTaskCompletionAtAdmission(task core.Task, event Event, record ProjectionRecord, graph core.DurableGraph, stream []Event, teamRecords [][]byte, inboxObservations map[string]InboxObservationBinding, blueprintRevisions map[core.ID]map[string]core.AgentBlueprint, profileRevisions map[core.ID]map[string]core.ExecutionProfile, startHistory *executionHistory) error {
	work, found := graph.Works[task.WorkID]
	if !found {
		return fmt.Errorf("completed Task lacks its durable Work")
	}
	intent, found := graph.Intents[work.Value.IntentID]
	if !found {
		return fmt.Errorf("completed Task lacks its durable Intent")
	}
	tasks := make([]WorkCompletionTaskBinding, 0)
	blueprints := make(map[core.ID]core.AgentBlueprint)
	profiles := make(map[core.ID]core.ExecutionProfile)
	addTask := func(candidate core.Task, version int, correlationID string) error {
		if candidate.WorkID != task.WorkID {
			return nil
		}
		tasks = append(tasks, WorkCompletionTaskBinding{Task: candidate, Version: version, CorrelationID: correlationID})
		if candidate.ExecutionKind != core.ExecutionAgent || candidate.AgentConfig == nil {
			return nil
		}
		config := candidate.AgentConfig
		blueprint, blueprintFound := blueprintRevisions[config.BlueprintID][config.BlueprintVersion]
		profile, profileFound := profileRevisions[config.ProfileID][config.ProfileVersion]
		if !blueprintFound || !profileFound {
			return fmt.Errorf("completed Agent Task lacks its pinned configuration revision")
		}
		blueprints[config.BlueprintID] = blueprint
		profiles[config.ProfileID] = profile
		return nil
	}
	for taskID, state := range graph.Tasks {
		if taskID == task.ID {
			continue
		}
		if err := addTask(state.Value, state.Version, state.CorrelationID); err != nil {
			return err
		}
	}
	if err := addTask(task, record.Version, record.CorrelationID); err != nil {
		return err
	}
	teamRevisions, err := ResolveTeamRevisionBindings(event.OrganizationID, teamRecords, stream)
	if err != nil {
		return fmt.Errorf("resolve completed Task Team history: %w", err)
	}
	binding := WorkCompletionBinding{
		executionHistory: startHistory,
		OrganizationID:   event.OrganizationID, CorrelationID: record.CorrelationID,
		Work: work.Value, WorkVersion: work.Version, Intent: intent.Value, Tasks: tasks,
		TeamRevisions: teamRevisions, InboxObservations: inboxObservations, AgentBlueprints: blueprints, ExecutionProfiles: profiles,
	}
	_, err = ValidateTaskCompletionEvidenceChain(binding, WorkCompletionTaskBinding{Task: task, Version: record.Version, CorrelationID: record.CorrelationID}, event, stream)
	return err
}

func validateProjectionEventLifecycle[T any](event Event, record ProjectionRecord, kind string, history map[core.ID]core.DurableState[T], identity func(T) core.ID, correlationStable bool, validate func(string, int, *T, T) error) error {
	var value T
	if boundaryjson.Unmarshal(record.Value, &value) != nil || identity(value) != core.ID(record.RecordID) {
		return fmt.Errorf("contains an invalid %s projection", kind)
	}
	id := identity(value)
	previous, found := history[id]
	var prior *T
	if found {
		prior = &previous.Value
		if record.Version != previous.Version+1 || correlationStable && record.CorrelationID != previous.CorrelationID {
			return fmt.Errorf("contains noncontiguous %s history", kind)
		}
	}
	if err := validate(event.EventType, record.Version, prior, value); err != nil {
		return err
	}
	history[id] = core.DurableState[T]{Version: record.Version, CorrelationID: record.CorrelationID, Value: value}
	return nil
}

func admitLifecycleProjection[T any](event Event, record ProjectionRecord, kind string, history, target map[core.ID]core.DurableState[T], identity func(T) core.ID, validateTransition func(string, int, *T, T) error, validateAdmission func(T) error, validRevision func(T, T) bool) error {
	var value T
	if boundaryjson.Unmarshal(record.Value, &value) != nil {
		return fmt.Errorf("contains an invalid %s projection", kind)
	}
	if err := validateProjectionEventLifecycle(event, record, kind, history, identity, true, validateTransition); err != nil {
		return err
	}
	if err := validateAdmission(value); err != nil {
		return err
	}
	return core.AdmitDurableRevision(target, identity(value), record.Version, record.CorrelationID, value, true, validRevision)
}

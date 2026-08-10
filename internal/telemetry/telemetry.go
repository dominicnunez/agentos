// Package telemetry builds a deterministic per-run operational summary from
// the authoritative Event Contract stream. It does not own a second source of
// truth; the durable RUN_TELEMETRY_RECORDED event is a replayable projection.
package telemetry

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

const SchemaVersion = 1

type MechanismCount struct {
	Kind  core.ExecutionKind `json:"kind"`
	Count int                `json:"count"`
}

type ModelUse struct {
	ExecutionID             string   `json:"execution_id"`
	ExecutionProfileVersion string   `json:"execution_profile_version,omitempty"`
	Provider                string   `json:"provider"`
	Model                   string   `json:"model"`
	UsageSource             string   `json:"usage_source,omitempty"`
	InputTokens             int      `json:"input_tokens"`
	OutputTokens            int      `json:"output_tokens"`
	TotalTokens             int      `json:"total_tokens"`
	CostUSD                 *float64 `json:"cost_usd,omitempty"`
}

type Run struct {
	SchemaVersion                  int              `json:"schema_version"`
	CorrelationID                  string           `json:"correlation_id"`
	OrganizationID                 string           `json:"organization_id"`
	Outcome                        string           `json:"outcome"`
	StartedAt                      time.Time        `json:"started_at"`
	FinishedAt                     time.Time        `json:"finished_at"`
	WallTimeMilliseconds           int64            `json:"wall_time_milliseconds"`
	ExecutionMechanisms            []MechanismCount `json:"execution_mechanisms"`
	ModelUses                      []ModelUse       `json:"model_uses"`
	TotalCostUSD                   float64          `json:"total_cost_usd"`
	CostComplete                   bool             `json:"cost_complete"`
	ToolCalls                      int              `json:"tool_calls"`
	Messages                       int              `json:"messages"`
	Blocks                         int              `json:"blocks"`
	Retries                        int              `json:"retries"`
	HumanInterventions             int              `json:"human_interventions"`
	SafetyDenials                  int              `json:"safety_denials"`
	CompletionEvidenceEventRefs    []string         `json:"completion_evidence_event_refs"`
	CompletionEvidenceArtifactRefs []string         `json:"completion_evidence_artifact_refs"`
}

func Recorded(stream []events.Event) (Run, bool, error) {
	var recorded Run
	found := false
	for _, event := range stream {
		if event.EventType != "RUN_TELEMETRY_RECORDED" {
			continue
		}
		if found {
			return Run{}, false, fmt.Errorf("run contains duplicate telemetry contracts")
		}
		if err := json.Unmarshal(event.Payload, &recorded); err != nil {
			return Run{}, false, fmt.Errorf("decode recorded run telemetry: %w", err)
		}
		if recorded.SchemaVersion != SchemaVersion || recorded.CorrelationID == "" || recorded.OrganizationID == "" || recorded.Outcome == "" {
			return Run{}, false, fmt.Errorf("recorded run telemetry is invalid")
		}
		if event.OrganizationID != recorded.OrganizationID || (event.CorrelationID != "" && event.CorrelationID != recorded.CorrelationID) {
			return Run{}, false, fmt.Errorf("recorded run telemetry envelope does not match its payload")
		}
		found = true
	}
	return recorded, found, nil
}

func Project(correlationID string, stream []events.Event) (Run, error) {
	if correlationID == "" || len(stream) == 0 {
		return Run{}, fmt.Errorf("correlation id and non-empty event stream are required")
	}
	run := Run{SchemaVersion: SchemaVersion, CorrelationID: correlationID, CostComplete: true}
	taskKinds := make(map[string]core.ExecutionKind)
	mechanisms := make(map[core.ExecutionKind]int)
	modelIndexes := make(map[string]int)
	artifactRefs := make(map[string]struct{})
	taskStatuses := make(map[string]core.TaskStatus)
	var terminalAt time.Time

	for _, event := range stream {
		if event.CorrelationID != "" && event.CorrelationID != correlationID {
			return Run{}, fmt.Errorf("event %s belongs to a different correlation", event.EventID)
		}
		if run.OrganizationID == "" {
			run.OrganizationID = event.OrganizationID
		} else if event.OrganizationID != run.OrganizationID {
			return Run{}, fmt.Errorf("run stream crosses organization boundary")
		}
		if run.StartedAt.IsZero() || (!event.CreatedAt.IsZero() && event.CreatedAt.Before(run.StartedAt)) {
			run.StartedAt = event.CreatedAt
		}
		var projection events.ProjectionEventPayload
		if json.Unmarshal(event.Payload, &projection) == nil && projection.Projection.ProjectionKind == "task" {
			var task core.Task
			if err := json.Unmarshal(projection.Projection.Value, &task); err != nil || task.ID == "" || task.ExecutionKind == "" {
				return Run{}, fmt.Errorf("event %s has an invalid task projection", event.EventID)
			}
			taskKinds[string(task.ID)] = task.ExecutionKind
			taskStatuses[string(task.ID)] = task.Status
			if task.Status == core.TaskCompleted || task.Status == core.TaskFailed {
				if terminalAt.IsZero() || event.CreatedAt.After(terminalAt) {
					terminalAt = event.CreatedAt
				}
			}
		}

		switch event.EventType {
		case "TASK_CREATED":
			if _, ok := taskKinds[event.TaskID]; !ok {
				return Run{}, fmt.Errorf("TASK_CREATED event %s has no valid task projection", event.EventID)
			}
		case "EXECUTION_STARTED":
			kind, ok := taskKinds[event.TaskID]
			if !ok {
				return Run{}, fmt.Errorf("execution start for task %s has no task contract", event.TaskID)
			}
			mechanisms[kind]++
		case "EXECUTION_CONTEXT_MANIFESTED":
			var manifest core.ExecutionContextManifest
			if err := json.Unmarshal(event.Payload, &manifest); err != nil || manifest.ExecutionID == "" || manifest.Provider == "" || manifest.Model == "" {
				return Run{}, fmt.Errorf("model execution manifest is invalid")
			}
			if _, exists := modelIndexes[string(manifest.ExecutionID)]; exists {
				return Run{}, fmt.Errorf("duplicate model execution manifest for %s", manifest.ExecutionID)
			}
			modelIndexes[string(manifest.ExecutionID)] = len(run.ModelUses)
			run.ModelUses = append(run.ModelUses, ModelUse{ExecutionID: string(manifest.ExecutionID), ExecutionProfileVersion: manifest.ExecutionProfileVersion, Provider: manifest.Provider, Model: manifest.Model})
		case "INFERENCE_USAGE_RECORDED":
			var usage events.InferenceUsageRecordedPayload
			if err := json.Unmarshal(event.Payload, &usage); err != nil || !usage.Valid() || event.SourceExecutionID == "" {
				return Run{}, fmt.Errorf("inference usage event %s is invalid", event.EventID)
			}
			index, ok := modelIndexes[event.SourceExecutionID]
			if !ok {
				index = len(run.ModelUses)
				modelIndexes[event.SourceExecutionID] = index
				run.ModelUses = append(run.ModelUses, ModelUse{ExecutionID: event.SourceExecutionID, Provider: usage.Provider, Model: usage.Model})
			}
			use := &run.ModelUses[index]
			if use.UsageSource != "" {
				return Run{}, fmt.Errorf("duplicate inference usage for execution %s", event.SourceExecutionID)
			}
			if use.Provider != usage.Provider || use.Model != usage.Model {
				return Run{}, fmt.Errorf("inference usage does not match execution manifest")
			}
			use.UsageSource = usage.Source
			use.InputTokens = usage.InputTokens
			use.OutputTokens = usage.OutputTokens
			use.TotalTokens = usage.TotalTokens
			use.CostUSD = usage.CostUSD
		case "TOOL_OUTCOME_RECORDED":
			run.ToolCalls++
			var outcome core.ToolOutcome
			if err := json.Unmarshal(event.Payload, &outcome); err != nil {
				return Run{}, fmt.Errorf("decode tool outcome: %w", err)
			}
			if outcome.RecoveryAttempted {
				run.Retries++
			}
			run.CompletionEvidenceEventRefs = append(run.CompletionEvidenceEventRefs, event.EventID)
		case "MESSAGE":
			run.Messages++
		case "TASK_BLOCKED":
			run.Blocks++
		case "TASK_RECOVERED":
			run.Retries++
		case "A2A_INPUT_RECEIVED", "HUMAN_INPUT_RECEIVED", "APPROVAL_DECIDED", "COMPLETION_REVIEW_DECIDED":
			run.HumanInterventions++
			if event.EventType == "COMPLETION_REVIEW_DECIDED" {
				run.CompletionEvidenceEventRefs = append(run.CompletionEvidenceEventRefs, event.EventID)
			}
			if event.EventType == "APPROVAL_DECIDED" {
				var approval core.HumanApproval
				if err := json.Unmarshal(event.Payload, &approval); err != nil {
					return Run{}, fmt.Errorf("decode approval decision: %w", err)
				}
				if approval.Status == core.ApprovalDenied {
					run.SafetyDenials++
				}
			}
		case "CAPABILITY_DENIED":
			run.SafetyDenials++
		case "RESULT_PUBLISHED", "COMPLETION_VERIFIED", "COMPLETION_REJECTED":
			run.CompletionEvidenceEventRefs = append(run.CompletionEvidenceEventRefs, event.EventID)
			for _, ref := range event.ArtifactRefs {
				artifactRefs[ref] = struct{}{}
			}
		}
	}
	if len(taskStatuses) == 0 {
		return Run{}, fmt.Errorf("run has no task contracts")
	}
	run.Outcome = "VERIFIED_COMPLETE"
	for taskID, status := range taskStatuses {
		switch status {
		case core.TaskCompleted:
		case core.TaskFailed:
			run.Outcome = "REJECTED"
		case core.TaskPending, core.TaskRunning, core.TaskBlocked:
			return Run{}, fmt.Errorf("task %s is not terminal", taskID)
		default:
			return Run{}, fmt.Errorf("task %s has unknown status %q", taskID, status)
		}
	}
	if run.OrganizationID == "" || terminalAt.IsZero() {
		return Run{}, fmt.Errorf("run is not terminal and cannot be summarized")
	}
	run.FinishedAt = terminalAt
	if run.FinishedAt.After(run.StartedAt) {
		run.WallTimeMilliseconds = run.FinishedAt.Sub(run.StartedAt).Milliseconds()
	}
	for kind, count := range mechanisms {
		run.ExecutionMechanisms = append(run.ExecutionMechanisms, MechanismCount{Kind: kind, Count: count})
	}
	sort.Slice(run.ExecutionMechanisms, func(i, j int) bool { return run.ExecutionMechanisms[i].Kind < run.ExecutionMechanisms[j].Kind })
	for _, use := range run.ModelUses {
		if use.UsageSource == "" || use.CostUSD == nil {
			run.CostComplete = false
			continue
		}
		run.TotalCostUSD += *use.CostUSD
	}
	for ref := range artifactRefs {
		run.CompletionEvidenceArtifactRefs = append(run.CompletionEvidenceArtifactRefs, ref)
	}
	sort.Strings(run.CompletionEvidenceArtifactRefs)
	return run, nil
}

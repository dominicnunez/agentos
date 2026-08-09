package core

import "time"

type ID string

type Organization struct {
	ID            ID        `json:"id"`
	Name          string    `json:"name"`
	PolicyVersion string    `json:"policy_version"`
	CreatedAt     time.Time `json:"created_at"`
}
type Team struct {
	ID             ID        `json:"id"`
	OrganizationID ID        `json:"organization_id"`
	Name           string    `json:"name"`
	Mission        string    `json:"mission,omitempty"`
	MemberAgentIDs []ID      `json:"member_agent_ids"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"created_at"`
}
type Agent struct {
	ID                      ID     `json:"id"`
	OrganizationID          ID     `json:"organization_id"`
	BlueprintVersion        string `json:"blueprint_version"`
	ExecutionProfileVersion string `json:"execution_profile_version"`
	RuntimeAdapter          string `json:"runtime_adapter"`
	Status                  string `json:"status"`
}
type Intent struct {
	ID                    ID        `json:"id"`
	OrganizationID        ID        `json:"organization_id"`
	OriginalInstruction   string    `json:"original_instruction"`
	NormalizedObjective   string    `json:"normalized_objective"`
	HardConstraints       []string  `json:"hard_constraints"`
	ConsequenceBoundaries []string  `json:"consequence_boundaries"`
	SourceHumanID         ID        `json:"source_human_id,omitempty"`
	CreatedAt             time.Time `json:"created_at"`
}
type Goal struct {
	ID        ID        `json:"id"`
	IntentID  ID        `json:"intent_envelope_id"`
	Objective string    `json:"objective"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

type ExecutionKind string

const (
	ExecutionDeterministic ExecutionKind = "DETERMINISTIC"
	ExecutionTool          ExecutionKind = "TOOL"
	ExecutionAgent         ExecutionKind = "AGENT"
	ExecutionTeam          ExecutionKind = "TEAM"
	ExecutionHuman         ExecutionKind = "HUMAN"
	ExecutionMixed         ExecutionKind = "MIXED"
)

type TaskStatus string

const (
	TaskPending   TaskStatus = "PENDING"
	TaskRunning   TaskStatus = "RUNNING"
	TaskCompleted TaskStatus = "COMPLETED"
	TaskFailed    TaskStatus = "FAILED"
	TaskBlocked   TaskStatus = "BLOCKED"
)

type ModelInferencePolicy string

const (
	InferenceForbidden ModelInferencePolicy = "DISALLOWED"
	InferenceAllowed   ModelInferencePolicy = "ALLOWED_IF_JUSTIFIED"
	InferenceRequired  ModelInferencePolicy = "REQUIRED"
)

type Task struct {
	ID                   ID                   `json:"id"`
	GoalID               ID                   `json:"goal_id"`
	Description          string               `json:"description"`
	ExecutionKind        ExecutionKind        `json:"execution_kind"`
	ModelInferencePolicy ModelInferencePolicy `json:"model_inference_policy"`
	DependsOn            []ID                 `json:"depends_on,omitempty"`
	Status               TaskStatus           `json:"status"`
}

func (t Task) Ready(tasks map[ID]Task) bool {
	for _, id := range t.DependsOn {
		if dep, ok := tasks[id]; !ok || dep.Status != TaskCompleted {
			return false
		}
	}
	return true
}

type Assurance string

const AssuranceDeterministic Assurance = "DETERMINISTIC"

type CompletionCriterion struct {
	ID          string    `json:"id"`
	Description string    `json:"description"`
	Assurance   Assurance `json:"assurance"`
	Required    bool      `json:"required"`
}
type CompletionContract struct {
	TaskID            ID                    `json:"task_id"`
	TaskVersion       int                   `json:"task_version"`
	Criteria          []CompletionCriterion `json:"criteria"`
	RequiredArtifacts []string              `json:"required_artifacts,omitempty"`
}
type MaterializationState string

const MaterializedFull MaterializationState = "FULL"

type VersionedRef struct {
	ID                   string               `json:"id"`
	Version              string               `json:"version"`
	MaterializationState MaterializationState `json:"materialization_state"`
}
type ExecutionContextManifest struct {
	ExecutionID             ID             `json:"execution_id"`
	AgentID                 ID             `json:"agent_id"`
	ExecutionProfileVersion string         `json:"execution_profile_version"`
	Provider                string         `json:"provider,omitempty"`
	Model                   string         `json:"model,omitempty"`
	TaskID                  ID             `json:"task_id"`
	TaskContractVersion     string         `json:"task_contract_version"`
	PromptVersion           string         `json:"prompt_version,omitempty"`
	PolicyVersion           string         `json:"policy_version,omitempty"`
	EventRefs               []string       `json:"event_refs"`
	KnowledgeRefs           []VersionedRef `json:"knowledge_refs"`
	SkillRefs               []VersionedRef `json:"skill_refs"`
	ToolDefinitions         []VersionedRef `json:"tool_definitions"`
	ContextBuilderVersion   string         `json:"context_builder_version"`
	CreatedAt               time.Time      `json:"created_at"`
}

type ToolOutcomeStatus string

const (
	OutcomeSucceeded ToolOutcomeStatus = "SUCCESS"
	OutcomeFailed    ToolOutcomeStatus = "FAILED"
	OutcomePartial   ToolOutcomeStatus = "PARTIAL"
)

type PostconditionStatus string

const (
	PostconditionVerified   PostconditionStatus = "VERIFIED"
	PostconditionFailed     PostconditionStatus = "FAILED"
	PostconditionNotChecked PostconditionStatus = "NOT_CHECKED"
)

type Retryability string

const (
	Retryable        Retryability = "RETRYABLE"
	NotRetryable     Retryability = "NOT_RETRYABLE"
	RetryAfterChange Retryability = "RETRY_AFTER_CHANGE"
)

type ToolOutcome struct {
	ToolInvocationID    ID                  `json:"tool_invocation_id"`
	ToolID              string              `json:"tool_id"`
	ToolVersion         string              `json:"tool_version,omitempty"`
	Status              ToolOutcomeStatus   `json:"status"`
	ObservedEffect      any                 `json:"observed_effect,omitempty"`
	PostconditionStatus PostconditionStatus `json:"postcondition_status"`
	Retryability        Retryability        `json:"retryability"`
	RecoveryAttempted   bool                `json:"recovery_attempted"`
	RecoveryResult      any                 `json:"recovery_result,omitempty"`
	ArtifactRefs        []string            `json:"artifact_refs,omitempty"`
	ErrorClass          string              `json:"error_class,omitempty"`
	ErrorDetail         string              `json:"error_detail,omitempty"`
	StartedAt           time.Time           `json:"started_at"`
	FinishedAt          time.Time           `json:"finished_at"`
}

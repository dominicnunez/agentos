package core

import "time"

type ID string

type PrincipalKind string

const (
	PrincipalRuntime       PrincipalKind = "RUNTIME"
	PrincipalHuman         PrincipalKind = "HUMAN"
	PrincipalExternalAgent PrincipalKind = "EXTERNAL_AGENT"
)

type Organization struct {
	ID            ID        `json:"id"`
	Name          string    `json:"name"`
	PolicyVersion string    `json:"policy_version"`
	CreatedAt     time.Time `json:"created_at"`
}

type MissionStatus string

const (
	MissionActive  MissionStatus = "ACTIVE"
	MissionRetired MissionStatus = "RETIRED"
)

// Mission is enduring organizational direction. It is revised or retired,
// never completed by a Work or Task transition.
type Mission struct {
	ID             ID            `json:"id"`
	OrganizationID ID            `json:"organization_id"`
	Statement      string        `json:"statement"`
	Status         MissionStatus `json:"status"`
	CreatedAt      time.Time     `json:"created_at"`
}

type GoalMode string

const (
	GoalTarget     GoalMode = "TARGET"
	GoalContinuous GoalMode = "CONTINUOUS"
)

type GoalStatus string

const (
	GoalActive   GoalStatus = "ACTIVE"
	GoalPaused   GoalStatus = "PAUSED"
	GoalAchieved GoalStatus = "ACHIEVED"
	GoalRetired  GoalStatus = "RETIRED"
)

// Goal is a measurable outcome under a Mission. Projection versions preserve
// refinements; achieving it requires a separate evidence evaluation.
type Goal struct {
	ID              ID            `json:"id"`
	OrganizationID  ID            `json:"organization_id"`
	MissionID       ID            `json:"mission_id"`
	Objective       string        `json:"objective"`
	Mode            GoalMode      `json:"mode"`
	SuccessCriteria []IntentValue `json:"success_criteria"`
	Status          GoalStatus    `json:"status"`
	CreatedAt       time.Time     `json:"created_at"`
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
type AgentBlueprint struct {
	ID                        ID        `json:"id"`
	OrganizationID            ID        `json:"organization_id"`
	Version                   string    `json:"version"`
	Role                      string    `json:"role"`
	OperatingInstructions     string    `json:"operating_instructions"`
	RequiredCapabilityClasses []string  `json:"required_capability_classes"`
	Status                    string    `json:"status"`
	CreatedAt                 time.Time `json:"created_at"`
}
type ExecutionProfile struct {
	ID               ID        `json:"id"`
	OrganizationID   ID        `json:"organization_id"`
	Version          string    `json:"version"`
	ModelProvider    string    `json:"model_provider"`
	Model            string    `json:"model"`
	ReasoningSetting string    `json:"reasoning_setting,omitempty"`
	PromptVersion    string    `json:"prompt_version"`
	ToolRefs         []string  `json:"tool_refs"`
	Status           string    `json:"status"`
	CreatedAt        time.Time `json:"created_at"`
}
type Agent struct {
	ID                      ID     `json:"id"`
	OrganizationID          ID     `json:"organization_id"`
	BlueprintID             ID     `json:"blueprint_id"`
	BlueprintVersion        string `json:"blueprint_version"`
	ExecutionProfileID      ID     `json:"execution_profile_id"`
	ExecutionProfileVersion string `json:"execution_profile_version"`
	RuntimeAdapter          string `json:"runtime_adapter"`
	Status                  string `json:"status"`
}

type IntentStatus string

const (
	IntentStatusAwaitingInput  IntentStatus = "AWAITING_USER_INPUT"
	IntentStatusReadyForReview IntentStatus = "READY_FOR_REVIEW"
)

type IntentValue struct {
	Value           string `json:"value"`
	Origin          string `json:"origin"`
	SourceMessageID string `json:"source_message_id,omitempty"`
}

type IntentDecision struct {
	Subject         string `json:"subject"`
	Value           string `json:"value"`
	Origin          string `json:"origin"`
	SourceMessageID string `json:"source_message_id,omitempty"`
}

// IntentDraft is untrusted model output until the runtime validates it and an
// authorized operator confirms its exact version and fingerprint. A draft may
// describe missing input; a reviewable or accepted draft may not.
type IntentDraft struct {
	ID                     ID               `json:"id"`
	OrganizationID         ID               `json:"organization_id"`
	Version                int              `json:"version"`
	Status                 IntentStatus     `json:"status"`
	RequestedExecutionKind ExecutionKind    `json:"requested_execution_kind"`
	Objective              string           `json:"objective"`
	Context                []IntentValue    `json:"context"`
	Deliverables           []IntentValue    `json:"deliverables"`
	CompletionCriteria     []IntentValue    `json:"completion_criteria"`
	Constraints            []IntentValue    `json:"constraints"`
	ResolvedDecisions      []IntentDecision `json:"resolved_decisions"`
	ConsequenceCandidates  []string         `json:"consequence_candidates"`
	MissingUserInputs      []IntentValue    `json:"missing_user_inputs,omitempty"`
	Fingerprint            string           `json:"fingerprint"`
	CreatedAt              time.Time        `json:"created_at"`
}

type Intent struct {
	ID                    ID               `json:"id"`
	OrganizationID        ID               `json:"organization_id"`
	OriginalInstruction   string           `json:"original_instruction"`
	NormalizedObjective   string           `json:"normalized_objective"`
	HardConstraints       []string         `json:"hard_constraints"`
	ConsequenceBoundaries []string         `json:"consequence_boundaries"`
	SourcePrincipalID     ID               `json:"source_principal_id"`
	SourcePrincipalKind   PrincipalKind    `json:"source_principal_kind"`
	SourceChannel         string           `json:"source_channel"`
	ExternalRequestID     string           `json:"external_request_id,omitempty"`
	SourceMessageID       string           `json:"source_message_id,omitempty"`
	SourceHumanID         ID               `json:"source_human_id,omitempty"`
	Context               []IntentValue    `json:"context"`
	Deliverables          []IntentValue    `json:"deliverables"`
	CompletionCriteria    []IntentValue    `json:"completion_criteria"`
	ResolvedDecisions     []IntentDecision `json:"resolved_decisions"`
	AcceptedFingerprint   string           `json:"accepted_fingerprint,omitempty"`
	CreatedAt             time.Time        `json:"created_at"`
}
type WorkStatus string

const (
	WorkActive    WorkStatus = "ACTIVE"
	WorkCompleted WorkStatus = "COMPLETED"
	WorkFailed    WorkStatus = "FAILED"
)

type Work struct {
	ID        ID         `json:"id"`
	IntentID  ID         `json:"intent_id"`
	GoalID    ID         `json:"goal_id,omitempty"`
	Objective string     `json:"objective"`
	Status    WorkStatus `json:"status"`
	CreatedAt time.Time  `json:"created_at"`
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

type PlanTask struct {
	Key                  string               `json:"key"`
	Description          string               `json:"description"`
	ExecutionKind        ExecutionKind        `json:"execution_kind"`
	ModelInferencePolicy ModelInferencePolicy `json:"model_inference_policy"`
	DependsOn            []string             `json:"depends_on"`
}

// Plan is a runtime-validated Task-DAG contract bound to one exact accepted
// Intent. It is coordination data, not authority: tasks still pass through
// assignment, capability, consequence, effect, and completion boundaries.
type Plan struct {
	ID                ID         `json:"id"`
	IntentID          ID         `json:"intent_id"`
	IntentFingerprint string     `json:"intent_fingerprint"`
	Version           int        `json:"version"`
	Tasks             []PlanTask `json:"tasks"`
	Fingerprint       string     `json:"fingerprint"`
	CreatedAt         time.Time  `json:"created_at"`
}

// AgentConfig pins the exact reviewed configuration selected for a Task.
// Agent identity remains durable when its current configuration evolves.
type AgentConfig struct {
	BlueprintID      ID     `json:"blueprint_id"`
	BlueprintVersion string `json:"blueprint_version"`
	ProfileID        ID     `json:"profile_id"`
	ProfileVersion   string `json:"profile_version"`
	RuntimeAdapter   string `json:"runtime_adapter"`
}

type Task struct {
	ID                   ID                   `json:"id"`
	WorkID               ID                   `json:"work_id"`
	Description          string               `json:"description"`
	ExecutionBrief       string               `json:"execution_brief,omitempty"`
	AcceptanceCriteria   []IntentValue        `json:"acceptance_criteria,omitempty"`
	ExecutionKind        ExecutionKind        `json:"execution_kind"`
	ModelInferencePolicy ModelInferencePolicy `json:"model_inference_policy"`
	DependsOn            []ID                 `json:"depends_on,omitempty"`
	ParentID             ID                   `json:"parent_id,omitempty"`
	AssigneeType         string               `json:"assignee_type,omitempty"`
	AssigneeID           ID                   `json:"assignee_id,omitempty"`
	AgentConfig          *AgentConfig         `json:"agent_config,omitempty"`
	RuntimeHandlerRef    string               `json:"runtime_handler_ref,omitempty"`
	TaskContractVersion  string               `json:"task_contract_version"`
	CompletionContract   *CompletionContract  `json:"completion_contract,omitempty"`
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

const (
	AssuranceDeterministic Assurance = "DETERMINISTIC"
	AssuranceHumanJudgment Assurance = "HUMAN_JUDGMENT"
)

type CompletionReviewDecision string

const (
	CompletionReviewApprove CompletionReviewDecision = "APPROVE"
	CompletionReviewReject  CompletionReviewDecision = "REJECT"
	CompletionReviewRevise  CompletionReviewDecision = "REVISE"
)

type CompletionCriterion struct {
	ID          string    `json:"id"`
	Description string    `json:"description"`
	Assurance   Assurance `json:"assurance"`
	Required    bool      `json:"required"`
}
type CompletionFieldRequirement struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	MinBytes    int    `json:"min_bytes"`
	MaxBytes    int    `json:"max_bytes"`
}
type ArtifactRequirement struct {
	Role       string   `json:"role"`
	MediaTypes []string `json:"media_types"`
	MinCount   int      `json:"min_count"`
	MaxCount   int      `json:"max_count"`
}
type CompletionContract struct {
	TaskID               ID                           `json:"task_id"`
	TaskVersion          int                          `json:"task_version"`
	Criteria             []CompletionCriterion        `json:"criteria"`
	RequiredFields       []CompletionFieldRequirement `json:"required_fields,omitempty"`
	ArtifactRequirements []ArtifactRequirement        `json:"artifact_requirements,omitempty"`
	RequiredArtifacts    []string                     `json:"required_artifacts,omitempty"`
}
type ArtifactEvidence struct {
	Ref       string `json:"ref"`
	Role      string `json:"role"`
	Name      string `json:"name"`
	MediaType string `json:"media_type"`
	SHA256    string `json:"sha256"`
	Size      int64  `json:"size"`
	Origin    string `json:"origin"`
	Trust     string `json:"trust"`
}
type HumanTaskSubmission struct {
	MessageID string             `json:"message_id"`
	Fields    map[string]string  `json:"fields"`
	Artifacts []ArtifactEvidence `json:"artifacts,omitempty"`
}
type MaterializationState string

const MaterializedFull MaterializationState = "FULL"

const (
	MaterializedSummary       MaterializationState = "SUMMARY"
	MaterializedReferenceOnly MaterializationState = "REFERENCE_ONLY"
	MaterializedOmitted       MaterializationState = "OMITTED"
	MaterializedUnavailable   MaterializationState = "UNAVAILABLE"
)

type VersionedRef struct {
	ID                   string               `json:"id"`
	Version              string               `json:"version"`
	MaterializationState MaterializationState `json:"materialization_state"`
}
type ExecutionContextManifest struct {
	ExecutionID             ID             `json:"execution_id"`
	AgentID                 ID             `json:"agent_id"`
	AgentBlueprintVersion   string         `json:"agent_blueprint_version"`
	ExecutionProfileVersion string         `json:"execution_profile_version"`
	RuntimeAdapter          string         `json:"runtime_adapter"`
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
	ArtifactRefs            []VersionedRef `json:"artifact_refs"`
	AdditionalContextRefs   []VersionedRef `json:"additional_context_refs"`
	ContextBuilderVersion   string         `json:"context_builder_version"`
	CreatedAt               time.Time      `json:"created_at"`
}

type CapabilityLease struct {
	ID           ID         `json:"id"`
	ActorID      ID         `json:"actor_id"`
	Action       string     `json:"action"`
	Resource     string     `json:"resource"`
	Scope        string     `json:"scope"`
	ExpiresAt    *time.Time `json:"expires_at,omitempty"`
	OriginTaskID ID         `json:"origin_task_id"`
	RevokedAt    *time.Time `json:"revoked_at,omitempty"`
}

type AuthorizationTrace struct {
	Allowed  bool   `json:"allowed"`
	LeaseID  ID     `json:"lease_id,omitempty"`
	ActorID  ID     `json:"actor_id"`
	TaskID   ID     `json:"task_id"`
	Action   string `json:"action"`
	Resource string `json:"resource"`
	Scope    string `json:"scope"`
	Reason   string `json:"reason"`
}

type KnowledgeStatus string

const (
	KnowledgeCandidate   KnowledgeStatus = "CANDIDATE"
	KnowledgeActive      KnowledgeStatus = "ACTIVE"
	KnowledgeSuperseded  KnowledgeStatus = "SUPERSEDED"
	KnowledgeStale       KnowledgeStatus = "STALE"
	KnowledgeQuarantined KnowledgeStatus = "QUARANTINED"
)

type KnowledgeRecord struct {
	KnowledgeID          ID              `json:"knowledge_id"`
	Version              int             `json:"version"`
	Type                 string          `json:"type"`
	Scope                string          `json:"scope"`
	Tags                 []string        `json:"tags,omitempty"`
	Status               KnowledgeStatus `json:"status"`
	Content              string          `json:"content"`
	ProvenanceEventRefs  []string        `json:"provenance_event_refs"`
	EvidenceArtifactRefs []string        `json:"evidence_artifact_refs"`
	Applicability        string          `json:"applicability,omitempty"`
	CreatedBy            ID              `json:"created_by"`
	CreatedAt            time.Time       `json:"created_at"`
	LastVerifiedAt       *time.Time      `json:"last_verified_at,omitempty"`
	SupersedesVersion    *int            `json:"supersedes_version,omitempty"`
}
type Skill struct {
	SkillID                   ID              `json:"skill_id"`
	Version                   int             `json:"version"`
	Name                      string          `json:"name"`
	Description               string          `json:"description"`
	Scope                     string          `json:"scope"`
	Status                    KnowledgeStatus `json:"status"`
	InstructionsRef           string          `json:"instructions_ref"`
	ReferenceRefs             []string        `json:"reference_refs"`
	RequiredCapabilityClasses []string        `json:"required_capability_classes"`
	ProvenanceEventRefs       []string        `json:"provenance_event_refs"`
	ValidationEvidenceRefs    []string        `json:"validation_evidence_refs"`
	CreatedBy                 ID              `json:"created_by"`
	LastVerifiedAt            *time.Time      `json:"last_verified_at,omitempty"`
}

type EffectStatus string

const (
	EffectPending   EffectStatus = "PENDING"
	EffectAttempted EffectStatus = "ATTEMPTED"
	EffectConfirmed EffectStatus = "CONFIRMED"
	EffectFailed    EffectStatus = "FAILED"
	EffectCancelled EffectStatus = "CANCELLED"
)

type EffectObligation struct {
	ID                         ID                `json:"effect_obligation_id"`
	OrganizationID             ID                `json:"organization_id"`
	TaskID                     ID                `json:"task_id"`
	ActorID                    ID                `json:"actor_id"`
	Action                     string            `json:"action"`
	Resource                   string            `json:"resource"`
	Scope                      string            `json:"scope"`
	ConsequenceBoundary        string            `json:"consequence_boundary,omitempty"`
	Descriptor                 string            `json:"canonical_effect_descriptor"`
	EffectFingerprint          string            `json:"effect_fingerprint"`
	AuthorizationRefs          []string          `json:"authorization_refs"`
	ApprovalRef                string            `json:"approval_ref,omitempty"`
	IdempotencyKey             string            `json:"idempotency_key"`
	ReplayContext              map[string]string `json:"replay_context"`
	Status                     EffectStatus      `json:"status"`
	AttemptCount               int               `json:"attempt_count"`
	LastAttemptAt              *time.Time        `json:"last_attempt_at,omitempty"`
	ConfirmationEvidenceRefs   []string          `json:"confirmation_evidence_refs"`
	ReconciliationEvidenceRefs []string          `json:"reconciliation_evidence_refs,omitempty"`
	ReconciledAt               *time.Time        `json:"reconciled_at,omitempty"`
	CreatedAt                  time.Time         `json:"created_at"`
}

type ApprovalStatus string

const (
	ApprovalPending         ApprovalStatus = "PENDING"
	ApprovalNotified        ApprovalStatus = "NOTIFIED"
	ApprovalAcknowledged    ApprovalStatus = "ACKNOWLEDGED"
	ApprovalPendingDecision ApprovalStatus = "PENDING_DECISION"
	ApprovalApproved        ApprovalStatus = "APPROVED"
	ApprovalDenied          ApprovalStatus = "DENIED"
)

const (
	BoundaryFinancial              = "FINANCIAL"
	BoundaryPhysicalWorld          = "PHYSICAL_WORLD"
	BoundaryPublicExternal         = "PUBLIC_EXTERNAL"
	BoundaryDestructive            = "DESTRUCTIVE_IRREVERSIBLE"
	BoundarySensitiveDataExpansion = "SENSITIVE_DATA_BOUNDARY_EXPANSION"
	BoundaryPrivilegeExpansion     = "PRIVILEGE_TRUST_EXPANSION"
	BoundaryLegalBinding           = "LEGAL_BINDING"
	BoundaryDeployment             = "AGENT_OS_DEPLOYMENT"
	BoundaryTrustedCore            = "TRUSTED_CORE_SECURITY"
)

type HumanApproval struct {
	ID                 ID             `json:"id"`
	OrganizationID     ID             `json:"organization_id"`
	TaskID             ID             `json:"task_id"`
	EffectObligationID ID             `json:"effect_obligation_id"`
	Action             string         `json:"action"`
	Resource           string         `json:"resource"`
	Boundary           string         `json:"boundary"`
	Risk               string         `json:"risk"`
	Urgency            string         `json:"urgency"`
	EffectFingerprint  string         `json:"effect_fingerprint"`
	Status             ApprovalStatus `json:"status"`
	CreatedAt          time.Time      `json:"created_at"`
	AcknowledgedAt     *time.Time     `json:"acknowledged_at,omitempty"`
	AcknowledgedBy     ID             `json:"acknowledged_by,omitempty"`
	DecisionAt         *time.Time     `json:"decision_at,omitempty"`
	DecidedBy          ID             `json:"decided_by,omitempty"`
	ExpiresAt          *time.Time     `json:"expires_at,omitempty"`
	SingleUse          bool           `json:"single_use"`
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

package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"slices"
	"strings"
	"time"
)

const (
	ActionCodeIntroduce          = "code.introduce"
	ActionExecutionSurfaceMutate = "execution_surface.mutate"
)

const maximumSecurityBindingReferences = 64

// CapabilityRequirement describes one consequential capability exposed by an
// Agent-controlled operation. It is checked in addition to, not inherited
// from, the operation's top-level capability.
type CapabilityRequirement struct {
	Action   string `json:"action"`
	Resource string `json:"resource"`
	Scope    string `json:"scope"`
}

func (requirement CapabilityRequirement) Valid() bool {
	return boundedAuthorityName(requirement.Action) && boundedAuthorityName(requirement.Resource) && boundedAuthorityName(requirement.Scope)
}

type WorkspaceTrust string

const (
	WorkspaceTrusted   WorkspaceTrust = "TRUSTED"
	WorkspaceUntrusted WorkspaceTrust = "UNTRUSTED"
)

type WritableNamespace string

const (
	NamespaceReadOnlyShared   WritableNamespace = "READ_ONLY_SHARED"
	NamespaceTenantShared     WritableNamespace = "TENANT_SHARED"
	NamespaceWorkShared       WritableNamespace = "WORK_SHARED"
	NamespaceTaskShared       WritableNamespace = "TASK_SHARED"
	NamespaceExecutionPrivate WritableNamespace = "EXECUTION_PRIVATE"
)

// WorkspaceBinding makes writable sharing explicit. An untrusted writable
// workspace is currently valid only when it is private to one execution.
type WorkspaceBinding struct {
	WorkspaceID      string            `json:"workspace_id"`
	Trust            WorkspaceTrust    `json:"trust"`
	Namespace        WritableNamespace `json:"namespace"`
	OwnerExecutionID ID                `json:"owner_execution_id,omitempty"`
	Writable         bool              `json:"writable"`
}

func (binding WorkspaceBinding) Valid() bool {
	if !boundedAuthorityName(binding.WorkspaceID) {
		return false
	}
	switch binding.Trust {
	case WorkspaceTrusted, WorkspaceUntrusted:
	default:
		return false
	}
	switch binding.Namespace {
	case NamespaceReadOnlyShared:
		return !binding.Writable && binding.OwnerExecutionID == ""
	case NamespaceTenantShared, NamespaceWorkShared, NamespaceTaskShared:
		return !binding.Writable && binding.OwnerExecutionID == ""
	case NamespaceExecutionPrivate:
		return binding.OwnerExecutionID != ""
	default:
		return false
	}
}

// SafeForAdaptiveMutation prevents obvious writable side channels: adaptive
// mutation is confined to a workspace owned by exactly one execution.
func (binding WorkspaceBinding) SafeForAdaptiveMutation(executionID ID) bool {
	return binding.Valid() && binding.Writable && binding.Namespace == NamespaceExecutionPrivate && binding.OwnerExecutionID == executionID
}

type ExecutionEnvironmentManifest struct {
	ExecutionID                  ID        `json:"execution_id"`
	SandboxProvider              string    `json:"sandbox_provider"`
	SandboxImplementationVersion string    `json:"sandbox_implementation_version"`
	RequestedProfileID           string    `json:"requested_profile_id"`
	RequestedProfileSHA256       string    `json:"requested_profile_sha256"`
	RequestedWritableRoots       []string  `json:"requested_writable_roots"`
	RequestedNetworkPolicy       string    `json:"requested_network_policy"`
	RequestedReachableBrokers    []string  `json:"requested_reachable_brokers"`
	RequestedCredentialClasses   []string  `json:"requested_credential_classes"`
	EffectiveProfileID           string    `json:"effective_profile_id"`
	EffectiveProfileSHA256       string    `json:"effective_profile_sha256"`
	EffectiveIdentity            string    `json:"effective_identity"`
	IsolationIdentity            string    `json:"isolation_identity"`
	ReadableRoots                []string  `json:"readable_roots"`
	WritableRoots                []string  `json:"writable_roots"`
	NetworkPolicy                string    `json:"network_policy"`
	ReachableBrokers             []string  `json:"reachable_brokers"`
	CredentialClasses            []string  `json:"credential_classes"`
	ProcessPolicy                string    `json:"process_policy"`
	ResourcePolicy               string    `json:"resource_policy"`
	VerificationRefs             []string  `json:"verification_refs"`
	Verified                     bool      `json:"verified"`
	CreatedAt                    time.Time `json:"created_at"`
}

func (manifest ExecutionEnvironmentManifest) Valid() bool {
	return manifest.ExecutionID != "" && boundedAuthorityName(manifest.SandboxProvider) &&
		boundedAuthorityName(manifest.SandboxImplementationVersion) && boundedAuthorityName(manifest.RequestedProfileID) &&
		validSHA256(manifest.RequestedProfileSHA256) && validBoundedUniqueStrings(manifest.RequestedWritableRoots, maximumSecurityBindingReferences, true) &&
		boundedAuthorityName(manifest.RequestedNetworkPolicy) && validBoundedUniqueStrings(manifest.RequestedReachableBrokers, maximumSecurityBindingReferences, true) &&
		validBoundedUniqueStrings(manifest.RequestedCredentialClasses, maximumSecurityBindingReferences, true) && boundedAuthorityName(manifest.EffectiveProfileID) &&
		validSHA256(manifest.EffectiveProfileSHA256) && boundedAuthorityName(manifest.EffectiveIdentity) &&
		boundedAuthorityName(manifest.IsolationIdentity) && boundedAuthorityName(manifest.NetworkPolicy) &&
		boundedAuthorityName(manifest.ProcessPolicy) && boundedAuthorityName(manifest.ResourcePolicy) &&
		validBoundedUniqueStrings(manifest.ReadableRoots, maximumSecurityBindingReferences, false) &&
		validBoundedUniqueStrings(manifest.WritableRoots, maximumSecurityBindingReferences, false) &&
		validBoundedUniqueStrings(manifest.ReachableBrokers, maximumSecurityBindingReferences, true) &&
		validBoundedUniqueStrings(manifest.CredentialClasses, maximumSecurityBindingReferences, true) &&
		validBoundedUniqueStrings(manifest.VerificationRefs, maximumSecurityBindingReferences, false) &&
		manifest.Verified && !manifest.CreatedAt.IsZero() && utcTime(manifest.CreatedAt)
}

// MatchesRequestedContainment distinguishes requested configuration from
// runtime-attested effective containment. A label alone is never evidence.
func (manifest ExecutionEnvironmentManifest) MatchesRequestedContainment() bool {
	return manifest.Valid() && manifest.RequestedProfileID == manifest.EffectiveProfileID &&
		manifest.RequestedProfileSHA256 == manifest.EffectiveProfileSHA256 &&
		slices.Equal(manifest.RequestedWritableRoots, manifest.WritableRoots) &&
		manifest.RequestedNetworkPolicy == manifest.NetworkPolicy &&
		slices.Equal(manifest.RequestedReachableBrokers, manifest.ReachableBrokers) &&
		slices.Equal(manifest.RequestedCredentialClasses, manifest.CredentialClasses)
}

type ToolDefinitionBinding struct {
	ToolID                     string                  `json:"tool_id"`
	DeclaredVersion            string                  `json:"declared_version"`
	ServerIdentity             string                  `json:"server_identity"`
	EndpointIdentity           string                  `json:"endpoint_identity"`
	DefinitionSHA256           string                  `json:"definition_sha256"`
	DeclaredEffectCapabilities []CapabilityRequirement `json:"declared_effect_capabilities"`
}

func (binding ToolDefinitionBinding) Valid() bool {
	return boundedAuthorityName(binding.ToolID) && boundedAuthorityName(binding.DeclaredVersion) &&
		boundedAuthorityName(binding.ServerIdentity) && boundedAuthorityName(binding.EndpointIdentity) &&
		validSHA256(binding.DefinitionSHA256) && validCapabilityRequirements(binding.DeclaredEffectCapabilities)
}

// FingerprintToolDefinition binds all model-visible definition content and
// the consequential capabilities declared by the trusted adapter registry.
func FingerprintToolDefinition(toolID, version, serverIdentity, endpointIdentity, name, description string, inputSchema, outputSchema, metadata json.RawMessage, capabilities []CapabilityRequirement) (string, error) {
	if !validCapabilityRequirements(capabilities) || !boundedAuthorityName(toolID) || !boundedAuthorityName(version) ||
		!boundedAuthorityName(serverIdentity) || !boundedAuthorityName(endpointIdentity) || strings.TrimSpace(name) == "" {
		return "", fmt.Errorf("tool definition identity and consequential capabilities are invalid")
	}
	canonical, err := json.Marshal(struct {
		ToolID       string                  `json:"tool_id"`
		Version      string                  `json:"declared_version"`
		Server       string                  `json:"server_identity"`
		Endpoint     string                  `json:"endpoint_identity"`
		Name         string                  `json:"name"`
		Description  string                  `json:"description"`
		InputSchema  json.RawMessage         `json:"input_schema"`
		OutputSchema json.RawMessage         `json:"output_schema"`
		Metadata     json.RawMessage         `json:"model_visible_metadata"`
		Capabilities []CapabilityRequirement `json:"declared_effect_capabilities"`
	}{toolID, version, serverIdentity, endpointIdentity, name, description, inputSchema, outputSchema, metadata, capabilities})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

type ActionInfluenceBinding struct {
	ExecutionID          ID       `json:"execution_id,omitempty"`
	ManifestEventRef     string   `json:"manifest_event_ref,omitempty"`
	ExecutionInputSHA256 string   `json:"execution_input_sha256,omitempty"`
	SourceEventRefs      []string `json:"source_event_refs"`
}

func (binding ActionInfluenceBinding) Valid() bool {
	if !validBoundedUniqueStrings(binding.SourceEventRefs, maximumSecurityBindingReferences, false) {
		return false
	}
	bound := binding.ExecutionID != "" || binding.ManifestEventRef != "" || binding.ExecutionInputSHA256 != ""
	return !bound || binding.ExecutionID != "" && boundedAuthorityName(binding.ManifestEventRef) &&
		validSHA256(binding.ExecutionInputSHA256) && slices.Contains(binding.SourceEventRefs, binding.ManifestEventRef)
}

type EffectTrajectory struct {
	RelatedEffectRefs     []string `json:"related_effect_refs"`
	ProtectedEffectCount  int      `json:"protected_effect_count"`
	ApprovalRequestCount  int      `json:"approval_request_count"`
	ConsequenceBoundaries []string `json:"consequence_boundaries"`
	Destinations          []string `json:"destinations"`
}

func (trajectory EffectTrajectory) Valid() bool {
	return trajectory.ProtectedEffectCount >= 1 && trajectory.ProtectedEffectCount <= 1024 &&
		trajectory.ApprovalRequestCount >= 0 && trajectory.ApprovalRequestCount <= trajectory.ProtectedEffectCount &&
		validBoundedUniqueStrings(trajectory.RelatedEffectRefs, maximumSecurityBindingReferences, true) &&
		validBoundedUniqueStrings(trajectory.ConsequenceBoundaries, maximumSecurityBindingReferences, false) &&
		validBoundedUniqueStrings(trajectory.Destinations, maximumSecurityBindingReferences, true)
}

type StagedPromotionBinding struct {
	WorkspaceID        string `json:"workspace_id"`
	TrustedTarget      string `json:"trusted_target"`
	BaseTreeSHA256     string `json:"base_tree_sha256"`
	ResultTreeSHA256   string `json:"result_tree_sha256"`
	DiffSHA256         string `json:"diff_sha256"`
	VerificationSHA256 string `json:"verification_sha256"`
}

func (binding StagedPromotionBinding) Valid() bool {
	return boundedAuthorityName(binding.WorkspaceID) && boundedAuthorityName(binding.TrustedTarget) &&
		validSHA256(binding.BaseTreeSHA256) && validSHA256(binding.ResultTreeSHA256) &&
		binding.BaseTreeSHA256 != binding.ResultTreeSHA256 && validSHA256(binding.DiffSHA256) &&
		validSHA256(binding.VerificationSHA256)
}

type CodeIntroductionBinding struct {
	SourceType             string           `json:"source_type"`
	ArtifactName           string           `json:"artifact_name"`
	ExactVersion           string           `json:"exact_version"`
	Source                 string           `json:"source"`
	ArtifactSHA256         string           `json:"artifact_sha256"`
	PublisherIdentity      string           `json:"publisher_identity,omitempty"`
	SandboxProfile         string           `json:"sandbox_profile"`
	NetworkProfile         string           `json:"network_profile"`
	EnvironmentManifestRef string           `json:"environment_manifest_ref"`
	EnvironmentSHA256      string           `json:"environment_sha256"`
	Workspace              WorkspaceBinding `json:"workspace"`
}

func (binding CodeIntroductionBinding) Valid(executionID ID) bool {
	return boundedAuthorityName(binding.SourceType) && boundedAuthorityName(binding.ArtifactName) &&
		boundedAuthorityName(binding.ExactVersion) && boundedAuthorityName(binding.Source) && validSHA256(binding.ArtifactSHA256) &&
		boundedOptionalAuthorityName(binding.PublisherIdentity) && boundedAuthorityName(binding.SandboxProfile) &&
		boundedAuthorityName(binding.NetworkProfile) && boundedAuthorityName(binding.EnvironmentManifestRef) &&
		validSHA256(binding.EnvironmentSHA256) && binding.Workspace.Trust == WorkspaceUntrusted &&
		binding.Workspace.SafeForAdaptiveMutation(executionID)
}

type ExecutionSurfaceMutationKind string

const (
	ExecutionSurfaceCreate ExecutionSurfaceMutationKind = "CREATE"
	ExecutionSurfaceModify ExecutionSurfaceMutationKind = "MODIFY"
	ExecutionSurfaceDelete ExecutionSurfaceMutationKind = "DELETE"
)

type ExecutionSurfaceMutationBinding struct {
	Path         string                       `json:"path"`
	Kind         ExecutionSurfaceMutationKind `json:"kind"`
	BeforeSHA256 string                       `json:"before_sha256,omitempty"`
	AfterSHA256  string                       `json:"after_sha256,omitempty"`
	Promotion    StagedPromotionBinding       `json:"promotion"`
}

func (binding ExecutionSurfaceMutationBinding) Valid() bool {
	protected, _, err := ClassifyExecutionSurface(binding.Path)
	if err != nil || !protected || !binding.Promotion.Valid() {
		return false
	}
	switch binding.Kind {
	case ExecutionSurfaceCreate:
		return binding.BeforeSHA256 == "" && validSHA256(binding.AfterSHA256)
	case ExecutionSurfaceModify:
		return validSHA256(binding.BeforeSHA256) && validSHA256(binding.AfterSHA256) && binding.BeforeSHA256 != binding.AfterSHA256
	case ExecutionSurfaceDelete:
		return validSHA256(binding.BeforeSHA256) && binding.AfterSHA256 == ""
	default:
		return false
	}
}

// ValidateExecutionAuthorityEffect preserves two closed, non-interchangeable
// consequences. Generic shell/file-write authority satisfies neither one.
func ValidateExecutionAuthorityEffect(obligation EffectObligation) error {
	code := obligation.CodeIntroduction != nil
	surface := obligation.ExecutionSurfaceMutation != nil
	protectedAction := obligation.Action == ActionCodeIntroduce || obligation.Action == ActionExecutionSurfaceMutate
	protectedBoundary := obligation.ConsequenceBoundary == BoundaryCodeIntroduction || obligation.ConsequenceBoundary == BoundaryExecutionSurfaceMutation
	if !code && !surface && !protectedAction && !protectedBoundary {
		if obligation.Influence != nil && !obligation.Influence.Valid() || obligation.Trajectory != nil && !obligation.Trajectory.Valid() {
			return fmt.Errorf("optional effect influence or trajectory binding is invalid")
		}
		if len(obligation.RequiredCapabilities) == 0 {
			if obligation.ToolDefinition != nil {
				return fmt.Errorf("tool definition lacks its consequential capability closure")
			}
			return nil
		}
		if !validCapabilityRequirements(obligation.RequiredCapabilities) ||
			!slices.Contains(obligation.RequiredCapabilities, CapabilityRequirement{Action: obligation.Action, Resource: obligation.Resource, Scope: obligation.Scope}) {
			return fmt.Errorf("tool consequential capability closure omits its top-level operation")
		}
		if obligation.ToolDefinition != nil && (!obligation.ToolDefinition.Valid() || !slices.Equal(obligation.ToolDefinition.DeclaredEffectCapabilities, obligation.RequiredCapabilities)) {
			return fmt.Errorf("tool definition does not bind the exact consequential capability closure")
		}
		return nil
	}
	if code == surface || obligation.Influence == nil || !obligation.Influence.Valid() || obligation.Trajectory == nil || !obligation.Trajectory.Valid() {
		return fmt.Errorf("protected execution consequence has incomplete or ambiguous bindings")
	}
	if (obligation.ActorKind == PrincipalAgent || obligation.ActorKind == PrincipalExternalAgent) && obligation.Influence.ExecutionID == "" {
		return fmt.Errorf("agent-proposed protected consequence requires exact execution influence")
	}
	if !slices.Contains(obligation.Trajectory.ConsequenceBoundaries, obligation.ConsequenceBoundary) {
		return fmt.Errorf("effect trajectory omits the current consequence boundary")
	}
	if !validCapabilityRequirements(obligation.RequiredCapabilities) {
		return fmt.Errorf("protected consequence requires a closed consequential capability set")
	}
	required := CapabilityRequirement{Action: obligation.Action, Resource: obligation.Resource, Scope: obligation.Scope}
	if !slices.Contains(obligation.RequiredCapabilities, required) {
		return fmt.Errorf("consequential capability closure omits the protected operation")
	}
	if obligation.ToolDefinition != nil {
		if !obligation.ToolDefinition.Valid() || !slices.Equal(obligation.ToolDefinition.DeclaredEffectCapabilities, obligation.RequiredCapabilities) {
			return fmt.Errorf("tool definition does not bind the exact consequential capability closure")
		}
	}
	if code {
		if obligation.Action != ActionCodeIntroduce || obligation.ConsequenceBoundary != BoundaryCodeIntroduction ||
			!obligation.CodeIntroduction.Valid(obligation.Influence.ExecutionID) {
			return fmt.Errorf("code introduction requires its exact action, boundary, artifact, environment, and private workspace bindings")
		}
		return nil
	}
	if obligation.Action != ActionExecutionSurfaceMutate || obligation.ConsequenceBoundary != BoundaryExecutionSurfaceMutation ||
		!obligation.ExecutionSurfaceMutation.Valid() {
		return fmt.Errorf("execution-surface mutation requires its exact action, boundary, path, bytes, and staged promotion bindings")
	}
	return nil
}

// ClassifyExecutionSurface recognizes the deliberately small V1 set of files
// whose mutation can cause deferred execution. It rejects ambiguous paths.
func ClassifyExecutionSurface(name string) (bool, string, error) {
	normalized := strings.ReplaceAll(strings.TrimSpace(name), "\\", "/")
	if normalized == "" || strings.HasPrefix(normalized, "/") || strings.Contains(normalized, "\x00") || path.Clean(normalized) != normalized || normalized == "." || normalized == ".." || strings.HasPrefix(normalized, "../") {
		return false, "", fmt.Errorf("execution-surface path is not a normalized relative path")
	}
	lower := strings.ToLower(normalized)
	base := path.Base(lower)
	switch base {
	case "package.json", "package-lock.json", "pnpm-lock.yaml", "yarn.lock", "requirements.txt", "pyproject.toml", "poetry.lock", "cargo.toml", "cargo.lock", "go.mod", "go.sum", "dockerfile", "compose.yml", "compose.yaml", "docker-compose.yml", "docker-compose.yaml", "makefile", ".pre-commit-config.yaml", ".mcp.json", "mcp.json":
		return true, base, nil
	}
	if strings.HasPrefix(lower, ".github/workflows/") || strings.HasPrefix(lower, ".git/hooks/") ||
		strings.HasPrefix(lower, ".agentos/skills/") || strings.HasPrefix(lower, ".agentos/plugins/") {
		return true, strings.Split(lower, "/")[0], nil
	}
	return false, "", nil
}

// CommandCrossesCodeIntroduction is a conservative seam for future shell
// adapters. It does not authorize parsing or execution; recognized download,
// package, container, and repository acquisition paths must be separated from
// generic process authority. Unknown commands remain subject to their adapter's
// closed declaration and untrusted-workspace rules.
func CommandCrossesCodeIntroduction(command string) bool {
	lower := strings.ToLower(strings.TrimSpace(command))
	if lower == "" {
		return false
	}
	if (strings.Contains(lower, "curl ") || strings.Contains(lower, "wget ")) &&
		(strings.Contains(lower, "| sh") || strings.Contains(lower, "| bash") || strings.Contains(lower, "| zsh")) {
		return true
	}
	fields := strings.Fields(lower)
	if len(fields) == 0 {
		return false
	}
	first := path.Base(strings.Trim(fields[0], "'\""))
	joined := " " + strings.Join(fields, " ") + " "
	switch first {
	case "npx", "pnpx", "bunx":
		return true
	case "npm", "pnpm", "yarn", "bun":
		return strings.Contains(joined, " install ") || strings.Contains(joined, " add ") || strings.Contains(joined, " dlx ")
	case "pip", "pip3":
		return strings.Contains(joined, " install ")
	case "python", "python3":
		return strings.Contains(joined, " -m pip ") && strings.Contains(joined, " install ")
	case "uv":
		return strings.Contains(joined, " add ") || strings.Contains(joined, " pip install ") || strings.Contains(joined, " tool install ")
	case "poetry":
		return strings.Contains(joined, " add ") || strings.Contains(joined, " install ")
	case "go":
		return strings.Contains(joined, " get ") || strings.Contains(joined, " install ")
	case "cargo":
		return strings.Contains(joined, " install ") || strings.Contains(joined, " add ")
	case "docker", "podman", "nerdctl":
		return strings.Contains(joined, " pull ") || strings.Contains(joined, " run ") || strings.Contains(joined, " build ")
	case "git":
		return strings.Contains(joined, " clone ")
	default:
		return false
	}
}

// CommandHasInterpreterExpansion prevents a visible read-only verb from being
// treated as proof that a shell will not execute attacker-controlled input.
func CommandHasInterpreterExpansion(command string) bool {
	return strings.Contains(command, "$(") || strings.Contains(command, "`") || strings.Contains(command, "<(") || strings.Contains(command, ">(")
}

func validCapabilityRequirements(requirements []CapabilityRequirement) bool {
	if len(requirements) == 0 || len(requirements) > MaximumEffectAuthorizationRefs {
		return false
	}
	seen := make(map[CapabilityRequirement]struct{}, len(requirements))
	for _, requirement := range requirements {
		if !requirement.Valid() {
			return false
		}
		if _, duplicate := seen[requirement]; duplicate {
			return false
		}
		seen[requirement] = struct{}{}
	}
	return true
}

func validSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

func boundedAuthorityName(value string) bool {
	return strings.TrimSpace(value) == value && value != "" && len(value) <= 4096
}

func boundedOptionalAuthorityName(value string) bool {
	return value == "" || boundedAuthorityName(value)
}

func validBoundedUniqueStrings(values []string, limit int, allowEmpty bool) bool {
	if len(values) > limit || !allowEmpty && len(values) == 0 {
		return false
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !boundedAuthorityName(value) {
			return false
		}
		if _, duplicate := seen[value]; duplicate {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}

func utcTime(value time.Time) bool {
	_, offset := value.Zone()
	return offset == 0
}

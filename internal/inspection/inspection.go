// Package inspection produces deterministic, read-only findings about one
// durable Agent OS organization. Findings are evidence, not authority.
package inspection

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/dominicnunez/agentos/internal/audit"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/projections"
)

const (
	SchemaVersion = "agentos.governance.inspection.v1"
	MaximumEvents = 10_000
	MaximumBytes  = 2 << 20
)

type Severity string

const (
	Critical Severity = "CRITICAL"
	High     Severity = "HIGH"
	Medium   Severity = "MEDIUM"
	Low      Severity = "LOW"
)

type Boundary struct {
	Assessment string `json:"assessment"`
	Certified  bool   `json:"certified"`
	Authority  string `json:"authority"`
}

type Integrity struct {
	Algorithm      string `json:"algorithm"`
	Verification   string `json:"verification"`
	LedgerEvents   int64  `json:"ledger_events"`
	LedgerSequence int64  `json:"ledger_sequence"`
	LedgerEventID  string `json:"ledger_event_id"`
	LedgerSHA256   string `json:"ledger_sha256"`
}

type Rule struct {
	ID          string   `json:"id"`
	Category    string   `json:"category"`
	Severity    Severity `json:"severity"`
	Title       string   `json:"title"`
	Description string   `json:"description"`
}

type Finding struct {
	ID           string    `json:"id"`
	RuleID       string    `json:"rule_id"`
	Severity     Severity  `json:"severity"`
	Category     string    `json:"category"`
	ScopeKind    string    `json:"scope_kind"`
	ScopeID      string    `json:"scope_id"`
	Message      string    `json:"message"`
	EvidenceRefs []string  `json:"evidence_refs"`
	Status       string    `json:"status"`
	ObservedAt   time.Time `json:"observed_at"`
}

type Summary struct {
	RulesExecuted int `json:"rules_executed"`
	Findings      int `json:"findings"`
	Critical      int `json:"critical"`
	High          int `json:"high"`
	Medium        int `json:"medium"`
	Low           int `json:"low"`
}

type Report struct {
	SchemaVersion  string    `json:"schema_version"`
	ObservedAt     time.Time `json:"observed_at"`
	OrganizationID core.ID   `json:"organization_id"`
	Boundary       Boundary  `json:"boundary"`
	Integrity      Integrity `json:"integrity"`
	Rules          []Rule    `json:"rules"`
	Findings       []Finding `json:"findings"`
	Summary        Summary   `json:"summary"`
	SHA256         string    `json:"sha256"`
}

var rules = []Rule{
	{ID: "direction/active-mission-required", Category: "DIRECTION", Severity: High, Title: "Active Mission required", Description: "The organization has no active durable Mission."},
	{ID: "direction/active-goal-required", Category: "DIRECTION", Severity: High, Title: "Active Goal required", Description: "The organization has no active measurable Goal."},
	{ID: "direction/active-goal-mission", Category: "DIRECTION", Severity: High, Title: "Active Goal requires active Mission", Description: "An active Goal is attached to a Mission that is not active."},
	{ID: "operations/active-work-goal", Category: "OPERATIONS", Severity: High, Title: "Active Work requires active Goal", Description: "Active Work has no active durable Goal binding."},
	{ID: "roster/active-agent-configuration", Category: "ROSTER", Severity: High, Title: "Active Agent configuration must be active", Description: "An active Agent uses an inactive blueprint or execution profile."},
	{ID: "coordination/active-team-roster", Category: "COORDINATION", Severity: Medium, Title: "Active Team requires available members", Description: "An active Team has no available Agent member."},
	{ID: "execution/running-agent-assignment", Category: "EXECUTION", Severity: Critical, Title: "Running Agent Task requires available assignee", Description: "A running Agent Task has no currently available assigned Agent or Team member."},
	{ID: "audit/ledger-reference-integrity", Category: "EVIDENCE", Severity: High, Title: "Ledger reference integrity", Description: "An audit finding identified an incomplete ledger reference."},
	{ID: "audit/completion-evidence-order", Category: "COMPLETION", Severity: High, Title: "Completion evidence order", Description: "A Task completion projection precedes its completion verification evidence."},
	{ID: "audit/knowledge-projection-integrity", Category: "KNOWLEDGE", Severity: High, Title: "Knowledge projection integrity", Description: "A knowledge lifecycle event is not a valid admitted projection."},
	{ID: "audit/knowledge-provenance-invalid", Category: "KNOWLEDGE", Severity: High, Title: "Knowledge provenance", Description: "Active knowledge references missing, late, or cross-organization evidence."},
	{ID: "audit/knowledge-lineage-invalidated", Category: "KNOWLEDGE", Severity: High, Title: "Knowledge lineage", Description: "Active derived knowledge depends on lineage that is no longer current and active."},
	{ID: "audit/knowledge-verification-timestamp-invalid", Category: "KNOWLEDGE", Severity: High, Title: "Knowledge verification timestamp", Description: "Active knowledge has a missing or future verification timestamp."},
	{ID: "audit/knowledge-staleness-policy-missing", Category: "KNOWLEDGE", Severity: High, Title: "Knowledge staleness policy", Description: "Active knowledge has no configured maximum verification age."},
	{ID: "audit/knowledge-revalidation-due", Category: "KNOWLEDGE", Severity: High, Title: "Knowledge revalidation", Description: "Active knowledge exceeds its configured maximum verification age."},
}

var auditRuleIDs = map[string]string{
	"ledger_reference_integrity":               "audit/ledger-reference-integrity",
	"completion_evidence_order":                "audit/completion-evidence-order",
	"knowledge_projection_integrity":           "audit/knowledge-projection-integrity",
	"knowledge_provenance_invalid":             "audit/knowledge-provenance-invalid",
	"knowledge_lineage_invalidated":            "audit/knowledge-lineage-invalidated",
	"knowledge_verification_timestamp_invalid": "audit/knowledge-verification-timestamp-invalid",
	"knowledge_staleness_policy_missing":       "audit/knowledge-staleness-policy-missing",
	"knowledge_revalidation_due":               "audit/knowledge-revalidation-due",
}

type projectionAdmission struct {
	record events.ProjectionRecord
	event  events.Event
}

// Project inspects one current projection against a tenant-scoped event slice
// that was read with its complete-ledger integrity head. It never repairs
// state, runs a model, grants authority, or asserts ISO/IEC 42001 conformity.
func Project(snapshot projections.Snapshot, verified events.VerifiedEventSnapshot, organizationID core.ID, observedAt time.Time, knowledgeVerificationMaxAge time.Duration) (Report, error) {
	if organizationID == "" || observedAt.IsZero() || verified.OrganizationID != string(organizationID) || verified.CorrelationID != "" {
		return Report{}, fmt.Errorf("governance inspection requires one organization and observation time")
	}
	if verified.Algorithm != "SHA-256" || verified.LedgerEvents < int64(len(verified.Events)) || verified.LedgerSequence < 1 || verified.LedgerEventID == "" || !validSHA256(verified.LedgerSHA256) || len(verified.Events) == 0 || len(verified.Events) > MaximumEvents {
		return Report{}, fmt.Errorf("governance inspection integrity snapshot is invalid")
	}
	observedAt = observedAt.UTC()
	admissions, err := currentAdmissions(verified, organizationID)
	if err != nil {
		return Report{}, err
	}
	refs, err := matchCurrentProjection(snapshot, organizationID, admissions)
	if err != nil {
		return Report{}, fmt.Errorf("governance inspection projection boundary changed: %w", err)
	}
	findings, err := inspectOrganization(snapshot, organizationID, refs, observedAt)
	if err != nil {
		return Report{}, err
	}
	auditor := audit.New(nil).WithKnowledgeVerificationMaxAge(knowledgeVerificationMaxAge)
	auditFindings, err := auditor.RunEventsAt(verified.Events, observedAt)
	if err != nil {
		return Report{}, fmt.Errorf("run deterministic governance audit: %w", err)
	}
	for _, item := range auditFindings {
		ruleID, known := auditRuleIDs[item.Rule]
		if !known {
			return Report{}, fmt.Errorf("audit returned unregistered governance rule %s", item.Rule)
		}
		rule, err := ruleByID(ruleID)
		if err != nil {
			return Report{}, err
		}
		findings = append(findings, newFinding(rule, "EVENT", item.Scope, item.ID, rule.Description, item.EvidenceRefs, observedAt))
	}
	sortFindings(findings)
	report := Report{
		SchemaVersion: SchemaVersion, ObservedAt: observedAt, OrganizationID: organizationID,
		Boundary: Boundary{
			Assessment: "RUNTIME_GOVERNANCE_INSPECTION_ONLY", Certified: false,
			Authority: "Findings are read-only evidence and grant no authority, approval, capability, effect permission, completion status, conformity, or certification.",
		},
		Integrity: Integrity{
			Algorithm: verified.Algorithm, Verification: "COMPLETE_LEDGER_CHAIN",
			LedgerEvents: verified.LedgerEvents, LedgerSequence: verified.LedgerSequence,
			LedgerEventID: verified.LedgerEventID, LedgerSHA256: verified.LedgerSHA256,
		},
		Rules: append([]Rule(nil), rules...), Findings: findings,
	}
	report.Summary = summarize(report.Rules, findings)
	digest, err := reportDigest(report)
	if err != nil {
		return Report{}, err
	}
	report.SHA256 = digest
	encoded, err := json.Marshal(report)
	if err != nil {
		return Report{}, fmt.Errorf("encode governance inspection report: %w", err)
	}
	if len(encoded)+1 > MaximumBytes {
		return Report{}, fmt.Errorf("governance inspection report exceeds %d bytes", MaximumBytes)
	}
	return report, nil
}

func currentAdmissions(verified events.VerifiedEventSnapshot, organizationID core.ID) (map[string]projectionAdmission, error) {
	current := make(map[string]projectionAdmission)
	var previous int64
	seenEvents := make(map[string]struct{}, len(verified.Events))
	for _, event := range verified.Events {
		if event.EventID == "" || event.OrganizationID != string(organizationID) || event.Sequence <= previous || event.Sequence > verified.LedgerSequence || event.SchemaVersion != events.SchemaVersion {
			return nil, fmt.Errorf("governance inspection event envelope is invalid")
		}
		if _, duplicate := seenEvents[event.EventID]; duplicate {
			return nil, fmt.Errorf("governance inspection contains duplicate event %s", event.EventID)
		}
		seenEvents[event.EventID] = struct{}{}
		previous = event.Sequence
		payload, present, err := events.AdmittedProjection(event)
		if err != nil {
			return nil, fmt.Errorf("event %s has an invalid projection: %w", event.EventID, err)
		}
		if !present {
			continue
		}
		key := projectionKey(payload.Projection.ProjectionKind, payload.Projection.RecordID)
		if prior, exists := current[key]; exists && payload.Projection.Version <= prior.record.Version {
			return nil, fmt.Errorf("projection %s is not strictly versioned", key)
		}
		current[key] = projectionAdmission{record: payload.Projection, event: event}
	}
	return current, nil
}

func matchCurrentProjection(snapshot projections.Snapshot, organizationID core.ID, admissions map[string]projectionAdmission) (map[string]string, error) {
	expected := make(map[string]events.ProjectionRecord)
	organization, found := snapshot.Organizations[organizationID]
	if !found {
		return nil, fmt.Errorf("organization projection is missing")
	}
	if err := addExpected(expected, projections.KindOrganization, organizationID, organization); err != nil {
		return nil, err
	}
	collections := []func() error{
		func() error {
			return addExpectedStates(expected, projections.KindMission, snapshot.Missions, func(value core.Mission) bool { return value.OrganizationID == organizationID })
		},
		func() error {
			return addExpectedStates(expected, projections.KindGoal, snapshot.Goals, func(value core.Goal) bool { return value.OrganizationID == organizationID })
		},
		func() error {
			return addExpectedStates(expected, projections.KindTeam, snapshot.Teams, func(value core.Team) bool { return value.OrganizationID == organizationID })
		},
		func() error {
			return addExpectedStates(expected, projections.KindAgentBlueprint, snapshot.AgentBlueprints, func(value core.AgentBlueprint) bool { return value.OrganizationID == organizationID })
		},
		func() error {
			return addExpectedStates(expected, projections.KindExecutionProfile, snapshot.ExecutionProfiles, func(value core.ExecutionProfile) bool { return value.OrganizationID == organizationID })
		},
		func() error {
			return addExpectedStates(expected, projections.KindAgent, snapshot.Agents, func(value core.Agent) bool { return value.OrganizationID == organizationID })
		},
		func() error {
			return addExpectedStates(expected, projections.KindIntent, snapshot.Intents, func(value core.Intent) bool { return value.OrganizationID == organizationID })
		},
		func() error {
			return addExpectedStates(expected, projections.KindLabExperiment, snapshot.Experiments, func(value core.Experiment) bool { return value.OrganizationID == organizationID })
		},
		func() error {
			return addExpectedStates(expected, projections.KindLabPromotionCandidate, snapshot.PromotionCandidates, func(value core.PromotionCandidate) bool { return value.OrganizationID == organizationID })
		},
		func() error {
			return addExpectedStates(expected, projections.KindKnowledge, snapshot.Knowledge, func(value core.KnowledgeRecord) bool { return value.OrganizationID == organizationID })
		},
	}
	for _, addCollection := range collections {
		if err := addCollection(); err != nil {
			return nil, err
		}
	}
	workIDs := make(map[core.ID]struct{})
	for id, state := range snapshot.Works {
		intent, ok := snapshot.Intents[state.Value.IntentID]
		if ok && intent.Value.OrganizationID == organizationID {
			workIDs[id] = struct{}{}
			if err := addExpected(expected, projections.KindWork, id, state); err != nil {
				return nil, err
			}
		}
	}
	for id, state := range snapshot.Tasks {
		if _, ok := workIDs[state.Value.WorkID]; ok {
			if err := addExpected(expected, projections.KindTask, id, state); err != nil {
				return nil, err
			}
		}
	}
	if len(expected) != len(admissions) {
		return nil, fmt.Errorf("current projection count %d does not match event admissions %d", len(expected), len(admissions))
	}
	refs := make(map[string]string, len(expected))
	for key, want := range expected {
		got, found := admissions[key]
		if !found || got.record.ProjectionKind != want.ProjectionKind || got.record.RecordID != want.RecordID || got.record.Version != want.Version || got.record.CorrelationID != want.CorrelationID || !jsonEqual(got.record.Value, want.Value) {
			return nil, fmt.Errorf("projection %s does not match its verified event admission", key)
		}
		refs[key] = got.event.EventID
	}
	return refs, nil
}

func addExpected[T any](target map[string]events.ProjectionRecord, kind string, id core.ID, state core.DurableState[T]) error {
	value, err := json.Marshal(state.Value)
	if err != nil {
		return fmt.Errorf("encode current %s projection: %w", kind, err)
	}
	target[projectionKey(kind, string(id))] = events.ProjectionRecord{
		ProjectionKind: kind, RecordID: string(id), Version: state.Version,
		CorrelationID: state.CorrelationID, Value: value,
	}
	return nil
}

func addExpectedStates[T any](target map[string]events.ProjectionRecord, kind string, states map[core.ID]core.DurableState[T], include func(T) bool) error {
	for id, state := range states {
		if !include(state.Value) {
			continue
		}
		if err := addExpected(target, kind, id, state); err != nil {
			return err
		}
	}
	return nil
}

func inspectOrganization(snapshot projections.Snapshot, organizationID core.ID, refs map[string]string, observedAt time.Time) ([]Finding, error) {
	var findings []Finding
	organizationRef := refs[projectionKey(projections.KindOrganization, string(organizationID))]
	activeMissions := make(map[core.ID]struct{})
	for id, state := range snapshot.Missions {
		if state.Value.OrganizationID == organizationID && state.Value.Status == core.MissionActive {
			activeMissions[id] = struct{}{}
		}
	}
	if len(activeMissions) == 0 {
		if err := appendRuleFinding(&findings, "direction/active-mission-required", "ORGANIZATION", string(organizationID), "organization", []string{organizationRef}, observedAt); err != nil {
			return nil, err
		}
	}
	activeGoals := make(map[core.ID]struct{})
	for id, state := range snapshot.Goals {
		if state.Value.OrganizationID != organizationID || state.Value.Status != core.GoalActive {
			continue
		}
		activeGoals[id] = struct{}{}
		if _, active := activeMissions[state.Value.MissionID]; !active {
			evidence := []string{refs[projectionKey(projections.KindGoal, string(id))]}
			if ref := refs[projectionKey(projections.KindMission, string(state.Value.MissionID))]; ref != "" {
				evidence = append(evidence, ref)
			}
			if err := appendRuleFinding(&findings, "direction/active-goal-mission", "GOAL", string(id), "goal", evidence, observedAt); err != nil {
				return nil, err
			}
		}
	}
	if len(activeGoals) == 0 {
		if err := appendRuleFinding(&findings, "direction/active-goal-required", "ORGANIZATION", string(organizationID), "organization", []string{organizationRef}, observedAt); err != nil {
			return nil, err
		}
	}
	for id, state := range snapshot.Works {
		intent, ok := snapshot.Intents[state.Value.IntentID]
		if !ok || intent.Value.OrganizationID != organizationID || state.Value.Status != core.WorkActive {
			continue
		}
		if _, active := activeGoals[state.Value.GoalID]; !active {
			evidence := []string{refs[projectionKey(projections.KindWork, string(id))]}
			if ref := refs[projectionKey(projections.KindGoal, string(state.Value.GoalID))]; ref != "" {
				evidence = append(evidence, ref)
			}
			if err := appendRuleFinding(&findings, "operations/active-work-goal", "WORK", string(id), "work", evidence, observedAt); err != nil {
				return nil, err
			}
		}
	}
	availableAgents := make(map[core.ID]struct{})
	for id, state := range snapshot.Agents {
		if state.Value.OrganizationID != organizationID {
			continue
		}
		blueprint := snapshot.AgentBlueprints[state.Value.BlueprintID]
		profile := snapshot.ExecutionProfiles[state.Value.ExecutionProfileID]
		available := state.Value.Status == "ACTIVE" && blueprint.Value.Status == "ACTIVE" && profile.Value.Status == "ACTIVE"
		if available {
			availableAgents[id] = struct{}{}
		}
		if state.Value.Status == "ACTIVE" && !available {
			evidence := []string{
				refs[projectionKey(projections.KindAgent, string(id))],
				refs[projectionKey(projections.KindAgentBlueprint, string(state.Value.BlueprintID))],
				refs[projectionKey(projections.KindExecutionProfile, string(state.Value.ExecutionProfileID))],
			}
			if err := appendRuleFinding(&findings, "roster/active-agent-configuration", "AGENT", string(id), "agent", evidence, observedAt); err != nil {
				return nil, err
			}
		}
	}
	availableTeams := make(map[core.ID]struct{})
	for id, state := range snapshot.Teams {
		if state.Value.OrganizationID != organizationID || state.Value.Status != "ACTIVE" {
			continue
		}
		available := false
		for _, memberID := range state.Value.MemberAgentIDs {
			if _, ok := availableAgents[memberID]; ok {
				available = true
				break
			}
		}
		if available {
			availableTeams[id] = struct{}{}
			continue
		}
		if err := appendRuleFinding(&findings, "coordination/active-team-roster", "TEAM", string(id), "team", []string{refs[projectionKey(projections.KindTeam, string(id))]}, observedAt); err != nil {
			return nil, err
		}
	}
	for id, state := range snapshot.Tasks {
		work, ok := snapshot.Works[state.Value.WorkID]
		if !ok {
			continue
		}
		intent, ok := snapshot.Intents[work.Value.IntentID]
		if !ok || intent.Value.OrganizationID != organizationID || state.Value.Status != core.TaskRunning || state.Value.ExecutionKind != core.ExecutionAgent {
			continue
		}
		available := false
		switch state.Value.AssigneeType {
		case "AGENT":
			_, available = availableAgents[state.Value.AssigneeID]
		case "TEAM":
			_, available = availableTeams[state.Value.AssigneeID]
		}
		if !available {
			if err := appendRuleFinding(&findings, "execution/running-agent-assignment", "TASK", string(id), "task", []string{refs[projectionKey(projections.KindTask, string(id))]}, observedAt); err != nil {
				return nil, err
			}
		}
	}
	return findings, nil
}

func appendRuleFinding(findings *[]Finding, ruleID, scopeKind, scopeID, suffix string, evidenceRefs []string, observedAt time.Time) error {
	rule, err := ruleByID(ruleID)
	if err != nil {
		return err
	}
	*findings = append(*findings, newFinding(rule, scopeKind, scopeID, suffix, rule.Description, evidenceRefs, observedAt))
	return nil
}

func newFinding(rule Rule, scopeKind, scopeID, suffix, message string, evidenceRefs []string, observedAt time.Time) Finding {
	evidence := make([]string, 0, len(evidenceRefs))
	seen := make(map[string]struct{}, len(evidenceRefs))
	for _, ref := range evidenceRefs {
		if ref == "" {
			continue
		}
		if _, duplicate := seen[ref]; duplicate {
			continue
		}
		seen[ref] = struct{}{}
		evidence = append(evidence, ref)
	}
	slices.Sort(evidence)
	return Finding{
		ID: rule.ID + ":" + suffix + ":" + scopeID, RuleID: rule.ID, Severity: rule.Severity,
		Category: rule.Category, ScopeKind: scopeKind, ScopeID: scopeID, Message: message,
		EvidenceRefs: evidence, Status: "OPEN", ObservedAt: observedAt,
	}
}

func ruleByID(id string) (Rule, error) {
	for _, rule := range rules {
		if rule.ID == id {
			return rule, nil
		}
	}
	return Rule{}, fmt.Errorf("unregistered governance inspection rule %s", id)
}

func sortFindings(findings []Finding) {
	rank := map[Severity]int{Critical: 0, High: 1, Medium: 2, Low: 3}
	sort.Slice(findings, func(left, right int) bool {
		if rank[findings[left].Severity] != rank[findings[right].Severity] {
			return rank[findings[left].Severity] < rank[findings[right].Severity]
		}
		if findings[left].RuleID != findings[right].RuleID {
			return findings[left].RuleID < findings[right].RuleID
		}
		if findings[left].ScopeKind != findings[right].ScopeKind {
			return findings[left].ScopeKind < findings[right].ScopeKind
		}
		return findings[left].ID < findings[right].ID
	})
}

func summarize(catalog []Rule, findings []Finding) Summary {
	summary := Summary{RulesExecuted: len(catalog), Findings: len(findings)}
	for _, finding := range findings {
		switch finding.Severity {
		case Critical:
			summary.Critical++
		case High:
			summary.High++
		case Medium:
			summary.Medium++
		case Low:
			summary.Low++
		}
	}
	return summary
}

func reportDigest(report Report) (string, error) {
	report.SHA256 = ""
	encoded, err := json.Marshal(report)
	if err != nil {
		return "", fmt.Errorf("encode governance inspection digest: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func projectionKey(kind, id string) string { return kind + "\x00" + id }

func jsonEqual(left, right json.RawMessage) bool {
	var leftValue, rightValue any
	return json.Unmarshal(left, &leftValue) == nil && json.Unmarshal(right, &rightValue) == nil && reflect.DeepEqual(leftValue, rightValue)
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

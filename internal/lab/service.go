// Package lab composes bounded Work with experimental containment and a
// nomination-only promotion gate. It is not a second scheduler or authority
// system.
package lab

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/projections"
)

type Spec struct {
	SandboxRef           string
	CapabilityProfileRef string
	Budget               core.ExperimentBudget
}

const (
	DeterministicSandbox       = "lab-deterministic-no-effects-v1"
	NoEffectsCapabilityProfile = "lab-no-effects-v1"
	DeterministicInferencePool = "deterministic"
)

// DefaultSpec is the runtime-owned containment used by reviewed natural-
// language Lab requests. External callers cannot supply or widen these values.
func DefaultSpec() Spec {
	return Spec{
		SandboxRef: DeterministicSandbox, CapabilityProfileRef: NoEffectsCapabilityProfile,
		Budget: core.ExperimentBudget{
			MaxExecutions: 1, MaxUsageUnits: 1, MaxWallTimeSeconds: 60,
			MaxChildren: 0, AllowedInferencePools: []string{DeterministicInferencePool},
		},
	}
}

func ValidateDeterministicSpec(spec Spec) error {
	if spec.SandboxRef != DeterministicSandbox || spec.CapabilityProfileRef != NoEffectsCapabilityProfile || !core.ValidExperimentBudget(spec.Budget) ||
		spec.Budget.MaxMeteredCostMicrounits != 0 || !slices.Equal(spec.Budget.AllowedInferencePools, []string{DeterministicInferencePool}) {
		return fmt.Errorf("V1 Lab requires its deterministic sandbox, no-effects profile, zero metered cost, and deterministic-only pool")
	}
	return nil
}

// ValidatePlan enforces Lab ceilings before any Task projection is admitted or
// executed. Terminal reconciliation repeats these checks against durable Tasks.
func ValidatePlan(budget core.ExperimentBudget, tasks []core.PlanTask) error {
	if !core.ValidExperimentBudget(budget) || len(tasks) == 0 || len(tasks) > budget.MaxExecutions {
		return fmt.Errorf("lab plan exceeds its execution budget")
	}
	children := 0
	for _, task := range tasks {
		if task.Key != "root" {
			children++
		}
		if task.ExecutionKind != core.ExecutionDeterministic || task.ModelInferencePolicy != core.InferenceForbidden {
			return fmt.Errorf("lab plan crosses its deterministic no-inference containment")
		}
	}
	if children > budget.MaxChildren {
		return fmt.Errorf("lab plan exceeds its child budget")
	}
	return nil
}

type Nomination struct {
	OrganizationID           core.ID
	ExperimentID             core.ID
	TargetKind               core.PromotionTargetKind
	TargetRef                string
	Summary                  string
	ReproductionEvidenceRefs []string
}

type Service struct {
	gateway *events.Gateway
	state   *projections.Repository
	now     func() time.Time
}

func New(gateway *events.Gateway) *Service {
	if gateway == nil {
		panic("Lab requires an Event Gateway")
	}
	return &Service{gateway: gateway, state: projections.New(gateway), now: func() time.Time { return time.Now().UTC() }}
}

// StartSubmission atomically admits a new experimental Intent and Work with
// its containment. It closes the crash window in which a retry could otherwise
// reinterpret an experimental request as ordinary Work.
func (s *Service) StartSubmission(ctx context.Context, correlationID string, intent core.Intent, work core.Work, spec Spec) (core.Experiment, error) {
	if err := ValidateDeterministicSpec(spec); err != nil {
		return core.Experiment{}, err
	}
	snapshot, err := s.state.Load(ctx)
	if err != nil {
		return core.Experiment{}, err
	}
	if _, found := snapshot.Organizations[intent.OrganizationID]; !found || intent.OrganizationID == "" || work.IntentID != intent.ID || work.Status != core.WorkActive {
		return core.Experiment{}, fmt.Errorf("new Lab submission requires its organization, Intent, and active bounded Work")
	}
	experiment := newExperiment(intent.OrganizationID, work, spec, s.now())
	if !core.ValidExperiment(experiment) {
		return core.Experiment{}, fmt.Errorf("lab experiment containment or resource budget is invalid")
	}
	if _, found := snapshot.Intents[intent.ID]; found {
		return core.Experiment{}, fmt.Errorf("new Lab submission conflicts with an existing Intent")
	}
	if _, found := snapshot.Works[work.ID]; found {
		return core.Experiment{}, fmt.Errorf("new Lab submission conflicts with existing Work")
	}
	if _, found := snapshot.Experiments[experiment.ID]; found {
		return core.Experiment{}, fmt.Errorf("new Lab submission conflicts with an existing experiment")
	}
	if err := s.state.SaveExperimentalSubmission(ctx, correlationID, intent, work, experiment); err != nil {
		return core.Experiment{}, fmt.Errorf("persist atomic Lab submission: %w", err)
	}
	return experiment, nil
}

// Resume verifies that a retry matches the containment admitted atomically
// with its Work. It cannot convert ordinary Work into an experiment.
func (s *Service) Resume(ctx context.Context, organizationID, workID core.ID, spec Spec) (core.Experiment, error) {
	if err := ValidateDeterministicSpec(spec); err != nil {
		return core.Experiment{}, err
	}
	snapshot, err := s.state.Load(ctx)
	if err != nil {
		return core.Experiment{}, err
	}
	work, found := snapshot.Works[workID]
	if !found {
		return core.Experiment{}, fmt.Errorf("lab experiment requires bounded Work")
	}
	intent, found := snapshot.Intents[work.Value.IntentID]
	if !found || intent.Value.OrganizationID != organizationID {
		return core.Experiment{}, fmt.Errorf("lab experiment Work is outside the organization")
	}
	experiment := newExperiment(organizationID, work.Value, spec, s.now())
	if !core.ValidExperiment(experiment) {
		return core.Experiment{}, fmt.Errorf("lab experiment containment or resource budget is invalid")
	}
	existing, exists := snapshot.Experiments[experiment.ID]
	if !exists {
		return core.Experiment{}, fmt.Errorf("ordinary Work cannot be converted into a Lab experiment")
	}
	if existing.CorrelationID != work.CorrelationID || !sameExperimentContainment(existing.Value, experiment) {
		return core.Experiment{}, fmt.Errorf("lab experiment identity is already bound to different containment")
	}
	return existing.Value, nil
}

func newExperiment(organizationID core.ID, work core.Work, spec Spec, startedAt time.Time) core.Experiment {
	return core.Experiment{
		ID: core.ID("experiment-" + string(work.ID)), OrganizationID: organizationID, WorkID: work.ID, Objective: work.Objective,
		SandboxRef: spec.SandboxRef, CapabilityProfileRef: spec.CapabilityProfileRef, Budget: spec.Budget,
		Status: core.ExperimentRunning, TrustLabel: core.ExperimentTrustUnverified, StartedAt: startedAt,
	}
}

// Reconcile closes a running experiment only from the authoritative Work
// terminal transition. It never changes the EXPERIMENTAL_UNVERIFIED label.
func (s *Service) Reconcile(ctx context.Context, organizationID, workID core.ID) (core.Experiment, bool, error) {
	snapshot, err := s.state.Load(ctx)
	if err != nil {
		return core.Experiment{}, false, err
	}
	experiment, found := experimentForWork(snapshot, organizationID, workID)
	if !found || experiment.Value.Status != core.ExperimentRunning {
		return core.Experiment{}, false, nil
	}
	work := snapshot.Works[workID]
	if work.Value.Status == core.WorkActive {
		return experiment.Value, false, nil
	}
	finished := s.now()
	next := experiment.Value
	next.FinishedAt = &finished
	eventType := "LAB_EXPERIMENT_FAILED"
	if work.Value.Status == core.WorkCompleted {
		failureCode := experimentLimitFailure(snapshot, experiment.Value, finished)
		if failureCode == "" {
			completion, artifacts, findErr := exactWorkCompletion(ctx, s.gateway, organizationID, work)
			if findErr != nil {
				return core.Experiment{}, false, findErr
			}
			next.Status = core.ExperimentCompleted
			next.ResultEventRefs = []string{completion.EventID}
			next.ArtifactRefs = artifacts
			eventType = "LAB_EXPERIMENT_COMPLETED"
		} else {
			next.Status = core.ExperimentFailed
			next.FailureCode = failureCode
		}
	} else {
		next.Status = core.ExperimentFailed
		next.FailureCode = core.ExperimentFailureWorkFailed
	}
	if err := s.state.SaveExperiment(ctx, eventType, work.CorrelationID, experiment.Version+1, next); err != nil {
		return core.Experiment{}, false, fmt.Errorf("persist terminal Lab experiment: %w", err)
	}
	return next, true, nil
}

func (s *Service) ReconcileAll(ctx context.Context) error {
	snapshot, err := s.state.Load(ctx)
	if err != nil {
		return err
	}
	ids := make([]string, 0, len(snapshot.Experiments))
	byID := make(map[string]core.Experiment, len(snapshot.Experiments))
	for _, state := range snapshot.Experiments {
		if state.Value.Status == core.ExperimentRunning {
			key := string(state.Value.ID)
			ids = append(ids, key)
			byID[key] = state.Value
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		experiment := byID[id]
		if _, _, err := s.Reconcile(ctx, experiment.OrganizationID, experiment.WorkID); err != nil {
			return err
		}
	}
	return nil
}

// Nominate creates an immutable candidate backed by a completed experiment and
// fresh, distinct completed-Work evidence. It cannot activate the target.
func (s *Service) Nominate(ctx context.Context, request Nomination) (core.PromotionCandidate, error) {
	request.ReproductionEvidenceRefs = append([]string(nil), request.ReproductionEvidenceRefs...)
	sort.Strings(request.ReproductionEvidenceRefs)
	snapshot, err := s.state.Load(ctx)
	if err != nil {
		return core.PromotionCandidate{}, err
	}
	experiment, found := snapshot.Experiments[request.ExperimentID]
	if !found || experiment.Value.OrganizationID != request.OrganizationID || experiment.Value.Status != core.ExperimentCompleted {
		return core.PromotionCandidate{}, fmt.Errorf("promotion nomination requires a completed experiment in the organization")
	}
	work, found := snapshot.Works[experiment.Value.WorkID]
	if !found {
		return core.PromotionCandidate{}, fmt.Errorf("promotion nomination requires its experimental Work")
	}
	intent, found := snapshot.Intents[work.Value.IntentID]
	if !found || intent.Value.SourcePrincipalID == "" {
		return core.PromotionCandidate{}, fmt.Errorf("promotion nomination requires its commissioning actor")
	}
	nominatedBy := intent.Value.SourcePrincipalID
	candidate := core.PromotionCandidate{
		ID: nominationID(request, nominatedBy), OrganizationID: request.OrganizationID, ExperimentID: request.ExperimentID,
		ExperimentVersion: experiment.Version, TargetKind: request.TargetKind, TargetRef: request.TargetRef, Summary: request.Summary,
		ExperimentResultEventRefs: append([]string(nil), experiment.Value.ResultEventRefs...),
		ReproductionEvidenceRefs:  request.ReproductionEvidenceRefs,
		NominatedBy:               nominatedBy, Status: core.PromotionCandidateStatus, CreatedAt: s.now(),
	}
	if !core.ValidPromotionCandidate(candidate) {
		return core.PromotionCandidate{}, fmt.Errorf("promotion nomination is incomplete or reuses experiment evidence")
	}
	if existing, exists := snapshot.PromotionCandidates[candidate.ID]; exists {
		if existing.CorrelationID != experiment.CorrelationID || !slices.Equal(existing.Value.ReproductionEvidenceRefs, candidate.ReproductionEvidenceRefs) || existing.Value.TargetRef != candidate.TargetRef || existing.Value.Summary != candidate.Summary || existing.Value.NominatedBy != candidate.NominatedBy {
			return core.PromotionCandidate{}, fmt.Errorf("promotion-candidate identity conflicts with durable nomination")
		}
		return existing.Value, nil
	}
	if err := s.state.SavePromotionCandidate(ctx, experiment.CorrelationID, candidate); err != nil {
		return core.PromotionCandidate{}, fmt.Errorf("persist promotion nomination: %w", err)
	}
	return candidate, nil
}

func experimentForWork(snapshot projections.Snapshot, organizationID, workID core.ID) (projections.Versioned[core.Experiment], bool) {
	for _, experiment := range snapshot.Experiments {
		if experiment.Value.OrganizationID == organizationID && experiment.Value.WorkID == workID {
			return experiment, true
		}
	}
	return projections.Versioned[core.Experiment]{}, false
}

func experimentLimitFailure(snapshot projections.Snapshot, experiment core.Experiment, finished time.Time) string {
	if finished.Sub(experiment.StartedAt) > time.Duration(experiment.Budget.MaxWallTimeSeconds)*time.Second {
		return core.ExperimentFailureBudgetExceeded
	}
	executions := 0
	children := 0
	for _, task := range snapshot.Tasks {
		if task.Value.WorkID != experiment.WorkID {
			continue
		}
		executions++
		if task.Value.ParentID != "" {
			children++
		}
		if task.Value.ExecutionKind != core.ExecutionDeterministic || task.Value.ModelInferencePolicy != core.InferenceForbidden {
			return core.ExperimentFailureContainmentViolated
		}
	}
	if executions == 0 || executions > experiment.Budget.MaxExecutions || children > experiment.Budget.MaxChildren {
		return core.ExperimentFailureBudgetExceeded
	}
	return ""
}

func sameExperimentContainment(left, right core.Experiment) bool {
	return left.ID == right.ID && left.OrganizationID == right.OrganizationID && left.WorkID == right.WorkID && left.Objective == right.Objective &&
		left.SandboxRef == right.SandboxRef && left.CapabilityProfileRef == right.CapabilityProfileRef && left.TrustLabel == right.TrustLabel && reflect.DeepEqual(left.Budget, right.Budget)
}

func exactWorkCompletion(ctx context.Context, gateway *events.Gateway, organizationID core.ID, work projections.Versioned[core.Work]) (events.Event, []string, error) {
	stream, err := gateway.Events(ctx, work.CorrelationID)
	if err != nil {
		return events.Event{}, nil, err
	}
	var matched events.Event
	var artifacts []string
	for _, event := range stream {
		if event.EventType != "WORK_COMPLETED" || event.OrganizationID != string(organizationID) {
			continue
		}
		payload, admitted, projectionErr := events.AdmittedProjection(event)
		var projected core.Work
		var detail events.WorkCompletionTransitionPayload
		if projectionErr != nil || !admitted || payload.Projection.ProjectionKind != projections.KindWork || payload.Projection.RecordID != string(work.Value.ID) || payload.Projection.Version != work.Version ||
			json.Unmarshal(payload.Projection.Value, &projected) != nil || projected.ID != work.Value.ID || json.Unmarshal(payload.Detail, &detail) != nil || detail.EvidenceEventRef == "" {
			continue
		}
		if matched.EventID != "" {
			return events.Event{}, nil, fmt.Errorf("completed experimental Work has multiple terminal admissions")
		}
		matched = event
		evidenceFound := false
		for _, evidence := range stream {
			if evidence.EventID == detail.EvidenceEventRef && evidence.EventType == "WORK_COMPLETION_EVALUATED" && evidence.Sequence < event.Sequence {
				artifacts = append([]string(nil), evidence.ArtifactRefs...)
				evidenceFound = true
			}
		}
		if !evidenceFound {
			return events.Event{}, nil, fmt.Errorf("completed experimental Work lacks its exact evidence event")
		}
	}
	if matched.EventID == "" {
		return events.Event{}, nil, fmt.Errorf("completed experimental Work lacks its exact terminal admission")
	}
	return matched, artifacts, nil
}

func nominationID(request Nomination, nominatedBy core.ID) core.ID {
	parts := []string{string(request.ExperimentID), string(request.TargetKind), request.TargetRef, string(nominatedBy)}
	parts = append(parts, request.ReproductionEvidenceRefs...)
	digest := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return core.ID("promotion-" + hex.EncodeToString(digest[:16]))
}

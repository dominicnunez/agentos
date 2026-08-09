// Package approvals owns the durable human-approval lifecycle. Attention and
// decision authority are separate: acknowledgement never authorizes an effect.
package approvals

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

var (
	ErrApprovalNotFound        = errors.New("approval not found")
	ErrApprovalPending         = errors.New("approval decision pending")
	ErrApprovalDenied          = errors.New("approval denied")
	ErrDecisionUnauthorized    = errors.New("human is not authorized for approval boundary")
	ErrNotificationUnavailable = errors.New("approval notification unavailable")
	errRecordNotFound          = errors.New("record not found")
)

var humanBoundaries = map[string]struct{}{
	core.BoundaryFinancial:              {},
	core.BoundaryPhysicalWorld:          {},
	core.BoundaryPublicExternal:         {},
	core.BoundaryDestructive:            {},
	core.BoundarySensitiveDataExpansion: {},
	core.BoundaryPrivilegeExpansion:     {},
	core.BoundaryLegalBinding:           {},
	core.BoundaryDeployment:             {},
	core.BoundaryTrustedCore:            {},
}

// RequiresHumanApproval recognizes the closed V1 consequence-boundary set.
// Unknown non-empty boundaries fail closed instead of silently becoming
// unprotected work.
func RequiresHumanApproval(boundary string) (bool, error) {
	if boundary == "" {
		return false, nil
	}
	if _, ok := humanBoundaries[boundary]; !ok {
		return false, fmt.Errorf("unknown consequence boundary %q", boundary)
	}
	return true, nil
}

type Store interface {
	AppendRecord(context.Context, string, string, string, string, []string, []string, string, string, int, any) error
	Records(context.Context, string, string) ([][]byte, error)
}

type Notifier interface {
	Notify(context.Context, core.HumanApproval) error
}

type DecisionAuthorizer interface {
	CanDecide(context.Context, core.HumanApproval, core.ID) bool
}

type DecisionGrant struct {
	OrganizationID core.ID
	HumanID        core.ID
	Boundary       string
	Risk           string
}

type StaticAuthorizer []DecisionGrant

func (a StaticAuthorizer) CanDecide(_ context.Context, approval core.HumanApproval, humanID core.ID) bool {
	for _, grant := range a {
		if grant.OrganizationID == approval.OrganizationID && grant.HumanID == humanID && grant.Boundary == approval.Boundary && grant.Risk == approval.Risk {
			return true
		}
	}
	return false
}

type Service struct {
	store      Store
	notifier   Notifier
	authorizer DecisionAuthorizer
	now        func() time.Time
}

func New(store Store, notifier Notifier, authorizer DecisionAuthorizer) *Service {
	return &Service{store: store, notifier: notifier, authorizer: authorizer, now: func() time.Time { return time.Now().UTC() }}
}

func (s *Service) Request(ctx context.Context, approval core.HumanApproval) (core.HumanApproval, error) {
	if err := validateRequest(approval); err != nil {
		return core.HumanApproval{}, err
	}
	if s == nil || s.store == nil {
		return core.HumanApproval{}, fmt.Errorf("durable approval store is required")
	}
	if err := s.validatePreparedEffect(ctx, approval); err != nil {
		return core.HumanApproval{}, err
	}
	if _, err := s.Get(ctx, approval.ID); err == nil {
		return core.HumanApproval{}, fmt.Errorf("approval %s already exists", approval.ID)
	} else if !errors.Is(err, ErrApprovalNotFound) {
		return core.HumanApproval{}, err
	}
	approval.Status = core.ApprovalPending
	approval.CreatedAt = s.now()
	approval.AcknowledgedAt = nil
	approval.AcknowledgedBy = ""
	approval.DecisionAt = nil
	approval.DecidedBy = ""
	if err := s.append(ctx, "APPROVAL_REQUESTED", "runtime", 1, approval); err != nil {
		return core.HumanApproval{}, err
	}
	return s.Notify(ctx, approval.ID)
}

// Notify retries attention delivery for a durably pending approval. Failure
// leaves the PENDING record intact so restart cannot turn missing attention
// into implicit authorization.
func (s *Service) Notify(ctx context.Context, approvalID core.ID) (core.HumanApproval, error) {
	approval, version, err := s.load(ctx, approvalID)
	if err != nil {
		return core.HumanApproval{}, err
	}
	if approval.Status == core.ApprovalNotified {
		return approval, nil
	}
	if approval.Status != core.ApprovalPending {
		return approval, fmt.Errorf("approval %s cannot be notified from %s", approval.ID, approval.Status)
	}
	if s.notifier == nil {
		return approval, ErrNotificationUnavailable
	}
	if err := s.notifier.Notify(ctx, approval); err != nil {
		return approval, fmt.Errorf("%w: %v", ErrNotificationUnavailable, err)
	}
	approval.Status = core.ApprovalNotified
	if err := s.append(ctx, "APPROVAL_NOTIFIED", "runtime", version+1, approval); err != nil {
		return core.HumanApproval{}, err
	}
	return approval, nil
}

func (s *Service) Acknowledge(ctx context.Context, approvalID, humanID core.ID) (core.HumanApproval, error) {
	if humanID == "" {
		return core.HumanApproval{}, fmt.Errorf("acknowledging human identity is required")
	}
	approval, version, err := s.load(ctx, approvalID)
	if err != nil {
		return core.HumanApproval{}, err
	}
	if approval.Status != core.ApprovalPending && approval.Status != core.ApprovalNotified {
		return approval, fmt.Errorf("approval %s cannot be acknowledged from %s", approval.ID, approval.Status)
	}
	now := s.now()
	approval.Status = core.ApprovalAcknowledged
	approval.AcknowledgedAt = &now
	approval.AcknowledgedBy = humanID
	if err := s.append(ctx, "APPROVAL_ACKNOWLEDGED", string(humanID), version+1, approval); err != nil {
		return core.HumanApproval{}, err
	}
	return approval, nil
}

func (s *Service) BeginDecision(ctx context.Context, approvalID, humanID core.ID) (core.HumanApproval, error) {
	approval, version, err := s.load(ctx, approvalID)
	if err != nil {
		return core.HumanApproval{}, err
	}
	if err := s.authorizeDecision(ctx, humanID, approval); err != nil {
		return approval, err
	}
	if approval.Status != core.ApprovalAcknowledged {
		return approval, fmt.Errorf("approval %s cannot begin decision from %s", approval.ID, approval.Status)
	}
	approval.Status = core.ApprovalPendingDecision
	if err := s.append(ctx, "APPROVAL_DECISION_STARTED", string(humanID), version+1, approval); err != nil {
		return core.HumanApproval{}, err
	}
	return approval, nil
}

type Decision struct {
	ApprovalID        core.ID
	HumanID           core.ID
	EffectFingerprint string
	Approve           bool
}

func (s *Service) Decide(ctx context.Context, decision Decision) (core.HumanApproval, error) {
	approval, version, err := s.load(ctx, decision.ApprovalID)
	if err != nil {
		return core.HumanApproval{}, err
	}
	if err := s.authorizeDecision(ctx, decision.HumanID, approval); err != nil {
		return approval, err
	}
	if approval.Status != core.ApprovalPendingDecision {
		return approval, fmt.Errorf("approval %s cannot be decided from %s", approval.ID, approval.Status)
	}
	if decision.EffectFingerprint == "" || decision.EffectFingerprint != approval.EffectFingerprint {
		return approval, fmt.Errorf("decision does not match the exact effect fingerprint")
	}
	now := s.now()
	if decision.Approve && approval.ExpiresAt != nil && !now.Before(*approval.ExpiresAt) {
		return approval, fmt.Errorf("approval expired before decision")
	}
	approval.Status = core.ApprovalDenied
	if decision.Approve {
		approval.Status = core.ApprovalApproved
	}
	approval.DecisionAt = &now
	approval.DecidedBy = decision.HumanID
	if err := s.append(ctx, "APPROVAL_DECIDED", string(decision.HumanID), version+1, approval); err != nil {
		return core.HumanApproval{}, err
	}
	return approval, nil
}

func (s *Service) Get(ctx context.Context, approvalID core.ID) (core.HumanApproval, error) {
	approval, _, err := s.load(ctx, approvalID)
	return approval, err
}

func (s *Service) load(ctx context.Context, approvalID core.ID) (core.HumanApproval, int, error) {
	if s == nil {
		return core.HumanApproval{}, 0, ErrApprovalNotFound
	}
	body, version, err := latestRecord(ctx, s.store, "approval", string(approvalID))
	if errors.Is(err, errRecordNotFound) {
		return core.HumanApproval{}, 0, ErrApprovalNotFound
	}
	if err != nil {
		return core.HumanApproval{}, 0, err
	}
	var approval core.HumanApproval
	if err := json.Unmarshal(body, &approval); err != nil {
		return core.HumanApproval{}, 0, fmt.Errorf("decode approval %s: %w", approvalID, err)
	}
	return approval, version, nil
}

func (s *Service) authorizeDecision(ctx context.Context, humanID core.ID, approval core.HumanApproval) error {
	if humanID == "" || s.authorizer == nil || !s.authorizer.CanDecide(ctx, approval, humanID) {
		return ErrDecisionUnauthorized
	}
	return nil
}

func (s *Service) append(ctx context.Context, eventType, actorID string, version int, approval core.HumanApproval) error {
	return s.store.AppendRecord(ctx, string(approval.OrganizationID), eventType, actorID, string(approval.TaskID), nil, nil, "approval", string(approval.ID), version, approval)
}

func (s *Service) validatePreparedEffect(ctx context.Context, approval core.HumanApproval) error {
	body, _, err := latestRecord(ctx, s.store, "effect", string(approval.EffectObligationID))
	if errors.Is(err, errRecordNotFound) {
		return fmt.Errorf("approval requires a prepared effect obligation")
	}
	if err != nil {
		return err
	}
	var obligation core.EffectObligation
	if err := json.Unmarshal(body, &obligation); err != nil {
		return fmt.Errorf("decode prepared effect %s: %w", approval.EffectObligationID, err)
	}
	if obligation.Status != core.EffectPending || obligation.ID != approval.EffectObligationID || obligation.ApprovalRef != string(approval.ID) || obligation.OrganizationID != approval.OrganizationID || obligation.TaskID != approval.TaskID || obligation.Action != approval.Action || obligation.Resource != approval.Resource || obligation.ConsequenceBoundary != approval.Boundary || obligation.EffectFingerprint != approval.EffectFingerprint {
		return fmt.Errorf("approval does not match the prepared effect obligation")
	}
	return nil
}

func latestRecord(ctx context.Context, store Store, kind, id string) ([]byte, int, error) {
	if store == nil || kind == "" || id == "" {
		return nil, 0, errRecordNotFound
	}
	rows, err := store.Records(ctx, kind, id)
	if err != nil {
		return nil, 0, err
	}
	if len(rows) == 0 {
		return nil, 0, errRecordNotFound
	}
	return rows[len(rows)-1], len(rows), nil
}

func validateRequest(approval core.HumanApproval) error {
	if approval.ID == "" || approval.OrganizationID == "" || approval.TaskID == "" || approval.EffectObligationID == "" || approval.Action == "" || approval.Resource == "" || approval.EffectFingerprint == "" || approval.Risk == "" || approval.Urgency == "" {
		return fmt.Errorf("approval identity, organization, task, effect, risk, and urgency are required")
	}
	required, err := RequiresHumanApproval(approval.Boundary)
	if err != nil {
		return err
	}
	if !required {
		return fmt.Errorf("approval requires a human consequence boundary")
	}
	return nil
}


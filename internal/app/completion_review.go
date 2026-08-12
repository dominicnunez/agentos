package app

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/dominicnunez/agentos/internal/completion"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

type CompletionReviewView struct {
	Request   completion.ReviewRequest
	Result    string
	Decision  completion.ReviewDecision
	UpdatedAt time.Time
}

type CompletionReviewInput struct {
	OrganizationID string
	TaskID         string
	ReviewID       string
	Fingerprint    string
	Decision       completion.ReviewDecision
	ReviewerID     string
	ReviewerKind   core.PrincipalKind
	SourceChannel  string
	Feedback       string
}

type recordedReview struct {
	Review completion.HumanReview
	Event  events.Event
}

type candidateCompletePayload struct {
	ToolInvocationID string   `json:"tool_invocation_id"`
	ResultEventID    string   `json:"result_event_id"`
	ArtifactRefs     []string `json:"artifact_refs"`
}

type CompletionReviewPage struct {
	Reviews   []CompletionReviewView
	NextAfter core.ID
}

// PendingCompletionReviews exposes only review contracts for one organization
// through the local control surface. Internal child Tasks remain absent from
// the external A2A task index.
func (s *Service) PendingCompletionReviews(ctx context.Context, organizationID string, after core.ID, limit int) (CompletionReviewPage, error) {
	if organizationID == "" || limit < 1 || limit > 100 {
		return CompletionReviewPage{}, fmt.Errorf("organization and review page limit are required")
	}
	if err := s.acquire(ctx); err != nil {
		return CompletionReviewPage{}, err
	}
	defer s.release()
	snapshot, err := s.state.Load(ctx)
	if err != nil {
		return CompletionReviewPage{}, err
	}
	page := CompletionReviewPage{Reviews: make([]CompletionReviewView, 0, limit)}
	for _, state := range sortedTaskStates(snapshot.Tasks) {
		if state.Value.ID <= after {
			continue
		}
		owner, err := taskOrganization(snapshot, state.Value)
		if err != nil {
			return CompletionReviewPage{}, err
		}
		if owner != core.ID(organizationID) {
			continue
		}
		view, found, err := s.completionReviewLocked(ctx, organizationID, string(state.Value.ID))
		if err != nil {
			return CompletionReviewPage{}, err
		}
		if !found {
			continue
		}
		if len(page.Reviews) == limit {
			page.NextAfter = page.Reviews[len(page.Reviews)-1].Request.TaskID
			break
		}
		page.Reviews = append(page.Reviews, view)
	}
	return page, nil
}

func (s *Service) CompletionReview(ctx context.Context, organizationID, taskID string) (CompletionReviewView, bool, error) {
	if err := s.acquire(ctx); err != nil {
		return CompletionReviewView{}, false, err
	}
	defer s.release()
	return s.completionReviewLocked(ctx, organizationID, taskID)
}

func (s *Service) completionReviewLocked(ctx context.Context, organizationID, taskID string) (CompletionReviewView, bool, error) {
	stream, err := s.internalTaskEvents(ctx, organizationID, taskID)
	if err != nil || len(stream) == 0 {
		return CompletionReviewView{}, false, err
	}
	requests, decisions, err := completionReviewRecords(stream)
	if err != nil {
		return CompletionReviewView{}, false, err
	}
	var pending completion.ReviewRequest
	for _, event := range stream {
		if event.EventType != "COMPLETION_REVIEW_REQUESTED" {
			continue
		}
		var request completion.ReviewRequest
		if err := json.Unmarshal(event.Payload, &request); err != nil {
			return CompletionReviewView{}, false, fmt.Errorf("decode completion review request: %w", err)
		}
		if request.TaskID == core.ID(taskID) {
			if _, decided := decisions[request.ID]; !decided {
				pending = requests[request.ID]
			}
		}
	}
	if pending.ID == "" || pending.TaskID != core.ID(taskID) {
		return CompletionReviewView{}, false, nil
	}
	snapshot, err := s.state.Load(ctx)
	if err != nil {
		return CompletionReviewView{}, false, err
	}
	state, ok := snapshot.Tasks[pending.TaskID]
	if !ok || state.Value.Status != core.TaskBlocked || state.CorrelationID != stream[0].CorrelationID {
		return CompletionReviewView{}, false, nil
	}
	_, result, err := reviewEvidence(stream, pending)
	if err != nil {
		return CompletionReviewView{}, false, err
	}
	return CompletionReviewView{Request: pending, Result: result.Summary, UpdatedAt: pending.CreatedAt}, true, nil
}

func (s *Service) ReviewCompletion(ctx context.Context, input CompletionReviewInput) (CompletionReviewView, error) {
	if err := s.acquire(ctx); err != nil {
		return CompletionReviewView{}, err
	}
	defer s.release()
	if input.ReviewerKind != core.PrincipalHuman || input.SourceChannel != "HUMAN_DIRECT" {
		return CompletionReviewView{}, fmt.Errorf("completion review requires an authenticated human-direct principal")
	}

	stream, err := s.internalTaskEvents(ctx, input.OrganizationID, input.TaskID)
	if err != nil {
		return CompletionReviewView{}, err
	}
	if len(stream) == 0 {
		return CompletionReviewView{}, fmt.Errorf("completion review task does not exist")
	}
	requests, decisions, err := completionReviewRecords(stream)
	if err != nil {
		return CompletionReviewView{}, err
	}
	request, ok := requests[core.ID(input.ReviewID)]
	if !ok || request.TaskID != core.ID(input.TaskID) || request.OrganizationID != core.ID(input.OrganizationID) {
		return CompletionReviewView{}, fmt.Errorf("completion review does not exist")
	}
	view := CompletionReviewView{Request: request, Decision: input.Decision}
	_, result, err := reviewEvidence(stream, request)
	if err != nil {
		return CompletionReviewView{}, err
	}
	view.Result = result.Summary
	review := completion.HumanReview{
		ReviewID: request.ID, OrganizationID: request.OrganizationID, TaskID: request.TaskID,
		TaskVersion: request.TaskVersion, Fingerprint: input.Fingerprint, Decision: input.Decision,
		ReviewerID: core.ID(input.ReviewerID), Method: core.AssuranceHumanJudgment,
		EvidenceRefs: append([]string(nil), request.EvidenceRefs...), Feedback: input.Feedback,
		DecidedAt: time.Now().UTC(),
	}
	if !review.ValidFor(request) {
		return CompletionReviewView{}, fmt.Errorf("completion review decision does not match its immutable request")
	}
	var decisionEvent events.Event
	if recorded, exists := decisions[request.ID]; exists {
		if !completion.SameHumanReview(recorded.Review, review) {
			return CompletionReviewView{}, fmt.Errorf("completion review already has a different decision")
		}
		decisionEvent = recorded.Event
		review = recorded.Review
	} else {
		pending, found, err := s.completionReviewLocked(ctx, input.OrganizationID, input.TaskID)
		if err != nil {
			return CompletionReviewView{}, err
		}
		if !found || pending.Request.ID != request.ID {
			return CompletionReviewView{}, fmt.Errorf("completion review is stale")
		}
		decisionEvent, err = s.gateway.PublishTrusted(ctx, events.TrustedDraft{
			OrganizationID: input.OrganizationID, EventType: "COMPLETION_REVIEW_DECIDED",
			SourceActorID: input.ReviewerID, TaskID: input.TaskID, Payload: review,
			CorrelationID: stream[0].CorrelationID,
		})
		if err != nil {
			return CompletionReviewView{}, fmt.Errorf("persist completion review decision: %w", err)
		}
	}
	if err := s.continueCompletionReview(ctx, request, review, decisionEvent); err != nil {
		return CompletionReviewView{}, err
	}
	if _, err := s.runReady(ctx); err != nil {
		return CompletionReviewView{}, err
	}
	if err := s.reconcileGoals(ctx); err != nil {
		return CompletionReviewView{}, err
	}
	view.Decision = review.Decision
	view.UpdatedAt = review.DecidedAt
	return view, nil
}

func (s *Service) continueCompletionReview(ctx context.Context, request completion.ReviewRequest, review completion.HumanReview, decisionEvent events.Event) error {
	if decisionEvent.EventID == "" || decisionEvent.EventType != "COMPLETION_REVIEW_DECIDED" || decisionEvent.SourceActorID != string(review.ReviewerID) || decisionEvent.TaskID != string(request.TaskID) || !review.ValidFor(request) {
		return fmt.Errorf("valid durable completion review decision is required")
	}
	stream, err := s.internalTaskEvents(ctx, string(request.OrganizationID), string(request.TaskID))
	if err != nil || len(stream) == 0 {
		return fmt.Errorf("reload completion review stream: %w", err)
	}
	var latest completion.ReviewRequest
	for _, event := range stream {
		if event.EventType != "COMPLETION_REVIEW_REQUESTED" || event.TaskID != string(request.TaskID) {
			continue
		}
		if err := json.Unmarshal(event.Payload, &latest); err != nil || !latest.Valid() {
			return fmt.Errorf("latest completion review request is invalid")
		}
	}
	if latest.ID != request.ID {
		return nil
	}
	snapshot, err := s.state.Load(ctx)
	if err != nil {
		return err
	}
	state, ok := snapshot.Tasks[request.TaskID]
	if !ok || state.CorrelationID != stream[0].CorrelationID {
		return fmt.Errorf("completion review task projection is unavailable")
	}
	task := state.Value
	switch review.Decision {
	case completion.ReviewApprove:
		outcome, _, err := reviewEvidence(stream, request)
		if err != nil {
			return err
		}
		result := s.completion.EvaluateHuman(request.Contract, outcome, true)
		if !result.Complete {
			return fmt.Errorf("approved completion review did not satisfy its contract")
		}
		detail := completionDetail{Contract: request.Contract, Result: result, JudgmentRef: decisionEvent.EventID}
		verified, err := completionVerification(stream, request.TaskID, decisionEvent.EventID)
		if err != nil {
			return err
		}
		if !verified {
			if _, err := s.gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: string(request.OrganizationID), EventType: "COMPLETION_VERIFIED", SourceActorID: "runtime", TaskID: string(request.TaskID), Payload: detail, CorrelationID: state.CorrelationID}); err != nil {
				return fmt.Errorf("persist reviewed completion verification: %w", err)
			}
		}
		if task.Status == core.TaskCompleted {
			return nil
		}
		if task.Status != core.TaskBlocked {
			return fmt.Errorf("completion review cannot approve task in status %s", task.Status)
		}
		task.Status = core.TaskCompleted
		return s.state.SaveTask(ctx, request.OrganizationID, "TASK_VERIFIED_COMPLETE", "runtime", state.CorrelationID, state.Version+1, task, detail)
	case completion.ReviewReject:
		if task.Status == core.TaskFailed {
			return nil
		}
		if task.Status != core.TaskBlocked {
			return fmt.Errorf("completion review cannot reject task in status %s", task.Status)
		}
		outcome, _, err := reviewEvidence(stream, request)
		if err != nil {
			return err
		}
		task.Status = core.TaskFailed
		detail := completionDetail{Contract: request.Contract, Result: s.completion.EvaluateHuman(request.Contract, outcome, false), JudgmentRef: decisionEvent.EventID}
		return s.state.SaveTask(ctx, request.OrganizationID, "COMPLETION_REJECTED", "runtime", state.CorrelationID, state.Version+1, task, detail)
	case completion.ReviewRevise:
		if task.Status == core.TaskPending || task.Status == core.TaskRunning {
			return nil
		}
		if task.Status != core.TaskBlocked {
			return fmt.Errorf("completion review cannot revise task in status %s", task.Status)
		}
		task.Status = core.TaskPending
		detail := map[string]string{"reason": "reviewer requested revision", "review_event_ref": decisionEvent.EventID}
		return s.state.SaveTask(ctx, request.OrganizationID, "TASK_RESUMED", "runtime", state.CorrelationID, state.Version+1, task, detail)
	default:
		return fmt.Errorf("unsupported completion review decision")
	}
}

func completionReviewRecords(stream []events.Event) (map[core.ID]completion.ReviewRequest, map[core.ID]recordedReview, error) {
	requests := make(map[core.ID]completion.ReviewRequest)
	decisions := make(map[core.ID]recordedReview)
	eventSequences := make(map[string]int64, len(stream))
	requestSequences := make(map[core.ID]int64)
	for _, event := range stream {
		if event.EventID == "" || event.Sequence < 1 {
			return nil, nil, fmt.Errorf("completion review stream has an invalid event envelope")
		}
		if _, exists := eventSequences[event.EventID]; exists {
			return nil, nil, fmt.Errorf("completion review stream has a duplicate event id")
		}
		eventSequences[event.EventID] = event.Sequence
	}
	for _, event := range stream {
		switch event.EventType {
		case "COMPLETION_REVIEW_REQUESTED":
			var request completion.ReviewRequest
			if err := json.Unmarshal(event.Payload, &request); err != nil || !request.Valid() || event.OrganizationID != string(request.OrganizationID) || event.TaskID != string(request.TaskID) || event.SourceActorID != "runtime" {
				return nil, nil, fmt.Errorf("durable completion review request is invalid")
			}
			if _, exists := requests[request.ID]; exists {
				return nil, nil, fmt.Errorf("duplicate completion review request %s", request.ID)
			}
			for _, ref := range request.EvidenceRefs {
				if sequence, exists := eventSequences[ref]; !exists || sequence >= event.Sequence {
					return nil, nil, fmt.Errorf("completion review request does not follow its evidence")
				}
			}
			requests[request.ID] = request
			requestSequences[request.ID] = event.Sequence
		case "COMPLETION_REVIEW_DECIDED":
			var review completion.HumanReview
			if err := json.Unmarshal(event.Payload, &review); err != nil || event.SourceActorID != string(review.ReviewerID) || event.SourceExecutionID != "" || event.TaskID != string(review.TaskID) || event.OrganizationID != string(review.OrganizationID) {
				return nil, nil, fmt.Errorf("durable completion review decision is invalid")
			}
			request, exists := requests[review.ReviewID]
			if !exists || requestSequences[review.ReviewID] >= event.Sequence || !review.ValidFor(request) {
				return nil, nil, fmt.Errorf("completion review decision does not follow its exact request")
			}
			if _, exists := decisions[review.ReviewID]; exists {
				return nil, nil, fmt.Errorf("duplicate completion review decision %s", review.ReviewID)
			}
			decisions[review.ReviewID] = recordedReview{Review: review, Event: event}
		}
	}
	for reviewID, recorded := range decisions {
		request, ok := requests[reviewID]
		if !ok || !recorded.Review.ValidFor(request) {
			return nil, nil, fmt.Errorf("completion review decision has no exact request binding")
		}
	}
	return requests, decisions, nil
}

func reviewEvidence(stream []events.Event, request completion.ReviewRequest) (core.ToolOutcome, events.ResultPublishedPayload, error) {
	indexed := make(map[string]events.Event, len(stream))
	for _, event := range stream {
		if _, exists := indexed[event.EventID]; exists {
			return core.ToolOutcome{}, events.ResultPublishedPayload{}, fmt.Errorf("duplicate event id in completion review stream")
		}
		indexed[event.EventID] = event
	}
	selected := make([]events.Event, len(request.EvidenceRefs))
	for index, ref := range request.EvidenceRefs {
		event, ok := indexed[ref]
		if !ok || event.OrganizationID != string(request.OrganizationID) || event.TaskID != string(request.TaskID) {
			return core.ToolOutcome{}, events.ResultPublishedPayload{}, fmt.Errorf("completion review evidence is unavailable")
		}
		selected[index] = event
	}
	if selected[0].EventType != "TOOL_OUTCOME_RECORDED" || selected[1].EventType != "RESULT_PUBLISHED" || selected[2].EventType != "CANDIDATE_COMPLETE" {
		return core.ToolOutcome{}, events.ResultPublishedPayload{}, fmt.Errorf("completion review evidence types are invalid")
	}
	if selected[0].Sequence >= selected[1].Sequence || selected[1].Sequence >= selected[2].Sequence {
		return core.ToolOutcome{}, events.ResultPublishedPayload{}, fmt.Errorf("completion review evidence order is invalid")
	}
	var outcome core.ToolOutcome
	var result events.ResultPublishedPayload
	var candidate candidateCompletePayload
	if json.Unmarshal(selected[0].Payload, &outcome) != nil || json.Unmarshal(selected[1].Payload, &result) != nil || json.Unmarshal(selected[2].Payload, &candidate) != nil {
		return core.ToolOutcome{}, events.ResultPublishedPayload{}, fmt.Errorf("completion review evidence payload is invalid")
	}
	if selected[0].SourceExecutionID == "" || selected[0].SourceExecutionID != selected[1].SourceExecutionID || selected[0].SourceExecutionID != selected[2].SourceExecutionID {
		return core.ToolOutcome{}, events.ResultPublishedPayload{}, fmt.Errorf("completion review evidence crosses execution boundaries")
	}
	if outcome.ToolInvocationID == "" || outcome.ToolID == "" || outcome.StartedAt.IsZero() || outcome.FinishedAt.Before(outcome.StartedAt) || outcome.Status != core.OutcomeSucceeded || !result.ValidFor(selected[1].ArtifactRefs) || !slices.Equal(selected[1].ArtifactRefs, outcome.ArtifactRefs) || candidate.ToolInvocationID != string(outcome.ToolInvocationID) || candidate.ResultEventID != selected[1].EventID || !slices.Equal(candidate.ArtifactRefs, outcome.ArtifactRefs) || !slices.Equal(selected[2].ArtifactRefs, outcome.ArtifactRefs) {
		return core.ToolOutcome{}, events.ResultPublishedPayload{}, fmt.Errorf("completion review evidence bindings are invalid")
	}
	return outcome, result, nil
}

func completionVerification(stream []events.Event, taskID core.ID, judgmentRef string) (bool, error) {
	for _, event := range stream {
		if event.EventType != "COMPLETION_VERIFIED" || event.TaskID != string(taskID) {
			continue
		}
		var detail completionDetail
		if err := json.Unmarshal(event.Payload, &detail); err != nil {
			return false, err
		}
		if detail.JudgmentRef == judgmentRef {
			return true, nil
		}
	}
	return false, nil
}

func latestRevision(stream []events.Event, taskID core.ID) (completion.HumanReview, events.Event, bool, error) {
	requests, decisions, err := completionReviewRecords(stream)
	if err != nil {
		return completion.HumanReview{}, events.Event{}, false, err
	}
	for index := len(stream) - 1; index >= 0; index-- {
		event := stream[index]
		if event.EventType != "COMPLETION_REVIEW_DECIDED" {
			continue
		}
		var review completion.HumanReview
		if err := json.Unmarshal(event.Payload, &review); err != nil {
			return completion.HumanReview{}, events.Event{}, false, err
		}
		if review.TaskID == taskID && review.Decision == completion.ReviewRevise {
			recorded := decisions[review.ReviewID]
			if !recorded.Review.ValidFor(requests[review.ReviewID]) {
				return completion.HumanReview{}, events.Event{}, false, fmt.Errorf("revision review binding is invalid")
			}
			return recorded.Review, recorded.Event, true, nil
		}
	}
	return completion.HumanReview{}, events.Event{}, false, nil
}

package completion

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/dominicnunez/agentos/internal/core"
)

const (
	MaximumReviewFeedbackBytes  = 64 << 10
	MaximumReviewObjectiveBytes = 256 << 10
)

type ReviewDecision = core.CompletionReviewDecision

const (
	ReviewApprove = core.CompletionReviewApprove
	ReviewReject  = core.CompletionReviewReject
	ReviewRevise  = core.CompletionReviewRevise
)

// ReviewRequest binds one subjective CompletionContract to immutable ledger
// evidence. Its fingerprint is a stale-decision guard; trusted event envelopes
// remain the source of authority.
type ReviewRequest struct {
	ID             core.ID                 `json:"id"`
	OrganizationID core.ID                 `json:"organization_id"`
	TaskID         core.ID                 `json:"task_id"`
	TaskVersion    int                     `json:"task_version"`
	Objective      string                  `json:"objective"`
	Contract       core.CompletionContract `json:"contract"`
	EvidenceRefs   []string                `json:"evidence_refs"`
	CreatedAt      time.Time               `json:"created_at"`
	Fingerprint    string                  `json:"fingerprint"`
}

type HumanReview struct {
	ReviewID       core.ID        `json:"review_id"`
	OrganizationID core.ID        `json:"organization_id"`
	TaskID         core.ID        `json:"task_id"`
	TaskVersion    int            `json:"task_version"`
	Fingerprint    string         `json:"fingerprint"`
	Decision       ReviewDecision `json:"decision"`
	ReviewerID     core.ID        `json:"reviewer_id"`
	Method         core.Assurance `json:"method"`
	EvidenceRefs   []string       `json:"evidence_refs"`
	Feedback       string         `json:"feedback,omitempty"`
	DecidedAt      time.Time      `json:"decided_at"`
}

func NewReviewRequest(organizationID, taskID core.ID, taskVersion int, objective string, contract core.CompletionContract, evidenceRefs []string, createdAt time.Time) (ReviewRequest, error) {
	request := ReviewRequest{
		ID:             core.ID(fmt.Sprintf("review-%s-v%d", taskID, taskVersion)),
		OrganizationID: organizationID,
		TaskID:         taskID,
		TaskVersion:    taskVersion,
		Objective:      objective,
		Contract:       contract,
		EvidenceRefs:   append([]string(nil), evidenceRefs...),
		CreatedAt:      createdAt.UTC(),
	}
	fingerprint, err := request.expectedFingerprint()
	if err != nil {
		return ReviewRequest{}, err
	}
	request.Fingerprint = fingerprint
	if !request.Valid() {
		return ReviewRequest{}, fmt.Errorf("completion review request is invalid")
	}
	return request, nil
}

func (r ReviewRequest) Valid() bool {
	if r.ID == "" || r.OrganizationID == "" || r.TaskID == "" || r.TaskVersion < 1 || r.CreatedAt.IsZero() || len(r.EvidenceRefs) != 3 {
		return false
	}
	if r.ID != core.ID(fmt.Sprintf("review-%s-v%d", r.TaskID, r.TaskVersion)) {
		return false
	}
	if r.Contract.TaskID != r.TaskID || r.Contract.TaskVersion != r.TaskVersion || len(r.Contract.Criteria) == 0 {
		return false
	}
	if strings.TrimSpace(r.Objective) == "" || len(r.Objective) > MaximumReviewObjectiveBytes || !utf8.ValidString(r.Objective) {
		return false
	}
	hasHumanJudgment := false
	for _, criterion := range r.Contract.Criteria {
		if criterion.Required && criterion.Assurance == core.AssuranceHumanJudgment {
			hasHumanJudgment = true
		}
	}
	if !hasHumanJudgment {
		return false
	}
	seen := make(map[string]struct{}, len(r.EvidenceRefs))
	for _, ref := range r.EvidenceRefs {
		if ref == "" {
			return false
		}
		if _, exists := seen[ref]; exists {
			return false
		}
		seen[ref] = struct{}{}
	}
	expected, err := r.expectedFingerprint()
	return err == nil && len(r.Fingerprint) == sha256.Size*2 && r.Fingerprint == expected
}

func (r ReviewRequest) expectedFingerprint() (string, error) {
	binding := r
	binding.Fingerprint = ""
	body, err := json.Marshal(binding)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:]), nil
}

func (r HumanReview) ValidFor(request ReviewRequest) bool {
	if !request.Valid() || r.ReviewID != request.ID || r.OrganizationID != request.OrganizationID || r.TaskID != request.TaskID || r.TaskVersion != request.TaskVersion || r.Fingerprint != request.Fingerprint || r.ReviewerID == "" || r.Method != core.AssuranceHumanJudgment || r.DecidedAt.IsZero() {
		return false
	}
	if !sameReviewRefs(r.EvidenceRefs, request.EvidenceRefs) || !utf8.ValidString(r.Feedback) || len(r.Feedback) > MaximumReviewFeedbackBytes {
		return false
	}
	if r.Feedback != "" && strings.TrimSpace(r.Feedback) == "" {
		return false
	}
	switch r.Decision {
	case ReviewApprove, ReviewReject:
		return true
	case ReviewRevise:
		return strings.TrimSpace(r.Feedback) != ""
	default:
		return false
	}
}

func SameHumanReview(left, right HumanReview) bool {
	return left.ReviewID == right.ReviewID && left.OrganizationID == right.OrganizationID && left.TaskID == right.TaskID && left.TaskVersion == right.TaskVersion && left.Fingerprint == right.Fingerprint && left.Decision == right.Decision && left.ReviewerID == right.ReviewerID && left.Method == right.Method && left.Feedback == right.Feedback && sameReviewRefs(left.EvidenceRefs, right.EvidenceRefs)
}

func sameReviewRefs(left, right []string) bool {
	return slices.Equal(left, right)
}

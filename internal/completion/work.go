package completion

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/dominicnunez/agentos/internal/core"
)

const (
	maximumWorkCriteria  = 256
	maximumWorkTasks     = 4096
	maximumTaskArtifacts = 1024
)

var ErrInvalidWorkEvidence = errors.New("work completion evidence is invalid")

// WorkTaskEvidence binds one terminal Task projection to the trusted
// completion decision that preceded it. Neither reference is supplied by the
// worker whose output was evaluated.
type WorkTaskEvidence struct {
	TaskID               core.ID  `json:"task_id"`
	TaskVersion          int      `json:"task_version"`
	VerificationEventRef string   `json:"verification_event_ref"`
	CompletionEventRef   string   `json:"completion_event_ref"`
	ArtifactRefs         []string `json:"artifact_refs"`
}

// WorkEvidence is the runtime-owned terminal proof for one Work. It binds the
// accepted Intent and immutable Plan to every independently verified Task.
// The record is evidence, not authority, and cannot grant capabilities or
// approve effects.
type WorkEvidence struct {
	WorkID            core.ID            `json:"work_id"`
	WorkVersion       int                `json:"work_version"`
	IntentID          core.ID            `json:"intent_id"`
	IntentFingerprint string             `json:"intent_fingerprint"`
	PlanID            core.ID            `json:"plan_id"`
	PlanVersion       int                `json:"plan_version"`
	Criteria          []core.IntentValue `json:"criteria"`
	Tasks             []WorkTaskEvidence `json:"tasks"`
	ArtifactRefs      []string           `json:"artifact_refs"`
	CreatedAt         time.Time          `json:"created_at"`
	Fingerprint       string             `json:"fingerprint"`
}

func NewWorkEvidence(work core.Work, workVersion int, intent core.Intent, plan core.Plan, tasks []WorkTaskEvidence, createdAt time.Time) (WorkEvidence, error) {
	record := WorkEvidence{
		WorkID: work.ID, WorkVersion: workVersion,
		IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint,
		PlanID: plan.ID, PlanVersion: plan.Version,
		Criteria:  append([]core.IntentValue(nil), intent.CompletionCriteria...),
		Tasks:     cloneWorkTaskEvidence(tasks),
		CreatedAt: createdAt.UTC(),
	}
	sort.Slice(record.Tasks, func(i, j int) bool { return record.Tasks[i].TaskID < record.Tasks[j].TaskID })
	record.ArtifactRefs = workArtifactRefs(record.Tasks)
	fingerprint, err := record.expectedFingerprint()
	if err != nil {
		return WorkEvidence{}, err
	}
	record.Fingerprint = fingerprint
	if !record.Valid() {
		return WorkEvidence{}, ErrInvalidWorkEvidence
	}
	return record, nil
}

func (r WorkEvidence) Valid() bool {
	_, offset := r.CreatedAt.Zone()
	if r.WorkID == "" || r.WorkVersion < 1 || r.IntentID == "" || !validDigest(r.IntentFingerprint) || r.PlanID == "" || r.PlanVersion < 1 || r.CreatedAt.IsZero() || offset != 0 {
		return false
	}
	if len(r.Criteria) == 0 || len(r.Criteria) > maximumWorkCriteria || len(r.Tasks) == 0 || len(r.Tasks) > maximumWorkTasks {
		return false
	}
	for _, criterion := range r.Criteria {
		if strings.TrimSpace(criterion.Value) == "" || criterion.Origin == "" || !utf8.ValidString(criterion.Value) || !utf8.ValidString(criterion.Origin) || !utf8.ValidString(criterion.SourceMessageID) {
			return false
		}
	}
	for index, task := range r.Tasks {
		if task.TaskID == "" || task.TaskVersion < 1 || !validRef(task.VerificationEventRef) || !validRef(task.CompletionEventRef) || task.VerificationEventRef == task.CompletionEventRef || len(task.ArtifactRefs) > maximumTaskArtifacts {
			return false
		}
		if index > 0 && r.Tasks[index-1].TaskID >= task.TaskID {
			return false
		}
		if !distinctNonEmpty(task.ArtifactRefs) {
			return false
		}
	}
	if !slices.Equal(r.ArtifactRefs, workArtifactRefs(r.Tasks)) {
		return false
	}
	expected, err := r.expectedFingerprint()
	return err == nil && validDigest(r.Fingerprint) && r.Fingerprint == expected
}

// MatchesCurrent proves that a previously persisted record still describes
// the exact current terminal state. CreatedAt and Fingerprint are validated by
// Valid and are intentionally not regenerated on recovery.
func (r WorkEvidence) MatchesCurrent(work core.Work, workVersion int, intent core.Intent, plan core.Plan, tasks []WorkTaskEvidence) bool {
	if !r.Valid() || r.WorkID != work.ID || r.WorkVersion != workVersion || r.IntentID != intent.ID || r.IntentFingerprint != intent.AcceptedFingerprint || r.PlanID != plan.ID || r.PlanVersion != plan.Version || !reflect.DeepEqual(r.Criteria, intent.CompletionCriteria) {
		return false
	}
	expectedTasks := cloneWorkTaskEvidence(tasks)
	sort.Slice(expectedTasks, func(i, j int) bool { return expectedTasks[i].TaskID < expectedTasks[j].TaskID })
	return reflect.DeepEqual(r.Tasks, expectedTasks) && slices.Equal(r.ArtifactRefs, workArtifactRefs(expectedTasks))
}

func (r WorkEvidence) expectedFingerprint() (string, error) {
	binding := r
	binding.Fingerprint = ""
	body, err := json.Marshal(binding)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:]), nil
}

func cloneWorkTaskEvidence(tasks []WorkTaskEvidence) []WorkTaskEvidence {
	cloned := make([]WorkTaskEvidence, len(tasks))
	for index, task := range tasks {
		cloned[index] = task
		cloned[index].ArtifactRefs = append([]string(nil), task.ArtifactRefs...)
	}
	return cloned
}

func workArtifactRefs(tasks []WorkTaskEvidence) []string {
	seen := make(map[string]struct{})
	for _, task := range tasks {
		for _, ref := range task.ArtifactRefs {
			seen[ref] = struct{}{}
		}
	}
	refs := make([]string, 0, len(seen))
	for ref := range seen {
		refs = append(refs, ref)
	}
	sort.Strings(refs)
	return refs
}

func distinctNonEmpty(values []string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !validRef(value) {
			return false
		}
		if _, exists := seen[value]; exists {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}

func validRef(value string) bool {
	return value != "" && len(value) <= 4096 && utf8.ValidString(value)
}

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}

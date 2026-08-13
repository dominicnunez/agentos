package completion

import (
	"errors"
	"reflect"
	"slices"
	"sort"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

var ErrInvalidWorkEvidence = errors.New("work completion evidence is invalid")

// WorkTaskEvidence binds one terminal Task projection to the trusted
// completion decision that preceded it. Neither reference is supplied by the
// worker whose output was evaluated.
type WorkTaskEvidence = events.WorkCompletionTaskEvidencePayload

// WorkEvidence is the runtime-owned terminal proof for one Work. It binds the
// accepted Intent and immutable Plan to every independently verified Task.
// The record is evidence, not authority, and cannot grant capabilities or
// approve effects.
type WorkEvidence events.WorkCompletionEvidencePayload

func NewWorkEvidence(work core.Work, workVersion int, intent core.Intent, plan core.Plan, tasks []WorkTaskEvidence, createdAt time.Time) (WorkEvidence, error) {
	record := WorkEvidence{
		WorkID: work.ID, WorkVersion: workVersion, GoalID: work.GoalID,
		IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint,
		PlanID: plan.ID, PlanVersion: plan.Version,
		Criteria:  append([]core.IntentValue(nil), intent.CompletionCriteria...),
		Tasks:     cloneWorkTaskEvidence(tasks),
		CreatedAt: createdAt.UTC(),
	}
	sort.Slice(record.Tasks, func(i, j int) bool { return record.Tasks[i].TaskID < record.Tasks[j].TaskID })
	record.ArtifactRefs = events.WorkCompletionArtifactRefs(record.Tasks)
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
	return events.WorkCompletionEvidencePayload(r).Valid()
}

// MatchesCurrent proves that a previously persisted record still describes
// the exact current terminal state. CreatedAt and Fingerprint are validated by
// Valid and are intentionally not regenerated on recovery.
func (r WorkEvidence) MatchesCurrent(work core.Work, workVersion int, intent core.Intent, plan core.Plan, tasks []WorkTaskEvidence) bool {
	if !r.Valid() || r.WorkID != work.ID || r.WorkVersion != workVersion || r.GoalID != work.GoalID || r.IntentID != intent.ID || r.IntentFingerprint != intent.AcceptedFingerprint || r.PlanID != plan.ID || r.PlanVersion != plan.Version || !reflect.DeepEqual(r.Criteria, intent.CompletionCriteria) {
		return false
	}
	expectedTasks := cloneWorkTaskEvidence(tasks)
	sort.Slice(expectedTasks, func(i, j int) bool { return expectedTasks[i].TaskID < expectedTasks[j].TaskID })
	return reflect.DeepEqual(r.Tasks, expectedTasks) && slices.Equal(r.ArtifactRefs, events.WorkCompletionArtifactRefs(expectedTasks))
}

func (r WorkEvidence) expectedFingerprint() (string, error) {
	return events.WorkCompletionEvidencePayload(r).ExpectedFingerprint()
}

func cloneWorkTaskEvidence(tasks []WorkTaskEvidence) []WorkTaskEvidence {
	cloned := make([]WorkTaskEvidence, len(tasks))
	for index, task := range tasks {
		cloned[index] = task
		cloned[index].ArtifactRefs = append([]string(nil), task.ArtifactRefs...)
	}
	return cloned
}

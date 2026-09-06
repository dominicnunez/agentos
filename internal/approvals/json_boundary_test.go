package approvals_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/approvals"
	"github.com/dominicnunez/agentos/internal/boundaryjson"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

type rawApprovalStore struct {
	latestInboxStore
	body []byte
}

func (s rawApprovalStore) Records(context.Context, string, string) ([][]byte, error) {
	return [][]byte{s.body}, nil
}

func (s rawApprovalStore) PendingApprovalRecords(context.Context, string, time.Time, int) ([][]byte, error) {
	return [][]byte{s.body}, nil
}

func (s rawApprovalStore) RecentEvents(context.Context, string, string, int) ([]events.Event, error) {
	return []events.Event{{Payload: s.body}}, nil
}

func TestApprovalReadRejectsAmbiguousJSON(t *testing.T) {
	for _, body := range []string{
		`{"id":"approval-1","status":"DENIED","status":"APPROVED"}`,
		`{"id":"approval-1","STATUS":"APPROVED"}`,
		`{"id":"approval-1","unknown_authority":true}`,
		"{\"id\":\"\xff\"}",
		`{"id":"approval-1"} {}`,
	} {
		service := approvals.New(rawApprovalStore{body: []byte(body)}, nil, nil)
		_, loadErr := service.Get(t.Context(), "approval-1")
		_, inboxErr := service.PendingDecisionContexts(t.Context(), "org-1", "human-1")
		_, historyErr := service.RecentDecisionContexts(t.Context(), "org-1", "human-1", 10)
		for name, err := range map[string]error{"load": loadErr, "inbox": inboxErr, "history": historyErr} {
			if !errors.Is(err, boundaryjson.ErrInvalid) && !errors.Is(err, boundaryjson.ErrSchema) {
				t.Errorf("%s did not reject malformed JSON at decoder: %v", name, err)
			}
		}
	}
}

func TestApprovalReadPreservesCanonicalRecord(t *testing.T) {
	want := core.HumanApproval{ID: "approval-1", OrganizationID: "org-1", TaskID: "task-1", Status: core.ApprovalDenied}
	body, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := approvals.New(rawApprovalStore{body: body}, nil, nil).Get(t.Context(), want.ID)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("canonical approval changed: %+v, %v", got, err)
	}
}

package audit

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
)

type reader struct{ events []events.Event }

func (r reader) Events(context.Context, string) ([]events.Event, error) { return r.events, nil }
func TestFindsCompletionWithoutVerification(t *testing.T) {
	f, e := New(reader{[]events.Event{{EventID: "e", Sequence: 1, EventType: "TASK_VERIFIED_COMPLETE", TaskID: "t"}}}).Run(context.Background())
	if e != nil || len(f) != 1 {
		t.Fatalf("findings=%v err=%v", f, e)
	}
}

func TestRunAtProducesRepeatableSingleTimestampFindings(t *testing.T) {
	observedAt := time.Date(2026, 8, 26, 22, 15, 0, 123, time.FixedZone("audit", -5*60*60))
	service := New(reader{[]events.Event{
		{Sequence: 0, EventType: "BROKEN"},
		{EventID: "event-2", Sequence: 2, EventType: "TASK_VERIFIED_COMPLETE", TaskID: "task-1"},
	}})
	first, err := service.RunAt(context.Background(), observedAt)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.RunAt(context.Background(), observedAt)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || !reflect.DeepEqual(first, second) {
		t.Fatalf("audit replay changed findings: first=%+v second=%+v", first, second)
	}
	want := observedAt.UTC()
	for _, finding := range first {
		if !finding.CreatedAt.Equal(want) || finding.CreatedAt.Location() != time.UTC {
			t.Fatalf("finding timestamp=%v want=%v", finding.CreatedAt, want)
		}
	}
	firstJSON, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	secondJSON, err := json.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	if string(firstJSON) != string(secondJSON) {
		t.Fatalf("serialized audit replay changed: first=%s second=%s", firstJSON, secondJSON)
	}
}

func TestRunCapturesClockOnceAndRunAtRejectsMissingTime(t *testing.T) {
	service := New(reader{[]events.Event{
		{Sequence: 0, EventType: "BROKEN"},
		{EventID: "event-2", Sequence: 2, EventType: "TASK_VERIFIED_COMPLETE", TaskID: "task-1"},
	}})
	want := time.Date(2026, 8, 26, 22, 30, 0, 0, time.UTC)
	calls := 0
	service.now = func() time.Time {
		calls++
		return want.Add(time.Duration(calls-1) * time.Hour)
	}
	findings, err := service.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("audit pass read its clock %d times", calls)
	}
	for _, finding := range findings {
		if !finding.CreatedAt.Equal(want) {
			t.Fatalf("finding timestamp=%v want=%v", finding.CreatedAt, want)
		}
	}
	if _, err := service.RunAt(context.Background(), time.Time{}); err == nil {
		t.Fatal("audit accepted a missing observation time")
	}
}

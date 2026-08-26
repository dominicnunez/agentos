package audit

import (
	"context"
	"github.com/dominicnunez/agentos/internal/events"
	"testing"
)

type reader struct{ events []events.Event }

func (r reader) Events(context.Context, string) ([]events.Event, error) { return r.events, nil }
func TestFindsCompletionWithoutVerification(t *testing.T) {
	f, e := New(reader{[]events.Event{{EventID: "e", Sequence: 1, EventType: "TASK_VERIFIED_COMPLETE", TaskID: "t"}}}).Run(context.Background())
	if e != nil || len(f) != 1 {
		t.Fatalf("findings=%v err=%v", f, e)
	}
}

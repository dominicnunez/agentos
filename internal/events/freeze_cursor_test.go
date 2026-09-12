package events

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestFreezeCursorAppendMatchesFullReplay(t *testing.T) {
	states := []core.FreezeState{
		cursorFreezeState(true, "hold", 1, "", 0),
		cursorFreezeState(false, "release", 2, "freeze-1", 1),
		cursorFreezeState(true, "hold again", 3, "freeze-2", 2),
	}
	stream, records := freezeHistory(t, states)
	cursor, prefix, err := ResolveFreezeHistory(stream[:2], records[:2])
	if err != nil {
		t.Fatal(err)
	}
	cursor, suffix, err := cursor.Append(stream[2:], records[2:])
	if err != nil {
		t.Fatal(err)
	}
	_, full, err := ResolveAuthorityAdmissions(stream, records)
	if err != nil {
		t.Fatal(err)
	}
	incremental := append(append([]OrganizationFreezeAdmission(nil), prefix...), suffix...)
	if !reflect.DeepEqual(incremental, full) {
		t.Fatalf("incremental admissions differ from full replay:\nincremental=%+v\nfull=%+v", incremental, full)
	}

	// Mutating a returned view cannot change the proof retained by the cursor.
	suffix[0].Control.ActorID = "attacker"
	fourthState := cursorFreezeState(false, "final release", 4, "freeze-3", 3)
	fourthStream, fourthRecords := freezeHistory(t, []core.FreezeState{states[0], states[1], states[2], fourthState})
	_, fourth, err := cursor.Append(fourthStream[3:], fourthRecords[3:])
	if err != nil || len(fourth) != 1 || fourth[0].Version != 4 || fourth[0].Frozen {
		t.Fatalf("returned admission mutated cursor proof: admissions=%+v err=%v", fourth, err)
	}
}

func TestFreezeCursorPreservesLegacyPrefixThenRequiresControl(t *testing.T) {
	legacyHold := core.FreezeState{OrganizationID: "org-1", Frozen: true, Reason: "legacy hold", UpdatedAt: time.Unix(1, 0).UTC()}
	legacyRelease := core.FreezeState{OrganizationID: "org-1", Frozen: false, Reason: "legacy release", UpdatedAt: time.Unix(2, 0).UTC()}
	controlled := cursorFreezeState(true, "controlled hold", 3, "freeze-2", 2)
	stream, records := freezeHistory(t, []core.FreezeState{legacyHold, legacyRelease, controlled})
	cursor, legacy, err := ResolveFreezeHistory(stream[:2], records[:2])
	if err != nil || len(legacy) != 2 || legacy[0].Control != nil || legacy[1].Control != nil {
		t.Fatalf("legacy prefix rejected or rewritten: admissions=%+v err=%v", legacy, err)
	}
	cursor, admitted, err := cursor.Append(stream[2:], records[2:])
	if err != nil || len(admitted) != 1 || admitted[0].Control == nil {
		t.Fatalf("controlled suffix after legacy prefix rejected: admissions=%+v err=%v", admitted, err)
	}

	downgrade := core.FreezeState{OrganizationID: "org-1", Frozen: false, Reason: "missing control", UpdatedAt: time.Unix(4, 0).UTC()}
	downgradeStream, downgradeRecords := freezeHistory(t, []core.FreezeState{legacyHold, legacyRelease, controlled, downgrade})
	if _, _, err := cursor.Append(downgradeStream[3:], downgradeRecords[3:]); err == nil {
		t.Fatal("controlled history returned to legacy evidence")
	}
}

func TestFreezeCursorRejectsCorruptSuffixWithoutChangingPrefix(t *testing.T) {
	first := cursorFreezeState(true, "hold", 1, "", 0)
	second := cursorFreezeState(false, "release", 2, "freeze-1", 1)
	stream, records := freezeHistory(t, []core.FreezeState{first, second})
	cursor, _, err := ResolveFreezeHistory(stream[:1], records[:1])
	if err != nil {
		t.Fatal(err)
	}
	goodEvent, goodRecord := stream[1], records[1]

	tests := []struct {
		name   string
		mutate func(*[]Event, *[]AuthorityRecord)
	}{
		{name: "wrong prior event", mutate: func(events *[]Event, records *[]AuthorityRecord) {
			state := cloneFreezeState(second)
			state.Control.PriorEventRef = "wrong-event"
			setFreezePairBody(t, &(*events)[0], &(*records)[0], state)
		}},
		{name: "wrong prior version", mutate: func(events *[]Event, records *[]AuthorityRecord) {
			state := cloneFreezeState(second)
			state.Control.PriorVersion = 0
			setFreezePairBody(t, &(*events)[0], &(*records)[0], state)
		}},
		{name: "time reversal", mutate: func(events *[]Event, records *[]AuthorityRecord) {
			state := second
			state.UpdatedAt = first.UpdatedAt
			setFreezePairBody(t, &(*events)[0], &(*records)[0], state)
		}},
		{name: "noncontiguous record", mutate: func(_ *[]Event, records *[]AuthorityRecord) {
			(*records)[0].Version = 3
		}},
		{name: "nonincreasing sequence", mutate: func(events *[]Event, _ *[]AuthorityRecord) {
			(*events)[0].Sequence = 1
		}},
		{name: "nonhuman control", mutate: func(events *[]Event, records *[]AuthorityRecord) {
			state := cloneFreezeState(second)
			state.Control.ActorKind = core.PrincipalAgent
			setFreezePairBody(t, &(*events)[0], &(*records)[0], state)
		}},
		{name: "envelope actor mismatch", mutate: func(events *[]Event, _ *[]AuthorityRecord) {
			(*events)[0].SourceActorID = "attacker"
		}},
		{name: "body mismatch", mutate: func(events *[]Event, _ *[]AuthorityRecord) {
			(*events)[0].Payload = json.RawMessage(`{}`)
		}},
		{name: "orphan event", mutate: func(events *[]Event, _ *[]AuthorityRecord) {
			orphan := (*events)[0]
			orphan.EventID = "orphan-freeze"
			orphan.Sequence++
			*events = append(*events, orphan)
		}},
		{name: "duplicate suffix event", mutate: func(events *[]Event, _ *[]AuthorityRecord) {
			*events = append(*events, (*events)[0])
		}},
		{name: "duplicate prefix event", mutate: func(events *[]Event, records *[]AuthorityRecord) {
			(*events)[0].EventID = "freeze-1"
			(*records)[0].AdmissionEventID = "freeze-1"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tailEvents := []Event{goodEvent}
			tailRecords := []AuthorityRecord{goodRecord}
			test.mutate(&tailEvents, &tailRecords)
			if _, _, err := cursor.Append(tailEvents, tailRecords); err == nil {
				t.Fatal("corrupt suffix was accepted")
			}
			_, admissions, err := cursor.Append([]Event{goodEvent}, []AuthorityRecord{goodRecord})
			if err != nil || len(admissions) != 1 || admissions[0].EventRef != "freeze-2" {
				t.Fatalf("failed append mutated prefix cursor: admissions=%+v err=%v", admissions, err)
			}
		})
	}
}

func TestFreezeCursorSupportsIndependentBranches(t *testing.T) {
	first := cursorFreezeState(true, "hold", 1, "", 0)
	prefixStream, prefixRecords := freezeHistory(t, []core.FreezeState{first})
	cursor, _, err := ResolveFreezeHistory(prefixStream, prefixRecords)
	if err != nil {
		t.Fatal(err)
	}

	release := cursorFreezeState(false, "release", 2, "freeze-1", 1)
	releaseStream, releaseRecords := freezeHistory(t, []core.FreezeState{first, release})
	_, releaseAdmissions, err := cursor.Append(releaseStream[1:], releaseRecords[1:])
	if err != nil || len(releaseAdmissions) != 1 || releaseAdmissions[0].Frozen {
		t.Fatalf("release branch failed: admissions=%+v err=%v", releaseAdmissions, err)
	}

	renew := cursorFreezeState(true, "renew hold", 2, "freeze-1", 1)
	renewStream, renewRecords := freezeHistory(t, []core.FreezeState{first, renew})
	renewStream[1].EventID = "freeze-2-renew"
	renewRecords[1].AdmissionEventID = "freeze-2-renew"
	_, renewAdmissions, err := cursor.Append(renewStream[1:], renewRecords[1:])
	if err != nil || len(renewAdmissions) != 1 || !renewAdmissions[0].Frozen || renewAdmissions[0].EventRef != "freeze-2-renew" {
		t.Fatalf("sibling branch inherited mutation: admissions=%+v err=%v", renewAdmissions, err)
	}
}

func cursorFreezeState(frozen bool, reason string, version int, priorEvent string, priorVersion int) core.FreezeState {
	return core.FreezeState{
		OrganizationID: "org-1",
		Frozen:         frozen,
		Reason:         reason,
		UpdatedAt:      time.Unix(int64(version), 0).UTC(),
		Control: &core.FreezeEvidence{
			ActorID:       "owner-1",
			ActorKind:     core.PrincipalHuman,
			PriorEventRef: priorEvent,
			PriorVersion:  priorVersion,
		},
	}
}

func cloneFreezeState(state core.FreezeState) core.FreezeState {
	if state.Control != nil {
		control := *state.Control
		state.Control = &control
	}
	return state
}

func setFreezePairBody(t *testing.T, event *Event, record *AuthorityRecord, state core.FreezeState) {
	t.Helper()
	body, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	event.Payload = body
	record.Body = body
}

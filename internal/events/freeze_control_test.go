package events

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func freezeHistory(t *testing.T, states []core.FreezeState) ([]Event, []AuthorityRecord) {
	t.Helper()
	var stream []Event
	var records []AuthorityRecord
	for i, state := range states {
		body, err := json.Marshal(state)
		if err != nil {
			t.Fatal(err)
		}
		id := fmt.Sprintf("freeze-%d", i+1)
		stream = append(stream, Event{EventID: id, Sequence: int64(i + 1), OrganizationID: "org-1", EventType: "FREEZE_SET", SourceActorID: "owner-1", Payload: body, CreatedAt: state.UpdatedAt, SchemaVersion: SchemaVersion})
		records = append(records, AuthorityRecord{Kind: authorityKindFreeze, RecordID: "org-1", Version: i + 1, Body: body, AdmissionEventID: id})
	}
	return stream, records
}

func TestFreezeControlHistory(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy-%v", legacy), func(t *testing.T) {
			first := core.FreezeState{OrganizationID: "org-1", Frozen: true, UpdatedAt: time.Unix(1, 0).UTC(), Control: &core.FreezeEvidence{ActorID: "owner-1", ActorKind: core.PrincipalHuman}}
			if legacy {
				first.Control = nil
			}
			last := core.FreezeState{OrganizationID: "org-1", Frozen: false, UpdatedAt: time.Unix(2, 0).UTC(), Control: &core.FreezeEvidence{ActorID: "owner-1", ActorKind: core.PrincipalHuman, PriorEventRef: "freeze-1", PriorVersion: 1}}
			stream, records := freezeHistory(t, []core.FreezeState{first, last})
			_, freezes, err := ResolveAuthorityAdmissions(stream, records)
			if err != nil {
				t.Fatal(err)
			}
			if len(freezes) != 2 || freezes[1].Frozen || freezes[1].EventRef != "freeze-2" {
				t.Fatalf("incorrect release: %+v", freezes)
			}
		})
	}
}

func TestFreezeControlRejectsFalseEvidence(t *testing.T) {
	for _, name := range []string{"actor", "kind", "prior-event", "prior-version", "downgrade", "release-without-hold", "task", "authorization"} {
		t.Run(name, func(t *testing.T) {
			first := core.FreezeState{OrganizationID: "org-1", Frozen: true, UpdatedAt: time.Unix(1, 0).UTC(), Control: &core.FreezeEvidence{ActorID: "owner-1", ActorKind: core.PrincipalHuman}}
			last := core.FreezeState{OrganizationID: "org-1", Frozen: false, UpdatedAt: time.Unix(2, 0).UTC(), Control: &core.FreezeEvidence{ActorID: "owner-1", ActorKind: core.PrincipalHuman, PriorEventRef: "freeze-1", PriorVersion: 1}}
			switch name {
			case "actor":
				last.Control.ActorID = "another-owner"
			case "kind":
				last.Control.ActorKind = core.PrincipalAgent
			case "prior-event":
				last.Control.PriorEventRef = "another-hold"
			case "prior-version":
				last.Control.PriorVersion = 2
			case "downgrade":
				last.Control = nil
			case "release-without-hold":
				first.Frozen = false
			}
			stream, records := freezeHistory(t, []core.FreezeState{first, last})
			if name == "task" {
				stream[1].TaskID = "task-1"
			}
			if name == "authorization" {
				stream[1].AuthorizationRefs = []string{"a-lease"}
			}
			if _, _, err := ResolveAuthorityAdmissions(stream, records); err == nil {
				t.Fatal("invalid owner control history accepted")
			}
		})
	}
}

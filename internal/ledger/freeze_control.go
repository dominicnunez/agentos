package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/dominicnunez/agentos/internal/authority"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func (l *SQLite) ReadFreeze(ctx context.Context, organization core.ID) (authority.FreezeSnapshot, error) {
	if organization == "" {
		return authority.FreezeSnapshot{}, authority.ErrFreezeInvalid
	}
	view, err := l.prepareFreeze(ctx, string(organization))
	if err != nil {
		return authority.FreezeSnapshot{}, err
	}
	snapshot, _, _, err := freezeSnapshot(view.history, organization)
	return snapshot, err
}

func (l *SQLite) SetFreeze(ctx context.Context, organization, actorID core.ID, actorKind core.PrincipalKind, change authority.FreezeChange) (authority.FreezeSnapshot, error) {
	if actorID == "" || actorKind != core.PrincipalHuman {
		return authority.FreezeSnapshot{}, authority.ErrFreezeUnauthorized
	}
	if organization == "" || change.ExpectedVersion < 0 ||
		(change.ExpectedVersion == 0) != (change.ExpectedEventRef == "") ||
		len(change.Reason) > 2048 || !utf8.ValidString(change.Reason) || strings.ContainsRune(change.Reason, '\x00') {
		return authority.FreezeSnapshot{}, authority.ErrFreezeInvalid
	}
	l.live.mu.Lock()
	defer l.live.mu.Unlock()
	var result authority.FreezeSnapshot
	var hold *core.SecurityHoldCause
	var committed *freezeView
	err := l.withTx(ctx, func(tx *sql.Tx) error {
		view, err := l.freezeView(ctx, tx, string(organization), true)
		if err != nil {
			return err
		}
		current, prior, _, err := freezeSnapshot(view.history, organization)
		if err != nil {
			return err
		}
		// A retry of the last committed command returns its original evidence.
		// Once any newer state exists, the same predecessor no longer matches.
		if proof := current.State.Control; proof != nil &&
			proof.ActorID == actorID && proof.ActorKind == actorKind &&
			proof.PriorVersion == change.ExpectedVersion && proof.PriorEventRef == change.ExpectedEventRef &&
			current.State.Frozen == change.Frozen && current.State.Reason == change.Reason {
			result = current
			committed = view
			return nil
		}
		if current.Version != change.ExpectedVersion || current.EventRef != change.ExpectedEventRef ||
			!change.Frozen && !current.State.Frozen {
			return authority.ErrFreezeConflict
		}
		updated := l.nowUTC()
		if !updated.After(current.State.UpdatedAt) {
			updated = current.State.UpdatedAt.Add(time.Nanosecond)
		}
		state := core.FreezeState{OrganizationID: organization, Frozen: change.Frozen, Reason: change.Reason, UpdatedAt: updated,
			Control: &core.FreezeEvidence{ActorID: actorID, ActorKind: actorKind, PriorVersion: current.Version, PriorEventRef: current.EventRef}}
		body, err := json.Marshal(state)
		if err != nil {
			return err
		}
		record := events.AuthorityRecord{Kind: "organization_freeze", RecordID: string(organization), Version: current.Version + 1, Body: body}
		draft := events.TrustedDraft{OrganizationID: string(organization), EventType: "FREEZE_SET", SourceActorID: string(actorID), Payload: json.RawMessage(body)}
		if err := events.ValidateAuthorityRecordDraft(draft, record.Kind, record.RecordID, record.Version, body); err != nil {
			return err
		}
		if err := events.ValidateFreezeRecord(record, prior); err != nil {
			return err
		}
		if err := appendRecord(ctx, tx, draft, record.Kind, record.RecordID, record.Version, body); err != nil {
			return err
		}
		// The already validated prefix is private until commit, including when
		// this was a cold read. Validate the one appended revision directly.
		committed, err = l.appendFreezeView(ctx, tx, view)
		if err != nil {
			return err
		}
		var event events.Event
		result, _, event, err = freezeSnapshot(committed.history, organization)
		if err != nil {
			return err
		}
		if state.Frozen {
			hold = &core.SecurityHoldCause{OrganizationID: organization, EventRef: event.EventID, Sequence: event.Sequence}
		}
		return nil
	})
	if err != nil {
		return authority.FreezeSnapshot{}, err
	}
	l.freezes.put(committed)
	if hold != nil {
		l.cancelOrganizationLocked(string(organization), *hold)
	}
	return result, nil
}

func readFreeze(ctx context.Context, tx *sql.Tx, organization core.ID) (authority.FreezeSnapshot, events.AuthorityRecord, events.Event, error) {
	history, err := loadFreezeHistory(ctx, tx, string(organization))
	if err != nil {
		return authority.FreezeSnapshot{}, events.AuthorityRecord{}, events.Event{}, err
	}
	return freezeSnapshot(history, organization)
}

func freezeSnapshot(history freezeHistory, organization core.ID) (authority.FreezeSnapshot, events.AuthorityRecord, events.Event, error) {
	revision, found := history.latest()
	record, event := revision.record, revision.event
	if !found {
		return authority.FreezeSnapshot{State: core.FreezeState{OrganizationID: organization}}, record, event, nil
	}
	state := revision.state
	if state.OrganizationID != organization || event.OrganizationID != string(organization) {
		return authority.FreezeSnapshot{}, record, event, fmt.Errorf("freeze state crosses its organization")
	}
	if state.Control != nil {
		proof := *state.Control
		state.Control = &proof
	}
	return authority.FreezeSnapshot{State: state, EventRef: event.EventID, Version: record.Version}, record, event, nil
}

package ledger

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/dominicnunez/agentos/internal/core"
)

type freezeView struct {
	generation []byte
	rewrite    []byte
	schema     int
	history    freezeHistory
}

type freezeCacheEntry struct {
	view  *freezeView
	users int
	used  uint64
}

// Active organizations keep one shared proof. Retain only a small number of
// inactive organizations; release removes the live pin when work finishes.
type freezeCache struct {
	mu     sync.Mutex
	items  map[string]*freezeCacheEntry
	clock  uint64
	schema int
}

func (cache *freezeCache) entry(organization string) *freezeCacheEntry {
	if cache.items == nil {
		cache.items = make(map[string]*freezeCacheEntry)
	}
	entry := cache.items[organization]
	if entry == nil {
		entry = &freezeCacheEntry{}
		cache.items[organization] = entry
	}
	cache.clock++
	entry.used = cache.clock
	return entry
}

func (cache *freezeCache) retain(organization string) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.entry(organization).users++
}

func (cache *freezeCache) release(organization string) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if entry := cache.items[organization]; entry != nil && entry.users > 0 {
		entry.users--
	}
	cache.trim()
}

func (cache *freezeCache) trim() {
	for {
		var oldest string
		var age uint64
		inactive := 0
		for organization, entry := range cache.items {
			if entry.users == 0 {
				inactive++
				if oldest == "" || entry.used < age {
					oldest, age = organization, entry.used
				}
			}
		}
		if inactive <= 8 {
			return
		}
		delete(cache.items, oldest)
	}
}

func (cache *freezeCache) get(organization string) *freezeView {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if entry := cache.items[organization]; entry != nil {
		cache.clock++
		entry.used = cache.clock
		return entry.view
	}
	return nil
}

func (cache *freezeCache) put(view *freezeView) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.entry(view.history.organization).view = view
	cache.trim()
}

func (l *SQLite) freezeSchema(ctx context.Context, tx *sql.Tx, version int) error {
	l.freezes.mu.Lock()
	verified := l.freezes.schema == version && version != 0
	l.freezes.mu.Unlock()
	if verified {
		return nil
	}
	if _, err := validateStorageContract(ctx, tx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	l.freezes.mu.Lock()
	l.freezes.schema = version
	l.freezes.mu.Unlock()
	return nil
}

// The tokens and records are always read in the same transaction. A matching
// generation proves the immutable cached chain still describes this snapshot;
// an unchanged rewrite token permits validating only an appended suffix.
func (l *SQLite) freezeView(ctx context.Context, tx *sql.Tx, organization string, allowFull bool) (*freezeView, error) {
	view, err := l.freezeHeader(ctx, tx, organization)
	if err != nil {
		return nil, err
	}
	prior := l.freezes.get(organization)
	if prior != nil && prior.schema == view.schema {
		if bytes.Equal(prior.generation, view.generation) && bytes.Equal(prior.rewrite, view.rewrite) {
			return prior, ctx.Err()
		}
		if sameFreezePrefix(prior, view) {
			appended, err := fillFreezeTail(ctx, tx, organization, prior, view)
			if err != nil {
				return nil, err
			}
			// An older transaction may observe an earlier generation than the
			// shared cache. Empty tails cannot prove that newer cached prefix.
			if appended {
				return view, ctx.Err()
			}
		}
	}
	if !allowFull {
		return nil, core.ErrContainmentUnavailable
	}
	view.history, err = loadFreezeHistory(ctx, tx, organization)
	if err != nil {
		return nil, err
	}
	if len(view.generation) == 0 && len(view.history.revisions) != 0 {
		return nil, fmt.Errorf("freeze history lacks its observation tokens")
	}
	return view, ctx.Err()
}

func (l *SQLite) freezeHeader(ctx context.Context, tx *sql.Tx, organization string) (*freezeView, error) {
	view := &freezeView{}
	if err := tx.QueryRowContext(ctx, "PRAGMA schema_version").Scan(&view.schema); err != nil {
		return nil, err
	}
	if err := l.freezeSchema(ctx, tx, view.schema); err != nil {
		return nil, fmt.Errorf("freeze observation schema: %w", err)
	}
	err := tx.QueryRowContext(ctx, `SELECT generation,rewrite_generation FROM freeze_changes WHERE organization_id=?`, organization).Scan(&view.generation, &view.rewrite)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if err == nil && (len(view.generation) != 32 || len(view.rewrite) != 32) {
		return nil, fmt.Errorf("freeze observation tokens are invalid")
	}
	return view, ctx.Err()
}

func sameFreezePrefix(prior, view *freezeView) bool {
	// An empty validated prefix has no records to rewrite. Its suffix query
	// starts at zero and still validates every record and event now present.
	return len(view.generation) != 0 && (len(prior.history.revisions) == 0 || bytes.Equal(prior.rewrite, view.rewrite))
}

func fillFreezeTail(ctx context.Context, tx *sql.Tx, organization string, prior, view *freezeView) (bool, error) {
	last, found := prior.history.latest()
	var version int
	var sequence int64
	if found {
		version, sequence = last.record.Version, last.event.Sequence
	}
	records, stream, err := readFreezeRows(ctx, tx, freezeTailSQL, organization, organization, sequence, organization, version, organization, sequence)
	if err != nil {
		return false, err
	}
	if len(records) == 0 && len(stream) == 0 {
		return false, nil
	}
	view.history, err = resolveFreezeRows(ctx, organization, records, stream, &prior.history)
	return err == nil, err
}

// The writer already validated this prefix in its own transaction. Build a
// candidate from the appended pair, but publish nothing until commit succeeds.
func (l *SQLite) appendFreezeView(ctx context.Context, tx *sql.Tx, prior *freezeView) (*freezeView, error) {
	organization := prior.history.organization
	view, err := l.freezeHeader(ctx, tx, organization)
	if err != nil {
		return nil, err
	}
	if prior.schema != view.schema || !sameFreezePrefix(prior, view) {
		return nil, fmt.Errorf("freeze prefix changed during append")
	}
	appended, err := fillFreezeTail(ctx, tx, organization, prior, view)
	if err != nil {
		return nil, err
	}
	if !appended {
		return nil, fmt.Errorf("freeze append lacks its durable admission")
	}
	return view, ctx.Err()
}

// Warm cold or rewritten history on the ordinary pool before registering live
// work. A long cold validation must not occupy the safety observation connection.
func (l *SQLite) prepareFreeze(ctx context.Context, organization string) (*freezeView, error) {
	var view *freezeView
	if l.freezes.get(organization) != nil {
		err := l.withContainmentSnapshot(ctx, func(tx *sql.Tx) error {
			var err error
			view, err = l.freezeView(ctx, tx, organization, false)
			return err
		})
		if err == nil {
			l.freezes.put(view)
			return view, nil
		}
		if !errors.Is(err, core.ErrContainmentUnavailable) {
			return nil, err
		}
	}
	err := l.withTx(ctx, func(tx *sql.Tx) error {
		var err error
		view, err = l.freezeView(ctx, tx, organization, true)
		return err
	})
	if err != nil {
		return nil, err
	}
	l.freezes.put(view)
	return view, nil
}

const freezeTailSQL = `SELECT r.kind,r.record_id,r.version,r.body,r.admission_event_id,
e.event_id,e.sequence,e.organization_id,e.event_type,e.source_actor_id,e.source_execution_id,e.recipient_scope,e.recipient_id,e.task_id,e.authorization_refs,e.artifact_refs,e.payload,e.correlation_id,e.created_at,e.schema_version
FROM events e
LEFT JOIN records r ON r.admission_event_id<>'' AND r.admission_event_id=e.event_id AND r.kind='organization_freeze' AND r.record_id=?
WHERE e.event_type='FREEZE_SET' AND e.organization_id=? AND e.sequence>?
UNION ALL
SELECT r.kind,r.record_id,r.version,r.body,r.admission_event_id,
e.event_id,e.sequence,e.organization_id,e.event_type,e.source_actor_id,e.source_execution_id,e.recipient_scope,e.recipient_id,e.task_id,e.authorization_refs,e.artifact_refs,e.payload,e.correlation_id,e.created_at,e.schema_version
FROM records r
LEFT JOIN events e ON e.event_id=r.admission_event_id
WHERE r.kind='organization_freeze' AND r.record_id=? AND r.version>?
AND (e.event_id IS NULL OR e.event_type<>'FREEZE_SET' OR e.organization_id<>? OR e.sequence<=?)
ORDER BY 3,7`

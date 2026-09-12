package ledger

import (
	"context"
	"database/sql"
	"sync"
)

type freezeScopeKey struct{}

// This scope belongs to one writer transaction. Uncommitted views never enter
// the live cache, and derived contexts cannot transfer proof to another tx.
type freezeReadScope struct {
	store *SQLite
	tx    *sql.Tx
	mu    sync.Mutex
	views map[string]*freezeView
}

func (l *SQLite) withFreezeTx(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	return l.withTx(ctx, func(tx *sql.Tx) error {
		return fn(l.freezeContext(ctx, tx), tx)
	})
}

func (l *SQLite) freezeContext(ctx context.Context, tx *sql.Tx) context.Context {
	scope := &freezeReadScope{store: l, tx: tx, views: make(map[string]*freezeView)}
	return context.WithValue(ctx, freezeScopeKey{}, scope)
}

func loadFreezeHistory(ctx context.Context, tx *sql.Tx, organization string) (freezeHistory, error) {
	if scope, ok := ctx.Value(freezeScopeKey{}).(*freezeReadScope); ok && scope.tx == tx {
		return scope.read(ctx, organization)
	}
	return readFreezeHistory(ctx, tx, organization)
}

func (scope *freezeReadScope) read(ctx context.Context, organization string) (freezeHistory, error) {
	scope.mu.Lock()
	defer scope.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return freezeHistory{}, err
	}
	prior := scope.views[organization]
	if prior == nil {
		prior = scope.store.freezes.get(organization)
	}
	// Tokens are still read on every use. A mutation in this transaction must
	// invalidate the old proof just as a commit from another connection does.
	view, err := scope.store.resolveFreezeView(ctx, scope.tx, organization, prior, true)
	if err != nil {
		return freezeHistory{}, err
	}
	scope.views[organization] = view
	return view.history, nil
}

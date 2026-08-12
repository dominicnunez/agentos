package gateway

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"
)

type localUserAccess struct {
	uid               int
	slots             chan struct{}
	requestsPerMinute int
	mu                sync.Mutex
	windowStart       time.Time
	requests          int
}

func newLocalUserAccess(uid, maxConcurrent, requestsPerMinute int) *localUserAccess {
	return &localUserAccess{
		uid:               uid,
		slots:             make(chan struct{}, maxConcurrent),
		requestsPerMinute: requestsPerMinute,
	}
}

func (a *localUserAccess) acquire(ctx context.Context) (func(), error) {
	uid, ok := ctx.Value(peerUIDContextKey{}).(int)
	if !ok || uid != a.uid {
		return nil, ErrOperatorUnauthorized
	}
	select {
	case a.slots <- struct{}{}:
	default:
		return nil, ErrOperatorLimited
	}
	now := time.Now().UTC()
	a.mu.Lock()
	if a.windowStart.IsZero() || now.Sub(a.windowStart) >= time.Minute {
		a.windowStart = now
		a.requests = 0
	}
	if a.requests >= a.requestsPerMinute {
		a.mu.Unlock()
		<-a.slots
		return nil, ErrOperatorLimited
	}
	a.requests++
	a.mu.Unlock()
	return func() { <-a.slots }, nil
}

func acquireLocalUserRequest(w http.ResponseWriter, r *http.Request, access *localUserAccess, limitedMessage string) (func(), bool) {
	release, err := access.acquire(r.Context())
	if errors.Is(err, ErrOperatorLimited) {
		w.Header().Set("Retry-After", "60")
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": limitedMessage})
		return nil, false
	}
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "local user owner required"})
		return nil, false
	}
	return release, true
}

package gateway

import (
	"errors"
	"testing"
)

func TestLocalUserAccessRequiresExactPeerUID(t *testing.T) {
	access := newLocalUserAccess(1000, 1, 2)
	for _, ctx := range []struct {
		name string
		uid  *int
	}{
		{name: "missing"},
		{name: "different", uid: intPointer(1001)},
	} {
		t.Run(ctx.name, func(t *testing.T) {
			requestContext := t.Context()
			if ctx.uid != nil {
				requestContext = ContextWithPeerUID(requestContext, *ctx.uid)
			}
			if _, err := access.acquire(requestContext); !errors.Is(err, ErrOperatorUnauthorized) {
				t.Fatalf("acquire error=%v", err)
			}
		})
	}

	release, err := access.acquire(ContextWithPeerUID(t.Context(), 1000))
	if err != nil {
		t.Fatal(err)
	}
	release()
}

func TestLocalUserAccessEnforcesConcurrencyAndRateLimits(t *testing.T) {
	access := newLocalUserAccess(1000, 1, 2)
	ctx := ContextWithPeerUID(t.Context(), 1000)
	firstRelease, err := access.acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := access.acquire(ctx); !errors.Is(err, ErrOperatorLimited) {
		t.Fatalf("concurrent acquire error=%v", err)
	}
	firstRelease()

	secondRelease, err := access.acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	secondRelease()
	if _, err := access.acquire(ctx); !errors.Is(err, ErrOperatorLimited) {
		t.Fatalf("rate-limited acquire error=%v", err)
	}
}

func intPointer(value int) *int { return &value }

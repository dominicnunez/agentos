package app

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
)

// ModelOperation owns an auxiliary model call through durable result admission.
// Its caller may leave after the stop grace period; the runtime still owns the
// actual call and its late accounting until WaitForStops can join them.
type ModelOperation struct {
	service    *Service
	ctx        context.Context
	cancel     context.CancelCauseFunc
	mu         sync.Mutex
	manifest   string
	started    bool
	stopping   bool
	returned   *events.ModelStopReturn
	writeErr   error
	changed    chan struct{}
	callDone   chan struct{}
	finished   chan struct{}
	ready      chan struct{}
	finishOnce sync.Once
}

func (s *Service) BeginModelOperation(ctx context.Context, organization string) (context.Context, *ModelOperation, error) {
	tracked, untrack, err := s.trackExecution(ctx)
	if err != nil {
		return nil, nil, err
	}
	live, release, err := s.gateway.BeginExecutionContext(tracked, organization)
	if err != nil {
		untrack()
		return nil, nil, err
	}
	live, cancel := context.WithCancelCause(live)
	op := &ModelOperation{service: s, ctx: live, cancel: cancel, changed: make(chan struct{}, 1), callDone: make(chan struct{}), finished: make(chan struct{}), ready: make(chan struct{})}
	go func() {
		defer untrack()
		defer release()
		defer cancel(nil)
		op.watch()
		// The caller owns result/failure admission until Finish, even if the
		// callback and stop writer have already returned.
		<-op.finished
	}()
	return live, op, nil
}

// BindContext is called immediately after the manifest transaction commits,
// including when cancellation arrived during that transaction.
func (op *ModelOperation) BindContext(ref string) {
	op.mu.Lock()
	op.manifest = ref
	op.mu.Unlock()
	select {
	case op.changed <- struct{}{}:
	default:
	}
}

// Finish ends result admission, not necessarily the locally running callback.
// On interruption it waits only for the bounded stop evidence attempt.
func (op *ModelOperation) Finish() error {
	op.finishOnce.Do(func() { close(op.finished) })
	<-op.ready
	op.mu.Lock()
	defer op.mu.Unlock()
	return op.writeErr
}

// CallModel fences callback dispatch against a stop, and releases the caller
// after bounded stop bookkeeping even when the callback ignores cancellation.
func CallModel[T any](op *ModelOperation, ctx context.Context, call func(context.Context) (T, *events.InferenceUsageRecordedPayload, error)) (T, error) {
	var zero T
	op.mu.Lock()
	if cause := executionStopCause(op.ctx, ctx, nil); cause != nil || op.stopping {
		if cause == nil {
			cause = core.ErrExecutionStopped
		}
		op.cancel(cause)
		op.mu.Unlock()
		<-op.ready
		return zero, errors.Join(cause, op.stopError())
	}
	op.started = true
	op.mu.Unlock()
	type result struct {
		value T
		err   error
	}
	results := make(chan result, 1)
	go func() {
		value, usage, err := call(ctx)
		returned := &events.ModelStopReturn{LocalState: "RETURNED", ReturnedAt: time.Now().UTC(), Usage: usage}
		if stopped, ok := execution.StopOutcome(err); ok {
			returned.ProviderStop = &core.ProviderStopEvidence{LocalTurnStopped: stopped.LocalTurnStopped, LocalProcessStopAttempted: stopped.LocalProcessStopAttempted, LocalProcessStopped: stopped.LocalProcessStopped, RemoteStatus: string(stopped.RemoteStatus)}
		}
		op.mu.Lock()
		op.returned = returned
		close(op.callDone)
		op.mu.Unlock()
		results <- result{value, err}
	}()
	select {
	case returned := <-results:
		if cause := executionStopCause(op.ctx, ctx, returned.err); cause != nil {
			op.cancel(cause)
			<-op.ready
			return zero, errors.Join(cause, op.stopError())
		}
		return returned.value, returned.err
	case <-ctx.Done():
		op.cancel(context.Cause(ctx))
	case <-op.ctx.Done():
	}
	<-op.ready
	return zero, errors.Join(context.Cause(op.ctx), context.Cause(ctx), op.stopError())
}

func (op *ModelOperation) stopError() error {
	op.mu.Lock()
	defer op.mu.Unlock()
	return op.writeErr
}

func (op *ModelOperation) watch() {
	select {
	case <-op.ctx.Done():
	case <-op.finished:
	}
	if context.Cause(op.ctx) == nil {
		close(op.ready)
		return
	}
	// BindContext and Finish disambiguate cancellation during manifest commit
	// from preparation which ended before any durable attempt existed.
	var manifest string
	for {
		op.mu.Lock()
		op.stopping = true
		manifest = op.manifest
		op.mu.Unlock()
		if manifest != "" {
			break
		}
		select {
		case <-op.changed:
		case <-op.finished:
			op.mu.Lock()
			manifest = op.manifest
			op.mu.Unlock()
			if manifest == "" {
				close(op.ready)
				return
			}
		}
		if manifest != "" {
			break
		}
	}
	reason := "caller_cancelled"
	cause := context.Cause(op.ctx)
	switch {
	case errors.Is(cause, core.ErrOrganizationFrozen):
		reason = "security_hold"
	case errors.Is(cause, core.ErrContainmentUnavailable):
		reason = "containment_unavailable"
	case errors.Is(cause, core.ErrExecutionStopped):
		reason = "runtime_shutdown"
	case errors.Is(cause, context.DeadlineExceeded):
		reason = "deadline_exceeded"
	}
	var request events.Event
	var completed bool
	var err error
	for range 2 {
		writeCtx, cancel := context.WithTimeout(context.WithoutCancel(op.ctx), stopWriteTimeout)
		request, completed, err = op.service.gateway.RequestModelStop(writeCtx, manifest, reason)
		cancel()
		if err == nil {
			break
		}
	}
	op.mu.Lock()
	started := op.started
	returned := op.returned
	op.mu.Unlock()
	if !started {
		returned = &events.ModelStopReturn{LocalState: "NOT_STARTED"}
	}
	if started && returned == nil {
		timer := time.NewTimer(stopGracePeriod)
		select {
		case <-op.callDone:
		case <-timer.C:
		}
		timer.Stop()
		op.mu.Lock()
		returned = op.returned
		op.mu.Unlock()
	}
	if err == nil && !completed {
		err = op.record(request.EventID, returned)
	}
	op.mu.Lock()
	op.writeErr = err
	op.mu.Unlock()
	close(op.ready)
	// A failed write is not permission to stop owning an unreturned callback.
	if started && returned == nil {
		<-op.callDone
		op.mu.Lock()
		returned = op.returned
		op.mu.Unlock()
		if err == nil && !completed {
			err = op.record(request.EventID, returned)
		}
	}
	if err != nil {
		op.service.stopMu.Lock()
		op.service.stopErr = errors.Join(op.service.stopErr, err)
		op.service.stopMu.Unlock()
	}
}

func (op *ModelOperation) record(request string, returned *events.ModelStopReturn) error {
	var err error
	for range 2 {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(op.ctx), stopWriteTimeout)
		_, err = op.service.gateway.RecordModelStop(ctx, request, returned)
		cancel()
		if err == nil {
			return nil
		}
	}
	return err
}

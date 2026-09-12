package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
)

const (
	stopGracePeriod  = 250 * time.Millisecond
	stopWriteTimeout = time.Second
)

type handlerResult struct {
	result     execution.Result
	err        error
	finishedAt time.Time
}

type stoppedExecution struct {
	organization core.ID
	taskID       core.ID
	executionID  core.ID
	correlation  string
	startedAt    time.Time
}

type runningExecution struct{ cancel context.CancelCauseFunc }

func (s *Service) trackExecution(ctx context.Context) (context.Context, func(), error) {
	s.stopMu.Lock()
	defer s.stopMu.Unlock()
	if s.stopping {
		return nil, nil, core.ErrExecutionStopped
	}
	ctx, cancel := context.WithCancelCause(ctx)
	running := &runningExecution{cancel: cancel}
	if s.runningExecutions == nil {
		s.runningExecutions = make(map[*runningExecution]struct{})
	}
	s.runningExecutions[running] = struct{}{}
	s.activeExecutions.Add(1)
	return ctx, func() {
		cancel(nil)
		s.stopMu.Lock()
		delete(s.runningExecutions, running)
		s.stopMu.Unlock()
		s.activeExecutions.Done()
	}, nil
}

// StopExecutions closes task admission and asks each active task supervisor to
// record a durable stop independently of whether its handler has returned.
// Close owned providers next, then call WaitForStops before closing the ledger.
func (s *Service) StopExecutions() {
	s.stopMu.Lock()
	defer s.stopMu.Unlock()
	s.stopping = true
	for running := range s.runningExecutions {
		running.cancel(core.ErrExecutionStopped)
	}
}

func executionStopCause(liveCtx, executionCtx context.Context, executionErr error) error {
	cause := errors.Join(context.Cause(liveCtx), context.Cause(executionCtx), executionErr)
	if errors.Is(cause, core.ErrOrganizationFrozen) || errors.Is(cause, core.ErrContainmentUnavailable) || errors.Is(cause, core.ErrExecutionStopped) ||
		errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		return cause
	}
	return nil
}

func (s *Service) requestStop(ctx context.Context, run stoppedExecution, cause error) (events.Event, error) {
	reason := "execution_cancelled"
	if errors.Is(cause, core.ErrOrganizationFrozen) {
		reason = "security_hold"
	} else if errors.Is(cause, core.ErrContainmentUnavailable) {
		reason = "containment_unavailable"
	}
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stopWriteTimeout)
	defer cancel()
	request, err := s.gateway.RequestExecutionStop(writeCtx, string(run.organization), string(run.taskID), run.correlation, string(run.executionID), reason)
	if err != nil {
		// An allocated event does not prove the transaction committed. Retry
		// the idempotent request instead of acknowledging an uncommitted ID.
		return events.Event{}, err
	}
	return request, nil
}

func (s *Service) stopExecution(ctx context.Context, run stoppedExecution, cause error, pending <-chan handlerResult, ready *handlerResult) (taskRun, error) {
	request, requestErr := s.requestStop(ctx, run, cause)
	if ready == nil && requestErr == nil {
		timer := time.NewTimer(stopGracePeriod)
		defer timer.Stop()
		select {
		case value := <-pending:
			ready = &value
		case <-timer.C:
		}
	}
	if ready != nil && requestErr == nil {
		outcome, err := s.recordStopped(ctx, run, request, *ready)
		if err == nil {
			return taskRun{Outcome: outcome, ExecutionError: cause}, nil
		}
		requestErr = err
	}
	if ready == nil && requestErr == nil {
		writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stopWriteTimeout)
		_, requestErr = s.gateway.RecordExecutionStop(writeCtx, request.EventID, nil, nil)
		cancel()
	}
	// Retry failed stop intent independently of the handler, then wait only to
	// record its eventual acknowledgement. This cannot publish results, schedule
	// work, or release the task. A committed request survives a missing finish.
	s.stops.Add(1)
	go func() {
		defer s.stops.Done()
		var err error
		if request.EventID == "" {
			request, err = s.requestStop(ctx, run, cause)
		}
		if err == nil && ready == nil {
			writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stopWriteTimeout)
			_, err = s.gateway.RecordExecutionStop(writeCtx, request.EventID, nil, nil)
			cancel()
		}
		if err == nil && ready == nil {
			value := <-pending
			ready = &value
		}
		if err == nil {
			_, err = s.recordStopped(ctx, run, request, *ready)
		}
		if err != nil {
			s.stopMu.Lock()
			if s.stopErr == nil {
				s.stopErr = err
			}
			s.stopMu.Unlock()
			log.Printf("execution stop evidence remains uncertain: task_id=%s execution_id=%s error=%v", run.taskID, run.executionID, err)
		}
	}()
	return taskRun{ExecutionError: cause}, requestErr
}

func (s *Service) recordStopped(ctx context.Context, run stoppedExecution, request events.Event, returned handlerResult) (core.ToolOutcome, error) {
	var intent events.ExecutionStopRequest
	if json.Unmarshal(request.Payload, &intent) != nil || request.EventID == "" {
		return core.ToolOutcome{}, fmt.Errorf("invalid durable stop request")
	}
	evidence := core.ExecutionInterruptionEvidence{
		StopRequestRef: request.EventID, Hold: intent.Hold, LocalExecutionStopped: true,
		ExternalEffectsStatus: "REQUIRES_RECONCILIATION", ReportedOutcome: returned.result.Outcome,
	}
	if stopped, ok := execution.StopOutcome(returned.err); ok {
		evidence.ProviderStop = &core.ProviderStopEvidence{
			LocalTurnStopped: stopped.LocalTurnStopped, LocalProcessStopAttempted: stopped.LocalProcessStopAttempted,
			LocalProcessStopped: stopped.LocalProcessStopped, RemoteStatus: string(stopped.RemoteStatus),
		}
	}
	outcome := core.ToolOutcome{
		ToolInvocationID: core.ID("held-" + string(run.executionID)), ToolID: "runtime-containment",
		Status: core.OutcomeFailed, PostconditionStatus: core.PostconditionNotChecked, Retryability: core.NotRetryable,
		ErrorClass: intent.ReasonClass, ErrorDetail: "execution interrupted before result admission",
		ObservedEffect: evidence, StartedAt: run.startedAt, FinishedAt: returned.finishedAt,
	}
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stopWriteTimeout)
	defer cancel()
	_, err := s.gateway.RecordExecutionStop(writeCtx, request.EventID, &outcome, returned.result.InferenceUsage)
	return outcome, err
}

// WaitForStops drains late audit writes after callers have stopped dispatching
// work and closed owned providers. It does not claim to terminate a handler.
func (s *Service) WaitForStops(ctx context.Context) error {
	done := make(chan struct{})
	go func() { s.activeExecutions.Wait(); s.stops.Wait(); close(done) }()
	select {
	case <-ctx.Done():
		return fmt.Errorf("execution stop acknowledgement remains uncertain: %w", ctx.Err())
	case <-done:
		s.stopMu.Lock()
		defer s.stopMu.Unlock()
		return s.stopErr
	}
}

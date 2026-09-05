package execution

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dominicnunez/agentos/internal/boundaryjson"
	sdk "github.com/dominicnunez/codex-sdk-go/appserver"
	protocol "github.com/dominicnunez/codex-sdk-go/appserver/protocol"
)

type codexRunSummary struct {
	sdk.StreamSummary
	EffectiveModel string
}

// The high-level SDK run API discards ThreadStartResponse.Model. Use the typed
// lifecycle so the model-only turn cannot start without observed identity.
func sdkStreamRun(process *sdk.Process, protocolErrors *codexProtocolErrors) codexRun {
	return func(ctx context.Context, options sdk.RunOptions) (*sdk.RunResult, codexRunSummary, error) {
		return runObservedCodexTurn(ctx, process.Client, protocolErrors, options)
	}
}

func runObservedCodexTurn(ctx context.Context, client *protocol.Client, protocolErrors *codexProtocolErrors, options sdk.RunOptions) (*sdk.RunResult, codexRunSummary, error) {
	var empty codexRunSummary
	if err := protocolErrors.take(); err != nil {
		return nil, empty, RequestNotSent(fmt.Errorf("codex protocol was already invalid"))
	}
	if options.Model == nil || *options.Model == "" {
		return nil, empty, RequestNotSent(fmt.Errorf("codex requested model is missing"))
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	observer := &codexTurnObserver{done: make(chan struct{}), cancel: cancel}
	var remove []func()
	for _, method := range codexObservedMethods {
		remove = append(remove, client.AddNotificationListener(method, func(_ context.Context, notice protocol.Notification) {
			observer.observe(notice)
		}))
	}
	defer func() {
		for _, unsubscribe := range remove {
			unsubscribe()
		}
	}()
	ephemeral := true
	thread, err := startObservedCodexThread(runCtx, client, protocol.ThreadStartParams{
		Cwd: options.Cwd, Ephemeral: &ephemeral, Config: options.Config,
		Model: options.Model, DeveloperInstructions: options.Instructions,
		ApprovalPolicy: options.ApprovalPolicy, Sandbox: options.Sandbox,
	})
	if err != nil {
		return nil, empty, RequestNotSent(fmt.Errorf("codex thread start failed: %w", err))
	}
	if thread.Model != *options.Model || thread.ModelProvider != "openai" || thread.Thread.ModelProvider != thread.ModelProvider || thread.Thread.ID == "" {
		return nil, empty, RequestNotSent(fmt.Errorf("codex effective model identity is missing or mismatched"))
	}
	observer.mu.Lock()
	observer.threadID = thread.Thread.ID
	priorErr := observer.err
	observer.mu.Unlock()
	if priorErr != nil {
		return nil, empty, RequestNotSent(priorErr)
	}
	turn, err := client.Turn.Start(runCtx, protocol.TurnStartParams{
		ThreadID: thread.Thread.ID, Input: []protocol.UserInput{&protocol.TextUserInput{Text: options.Prompt}},
		Model: options.Model, Cwd: options.Cwd, ApprovalPolicy: options.ApprovalPolicy,
		SandboxPolicy: &options.SandboxPolicy, Effort: options.Effort, OutputSchema: options.OutputSchema,
	})
	observer.mu.Lock()
	if err == nil && !observer.bindTurn(turn.Turn.ID) {
		observer.fail("codex turn response identity is inconsistent")
	}
	if err == nil {
		if turn.Turn.Error != nil {
			observer.fail("codex turn response contains an error")
		}
		for _, item := range turn.Turn.Items {
			if !codexModelItem(item) {
				observer.fail("codex turn response contains a disallowed item")
			}
		}
	}
	observer.mu.Unlock()
	if err == nil {
		select {
		case <-observer.done:
		case <-runCtx.Done():
			err = runCtx.Err()
		}
	}
	observer.mu.Lock()
	err = errors.Join(err, runCtx.Err(), observer.err, protocolErrors.take())
	observer.detached = true
	turnID := observer.turnID
	completed, items, usage := observer.completed, observer.items, observer.usage
	observer.mu.Unlock()
	if err != nil {
		if turnID != "" {
			interruptCtx, interruptCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer interruptCancel()
			_, _ = client.Turn.Interrupt(interruptCtx, protocol.TurnInterruptParams{ThreadID: thread.Thread.ID, TurnID: turnID})
		}
		return nil, empty, err
	}
	if completed == nil {
		return nil, empty, fmt.Errorf("codex turn has no completion evidence")
	}
	result := &sdk.RunResult{Thread: thread.Thread, Turn: *completed, Items: items}
	for _, item := range items {
		if message, ok := item.Value.(*sdk.AgentMessageThreadItem); ok {
			result.Response = message.Text
		}
	}
	return result, codexRunSummary{StreamSummary: sdk.StreamSummary{LatestTokenUsage: usage}, EffectiveModel: thread.Model}, nil
}

// Only one fresh thread is active in the isolated process. Foreign or changing
// turn identity cannot supply usage or completion evidence for this invocation.
type codexTurnObserver struct {
	mu               sync.Mutex
	threadID, turnID string
	items            []sdk.ThreadItemWrapper
	completed        *sdk.Turn
	usage            *sdk.ThreadTokenUsage
	err              error
	done             chan struct{}
	finished         bool
	detached         bool
	cancel           context.CancelFunc
	bytes, notices   int
	budget           codexStreamBudget
}

var codexObservedMethods = []string{
	"turn/started", "turn/completed", "item/started", "item/completed", "thread/tokenUsage/updated",
	"item/agentMessage/delta", "item/reasoning/textDelta", "item/reasoning/summaryTextDelta", "item/plan/delta",
	"model/rerouted", "error", "thread/realtime/error", "item/fileChange/outputDelta", "item/commandExecution/outputDelta",
	"item/mcpToolCall/progress", "item/collabAgentToolCall/started", "item/collabAgentToolCall/completed",
}

func (o *codexTurnObserver) fail(message string) {
	if o.err == nil {
		o.err = errors.New(message)
	}
	o.cancel()
	o.finish()
}

func (o *codexTurnObserver) finish() {
	if !o.finished {
		close(o.done)
		o.finished = true
	}
}

func (o *codexTurnObserver) bindTurn(id string) bool {
	if id == "" || o.turnID != "" && o.turnID != id {
		return false
	}
	o.turnID = id
	return true
}

func (o *codexTurnObserver) scope(threadID, turnID string) bool {
	return o.threadID != "" && threadID == o.threadID && o.bindTurn(turnID)
}

func (o *codexTurnObserver) observe(notice protocol.Notification) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.err != nil || o.detached {
		return
	}
	o.bytes += len(notice.Params)
	o.notices++
	if o.bytes > 4<<20 || o.notices > 16384 || len(notice.Params) > codexMaximumStreamBytes || boundaryjson.Validate(notice.Params) != nil {
		o.fail("codex notification is invalid or exceeds limits")
		return
	}
	switch notice.Method {
	case "turn/started", "turn/completed":
		var n protocol.TurnCompletedNotification
		if json.Unmarshal(notice.Params, &n) != nil || !o.scope(n.ThreadID, n.Turn.ID) || n.Turn.Error != nil {
			o.fail("codex turn notification identity is invalid")
			return
		}
		for _, item := range n.Turn.Items {
			if !codexModelItem(item) {
				o.fail("codex returned a disallowed turn item")
				return
			}
		}
		if notice.Method == "turn/completed" {
			if o.completed != nil || n.Turn.Status != sdk.TurnStatusCompleted {
				o.fail("codex completion is invalid")
				return
			}
			o.completed = &n.Turn
			o.finish()
		}
	case "item/started", "item/completed":
		var n protocol.ItemCompletedNotification
		var decodeErr error
		if notice.Method == "item/started" {
			var started protocol.ItemStartedNotification
			decodeErr = json.Unmarshal(notice.Params, &started)
			n.ThreadID, n.TurnID, n.Item = started.ThreadID, started.TurnID, started.Item
		} else {
			decodeErr = json.Unmarshal(notice.Params, &n)
		}
		if decodeErr != nil || !o.scope(n.ThreadID, n.TurnID) || !codexModelItem(n.Item) {
			o.fail("codex returned a disallowed or unbound item")
			return
		}
		if notice.Method == "item/completed" {
			if o.completed != nil || len(o.items) >= 1024 {
				o.fail("codex item sequence is invalid")
				return
			}
			o.items = append(o.items, n.Item)
		}
	case "thread/tokenUsage/updated":
		var n protocol.ThreadTokenUsageUpdatedNotification
		if json.Unmarshal(notice.Params, &n) != nil || !o.scope(n.ThreadID, n.TurnID) {
			o.fail("codex usage identity is invalid")
			return
		}
		o.usage = &n.TokenUsage
	case "item/agentMessage/delta", "item/reasoning/textDelta", "item/reasoning/summaryTextDelta", "item/plan/delta":
		var n struct {
			ThreadID string `json:"threadId"`
			TurnID   string `json:"turnId"`
			Delta    string `json:"delta"`
		}
		if json.Unmarshal(notice.Params, &n) != nil || !o.scope(n.ThreadID, n.TurnID) || len(n.Delta) > codexMaximumResponseBytes {
			o.fail("codex streamed content is invalid")
			return
		}
		var event sdk.Event = &sdk.ReasoningDelta{Delta: n.Delta}
		if notice.Method == "item/agentMessage/delta" {
			event = &sdk.TextDelta{Delta: n.Delta}
		}
		if o.budget.observe(event) != nil {
			o.fail("codex streamed content exceeds limits")
		}
	default:
		o.fail("codex reported a model change, execution error, or disallowed side effect")
	}
}

// Read the actual response bytes before the SDK can collapse duplicate or
// case-aliased model fields. Other protocol fields retain SDK validation.
func startObservedCodexThread(ctx context.Context, client *protocol.Client, params protocol.ThreadStartParams) (protocol.ThreadStartResponse, error) {
	var thread protocol.ThreadStartResponse
	body, err := json.Marshal(params)
	if err != nil {
		return thread, fmt.Errorf("invalid codex thread parameters")
	}
	response, err := client.Send(ctx, protocol.Request{JSONRPC: "2.0", ID: protocol.RequestID{Value: "agentos-identity-" + rand.Text()}, Method: "thread/start", Params: body})
	if err != nil {
		return thread, err
	}
	if len(response.Result) > codexMaximumStreamBytes || boundaryjson.Validate(response.Result) != nil {
		return thread, fmt.Errorf("codex thread identity response is invalid")
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(response.Result, &fields) != nil || !exactCodexIdentityKeys(fields, "model", "modelProvider", "thread") {
		return thread, fmt.Errorf("codex thread identity fields are ambiguous")
	}
	var nested map[string]json.RawMessage
	if json.Unmarshal(fields["thread"], &nested) != nil || !exactCodexIdentityKeys(nested, "id", "modelProvider") {
		return thread, fmt.Errorf("codex thread identity fields are ambiguous")
	}
	if json.Unmarshal(response.Result, &thread) != nil {
		return protocol.ThreadStartResponse{}, fmt.Errorf("codex thread identity response is invalid")
	}
	return thread, nil
}

func exactCodexIdentityKeys(fields map[string]json.RawMessage, names ...string) bool {
	for _, name := range names {
		if _, exists := fields[name]; !exists {
			return false
		}
		for key := range fields {
			if key != name && strings.EqualFold(key, name) {
				return false
			}
		}
	}
	return true
}

func codexModelItem(item sdk.ThreadItemWrapper) bool {
	switch value := item.Value.(type) {
	case *sdk.AgentMessageThreadItem:
		return value != nil && len(value.Text) <= codexMaximumResponseBytes
	case *sdk.UserMessageThreadItem, *sdk.ReasoningThreadItem, *sdk.PlanThreadItem:
		return true
	default:
		return false
	}
}

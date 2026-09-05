package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	sdk "github.com/dominicnunez/codex-sdk-go/appserver"
	protocol "github.com/dominicnunez/codex-sdk-go/appserver/protocol"
)

type identityTransport struct {
	model, provider      string
	notify               protocol.NotificationHandler
	notices              []protocol.Notification
	started, interrupted int
	waitForCancel        bool
	threadResponse       string
	turnResponse         string
	requests             map[string]json.RawMessage
}

func (f *identityTransport) Send(ctx context.Context, req protocol.Request) (protocol.Response, error) {
	if f.requests == nil {
		f.requests = make(map[string]json.RawMessage)
	}
	f.requests[req.Method] = append(json.RawMessage(nil), req.Params...)
	var body string
	switch req.Method {
	case "thread/start":
		body = fmt.Sprintf(`{"approvalPolicy":"never","approvalsReviewer":"user","cwd":"/tmp/run","model":%q,"modelProvider":%q,"sandbox":{"type":"readOnly"},"thread":{"id":"thread-1","cliVersion":"1.0.0","createdAt":1,"cwd":"/tmp/run","modelProvider":%q,"preview":"","source":"exec","status":{"type":"idle"},"turns":[],"updatedAt":1,"ephemeral":true}}`, f.model, f.provider, f.provider)
		if f.threadResponse != "" {
			body = f.threadResponse
		}
	case "turn/start":
		f.started++
		for _, notice := range f.notices {
			f.notify(ctx, notice)
		}
		if f.waitForCancel {
			<-ctx.Done()
			return protocol.Response{}, ctx.Err()
		}
		body = `{"turn":{"id":"turn-1","status":"inProgress","items":[]}}`
		if f.turnResponse != "" {
			body = f.turnResponse
		}
	case "turn/interrupt":
		f.interrupted++
		body = `{}`
	default:
		return protocol.Response{}, fmt.Errorf("unexpected test method")
	}
	return protocol.Response{ID: req.ID, Result: json.RawMessage(body)}, nil
}
func (*identityTransport) Notify(context.Context, protocol.Notification) error { return nil }
func (*identityTransport) OnRequest(protocol.RequestHandler)                   {}
func (f *identityTransport) OnNotify(handler protocol.NotificationHandler)     { f.notify = handler }
func (*identityTransport) Close() error                                        { return nil }

func identityNotice(method, params string) protocol.Notification {
	return protocol.Notification{Method: method, Params: json.RawMessage(params)}
}

func successfulIdentityNotices() []protocol.Notification {
	return []protocol.Notification{
		identityNotice("turn/started", `{"threadId":"thread-1","turn":{"id":"turn-1","status":"inProgress","items":[]}}`),
		identityNotice("item/completed", `{"threadId":"thread-1","turnId":"turn-1","completedAtMs":1,"item":{"type":"agentMessage","id":"answer-1","text":"answer"}}`),
		identityNotice("thread/tokenUsage/updated", `{"threadId":"thread-1","turnId":"turn-1","tokenUsage":{"last":{"inputTokens":3,"outputTokens":2,"totalTokens":5,"cachedInputTokens":0,"reasoningOutputTokens":0},"total":{"inputTokens":3,"outputTokens":2,"totalTokens":5,"cachedInputTokens":0,"reasoningOutputTokens":0},"modelContextWindow":10000}}`),
		identityNotice("turn/completed", `{"threadId":"thread-1","turn":{"id":"turn-1","status":"completed","items":[]}}`),
	}
}

func runIdentityTransport(t *testing.T, transport *identityTransport) (*sdk.RunResult, codexRunSummary, error) {
	t.Helper()
	protocolErrors := &codexProtocolErrors{}
	client := protocol.NewClient(transport, protocol.WithHandlerErrorCallback(protocolErrors.record))
	model, cwd := "gpt-test", "/tmp/run"
	var approval sdk.AskForApproval = sdk.ApprovalPolicyNever
	sandbox := sdk.SandboxModeReadOnly
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	return runObservedCodexTurn(ctx, client, protocolErrors, sdk.RunOptions{
		Model: &model, Cwd: &cwd, Prompt: "test", ApprovalPolicy: &approval, Sandbox: &sandbox,
		SandboxPolicy: sdk.SandboxPolicyReadOnly{},
	})
}

func TestCodexObservedIdentityIsRequiredBeforeTurnStart(t *testing.T) {
	for _, model := range []string{"", "other-model", "gpt-test "} {
		transport := &identityTransport{model: model, provider: "openai"}
		_, _, err := runIdentityTransport(t, transport)
		if err == nil || transport.started != 0 || !WasRequestNotSent(err) {
			t.Fatalf("unobserved identity reached inference: started=%d err=%v", transport.started, err)
		}
	}
	transport := &identityTransport{model: "gpt-test", provider: "other-provider"}
	if _, _, err := runIdentityTransport(t, transport); err == nil || transport.started != 0 {
		t.Fatal("unapproved provider reached inference")
	}
}

func TestCodexObservedIdentityBindsOutputAndUsage(t *testing.T) {
	transport := &identityTransport{model: "gpt-test", provider: "openai", notices: successfulIdentityNotices()}
	result, summary, err := runIdentityTransport(t, transport)
	if err != nil {
		t.Fatal(err)
	}
	response, err := validatedCodexResponse("gpt-test", result, summary)
	if err != nil || response.Text != "answer" || response.Usage.Model != "gpt-test" || response.Usage.TotalTokens != 5 {
		t.Fatalf("observed response=%+v err=%v", response, err)
	}
	summary.EffectiveModel = ""
	if _, err := validatedCodexResponse("gpt-test", result, summary); err == nil {
		t.Fatal("requested model substituted for missing observation")
	}
}

func TestCodexRerouteCancelsAndRejectsEvenWhenTurnResponseIsPending(t *testing.T) {
	transport := &identityTransport{model: "gpt-test", provider: "openai", waitForCancel: true,
		notices: []protocol.Notification{successfulIdentityNotices()[0], identityNotice("model/rerouted", `{"threadId":"thread-1","turnId":"turn-1","fromModel":"gpt-test","toModel":"synthetic-secret"}`)},
	}
	result, _, err := runIdentityTransport(t, transport)
	if err == nil || result != nil || transport.interrupted != 1 || strings.Contains(err.Error(), "synthetic-secret") {
		t.Fatalf("reroute not rejected safely: result=%v interrupted=%d err=%v", result, transport.interrupted, err)
	}
}

func TestCodexRerouteDiscardsAlreadyProducedOutput(t *testing.T) {
	transport := &identityTransport{model: "gpt-test", provider: "openai", notices: append(successfulIdentityNotices(), identityNotice("model/rerouted", `{}`))}
	result, summary, err := runIdentityTransport(t, transport)
	if err == nil || result != nil || summary.LatestTokenUsage != nil || WasRequestNotSent(err) {
		t.Fatal("rerouted output or unproven usage was accepted")
	}
}

func TestCodexDirectLifecycleInterruptsOnDeadline(t *testing.T) {
	transport := &identityTransport{model: "gpt-test", provider: "openai", waitForCancel: true, notices: successfulIdentityNotices()[:1]}
	result, _, err := runIdentityTransport(t, transport)
	if !errors.Is(err, context.DeadlineExceeded) || result != nil || transport.interrupted != 1 || WasRequestNotSent(err) {
		t.Fatal("deadline lost cancellation or invocation accounting")
	}
}

func TestCodexRejectsForeignCompletionAndUsageEvidence(t *testing.T) {
	for _, index := range []int{1, 2, 3} {
		notices := successfulIdentityNotices()
		notices[index].Params = json.RawMessage(strings.ReplaceAll(string(notices[index].Params), "thread-1", "foreign-thread"))
		transport := &identityTransport{model: "gpt-test", provider: "openai", notices: notices}
		if result, _, err := runIdentityTransport(t, transport); err == nil || result != nil {
			t.Fatal("foreign evidence accepted")
		}
	}
}

func TestCodexRawModelEvidenceRejectsDuplicateAndAliasedFields(t *testing.T) {
	for _, body := range []string{
		`{"model":"other","model":"gpt-test","modelProvider":"openai","thread":{"id":"thread-1","modelProvider":"openai"}}`,
		`{"model":"other","MODEL":"gpt-test","modelProvider":"openai","thread":{"id":"thread-1","modelProvider":"openai"}}`,
		`{"model":"gpt-test","modelProvider":"openai","thread":{"id":"thread-1","modelProvider":"other","ModelProvider":"openai"}}`,
	} {
		transport := &identityTransport{threadResponse: body}
		if _, _, err := runIdentityTransport(t, transport); err == nil || transport.started != 0 {
			t.Fatal("ambiguous reported model reached inference")
		}
	}
}

func TestCodexDirectLifecyclePreservesRequestedConfinement(t *testing.T) {
	transport := &identityTransport{model: "gpt-test", provider: "openai", notices: successfulIdentityNotices()}
	if _, _, err := runIdentityTransport(t, transport); err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{"thread/start", "turn/start"} {
		var request map[string]any
		if json.Unmarshal(transport.requests[method], &request) != nil || request["model"] != "gpt-test" || request["cwd"] != "/tmp/run" || request["approvalPolicy"] != "never" {
			t.Fatalf("lost confinement in %s", method)
		}
		if method == "thread/start" && (request["ephemeral"] != true || request["sandbox"] != "read-only") {
			t.Fatal("thread confinement changed")
		}
		if method == "turn/start" {
			policy, ok := request["sandboxPolicy"].(map[string]any)
			if !ok || policy["type"] != "readOnly" {
				t.Fatal("turn sandbox omitted")
			}
		}
	}
}

func TestCodexDirectLifecycleRejectsSideEffectsErrorsAndInconsistentTurns(t *testing.T) {
	for name, notice := range map[string]protocol.Notification{
		"side effect":    identityNotice("item/completed", `{"threadId":"thread-1","turnId":"turn-1","item":{"type":"commandExecution","id":"command-1"}}`),
		"unknown item":   identityNotice("item/started", `{"threadId":"thread-1","turnId":"turn-1","item":{"type":"futureTool","id":"tool-1"}}`),
		"provider error": identityNotice("error", `{"message":"synthetic-secret"}`),
		"changed turn":   identityNotice("turn/completed", `{"threadId":"thread-1","turn":{"id":"turn-2","status":"completed","items":[]}}`),
		"duplicate key":  identityNotice("turn/completed", `{"threadId":"other","threadId":"thread-1","turn":{"id":"turn-1","status":"completed","items":[]}}`),
	} {
		t.Run(name, func(t *testing.T) {
			notices := append(successfulIdentityNotices()[:1], notice)
			transport := &identityTransport{model: "gpt-test", provider: "openai", notices: notices}
			if result, _, err := runIdentityTransport(t, transport); err == nil || result != nil || strings.Contains(err.Error(), "synthetic-secret") {
				t.Fatal("invalid protocol evidence accepted or disclosed")
			}
		})
	}
	transport := &identityTransport{model: "gpt-test", provider: "openai", notices: successfulIdentityNotices(), turnResponse: `{"turn":{"id":"turn-2","status":"inProgress","items":[]}}`}
	if result, _, err := runIdentityTransport(t, transport); err == nil || result != nil {
		t.Fatal("response and notification turn IDs disagree")
	}
}

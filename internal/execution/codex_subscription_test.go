package execution

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	sdk "github.com/dominicnunez/codex-sdk-go/appserver"
	protocol "github.com/dominicnunez/codex-sdk-go/appserver/protocol"
)

func TestCodexSubscriptionCloseDoesNotWaitForRunningTurn(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	processClosed := make(chan struct{})
	adapter := &CodexSubscription{
		model:       "gpt-test",
		isolatedDir: t.TempDir(),
		runPermit:   make(chan struct{}, 1),
		run: func(context.Context, sdk.RunOptions) (*sdk.RunResult, codexRunSummary, error) {
			close(started)
			<-release
			return nil, codexRunSummary{}, context.Canceled
		},
		close: func() error {
			close(processClosed)
			return nil
		},
	}
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_, _ = adapter.Complete(t.Context(), "bounded prompt")
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("turn did not start")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- adapter.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(250 * time.Millisecond):
		close(release)
		<-runDone
		t.Fatal("Close waited for the ordinary turn permit")
	}
	select {
	case <-processClosed:
	default:
		t.Fatal("Close returned without invoking owned process shutdown")
	}
	close(release)
	<-runDone
}

func TestCodexSubscriptionHardStopRetiresRuntimeUntilNextTurn(t *testing.T) {
	lifetime, stopLifetime := context.WithCancel(context.Background())
	defer stopLifetime()
	var oldCalls, oldCloses, restarts, replacementCalls, replacementCloses int
	oldDir, replacementDir := t.TempDir(), t.TempDir()
	oldStopErr := errors.New("injected old process kill failure")
	adapter := &CodexSubscription{
		model:       "gpt-test",
		isolatedDir: oldDir,
		runPermit:   make(chan struct{}, 1),
		run: func(_ context.Context, options sdk.RunOptions) (*sdk.RunResult, codexRunSummary, error) {
			oldCalls++
			if options.Prompt != "first turn" || options.Cwd == nil || filepath.Dir(*options.Cwd) != oldDir {
				t.Errorf("old runtime request=%q cwd=%v", options.Prompt, options.Cwd)
			}
			return nil, codexRunSummary{}, withModelStopOutcome(context.Canceled, ModelStopOutcome{
				LocalProcessStopAttempted: true,
				RemoteStatus:              RemoteStopUncertain,
			})
		},
		close: func() error {
			oldCloses++
			return oldStopErr
		},
		lifetimeCtx:  lifetime,
		stopLifetime: stopLifetime,
		restart: func(context.Context) (codexRuntime, error) {
			restarts++
			return codexRuntime{
				run: func(_ context.Context, options sdk.RunOptions) (*sdk.RunResult, codexRunSummary, error) {
					replacementCalls++
					if options.Prompt != "second turn" || options.Cwd == nil || filepath.Dir(*options.Cwd) != replacementDir {
						t.Errorf("replacement runtime request=%q cwd=%v", options.Prompt, options.Cwd)
					}
					return successfulCodexRun("replacement answer"), successfulCodexSummary(4, 2), nil
				},
				close: func() error {
					replacementCloses++
					return nil
				},
				isolatedDir: replacementDir,
			}, nil
		},
	}
	if _, err := adapter.Complete(t.Context(), "first turn"); !errors.Is(err, context.Canceled) || !errors.Is(err, oldStopErr) {
		t.Fatalf("first turn error=%v", err)
	} else if stop, found := StopOutcome(err); !found || !stop.LocalProcessStopAttempted || stop.LocalProcessStopped || stop.RemoteStatus != RemoteStopUncertain {
		t.Fatalf("failed process stop evidence=%+v found=%v", stop, found)
	}
	if restarts != 0 {
		t.Fatalf("stopped request started %d replacement runtimes", restarts)
	}
	response, err := adapter.Complete(t.Context(), "second turn")
	if err != nil || response.Text != "replacement answer" {
		t.Fatalf("replacement turn response=%+v err=%v", response, err)
	}
	if oldCalls != 1 || oldCloses != 1 || restarts != 1 || replacementCalls != 1 {
		t.Fatalf("old calls=%d closes=%d restarts=%d replacement calls=%d", oldCalls, oldCloses, restarts, replacementCalls)
	}
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}
	if replacementCloses != 1 {
		t.Fatalf("replacement closes=%d", replacementCloses)
	}
}

func TestCodexSubscriptionQueuedTurnUsesReplacementRuntime(t *testing.T) {
	lifetime, stopLifetime := context.WithCancel(context.Background())
	defer stopLifetime()
	started, release := make(chan struct{}), make(chan struct{})
	var oldCalls, replacementCalls int
	adapter := &CodexSubscription{
		model: "gpt-test", isolatedDir: t.TempDir(), runPermit: make(chan struct{}, 1),
		run: func(_ context.Context, options sdk.RunOptions) (*sdk.RunResult, codexRunSummary, error) {
			oldCalls++
			if options.Prompt != "uncertain turn" {
				t.Errorf("old runtime received queued prompt %q", options.Prompt)
			}
			close(started)
			<-release
			return nil, codexRunSummary{}, withModelStopOutcome(context.Canceled, ModelStopOutcome{LocalProcessStopAttempted: true, RemoteStatus: RemoteStopUncertain})
		},
		close: func() error { return nil }, lifetimeCtx: lifetime, stopLifetime: stopLifetime,
		restart: func(context.Context) (codexRuntime, error) {
			return codexRuntime{
				run: func(_ context.Context, options sdk.RunOptions) (*sdk.RunResult, codexRunSummary, error) {
					replacementCalls++
					if options.Prompt != "queued independent turn" {
						t.Errorf("replacement runtime replayed prompt %q", options.Prompt)
					}
					return successfulCodexRun("queued answer"), successfulCodexSummary(2, 1), nil
				},
				close: func() error { return nil }, isolatedDir: t.TempDir(),
			}, nil
		},
	}
	firstDone := make(chan error, 1)
	go func() {
		_, err := adapter.Complete(t.Context(), "uncertain turn")
		firstDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first turn did not start")
	}
	secondDone := make(chan struct {
		response ModelResponse
		err      error
	}, 1)
	go func() {
		response, err := adapter.Complete(t.Context(), "queued independent turn")
		secondDone <- struct {
			response ModelResponse
			err      error
		}{response, err}
	}()
	select {
	case result := <-secondDone:
		t.Fatalf("queued turn bypassed serialization: %+v", result)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("uncertain turn error=%v", err)
	}
	result := <-secondDone
	if result.err != nil || result.response.Text != "queued answer" {
		t.Fatalf("queued replacement response=%+v err=%v", result.response, result.err)
	}
	if oldCalls != 1 || replacementCalls != 1 {
		t.Fatalf("old calls=%d replacement calls=%d", oldCalls, replacementCalls)
	}
}

func TestCodexSubscriptionRetriesFailedRuntimeRecreationWithoutReplay(t *testing.T) {
	lifetime, stopLifetime := context.WithCancel(context.Background())
	defer stopLifetime()
	restartFailure := errors.New("injected replacement failure")
	var oldCalls, restarts, replacementCalls int
	adapter := &CodexSubscription{
		model: "gpt-test", isolatedDir: t.TempDir(), runPermit: make(chan struct{}, 1),
		run: func(context.Context, sdk.RunOptions) (*sdk.RunResult, codexRunSummary, error) {
			oldCalls++
			return nil, codexRunSummary{}, withModelStopOutcome(context.Canceled, ModelStopOutcome{LocalProcessStopAttempted: true, RemoteStatus: RemoteStopUncertain})
		},
		close: func() error { return nil }, lifetimeCtx: lifetime, stopLifetime: stopLifetime,
		restart: func(context.Context) (codexRuntime, error) {
			restarts++
			if restarts == 1 {
				return codexRuntime{}, restartFailure
			}
			return codexRuntime{
				run: func(_ context.Context, options sdk.RunOptions) (*sdk.RunResult, codexRunSummary, error) {
					replacementCalls++
					if options.Prompt != "new request" {
						t.Errorf("replacement replayed %q", options.Prompt)
					}
					return successfulCodexRun("recovered"), successfulCodexSummary(1, 1), nil
				},
				close: func() error { return nil }, isolatedDir: t.TempDir(),
			}, nil
		},
	}
	if _, err := adapter.Complete(t.Context(), "uncertain request"); !errors.Is(err, context.Canceled) {
		t.Fatalf("stopped request error=%v", err)
	} else if stop, found := StopOutcome(err); !found || !stop.LocalProcessStopAttempted || stop.RemoteStatus != RemoteStopUncertain {
		t.Fatalf("stopped request lost stop evidence=%+v found=%v", stop, found)
	}
	if _, err := adapter.Complete(t.Context(), "new request"); !errors.Is(err, restartFailure) || !WasRequestNotSent(err) {
		t.Fatalf("failed recreation error=%v", err)
	}
	response, err := adapter.Complete(t.Context(), "new request")
	if err != nil || response.Text != "recovered" {
		t.Fatalf("runtime retry response=%+v err=%v", response, err)
	}
	if oldCalls != 1 || restarts != 2 || replacementCalls != 1 {
		t.Fatalf("old calls=%d restarts=%d replacement calls=%d", oldCalls, restarts, replacementCalls)
	}
}

func TestCodexSubscriptionCloseCancelsRuntimeReplacement(t *testing.T) {
	lifetime, stopLifetime := context.WithCancel(context.Background())
	restartStarted := make(chan struct{})
	credentialsCleared := make(chan struct{})
	var oldCloses, restarts, replacementCalls int
	adapter := &CodexSubscription{
		model: "gpt-test", isolatedDir: t.TempDir(), runPermit: make(chan struct{}, 1),
		run: func(context.Context, sdk.RunOptions) (*sdk.RunResult, codexRunSummary, error) {
			return nil, codexRunSummary{}, withModelStopOutcome(context.Canceled, ModelStopOutcome{LocalProcessStopAttempted: true, LocalProcessStopped: true, LocalTurnStopped: true, RemoteStatus: RemoteStopUncertain})
		},
		close: func() error { oldCloses++; return nil }, lifetimeCtx: lifetime, stopLifetime: stopLifetime,
		restart: func(ctx context.Context) (codexRuntime, error) {
			restarts++
			close(restartStarted)
			<-ctx.Done()
			select {
			case <-credentialsCleared:
				t.Error("credentials cleared before replacement callbacks drained")
			default:
			}
			return codexRuntime{}, ctx.Err()
		},
		clearCredentials: func() { close(credentialsCleared) },
	}
	if _, err := adapter.Complete(t.Context(), "stopping request"); !errors.Is(err, context.Canceled) {
		t.Fatalf("stopped request error=%v", err)
	}
	if restarts != 0 {
		t.Fatalf("stopped request started %d replacement runtimes", restarts)
	}
	completeDone := make(chan error, 1)
	go func() {
		_, err := adapter.Complete(t.Context(), "independent request")
		completeDone <- err
	}()
	select {
	case <-restartStarted:
	case <-time.After(time.Second):
		t.Fatal("replacement did not start")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- adapter.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel and drain replacement")
	}
	select {
	case <-credentialsCleared:
	default:
		t.Fatal("Close returned before clearing credentials")
	}
	if err := <-completeDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("independent request error=%v", err)
	}
	if _, err := adapter.Complete(t.Context(), "after close"); err == nil || !WasRequestNotSent(err) {
		t.Fatalf("closed adapter restarted: %v", err)
	}
	if oldCloses != 1 || restarts != 1 || replacementCalls != 0 {
		t.Fatalf("old closes=%d restarts=%d replacement calls=%d", oldCloses, restarts, replacementCalls)
	}
}

func TestCodexSubscriptionCloseWaitsForDetachedRuntimeCleanup(t *testing.T) {
	lifetime, cancelLifetime := context.WithCancel(context.Background())
	closeStarted := make(chan struct{})
	releaseClose := make(chan struct{})
	shutdownStarted := make(chan struct{})
	credentialsCleared := make(chan struct{})
	var closes int
	adapter := &CodexSubscription{
		model: "gpt-test", isolatedDir: t.TempDir(), runPermit: make(chan struct{}, 1),
		run: func(context.Context, sdk.RunOptions) (*sdk.RunResult, codexRunSummary, error) {
			return nil, codexRunSummary{}, withModelStopOutcome(context.Canceled, ModelStopOutcome{LocalProcessStopAttempted: true, RemoteStatus: RemoteStopUncertain})
		},
		close: func() error {
			closes++
			close(closeStarted)
			<-releaseClose
			return nil
		},
		lifetimeCtx: lifetime,
		stopLifetime: func() {
			cancelLifetime()
			close(shutdownStarted)
		},
		clearCredentials: func() { close(credentialsCleared) },
	}
	completeDone := make(chan error, 1)
	go func() {
		_, err := adapter.Complete(t.Context(), "stopping request")
		completeDone <- err
	}()
	select {
	case <-closeStarted:
	case <-time.After(time.Second):
		t.Fatal("detached runtime cleanup did not start")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- adapter.Close() }()
	select {
	case <-shutdownStarted:
	case <-time.After(time.Second):
		t.Fatal("Close did not begin shutdown")
	}
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before detached cleanup: %v", err)
	case <-credentialsCleared:
		t.Fatal("credentials cleared before detached cleanup")
	default:
	}
	close(releaseClose)
	if err := <-completeDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("stopped request error=%v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-credentialsCleared:
	default:
		t.Fatal("Close returned before clearing credentials")
	}
	if closes != 1 {
		t.Fatalf("runtime closes=%d", closes)
	}
}

func TestCodexSubscriptionCanceledRecoveryDoesNotStartRuntime(t *testing.T) {
	lifetime, stopLifetime := context.WithCancel(context.Background())
	defer stopLifetime()
	var restarts int
	adapter := &CodexSubscription{
		model: "gpt-test", runPermit: make(chan struct{}, 1), lifetimeCtx: lifetime, stopLifetime: stopLifetime,
		restart: func(context.Context) (codexRuntime, error) {
			restarts++
			return codexRuntime{}, errors.New("canceled recovery started")
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := adapter.Complete(ctx, "denied request"); !errors.Is(err, context.Canceled) || !WasRequestNotSent(err) {
		t.Fatalf("canceled recovery error=%v", err)
	}
	if restarts != 0 {
		t.Fatalf("canceled request started %d runtimes", restarts)
	}
}

func TestCodexSubscriptionModelDiscoveryBoundsRuntimeRecreation(t *testing.T) {
	lifetime, stopLifetime := context.WithCancel(context.Background())
	defer stopLifetime()
	restartFailure := errors.New("injected recreation failure")
	var restarts int
	adapter := &CodexSubscription{
		runPermit: make(chan struct{}, 1), lifetimeCtx: lifetime, stopLifetime: stopLifetime,
		restart: func(ctx context.Context) (codexRuntime, error) {
			restarts++
			deadline, found := ctx.Deadline()
			if !found || time.Until(deadline) <= 0 || time.Until(deadline) > codexRunTimeout {
				t.Errorf("recreation deadline=%v found=%v", deadline, found)
			}
			return codexRuntime{}, restartFailure
		},
	}
	if _, err := adapter.AvailableModels(context.Background()); !errors.Is(err, restartFailure) {
		t.Fatalf("model discovery error=%v", err)
	}
	if restarts != 1 {
		t.Fatalf("runtime recreations=%d", restarts)
	}
}

func TestCodexSubscriptionAppliesFailClosedRunProfile(t *testing.T) {
	root := t.TempDir()
	var captured sdk.RunOptions
	adapter := &CodexSubscription{
		model:       "gpt-test",
		isolatedDir: root,
		runPermit:   make(chan struct{}, 1),
		run: func(_ context.Context, options sdk.RunOptions) (*sdk.RunResult, codexRunSummary, error) {
			captured = options
			return successfulCodexRun("answer"), successfulCodexSummary(12, 3), nil
		},
	}
	response, err := adapter.Complete(context.Background(), "bounded prompt")
	if err != nil {
		t.Fatal(err)
	}
	if response.Text != "answer" || response.Usage.Provider != codexProviderName || response.Usage.Model != "gpt-test" || response.Usage.InputTokens != 12 || response.Usage.OutputTokens != 3 || response.Usage.TotalTokens != 15 || response.Usage.CostUSD != nil {
		t.Fatalf("response=%+v", response)
	}
	if captured.Cwd == nil || filepath.Dir(*captured.Cwd) != root || captured.Model == nil || *captured.Model != "gpt-test" {
		t.Fatalf("cwd=%v model=%v", captured.Cwd, captured.Model)
	}
	if captured.ApprovalPolicy == nil || *captured.ApprovalPolicy != sdk.ApprovalPolicyNever || captured.Sandbox == nil || *captured.Sandbox != sdk.SandboxModeReadOnly {
		t.Fatalf("approval=%v sandbox=%v", captured.ApprovalPolicy, captured.Sandbox)
	}
	policy, ok := captured.SandboxPolicy.(sdk.SandboxPolicyReadOnly)
	if !ok || policy.Access == nil {
		t.Fatalf("sandbox policy=%T", captured.SandboxPolicy)
	}
	access, ok := policy.Access.Value.(sdk.ReadOnlyAccessRestricted)
	if !ok || access.IncludePlatformDefaults == nil || *access.IncludePlatformDefaults || len(access.ReadableRoots) != 1 || access.ReadableRoots[0] != *captured.Cwd {
		t.Fatalf("read-only access=%+v", policy.Access.Value)
	}
	var config map[string]any
	if err := json.Unmarshal(captured.Config, &config); err != nil || config["web_search"] != "disabled" {
		t.Fatalf("config=%s err=%v", captured.Config, err)
	}
	features, ok := config["features"].(map[string]any)
	if !ok {
		t.Fatalf("features config=%#v", config["features"])
	}
	for _, name := range []string{"goals", "hooks", "memories", "multi_agent", "network_proxy", "remote_plugin", "shell_snapshot", "shell_tool", "unified_exec"} {
		if value, exists := features[name]; !exists || value != false {
			t.Fatalf("feature %q was not disabled: %#v", name, value)
		}
	}
	if _, err := os.Stat(*captured.Cwd); !os.IsNotExist(err) {
		t.Fatalf("ephemeral run directory still exists: %v", err)
	}
	if descriptor := adapter.Descriptor(); descriptor.Provider != codexProviderName || descriptor.Model != "gpt-test" || descriptor.ExecutionProfileVersion != codexExecutionProfile {
		t.Fatalf("descriptor=%+v", descriptor)
	}
}

func TestCodexSubscriptionDeadlineIncludesSerializedQueueWait(t *testing.T) {
	permit := make(chan struct{}, 1)
	permit <- struct{}{}
	adapter := &CodexSubscription{
		model:       "gpt-test",
		isolatedDir: t.TempDir(),
		runPermit:   permit,
		run: func(_ context.Context, _ sdk.RunOptions) (*sdk.RunResult, codexRunSummary, error) {
			t.Fatal("queued run unexpectedly started")
			return nil, codexRunSummary{}, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := adapter.Complete(ctx, "bounded prompt"); !errors.Is(err, context.Canceled) {
		t.Fatalf("queued run error=%v", err)
	}
}

func TestCodexStreamBudgetRejectsOversizedContentDuringStreaming(t *testing.T) {
	budget := &codexStreamBudget{}
	if err := budget.observe(&sdk.TextDelta{Delta: strings.Repeat("a", codexMaximumResponseBytes)}); err != nil {
		t.Fatal(err)
	}
	if err := budget.observe(&sdk.TextDelta{Delta: "b"}); err == nil {
		t.Fatal("oversized streamed response was accepted")
	}

	budget = &codexStreamBudget{}
	if err := budget.observe(&sdk.ReasoningDelta{Delta: strings.Repeat("r", codexMaximumStreamBytes)}); err != nil {
		t.Fatal(err)
	}
	if err := budget.observe(&sdk.PlanDelta{Delta: "p"}); err == nil {
		t.Fatal("oversized total stream was accepted")
	}
}

func TestCodexStreamBudgetRejectsSideEffectsImmediately(t *testing.T) {
	budget := &codexStreamBudget{}
	if err := budget.observe(&sdk.FileChangeDelta{Delta: "patch"}); err == nil {
		t.Fatal("streamed file change was accepted")
	}
	if err := budget.observe(&sdk.CollabToolCallEvent{}); err == nil {
		t.Fatal("collaboration tool call was accepted")
	}
}

func TestCodexSubscriptionRejectsAnySideEffectEvidence(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*codexRunSummary)
	}{
		{name: "command", mutate: func(summary *codexRunSummary) {
			summary.CommandExecutions["command"] = sdk.CommandExecutionLifecycle{}
		}},
		{name: "mcp", mutate: func(summary *codexRunSummary) { summary.McpToolCalls["mcp"] = sdk.McpToolCallLifecycle{} }},
		{name: "web", mutate: func(summary *codexRunSummary) { summary.WebSearches["web"] = sdk.WebSearchLifecycle{} }},
		{name: "file", mutate: func(summary *codexRunSummary) { summary.FileChanges["file"] = sdk.FileChangeLifecycle{} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			summary := successfulCodexSummary(1, 1)
			test.mutate(&summary)
			if _, err := validatedCodexResponse("gpt-test", successfulCodexRun("answer"), summary); err == nil {
				t.Fatal("side effect evidence was accepted")
			}
		})
	}
}

func TestCodexSubscriptionRejectsUnknownItemsAndInvalidUsage(t *testing.T) {
	result := successfulCodexRun("answer")
	result.Items = append(result.Items, sdk.ThreadItemWrapper{Value: &sdk.ContextCompactionThreadItem{}})
	if _, err := validatedCodexResponse("gpt-test", result, successfulCodexSummary(1, 1)); err == nil {
		t.Fatal("unexpected item type was accepted")
	}

	missing := successfulCodexSummary(1, 1)
	missing.LatestTokenUsage = nil
	if _, err := validatedCodexResponse("gpt-test", successfulCodexRun("answer"), missing); err == nil {
		t.Fatal("missing usage was accepted")
	}

	inconsistent := successfulCodexSummary(1, 1)
	inconsistent.LatestTokenUsage.Total.TotalTokens = 3
	if _, err := validatedCodexResponse("gpt-test", successfulCodexRun("answer"), inconsistent); err == nil {
		t.Fatal("inconsistent usage was accepted")
	}
}

func TestCodexSubscriptionDoesNotInheritAmbientSecrets(t *testing.T) {
	t.Setenv("AWS_SECRET_ACCESS_KEY", "must-not-leak")
	env := isolatedCodexEnvironment(t.TempDir())
	for _, entry := range env {
		if strings.HasPrefix(entry, "AWS_SECRET_ACCESS_KEY=") || strings.Contains(entry, "must-not-leak") {
			t.Fatalf("ambient secret leaked into child environment: %q", entry)
		}
	}
}

func TestCodexSubscriptionValidatesTrustedFilePaths(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "codex")
	credentials := filepath.Join(dir, "credentials.json")
	if err := os.WriteFile(binary, []byte("binary"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(credentials, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateCodexConfig(CodexSubscriptionConfig{BinaryPath: binary, CredentialsPath: credentials, Model: "gpt-test"}); err != nil {
		t.Fatal(err)
	}
	if err := validateCodexConfig(CodexSubscriptionConfig{BinaryPath: "codex", CredentialsPath: credentials, Model: "gpt-test"}); err == nil {
		t.Fatal("relative binary path was accepted")
	}
	if err := validateCodexConfig(CodexSubscriptionConfig{BinaryPath: binary, CredentialsPath: credentials, Model: " gpt-test"}); err == nil {
		t.Fatal("noncanonical model was accepted")
	}
	if runtime.GOOS != "windows" {
		link := filepath.Join(dir, "codex-link")
		if err := os.Symlink(binary, link); err != nil {
			t.Fatal(err)
		}
		if err := validateCodexConfig(CodexSubscriptionConfig{BinaryPath: link, CredentialsPath: credentials, Model: "gpt-test"}); err == nil {
			t.Fatal("symlinked binary was accepted")
		}
	}
}

func TestCodexSubscriptionListsBoundedVisibleModels(t *testing.T) {
	next := "page-2"
	adapter := &CodexSubscription{
		runPermit: make(chan struct{}, 1),
		models: func(_ context.Context, params protocol.ModelListParams) (protocol.ModelListResponse, error) {
			if params.Cursor == nil {
				return protocol.ModelListResponse{Data: []protocol.Model{
					{Model: "gpt-visible", DisplayName: "Visible"},
					{Model: "gpt-hidden", DisplayName: "Hidden", Hidden: true},
				}, NextCursor: &next}, nil
			}
			return protocol.ModelListResponse{Data: []protocol.Model{
				{Model: "gpt-default", DisplayName: "Default", IsDefault: true},
				{Model: "gpt-visible", DisplayName: "Duplicate"},
			}}, nil
		},
	}
	choices, err := adapter.AvailableModels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(choices) != 2 || choices[0].ID != "gpt-default" || !choices[0].Default || choices[1].ID != "gpt-visible" {
		t.Fatalf("choices=%+v", choices)
	}
}

func TestRemoveOwnedCodexDirectoryRefusesBroadTargets(t *testing.T) {
	root := filepath.Join(t.TempDir(), "agentos-codex-owned")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := removeOwnedCodexDirectory(root, filepath.Dir(root), "agentos-codex-"); err == nil {
		t.Fatal("parent directory removal was accepted")
	}
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("owned directory was unexpectedly removed: %v", err)
	}
}

func successfulCodexRun(response string) *sdk.RunResult {
	return &sdk.RunResult{
		Turn:     sdk.Turn{Status: sdk.TurnStatusCompleted},
		Items:    []sdk.ThreadItemWrapper{{Value: &sdk.AgentMessageThreadItem{Text: response}}},
		Response: response,
	}
}

func successfulCodexSummary(input, output int64) codexRunSummary {
	usage := &sdk.ThreadTokenUsage{}
	usage.Total.InputTokens = input
	usage.Total.OutputTokens = output
	usage.Total.TotalTokens = input + output
	return codexRunSummary{EffectiveModel: "gpt-test", StreamSummary: sdk.StreamSummary{
		LatestTokenUsage:  usage,
		CommandExecutions: map[string]sdk.CommandExecutionLifecycle{},
		McpToolCalls:      map[string]sdk.McpToolCallLifecycle{},
		WebSearches:       map[string]sdk.WebSearchLifecycle{},
		FileChanges:       map[string]sdk.FileChangeLifecycle{},
	}}
}

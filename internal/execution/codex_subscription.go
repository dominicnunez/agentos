package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	sdk "github.com/dominicnunez/codex-sdk-go/appserver"
	protocol "github.com/dominicnunez/codex-sdk-go/appserver/protocol"
	"github.com/dominicnunez/codex-sdk-go/login"
	"github.com/dominicnunez/codex-sdk-go/login/auth"
)

const (
	codexProviderName         = "codex-subscription"
	codexExecutionProfile     = "v1-codex-subscription-restricted"
	codexRunTimeout           = 20 * time.Second
	codexMaximumPromptBytes   = 256 << 10
	codexMaximumResponseBytes = 256 << 10
	codexMaximumStreamBytes   = 512 << 10
	codexMaximumModelBytes    = 128
	codexThreadConfigJSON     = `{"web_search":"disabled","history":{"persistence":"none"},"features":{"goals":false,"hooks":false,"memories":false,"multi_agent":false,"network_proxy":false,"remote_plugin":false,"shell_snapshot":false,"shell_tool":false,"unified_exec":false},"tools":{"web_search":false}}`
)

// CodexSubscriptionConfig is the deliberately small deployment boundary for
// subscription-backed Codex inference. Both paths must be absolute and point
// to regular, non-symlink files so startup never falls back to ambient state.
type CodexSubscriptionConfig struct {
	BinaryPath         string
	CredentialsPath    string
	Model              string
	PersistCredentials func([]byte) error
}

// CodexSubscription runs bounded, model-only Codex turns. It owns one isolated
// app-server process and serializes turns because the SDK client installs
// per-run notification listeners on that shared process.
type CodexSubscription struct {
	model       string
	run         codexRun
	models      func(context.Context, protocol.ModelListParams) (protocol.ModelListResponse, error)
	close       func() error
	isolatedDir string
	runPermit   chan struct{}
	closeOnce   sync.Once
	closeErr    error
}

type codexRun func(context.Context, sdk.RunOptions) (*sdk.RunResult, sdk.StreamSummary, error)

type codexProtocolErrors struct {
	mu  sync.Mutex
	err error
}

func (e *codexProtocolErrors) record(method string, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.err = errors.Join(e.err, fmt.Errorf("%s notification: %w", method, err))
}

func (e *codexProtocolErrors) take() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	err := e.err
	e.err = nil
	return err
}

// NewCodexSubscription starts an isolated Codex app-server and authenticates it
// with the explicitly configured SDK credential file. No ambient Codex home,
// project configuration, approval handler, MCP server, or operator authority is
// inherited.
func NewCodexSubscription(ctx context.Context, config CodexSubscriptionConfig) (*CodexSubscription, error) {
	if ctx == nil {
		return nil, fmt.Errorf("runtime context is required")
	}
	if err := validateCodexConfig(config); err != nil {
		return nil, err
	}

	creds, err := auth.LoadCredentials(config.CredentialsPath)
	if err != nil {
		return nil, fmt.Errorf("load Codex subscription credentials: %w", err)
	}
	isolatedDir, err := os.MkdirTemp("", "agentos-codex-")
	if err != nil {
		return nil, fmt.Errorf("create isolated Codex runtime directory: %w", err)
	}
	cleanup := func() error { return removeOwnedCodexDirectory(isolatedDir, isolatedDir, "agentos-codex-") }

	protocolErrors := &codexProtocolErrors{}
	process, err := sdk.StartProcess(ctx, &sdk.ProcessOptions{
		BinaryPath: config.BinaryPath,
		Dir:        isolatedDir,
		Env:        isolatedCodexEnvironment(isolatedDir),
		ClientOptions: []sdk.ClientOption{
			sdk.WithRequestTimeout(codexRunTimeout),
			sdk.WithHandlerErrorCallback(protocolErrors.record),
		},
	})
	if err != nil {
		return nil, errors.Join(fmt.Errorf("start isolated Codex app-server: %w", err), cleanup())
	}
	var credentialsMu sync.Mutex
	closeProcess := func() error {
		closeErr := process.Close()
		credentialsMu.Lock()
		creds = auth.Credentials{}
		credentialsMu.Unlock()
		return errors.Join(closeErr, cleanup())
	}

	process.Client.SetApprovalHandlers(protocol.ApprovalHandlers{
		OnChatgptAuthTokensRefresh: func(refreshCtx context.Context, _ protocol.ChatgptAuthTokensRefreshParams) (protocol.ChatgptAuthTokensRefreshResponse, error) {
			credentialsMu.Lock()
			defer credentialsMu.Unlock()
			refreshed, refreshErr := login.Refresh(refreshCtx, codexLoginConfig(), creds.RefreshToken)
			if refreshErr != nil {
				return protocol.ChatgptAuthTokensRefreshResponse{}, refreshErr
			}
			if config.PersistCredentials == nil {
				refreshErr = auth.SaveCredentials(config.CredentialsPath, refreshed)
			} else {
				var encoded []byte
				encoded, refreshErr = json.Marshal(refreshed)
				if refreshErr == nil {
					refreshErr = config.PersistCredentials(encoded)
				}
				clear(encoded)
			}
			if refreshErr != nil {
				return protocol.ChatgptAuthTokensRefreshResponse{}, fmt.Errorf("persist refreshed Codex credential: %w", refreshErr)
			}
			creds = refreshed
			return protocol.ChatgptAuthTokensRefreshResponse{
				AccessToken:      refreshed.AccessToken,
				ChatgptAccountID: refreshed.AccountID,
				ChatgptPlanType:  refreshed.PlanType,
			}, nil
		},
	})

	if _, err = process.Initialize(ctx); err != nil {
		return nil, errors.Join(fmt.Errorf("initialize Codex app-server: %w", err), closeProcess())
	}
	credentialsMu.Lock()
	loginParams := &protocol.ChatgptAuthTokensLoginAccountParams{
		AccessToken:      creds.AccessToken,
		ChatgptAccountId: creds.AccountID,
		ChatgptPlanType:  creds.PlanType,
	}
	credentialsMu.Unlock()
	if _, err = process.Client.Account.Login(ctx, loginParams); err != nil {
		return nil, errors.Join(fmt.Errorf("authenticate Codex subscription: %w", err), closeProcess())
	}

	return &CodexSubscription{
		model:       config.Model,
		run:         sdkStreamRun(process, protocolErrors),
		models:      process.Client.Model.List,
		close:       closeProcess,
		isolatedDir: isolatedDir,
		runPermit:   make(chan struct{}, 1),
	}, nil
}

type ModelChoice struct {
	ID          string
	DisplayName string
	Default     bool
}

// AvailableModels returns the authenticated subscription's visible picker
// models. It is used during setup; it does not grant the runtime model-routing
// authority or enable Codex tools.
func (a *CodexSubscription) AvailableModels(ctx context.Context) ([]ModelChoice, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is required")
	}
	if a == nil || a.models == nil || a.runPermit == nil {
		return nil, fmt.Errorf("Codex model discovery is unavailable")
	}
	if err := acquireCodexPermit(ctx, a.runPermit); err != nil {
		return nil, err
	}
	defer func() { <-a.runPermit }()
	const maximumPages = 20
	limit := uint32(100)
	var cursor *string
	seenCursors := make(map[string]struct{})
	seenModels := make(map[string]struct{})
	choices := make([]ModelChoice, 0)
	for page := 0; page < maximumPages; page++ {
		response, err := a.models(ctx, protocol.ModelListParams{Cursor: cursor, Limit: &limit})
		if err != nil {
			return nil, fmt.Errorf("list Codex models: %w", err)
		}
		for _, model := range response.Data {
			id := strings.TrimSpace(model.Model)
			if model.Hidden || id == "" || id != model.Model || len(id) > codexMaximumModelBytes || strings.ContainsAny(id, "\r\n\t ") {
				continue
			}
			if _, exists := seenModels[id]; exists {
				continue
			}
			seenModels[id] = struct{}{}
			choices = append(choices, ModelChoice{ID: id, DisplayName: strings.TrimSpace(model.DisplayName), Default: model.IsDefault})
		}
		if response.NextCursor == nil || *response.NextCursor == "" {
			break
		}
		if _, exists := seenCursors[*response.NextCursor]; exists {
			return nil, fmt.Errorf("Codex model pagination repeated a cursor")
		}
		seenCursors[*response.NextCursor] = struct{}{}
		cursor = response.NextCursor
		if page == maximumPages-1 {
			return nil, fmt.Errorf("Codex model list exceeds the setup safety limit")
		}
	}
	if len(choices) == 0 {
		return nil, fmt.Errorf("Codex returned no selectable models")
	}
	sort.SliceStable(choices, func(left, right int) bool {
		if choices[left].Default != choices[right].Default {
			return choices[left].Default
		}
		return choices[left].DisplayName < choices[right].DisplayName
	})
	return choices, nil
}

func acquireCodexPermit(ctx context.Context, permit chan struct{}) error {
	select {
	case permit <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func codexLoginConfig() login.Config {
	return login.Config{
		Originator: "agentos",
		HTTPClient: &http.Client{
			Timeout: codexRunTimeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func sdkStreamRun(process *sdk.Process, protocolErrors *codexProtocolErrors) codexRun {
	return func(ctx context.Context, options sdk.RunOptions) (*sdk.RunResult, sdk.StreamSummary, error) {
		if err := protocolErrors.take(); err != nil {
			return nil, sdk.StreamSummary{}, fmt.Errorf("codex protocol was already invalid: %w", err)
		}
		collector := sdk.NewStreamCollector()
		streamCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		stream := process.RunStreamedWithCollector(streamCtx, options, collector)
		budget := codexStreamBudget{}
		var streamErr error
		for event, err := range stream.Events() {
			if err != nil {
				streamErr = errors.Join(streamErr, err)
				continue
			}
			if err = budget.observe(event); err != nil {
				streamErr = errors.Join(streamErr, err)
				cancel()
			}
		}
		result := stream.Result()
		summary := collector.Summary()
		streamErr = errors.Join(streamErr, protocolErrors.take())
		if streamErr != nil {
			return nil, summary, streamErr
		}
		if result == nil {
			return nil, summary, fmt.Errorf("codex stream completed without a result")
		}
		return result, summary, nil
	}
}

type codexStreamBudget struct {
	responseBytes int
	totalBytes    int
}

func (b *codexStreamBudget) observe(event sdk.Event) error {
	var delta string
	switch typed := event.(type) {
	case *sdk.TextDelta:
		delta = typed.Delta
		b.responseBytes += len(delta)
		if b.responseBytes > codexMaximumResponseBytes {
			return fmt.Errorf("codex streamed response exceeded %d bytes", codexMaximumResponseBytes)
		}
	case *sdk.ReasoningDelta:
		delta = typed.Delta
	case *sdk.ReasoningSummaryDelta:
		delta = typed.Delta
	case *sdk.PlanDelta:
		delta = typed.Delta
	case *sdk.FileChangeDelta:
		return fmt.Errorf("codex attempted a disallowed file change")
	case *sdk.CollabToolCallEvent:
		return fmt.Errorf("codex attempted a disallowed collaboration tool call")
	}
	b.totalBytes += len(delta)
	if b.totalBytes > codexMaximumStreamBytes {
		return fmt.Errorf("codex streamed content exceeded %d bytes", codexMaximumStreamBytes)
	}
	return nil
}

func (a *CodexSubscription) Name() string { return codexProviderName + "/" + a.model }

func (a *CodexSubscription) Descriptor() ModelDescriptor {
	return ModelDescriptor{Provider: codexProviderName, Model: a.model, ExecutionProfileVersion: codexExecutionProfile}
}

func (a *CodexSubscription) Complete(ctx context.Context, prompt string) (response ModelResponse, err error) {
	if ctx == nil {
		return ModelResponse{}, fmt.Errorf("execution context is required")
	}
	if len(prompt) == 0 || len(prompt) > codexMaximumPromptBytes {
		return ModelResponse{}, fmt.Errorf("codex prompt must contain 1 to %d bytes", codexMaximumPromptBytes)
	}

	runCtx, cancel := context.WithTimeout(ctx, codexRunTimeout)
	defer cancel()
	if a.runPermit == nil {
		return ModelResponse{}, fmt.Errorf("codex subscription adapter is closed")
	}
	select {
	case a.runPermit <- struct{}{}:
		defer func() { <-a.runPermit }()
	case <-runCtx.Done():
		return ModelResponse{}, fmt.Errorf("wait for confined Codex turn: %w", runCtx.Err())
	}
	if a.run == nil || a.isolatedDir == "" {
		return ModelResponse{}, fmt.Errorf("codex subscription adapter is closed")
	}

	runDir, err := os.MkdirTemp(a.isolatedDir, "run-")
	if err != nil {
		return ModelResponse{}, fmt.Errorf("create isolated Codex turn directory: %w", err)
	}
	defer func() {
		err = errors.Join(err, removeOwnedCodexDirectory(a.isolatedDir, runDir, "run-"))
	}()

	includeDefaults := false
	model := a.model
	var approval sdk.AskForApproval = sdk.ApprovalPolicyNever
	sandbox := sdk.SandboxModeReadOnly
	result, summary, err := a.run(runCtx, sdk.RunOptions{
		Prompt:         prompt,
		Cwd:            &runDir,
		Config:         json.RawMessage(codexThreadConfigJSON),
		Model:          &model,
		ApprovalPolicy: &approval,
		Sandbox:        &sandbox,
		SandboxPolicy: sdk.SandboxPolicyReadOnly{Access: &sdk.ReadOnlyAccessWrapper{Value: sdk.ReadOnlyAccessRestricted{
			IncludePlatformDefaults: &includeDefaults,
			ReadableRoots:           []string{runDir},
		}}},
	})
	if err != nil {
		return ModelResponse{}, fmt.Errorf("run confined Codex turn: %w", err)
	}
	return validatedCodexResponse(a.model, result, summary)
}

func validatedCodexResponse(model string, result *sdk.RunResult, summary sdk.StreamSummary) (ModelResponse, error) {
	if result == nil || result.Turn.Status != sdk.TurnStatusCompleted {
		return ModelResponse{}, fmt.Errorf("codex turn did not complete successfully")
	}
	if result.Response == "" || len(result.Response) > codexMaximumResponseBytes {
		return ModelResponse{}, fmt.Errorf("codex response must contain 1 to %d bytes", codexMaximumResponseBytes)
	}
	if len(summary.NormalizedErrors) != 0 || summary.DroppedNormalizedErrors != 0 {
		return ModelResponse{}, fmt.Errorf("codex reported a streamed execution error")
	}
	if len(summary.CommandExecutions) != 0 || len(summary.McpToolCalls) != 0 || len(summary.WebSearches) != 0 || len(summary.FileChanges) != 0 {
		return ModelResponse{}, fmt.Errorf("codex attempted a disallowed side effect")
	}
	for _, item := range result.Items {
		switch item.Value.(type) {
		case *sdk.AgentMessageThreadItem, *sdk.UserMessageThreadItem, *sdk.ReasoningThreadItem, *sdk.PlanThreadItem:
			// These item types contain only the bounded conversation result.
		default:
			return ModelResponse{}, fmt.Errorf("codex returned disallowed item type %T", item.Value)
		}
	}
	if summary.LatestTokenUsage == nil {
		return ModelResponse{}, fmt.Errorf("codex returned no token usage")
	}
	input, err := codexTokenCount(summary.LatestTokenUsage.Total.InputTokens)
	if err != nil {
		return ModelResponse{}, err
	}
	output, err := codexTokenCount(summary.LatestTokenUsage.Total.OutputTokens)
	if err != nil {
		return ModelResponse{}, err
	}
	total, err := codexTokenCount(summary.LatestTokenUsage.Total.TotalTokens)
	if err != nil {
		return ModelResponse{}, err
	}
	usage := events.InferenceUsageRecordedPayload{
		Source:       "provider_cli",
		Provider:     codexProviderName,
		Model:        model,
		InputTokens:  input,
		OutputTokens: output,
		TotalTokens:  total,
	}
	if !usage.Valid() {
		return ModelResponse{}, fmt.Errorf("codex returned inconsistent token usage")
	}
	return ModelResponse{Text: result.Response, Usage: usage}, nil
}

func codexTokenCount(value int64) (int, error) {
	converted := int(value)
	if value < 0 || int64(converted) != value {
		return 0, fmt.Errorf("codex returned an invalid token count")
	}
	return converted, nil
}

func (a *CodexSubscription) Close() error {
	a.closeOnce.Do(func() {
		if a.runPermit == nil {
			return
		}
		a.runPermit <- struct{}{}
		defer func() { <-a.runPermit }()
		if a.close != nil {
			a.closeErr = a.close()
		}
		a.close = nil
		a.run = nil
		a.isolatedDir = ""
	})
	return a.closeErr
}

func validateCodexConfig(config CodexSubscriptionConfig) error {
	if strings.TrimSpace(config.Model) == "" || strings.TrimSpace(config.Model) != config.Model || len(config.Model) > codexMaximumModelBytes || strings.IndexFunc(config.Model, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0 {
		return fmt.Errorf("codex model must be a canonical identifier of at most %d bytes", codexMaximumModelBytes)
	}
	if err := validateRegularAbsoluteFile(config.BinaryPath, "Codex binary"); err != nil {
		return err
	}
	return validateRegularAbsoluteFile(config.CredentialsPath, "Codex credentials")
}

func validateRegularAbsoluteFile(path, name string) error {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return fmt.Errorf("%s path must be absolute and clean", name)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect %s: %w", name, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("%s path must identify a regular, non-symlink file", name)
	}
	if runtime.GOOS == "windows" {
		// Agent OS V1 deployment is Linux-only. Lstat still rejects a direct
		// Windows symlink, while parent traversal is enforced on Linux.
		return nil
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return fmt.Errorf("resolve %s: %w", name, err)
	}
	if resolved != path {
		return fmt.Errorf("%s path must not traverse a symlink", name)
	}
	if name == "Codex credentials" && runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("codex credentials must not be accessible by group or other users")
	}
	return nil
}

func isolatedCodexEnvironment(root string) []string {
	env := []string{
		"CODEX_HOME=" + root,
		"HOME=" + root,
		"TMPDIR=" + root,
		"TEMP=" + root,
		"TMP=" + root,
	}
	if runtime.GOOS == "windows" {
		env = append(env,
			"USERPROFILE="+root,
			"APPDATA="+filepath.Join(root, "AppData", "Roaming"),
			"LOCALAPPDATA="+filepath.Join(root, "AppData", "Local"),
		)
		for _, key := range []string{"SYSTEMDRIVE", "SYSTEMROOT"} {
			if value, ok := os.LookupEnv(key); ok {
				env = append(env, key+"="+value)
			}
		}
	}
	return env
}

func removeOwnedCodexDirectory(root, target, prefix string) error {
	root = filepath.Clean(root)
	target = filepath.Clean(target)
	if root == "" || target == "" || !filepath.IsAbs(root) || !filepath.IsAbs(target) {
		return fmt.Errorf("refuse to remove invalid Codex runtime directory")
	}
	if target == root {
		if !strings.HasPrefix(filepath.Base(target), prefix) {
			return fmt.Errorf("refuse to remove unowned Codex runtime directory")
		}
	} else {
		relative, err := filepath.Rel(root, target)
		if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || !strings.HasPrefix(filepath.Base(target), prefix) {
			return fmt.Errorf("refuse to remove unowned Codex turn directory")
		}
	}
	return os.RemoveAll(target)
}

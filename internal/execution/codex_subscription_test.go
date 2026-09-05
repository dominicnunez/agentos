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

	sdk "github.com/dominicnunez/codex-sdk-go/appserver"
	protocol "github.com/dominicnunez/codex-sdk-go/appserver/protocol"
)

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

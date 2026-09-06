package execution

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	sdk "github.com/dominicnunez/codex-sdk-go/appserver"
	"github.com/dominicnunez/codex-sdk-go/appserver/protocol"
)

func TestMain(m *testing.M) {
	if mode := os.Getenv("AGENTOS_CODEX_TEST_PROCESS"); mode != "" {
		if err := serveCodexTestProcess(mode); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// The test binary acts as a local stdio peer. No provider, credentials or
// networking are used; production startup, pipe ownership and SDK dispatch run.
func serveCodexTestProcess(mode string) error {
	d := json.NewDecoder(os.Stdin)
	e := json.NewEncoder(os.Stdout)
	initialized := false
	fake := &identityTransport{model: "gpt-test", provider: "openai"}
	for {
		var req protocol.Request
		if err := d.Decode(&req); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		var result json.RawMessage
		switch req.Method {
		case "initialize":
			result = json.RawMessage(`{"codexHome":"/isolated","platformFamily":"test","platformOs":"test","userAgent":"test"}`)
		case "initialized":
			initialized = true
			continue
		case "thread/start":
			if !initialized {
				return os.ErrInvalid
			}
			resp, err := fake.Send(context.Background(), req)
			if err != nil {
				return err
			}
			result = resp.Result
			var identity struct {
				Thread json.RawMessage `json:"thread"`
			}
			if err := json.Unmarshal(result, &identity); err != nil {
				return err
			}
			if err := e.Encode(map[string]any{"method": "thread/started", "params": map[string]any{"thread": identity.Thread}}); err != nil {
				return err
			}
		case "turn/start":
			if mode == "auth" {
				if err := e.Encode(map[string]any{"id": "refresh-1", "method": "account/chatgptAuthTokens/refresh", "params": map[string]string{"reason": "unauthorized"}}); err != nil {
					return err
				}
				var response struct {
					Result struct {
						AccessToken string `json:"accessToken"`
						AccountID   string `json:"chatgptAccountId"`
					} `json:"result"`
				}
				if err := d.Decode(&response); err != nil {
					return err
				}
				if response.Result.AccessToken != "synthetic-token" || response.Result.AccountID != "synthetic-account" {
					return os.ErrInvalid
				}
			}
			if mode == "approval" {
				if err := e.Encode(map[string]any{"id": "approval-1", "method": "item/commandExecution/requestApproval", "params": map[string]string{}}); err != nil {
					return err
				}
			}
			if mode == "reroute" {
				if err := e.Encode(map[string]any{"method": "model/rerouted", "params": json.RawMessage(`{}`)}); err != nil {
					return err
				}
			}
			if mode == "malformed" {
				if _, err := os.Stdout.WriteString("{\"method\":\"turn/completed\",\"method\":\"model/rerouted\",\"params\":{}}\n"); err != nil {
					return err
				}
			}
			for _, notice := range successfulIdentityNotices() {
				if err := e.Encode(map[string]any{"method": notice.Method, "params": notice.Params}); err != nil {
					return err
				}
			}
			result = json.RawMessage(`{"turn":{"id":"turn-1","status":"inProgress","items":[]}}`)
		case "turn/interrupt":
			result = json.RawMessage(`{}`)
		default:
			return os.ErrInvalid
		}
		if err := e.Encode(map[string]any{"id": req.ID, "result": result}); err != nil {
			return err
		}
	}
}

func TestCodexProcessRunsObservedWireLifecycle(t *testing.T) {
	for _, mode := range []string{"success", "auth", "approval", "reroute", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			p, err := startCodexProcess(ctx, &sdk.ProcessOptions{BinaryPath: binary, Dir: t.TempDir(), Env: []string{"AGENTOS_CODEX_TEST_PROCESS=" + mode, "GORACE=atexit_sleep_ms=0"}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := p.Close(); err != nil {
					t.Error(err)
				}
			})
			if _, err := p.Initialize(ctx); err != nil {
				t.Fatal(err)
			}
			p.Client.SetApprovalHandlers(protocol.ApprovalHandlers{OnChatgptAuthTokensRefresh: func(context.Context, protocol.ChatgptAuthTokensRefreshParams) (protocol.ChatgptAuthTokensRefreshResponse, error) {
				return protocol.ChatgptAuthTokensRefreshResponse{AccessToken: "synthetic-token", ChatgptAccountID: "synthetic-account"}, nil
			}})
			model, cwd := "gpt-test", t.TempDir()
			var approval sdk.AskForApproval = sdk.ApprovalPolicyNever
			sandbox := sdk.SandboxModeReadOnly
			result, summary, err := sdkStreamRun(p, &codexProtocolErrors{})(ctx, sdk.RunOptions{
				Model: &model, Cwd: &cwd, Prompt: "test", ApprovalPolicy: &approval, Sandbox: &sandbox, SandboxPolicy: sdk.SandboxPolicyReadOnly{},
			})
			if mode == "success" || mode == "auth" {
				if err != nil || result == nil || result.Response != "answer" || summary.EffectiveModel != model {
					t.Fatalf("production lifecycle failed: result=%v err=%v", result, err)
				}
			} else if err == nil || result != nil {
				t.Fatal("invalid wire evidence produced output")
			}
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
			if p.cmd.ProcessState == nil {
				t.Fatal("child was not reaped")
			}
		})
	}
}

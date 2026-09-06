package execution

import (
	"context"
	"errors"
	"io"
	"os/exec"
	"sync"
	"time"

	sdk "github.com/dominicnunez/codex-sdk-go/appserver"
	"github.com/dominicnunez/codex-sdk-go/appserver/transport"
)

// Own the pipes so the wire observer runs before SDK asynchronous dispatch.
// The SDK still owns protocol serialization, authentication and deny-by-default
// approval handlers; the executable, directory and environment remain explicit.
type codexProcess struct {
	Client    *sdk.Client
	wire      *codexWireReader
	transport *transport.StdioTransport
	stdin     io.Closer
	cmd       *exec.Cmd
	killTree  func() error
	closeTree func() error
	once      sync.Once
	err       error
}

func startCodexProcess(ctx context.Context, options *sdk.ProcessOptions) (*codexProcess, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(context.WithoutCancel(ctx), options.BinaryPath, "app-server", "--listen", "stdio://")
	cmd.Dir, cmd.Env = options.Dir, options.Env
	configureCodexCommand(cmd)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		_ = stdout.Close()
		_ = stdin.Close()
		return nil, err
	}
	killTree, closeTree, err := attachCodexProcessTree(cmd)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = stdin.Close()
		_ = stdout.Close()
		_ = cmd.Wait()
		return nil, err
	}
	wire := newCodexWireReader(stdout)
	tr := transport.NewStdioTransport(wire, stdin)
	p := &codexProcess{Client: sdk.NewClient(tr, options.ClientOptions...), wire: wire, transport: tr,
		stdin: stdin, cmd: cmd, killTree: killTree, closeTree: closeTree}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, p.Close())
	}
	return p, nil
}

func (p *codexProcess) Initialize(ctx context.Context) (sdk.InitializeResponse, error) {
	response, err := p.Client.Initialize(ctx, sdk.InitializeParams{ClientInfo: sdk.ClientInfo{Name: "agentos", Version: "1"}})
	if err == nil {
		err = p.transport.Notify(ctx, sdk.Notification{Method: "initialized"})
	}
	return response, err
}

func (p *codexProcess) Close() error {
	p.once.Do(func() {
		// Close input first, giving the CLI a bounded opportunity to exit. Keep
		// stdout open until the reader drains or the grace period expires.
		_ = p.stdin.Close()
		select {
		case <-p.transport.ReaderStopped():
		case <-time.After(3 * time.Second):
		}
		// Kill the tree even if the parent has exited: descendants may survive
		// after closing stdout. The platform helper treats an absent tree as success.
		p.err = errors.Join(p.killTree(), p.transport.Close())
		_ = p.cmd.Wait()
		p.err = errors.Join(p.err, p.closeTree())
	})
	return p.err
}

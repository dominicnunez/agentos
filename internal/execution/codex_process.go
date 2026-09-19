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
	stopOnce  sync.Once
	abortOnce sync.Once
	abort     chan struct{}
	stopped   chan struct{}
	waitDone  chan struct{}
	localStop bool
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
		stdin: stdin, cmd: cmd, killTree: killTree, closeTree: closeTree, abort: make(chan struct{}), stopped: make(chan struct{}), waitDone: make(chan struct{})}
	go reapCodexProcess(tr.ReaderStopped(), cmd.Wait, p.waitDone)
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, p.Close())
	}
	return p, nil
}

func reapCodexProcess(readerStopped <-chan struct{}, wait func() error, waitDone chan<- struct{}) {
	<-readerStopped
	_ = wait()
	close(waitDone)
}

func (p *codexProcess) Initialize(ctx context.Context) (sdk.InitializeResponse, error) {
	response, err := p.Client.Initialize(ctx, sdk.InitializeParams{ClientInfo: sdk.ClientInfo{Name: "agentos", Version: "1"}})
	if err == nil {
		err = p.transport.Notify(ctx, sdk.Notification{Method: "initialized"})
	}
	return response, err
}

func (p *codexProcess) Close() error {
	p.stopOnce.Do(func() {
		p.stop(true)
	})
	<-p.stopped
	return p.err
}

// Abort bypasses the graceful stdin drain and stops the owned process tree.
// A concurrent graceful Close observes the abort signal and skips its grace.
func (p *codexProcess) Abort() (bool, error) {
	p.abortOnce.Do(func() { close(p.abort) })
	p.stopOnce.Do(func() {
		p.stop(false)
	})
	<-p.stopped
	return p.localStop, p.err
}

func (p *codexProcess) stop(graceful bool) {
	defer close(p.stopped)
	if graceful {
		// Close input first, giving the CLI a bounded opportunity to exit. Keep
		// stdout open until the reader drains or the grace period expires.
		_ = p.stdin.Close()
		select {
		case <-p.transport.ReaderStopped():
		case <-time.After(3 * time.Second):
		case <-p.abort:
		}
	} else {
		_ = p.stdin.Close()
	}
	// Kill the tree even if the parent has exited: descendants may survive
	// after closing stdout. The platform helper treats an absent tree as success.
	killErr := p.killTree()
	transportErr := p.transport.Close()
	waitErr := error(nil)
	if killErr == nil {
		select {
		case <-p.waitDone:
			p.localStop = p.cmd.ProcessState != nil
		case <-time.After(3 * time.Second):
			waitErr = errors.New("codex process tree termination was not observed")
		}
	}
	p.err = errors.Join(killErr, transportErr, waitErr, p.closeTree())
}

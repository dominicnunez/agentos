//go:build !windows

package execution

import (
	"errors"
	"os/exec"
	"syscall"
)

func configureCodexCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func attachCodexProcessTree(cmd *exec.Cmd) (func() error, func() error, error) {
	return func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return err
	}, func() error { return nil }, nil
}

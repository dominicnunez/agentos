//go:build !linux

package fileguard

import (
	"fmt"
	"os"
)

type ProcessLock struct{}

func AcquireProcessLock(_ string, _ os.FileMode) (*ProcessLock, error) {
	return nil, fmt.Errorf("Agent OS V1 process locks are supported on Linux")
}

func (l *ProcessLock) Close() error { return nil }

//go:build linux

package fileguard

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// ProcessLock is an advisory, non-blocking Linux lock held by an open file
// descriptor. The stable lock file remains on disk; process exit releases the
// kernel lock without creating stale ownership evidence.
type ProcessLock struct {
	file *os.File
}

func AcquireProcessLock(path string, directoryMode os.FileMode) (*ProcessLock, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || directoryMode == 0 || directoryMode != directoryMode.Perm() {
		return nil, fmt.Errorf("process lock path or directory mode is invalid")
	}
	directory := filepath.Dir(path)
	if err := ValidateDirectoryChain(directory); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(directory, directoryMode); err != nil {
		return nil, err
	}
	if err := ValidateDirectoryChain(directory); err != nil {
		return nil, err
	}
	fd, err := syscall.Open(path, syscall.O_CREAT|syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = syscall.Close(fd)
		return nil, fmt.Errorf("open process lock")
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return nil, errors.Join(fmt.Errorf("process lock must be a private regular file"), err, file.Close())
	}
	if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, errors.Join(fmt.Errorf("process lock is already held: %w", err), file.Close())
	}
	return &ProcessLock{file: file}, nil
}

func (l *ProcessLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	file := l.file
	l.file = nil
	return errors.Join(syscall.Flock(int(file.Fd()), syscall.LOCK_UN), file.Close())
}

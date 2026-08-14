// Package fileguard provides local filesystem mutations that must remain
// shared across callers without merging their separate trust policies.
package fileguard

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// WriteAtomically replaces path through a same-directory temporary file.
// Callers remain responsible for any ownership, privacy, and content policy
// beyond the symlink and permission-shape checks enforced here.
func WriteAtomically(path string, body []byte, fileMode, directoryMode os.FileMode) error {
	if fileMode == 0 || fileMode != fileMode.Perm() || directoryMode == 0 || directoryMode != directoryMode.Perm() {
		return fmt.Errorf("atomic file modes must contain permissions only")
	}
	directory := filepath.Dir(path)
	if err := rejectSymlinkComponents(directory); err != nil {
		return err
	}
	if err := os.MkdirAll(directory, directoryMode); err != nil {
		return fmt.Errorf("create %s: %w", directory, err)
	}
	if err := rejectSymlinkComponents(directory); err != nil {
		return err
	}
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("refuse to replace symlink %s", path)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".agentos-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(fileMode); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(body); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if runtime.GOOS == "windows" {
		_ = os.Remove(path)
	}
	return os.Rename(temporaryPath, path)
}

func rejectSymlinkComponents(path string) error {
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) {
		return fmt.Errorf("path must be absolute")
	}
	volume := filepath.VolumeName(cleaned)
	remainder := strings.TrimPrefix(cleaned, volume)
	current := volume + string(filepath.Separator)
	for _, component := range strings.FieldsFunc(remainder, func(character rune) bool { return character == '/' || character == '\\' }) {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("path component %s must be a directory, not a link", current)
		}
	}
	return nil
}

// Package fileguard provides local filesystem mutations that must remain
// shared across callers without merging their separate trust policies.
package fileguard

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// ReadUnchangedBoundedFile reads the exact regular-file snapshot already
// inspected by a caller. Callers retain ownership, permission, type, and
// content policy; this function centralizes the stable-open and bounded-read
// race checks.
func ReadUnchangedBoundedFile(path string, before os.FileInfo, maximum int64, label string) ([]byte, error) {
	if before == nil || maximum <= 0 || !before.Mode().IsRegular() || before.Size() <= 0 || before.Size() > maximum || label == "" {
		return nil, fmt.Errorf("%s file snapshot is invalid", label)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return nil, fmt.Errorf("%s changed while it was opened", label)
	}
	body, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || int64(len(body)) != before.Size() {
		clear(body)
		return nil, fmt.Errorf("%s changed while it was read", label)
	}
	return body, nil
}

// WriteAtomically replaces path through a same-directory temporary file.
// Callers remain responsible for any ownership, privacy, and content policy
// beyond the symlink and permission-shape checks enforced here.
func WriteAtomically(path string, body []byte, fileMode, directoryMode os.FileMode) error {
	directory, temporaryPath, err := prepareAtomicWrite(path, body, fileMode, directoryMode)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(temporaryPath) }()
	if runtime.GOOS == "windows" {
		_ = os.Remove(path)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	return syncDirectory(directory)
}

// WriteAtomicallyNew publishes a complete same-directory temporary file only
// when path does not exist. The hard-link step is atomic and cannot replace an
// attacker-created or concurrently-created destination.
func WriteAtomicallyNew(path string, body []byte, fileMode, directoryMode os.FileMode) error {
	directory, temporaryPath, err := prepareAtomicWrite(path, body, fileMode, directoryMode)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := os.Link(temporaryPath, path); err != nil {
		return err
	}
	if err := os.Remove(temporaryPath); err != nil {
		return err
	}
	return syncDirectory(directory)
}

func prepareAtomicWrite(path string, body []byte, fileMode, directoryMode os.FileMode) (string, string, error) {
	if fileMode == 0 || fileMode != fileMode.Perm() || directoryMode == 0 || directoryMode != directoryMode.Perm() {
		return "", "", fmt.Errorf("atomic file modes must contain permissions only")
	}
	directory := filepath.Dir(path)
	if err := rejectSymlinkComponents(directory); err != nil {
		return "", "", err
	}
	if err := os.MkdirAll(directory, directoryMode); err != nil {
		return "", "", fmt.Errorf("create %s: %w", directory, err)
	}
	if err := rejectSymlinkComponents(directory); err != nil {
		return "", "", err
	}
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return "", "", fmt.Errorf("refuse to replace symlink %s", path)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", "", err
	}
	temporary, err := os.CreateTemp(directory, ".agentos-*")
	if err != nil {
		return "", "", err
	}
	temporaryPath := temporary.Name()
	if err := temporary.Chmod(fileMode); err != nil {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
		return "", "", err
	}
	if _, err := temporary.Write(body); err != nil {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
		return "", "", err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
		return "", "", err
	}
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return "", "", err
	}
	return directory, temporaryPath, nil
}

func syncDirectory(directory string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	directoryFile, err := os.Open(directory)
	if err != nil {
		return err
	}
	return errors.Join(directoryFile.Sync(), directoryFile.Close())
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

// ValidateDirectoryChain rejects non-absolute paths and any existing path
// component that is not a real directory. Missing trailing components are
// permitted so callers can create them after validation and then revalidate.
func ValidateDirectoryChain(path string) error {
	return rejectSymlinkComponents(path)
}

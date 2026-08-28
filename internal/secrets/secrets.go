// Package secrets keeps credential resolution behind runtime adapters.
package secrets

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type Ref string
type Value string
type Source interface {
	Resolve(context.Context, Ref) (Value, error)
}
type Environment struct{ Prefix string }
type Values map[Ref]Value

func (e Environment) Resolve(_ context.Context, ref Ref) (Value, error) {
	v, ok := os.LookupEnv(e.Prefix + string(ref))
	if !ok {
		return "", fmt.Errorf("secret %q is unavailable", ref)
	}
	return Value(v), nil
}

func (v Values) Resolve(_ context.Context, ref Ref) (Value, error) {
	value, ok := v[ref]
	if !ok || value == "" {
		return "", fmt.Errorf("secret %q is unavailable", ref)
	}
	return value, nil
}

// CredentialDirectory resolves credentials provisioned by a service manager,
// such as systemd's LoadCredentialEncrypted=. References are names, never paths.
type CredentialDirectory struct{ Path string }

func (c CredentialDirectory) Resolve(_ context.Context, ref Ref) (Value, error) {
	name := string(ref)
	if name == "" || len(name) > 128 || filepath.Base(name) != name || strings.ContainsAny(name, `/\\`) {
		return "", fmt.Errorf("secret reference is invalid")
	}
	directory := c.Path
	if directory == "" {
		directory = os.Getenv("CREDENTIALS_DIRECTORY")
	}
	if !filepath.IsAbs(directory) {
		return "", fmt.Errorf("service credential directory is unavailable")
	}
	path := filepath.Join(directory, name)
	info, err := os.Lstat(path)
	if err != nil {
		return "", fmt.Errorf("secret %q is unavailable", ref)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 64<<10 || !privateCredentialFile(path, info) {
		return "", fmt.Errorf("secret %q is unavailable", ref)
	}
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("secret %q is unavailable", ref)
	}
	defer func() { _ = file.Close() }()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return "", fmt.Errorf("secret %q is unavailable", ref)
	}
	value, err := io.ReadAll(io.LimitReader(file, 64<<10+1))
	if err != nil || int64(len(value)) != info.Size() {
		clear(value)
		return "", fmt.Errorf("secret %q is unavailable", ref)
	}
	value = bytes.TrimSuffix(value, []byte{'\n'})
	if len(value) == 0 {
		return "", fmt.Errorf("secret %q is unavailable", ref)
	}
	return Value(value), nil
}

//go:build linux

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/dominicnunez/agentos/internal/bootstrap"
	"github.com/dominicnunez/agentos/internal/gateway"
	"golang.org/x/sys/unix"
)

type peerUIDConn struct {
	net.Conn
	uid int
}

type peerUIDListener struct{ net.Listener }

func (l peerUIDListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	unixConnection, ok := connection.(*net.UnixConn)
	if !ok {
		_ = connection.Close()
		return nil, fmt.Errorf("local user listener accepted a non-Unix connection")
	}
	raw, err := unixConnection.SyscallConn()
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	var credential *unix.Ucred
	var socketErr error
	if err := raw.Control(func(fd uintptr) {
		credential, socketErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		_ = connection.Close()
		return nil, err
	}
	if socketErr != nil || credential == nil {
		_ = connection.Close()
		return nil, fmt.Errorf("local user peer identity is unavailable")
	}
	return &peerUIDConn{Conn: connection, uid: int(credential.Uid)}, nil
}

func listenLocalHuman(ctx context.Context, path string, ownerUID, ownerGID int) (net.Listener, error) {
	if ownerUID < 0 || ownerGID < 0 || !filepath.IsAbs(path) {
		return nil, fmt.Errorf("local user socket requires an absolute path and valid owner")
	}
	if listener, found, err := activatedHumanListener(path, ownerUID); found || err != nil {
		return listener, err
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o711); err != nil {
		return nil, fmt.Errorf("create user runtime directory: %w", err)
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("refuse to replace non-socket user endpoint %s", path)
		}
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("remove stale user socket: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "unix", path)
	if err != nil {
		return nil, fmt.Errorf("listen on local user socket: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		return nil, err
	}
	if err := os.Chown(path, ownerUID, ownerGID); err != nil {
		_ = listener.Close()
		return nil, err
	}
	return peerUIDListener{Listener: listener}, nil
}

func validateRuntimeBoundary(config bootstrap.Config) error {
	socketDirectoryUID := config.Owner.UID
	socketDirectoryMode := os.FileMode(0o700)
	if config.Mode == bootstrap.ModeSystem {
		socketDirectoryUID = 0
		socketDirectoryMode = 0o711
	}
	if err := ensureOwnedRuntimeDirectory(config.Paths.RuntimeDir, socketDirectoryUID, socketDirectoryMode); err != nil {
		return fmt.Errorf("user socket directory: %w", err)
	}
	if err := ensureOwnedRuntimeDirectory(providerRuntimeDirectory(config), effectiveUID(), 0o700); err != nil {
		return fmt.Errorf("provider runtime directory: %w", err)
	}
	return nil
}

func ensureOwnedRuntimeDirectory(path string, ownerUID int, mode os.FileMode) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) || ownerUID < 0 {
		return fmt.Errorf("runtime directory path or owner is invalid")
	}
	created := false
	if info, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(path, mode); err != nil {
			return err
		}
		created = true
	} else if err != nil {
		return err
	} else if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("runtime path must be a directory, not a link")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return fmt.Errorf("runtime directory must not traverse a link")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != ownerUID {
		return fmt.Errorf("runtime directory owner is invalid")
	}
	if created {
		if err := os.Chmod(path, mode); err != nil {
			return err
		}
		info, err = os.Lstat(path)
		if err != nil {
			return err
		}
	}
	if info.Mode().Perm() != mode.Perm() {
		return fmt.Errorf("runtime directory mode must be %04o", mode.Perm())
	}
	return nil
}

func activatedHumanListener(path string, ownerUID int) (net.Listener, bool, error) {
	pid, pidErr := strconv.Atoi(os.Getenv("LISTEN_PID"))
	fds, fdsErr := strconv.Atoi(os.Getenv("LISTEN_FDS"))
	if pidErr != nil || fdsErr != nil || pid != os.Getpid() || fds == 0 {
		return nil, false, nil
	}
	if fds != 1 {
		return nil, true, fmt.Errorf("Agent OS requires exactly one activated user socket")
	}
	file := os.NewFile(3, "agentos-user.socket")
	if file == nil {
		return nil, true, fmt.Errorf("activated user socket is unavailable")
	}
	defer func() { _ = file.Close() }()
	listener, err := net.FileListener(file)
	if err != nil {
		return nil, true, fmt.Errorf("adopt activated user socket: %w", err)
	}
	if listener.Addr().Network() != "unix" || listener.Addr().String() != path {
		_ = listener.Close()
		return nil, true, fmt.Errorf("activated user socket does not match configuration")
	}
	uid, mode, err := fileOwner(path)
	if err != nil || uid != ownerUID || mode.Perm() != 0o600 || mode&os.ModeSocket == 0 {
		_ = listener.Close()
		return nil, true, fmt.Errorf("activated user socket ownership or mode is invalid")
	}
	_ = os.Unsetenv("LISTEN_PID")
	_ = os.Unsetenv("LISTEN_FDS")
	_ = os.Unsetenv("LISTEN_FDNAMES")
	return peerUIDListener{Listener: listener}, true, nil
}

func localConnContext(ctx context.Context, connection net.Conn) context.Context {
	if peer, ok := connection.(*peerUIDConn); ok {
		return gateway.ContextWithPeerUID(ctx, peer.uid)
	}
	return ctx
}

func effectiveUID() int { return syscall.Geteuid() }

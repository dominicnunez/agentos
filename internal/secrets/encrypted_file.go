package secrets

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
)

const (
	encryptedFileHeader = "AGENTOS-SEAL-1\x00"
	MaximumSealedBytes  = 64 << 10
)

func SealFile(path, purpose string, key, plaintext []byte) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || purpose == "" || len(key) != 32 || len(plaintext) == 0 || len(plaintext) > MaximumSealedBytes {
		return fmt.Errorf("sealed credential path, purpose, key, or content is invalid")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return err
	}
	sealed := aead.Seal(nil, nonce, plaintext, []byte(purpose))
	body := make([]byte, 0, len(encryptedFileHeader)+len(nonce)+len(sealed))
	body = append(body, encryptedFileHeader...)
	body = append(body, nonce...)
	body = append(body, sealed...)
	directory := filepath.Dir(path)
	if info, err := os.Lstat(directory); err == nil && (info.Mode()&os.ModeSymlink != 0 || !info.IsDir()) {
		return fmt.Errorf("sealed credential directory must not be a link")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	if runtime.GOOS == "linux" {
		resolved, err := filepath.EvalSymlinks(directory)
		info, statErr := os.Lstat(directory)
		if err != nil || statErr != nil || resolved != directory || info.Mode().Perm()&0o077 != 0 {
			return fmt.Errorf("sealed credential directory must be private and must not traverse a link")
		}
	}
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("refuse to replace sealed credential symlink")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".agentos-seal-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o600); err != nil {
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
	return os.Rename(temporaryPath, path)
}

func OpenSealedFile(path, purpose string, key []byte) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || purpose == "" || len(key) != 32 {
		return nil, fmt.Errorf("sealed credential path, purpose, or key is invalid")
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() <= int64(len(encryptedFileHeader)) || info.Size() > MaximumSealedBytes+1024 || (runtime.GOOS == "linux" && info.Mode().Perm()&0o077 != 0) {
		return nil, fmt.Errorf("sealed credential is not a private bounded regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, fmt.Errorf("sealed credential changed while it was opened")
	}
	body, err := io.ReadAll(io.LimitReader(file, MaximumSealedBytes+1025))
	if err != nil || int64(len(body)) != info.Size() {
		return nil, fmt.Errorf("sealed credential changed while it was read")
	}
	if len(body) < len(encryptedFileHeader) || string(body[:len(encryptedFileHeader)]) != encryptedFileHeader {
		return nil, fmt.Errorf("sealed credential format is invalid")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	payload := body[len(encryptedFileHeader):]
	if len(payload) <= aead.NonceSize() {
		return nil, fmt.Errorf("sealed credential payload is invalid")
	}
	plaintext, err := aead.Open(nil, payload[:aead.NonceSize()], payload[aead.NonceSize():], []byte(purpose))
	if err != nil || len(plaintext) == 0 || len(plaintext) > MaximumSealedBytes {
		clear(plaintext)
		return nil, fmt.Errorf("sealed credential authentication failed")
	}
	return plaintext, nil
}

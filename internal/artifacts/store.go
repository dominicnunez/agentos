// Package artifacts owns bounded, content-addressed artifact storage.
package artifacts

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/dominicnunez/agentos/internal/core"
)

const MaximumArtifactBytes = 16 << 20

type Upload struct {
	Role      string `json:"role"`
	Name      string `json:"name"`
	MediaType string `json:"media_type"`
	Data      []byte `json:"data"`
}

type Store struct{ Root string }

func (s Store) Put(organizationID, taskID, principalID string, upload Upload) (core.ArtifactEvidence, bool, error) {
	if !filepath.IsAbs(s.Root) || organizationID == "" || taskID == "" || principalID == "" {
		return core.ArtifactEvidence{}, false, fmt.Errorf("artifact store and durable origin are required")
	}
	evidence, err := Inspect(principalID, upload)
	if err != nil {
		return core.ArtifactEvidence{}, false, err
	}
	hash := evidence.SHA256
	directory := filepath.Join(s.Root, hash[:2])
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return core.ArtifactEvidence{}, false, err
	}
	path := filepath.Join(directory, hash)
	created := false
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		existing, readErr := readExistingArtifact(path, int64(len(upload.Data)))
		if readErr != nil || !bytes.Equal(existing, upload.Data) {
			return core.ArtifactEvidence{}, false, fmt.Errorf("content-addressed artifact collision")
		}
	} else if err != nil {
		return core.ArtifactEvidence{}, false, err
	} else {
		created = true
		if _, err := file.Write(upload.Data); err != nil {
			_ = file.Close()
			_ = os.Remove(path)
			return core.ArtifactEvidence{}, false, err
		}
		if err := file.Sync(); err != nil {
			_ = file.Close()
			_ = os.Remove(path)
			return core.ArtifactEvidence{}, false, err
		}
		if err := file.Close(); err != nil {
			_ = os.Remove(path)
			return core.ArtifactEvidence{}, false, err
		}
	}
	return evidence, created, nil
}

// Inspect validates an upload and derives its immutable evidence metadata
// without writing any bytes. Callers can use it to validate an entire
// completion contract before admitting artifacts to durable storage.
func Inspect(principalID string, upload Upload) (core.ArtifactEvidence, error) {
	if principalID == "" {
		return core.ArtifactEvidence{}, fmt.Errorf("artifact origin is required")
	}
	mediaType, err := validateUpload(upload)
	if err != nil {
		return core.ArtifactEvidence{}, err
	}
	digest := sha256.Sum256(upload.Data)
	hash := hex.EncodeToString(digest[:])
	return core.ArtifactEvidence{
		Ref: "artifact/sha256/" + hash, Role: upload.Role, Name: upload.Name, MediaType: mediaType,
		SHA256: hash, Size: int64(len(upload.Data)), Origin: principalID, Trust: "UNTRUSTED_USER_ARTIFACT",
	}, nil
}

func readExistingArtifact(path string, expectedSize int64) ([]byte, error) {
	before, err := os.Lstat(path)
	if err != nil || before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() || (runtime.GOOS == "linux" && before.Mode().Perm()&0o077 != 0) || before.Size() != expectedSize {
		return nil, fmt.Errorf("existing artifact is not a private regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) {
		return nil, fmt.Errorf("existing artifact changed during validation")
	}
	value := make([]byte, expectedSize)
	if _, err := io.ReadFull(file, value); err != nil {
		return nil, err
	}
	var trailing [1]byte
	if count, err := file.Read(trailing[:]); count != 0 || (err != nil && !errors.Is(err, io.EOF)) {
		return nil, fmt.Errorf("existing artifact size changed during validation")
	}
	return value, nil
}

func validateUpload(upload Upload) (string, error) {
	if upload.Role == "" || len(upload.Role) > 64 || strings.TrimSpace(upload.Role) != upload.Role || strings.ContainsAny(upload.Role, `/\\`) || hasUnsafeTextControl(upload.Role) {
		return "", fmt.Errorf("artifact role is invalid")
	}
	if upload.Name == "" || len(upload.Name) > 255 || !utf8.ValidString(upload.Name) || filepath.Base(upload.Name) != upload.Name || hasUnsafeTextControl(upload.Name) {
		return "", fmt.Errorf("artifact name is invalid")
	}
	if len(upload.Data) == 0 || len(upload.Data) > MaximumArtifactBytes {
		return "", fmt.Errorf("artifact must contain 1 to %d bytes", MaximumArtifactBytes)
	}
	detected, _, err := mime.ParseMediaType(http.DetectContentType(upload.Data))
	if err != nil {
		return "", fmt.Errorf("artifact content type is invalid")
	}
	if upload.MediaType == "" {
		return detected, nil
	}
	mediaType, parameters, err := mime.ParseMediaType(upload.MediaType)
	if err != nil || len(parameters) != 0 || mediaType != upload.MediaType {
		return "", fmt.Errorf("artifact media type is invalid")
	}
	if detected != mediaType {
		return "", fmt.Errorf("artifact content does not match its media type")
	}
	return detected, nil
}

func hasUnsafeTextControl(value string) bool {
	return strings.IndexFunc(value, func(character rune) bool {
		return unicode.IsControl(character) || unicode.Is(unicode.Cf, character)
	}) >= 0
}

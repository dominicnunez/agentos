// Package anchor binds a verified Agent OS event-ledger head to a signed
// checkpoint outside SQLite. It detects database-only rollback, truncation,
// and substitution while deliberately making no trusted-time,
// non-repudiation, conformity, or certification claim.
package anchor

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/dominicnunez/agentos/internal/fileguard"
)

const (
	SchemaVersion      = 2
	SignatureAlgorithm = "Ed25519"
	TimeEvidence       = "SYSTEM_WALL_CLOCK_UNTRUSTED"
	MaximumFileBytes   = 64 << 10
	signatureDomain    = "agentos.ledger-anchor.v1"
)

var ErrAmbiguousPending = errors.New("ledger anchor has an ambiguous pending checkpoint")

// LedgerState is the complete durable state bound by one checkpoint. A
// non-empty Agent OS ledger is contiguous, so its event count equals its tip
// sequence.
type LedgerState struct {
	ApplicationID      int    `json:"application_id"`
	StorageVersion     int    `json:"storage_version"`
	EventSchemaVersion int    `json:"event_schema_version"`
	EventCount         int64  `json:"event_count"`
	Sequence           int64  `json:"sequence"`
	EventID            string `json:"event_id,omitempty"`
	ChainAlgorithm     string `json:"chain_algorithm"`
	ChainHead          string `json:"chain_head,omitempty"`
	AuthorityCount     int64  `json:"authority_count"`
	AuthorityAlgorithm string `json:"authority_algorithm"`
	AuthoritySHA256    string `json:"authority_sha256"`
}

// Checkpoint is public evidence. Its time is ordinary host wall-clock
// evidence, not a trusted timestamp.
type Checkpoint struct {
	SchemaVersion            int         `json:"schema_version"`
	InstallationID           string      `json:"installation_id"`
	Generation               int64       `json:"generation"`
	Ledger                   LedgerState `json:"ledger"`
	ObservedAt               time.Time   `json:"observed_at"`
	TimeEvidence             string      `json:"time_evidence"`
	PreviousCheckpointSHA256 string      `json:"previous_checkpoint_sha256,omitempty"`
	SignatureAlgorithm       string      `json:"signature_algorithm"`
	KeyID                    string      `json:"key_id"`
	Signature                string      `json:"signature"`
}

type Store struct {
	mu             sync.Mutex
	path           string
	pendingPath    string
	installationID string
	publicKey      ed25519.PublicKey
	privateKey     ed25519.PrivateKey
	now            func() time.Time
	committed      Checkpoint
	committedBytes []byte
	prepared       *Checkpoint
	preparedBytes  []byte
	poisoned       error
}

func PublicKeyID(publicKey ed25519.PublicKey) (string, error) {
	if len(publicKey) != ed25519.PublicKeySize {
		return "", fmt.Errorf("ledger anchor public key is invalid")
	}
	digest := sha256.Sum256(publicKey)
	return hex.EncodeToString(digest[:]), nil
}

func PublicKeyFromPrivate(privateKey ed25519.PrivateKey) (ed25519.PublicKey, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("ledger anchor private key is invalid")
	}
	return append(ed25519.PublicKey(nil), privateKey[ed25519.SeedSize:]...), nil
}

func EncodePublicKey(publicKey ed25519.PublicKey) (string, error) {
	if _, err := PublicKeyID(publicKey); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(publicKey), nil
}

func DecodePublicKey(encoded string) (ed25519.PublicKey, error) {
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(decoded) != ed25519.PublicKeySize {
		clear(decoded)
		return nil, fmt.Errorf("ledger anchor public key is invalid")
	}
	return ed25519.PublicKey(decoded), nil
}

// Initialize writes the signed generation-zero checkpoint. It never replaces
// an existing committed or pending checkpoint.
func Initialize(path, installationID string, privateKey ed25519.PrivateKey, state LedgerState, observedAt time.Time) (Checkpoint, error) {
	if err := validatePath(path); err != nil {
		return Checkpoint{}, err
	}
	if _, err := os.Lstat(path); err == nil {
		return Checkpoint{}, fmt.Errorf("ledger anchor checkpoint already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return Checkpoint{}, fmt.Errorf("inspect ledger anchor checkpoint: %w", err)
	}
	if _, err := os.Lstat(pendingPath(path)); err == nil {
		return Checkpoint{}, fmt.Errorf("ledger anchor pending checkpoint already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return Checkpoint{}, fmt.Errorf("inspect ledger anchor pending checkpoint: %w", err)
	}
	checkpoint, body, err := newCheckpoint(installationID, 0, state, observedAt, "", privateKey)
	if err != nil {
		return Checkpoint{}, err
	}
	if err := fileguard.WriteAtomicallyNew(path, body, 0o600, 0o700); err != nil {
		return Checkpoint{}, fmt.Errorf("initialize ledger anchor checkpoint: %w", err)
	}
	return checkpoint, nil
}

// Open verifies the external checkpoint and reconciles only the unambiguous
// crash states. A pending checkpoint paired with the older database head is
// deliberately ambiguous and fails closed.
func Open(path, installationID string, publicKey ed25519.PublicKey, privateKey ed25519.PrivateKey, state LedgerState, now func() time.Time) (*Store, error) {
	if err := validatePath(path); err != nil {
		return nil, err
	}
	derivedPublicKey, keyErr := PublicKeyFromPrivate(privateKey)
	if !validInstallationID(installationID) || len(publicKey) != ed25519.PublicKeySize || keyErr != nil || !bytes.Equal(derivedPublicKey, publicKey) {
		return nil, fmt.Errorf("ledger anchor identity or signing key is invalid")
	}
	if err := state.Valid(); err != nil {
		return nil, err
	}
	committed, committedBytes, err := readCheckpoint(path, installationID, publicKey)
	if err != nil {
		return nil, fmt.Errorf("read committed ledger anchor: %w", err)
	}
	store := &Store{
		path: path, pendingPath: pendingPath(path), installationID: installationID,
		publicKey: append(ed25519.PublicKey(nil), publicKey...), privateKey: append(ed25519.PrivateKey(nil), privateKey...),
		now: now, committed: committed, committedBytes: committedBytes,
	}
	if store.now == nil {
		store.now = time.Now
	}
	pending, pendingBytes, pendingErr := readCheckpoint(store.pendingPath, installationID, publicKey)
	if errors.Is(pendingErr, os.ErrNotExist) {
		if !committed.Ledger.Equal(state) {
			return nil, fmt.Errorf("ledger head does not match its external checkpoint")
		}
		return store, nil
	}
	if pendingErr != nil {
		return nil, fmt.Errorf("read pending ledger anchor: %w", pendingErr)
	}
	if bytes.Equal(pendingBytes, committedBytes) {
		if err := removeAndSync(store.pendingPath); err != nil {
			return nil, fmt.Errorf("remove promoted ledger anchor checkpoint: %w", err)
		}
		if !committed.Ledger.Equal(state) {
			return nil, fmt.Errorf("ledger head does not match its external checkpoint")
		}
		return store, nil
	}
	if err := validateSuccessor(committed, committedBytes, pending); err != nil {
		return nil, err
	}
	if pending.Ledger.Equal(state) {
		if err := store.promote(pending, pendingBytes); err != nil {
			return nil, fmt.Errorf("finish committed ledger anchor promotion: %w", err)
		}
		return store, nil
	}
	if committed.Ledger.Equal(state) {
		return nil, fmt.Errorf("%w: reviewed recovery must decide whether the SQLite commit occurred", ErrAmbiguousPending)
	}
	return nil, fmt.Errorf("ledger head matches neither committed nor pending external checkpoint")
}

// Prepare writes a signed successor before the SQLite transaction commits.
// It returns false when the transaction did not change the ledger head.
func (s *Store) Prepare(state LedgerState) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned != nil {
		return false, s.poisoned
	}
	if s.prepared != nil {
		return false, fmt.Errorf("ledger anchor already has a prepared checkpoint")
	}
	if err := state.Valid(); err != nil {
		return false, err
	}
	if s.committed.Ledger.Equal(state) {
		return false, nil
	}
	if !state.ForwardFrom(s.committed.Ledger) {
		return false, fmt.Errorf("ledger anchor successor does not advance the committed head")
	}
	observedAt := s.now().UTC()
	checkpoint, body, err := newCheckpoint(s.installationID, s.committed.Generation+1, state, observedAt, checkpointSHA256(s.committedBytes), s.privateKey)
	if err != nil {
		return false, err
	}
	if observedAt.Before(s.committed.ObservedAt) {
		return false, fmt.Errorf("ledger anchor wall clock moved behind its committed checkpoint")
	}
	if err := fileguard.WriteAtomically(s.pendingPath, body, 0o600, 0o700); err != nil {
		return false, fmt.Errorf("write pending ledger anchor checkpoint: %w", err)
	}
	s.prepared, s.preparedBytes = &checkpoint, body
	return true, nil
}

// CommitPrepared promotes the already durable pending checkpoint after the
// SQLite commit. Failure poisons the Store so no later write can continue.
func (s *Store) CommitPrepared() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned != nil {
		return s.poisoned
	}
	if s.prepared == nil {
		return fmt.Errorf("ledger anchor has no prepared checkpoint")
	}
	if err := s.promote(*s.prepared, s.preparedBytes); err != nil {
		s.poisoned = fmt.Errorf("promote ledger anchor checkpoint: %w", err)
		return s.poisoned
	}
	s.prepared, s.preparedBytes = nil, nil
	return nil
}

// VerifyState rechecks the live ledger against the committed checkpoint before
// the Store is attached to a writer.
func (s *Store) VerifyState(state LedgerState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned != nil {
		return s.poisoned
	}
	if s.prepared != nil {
		return fmt.Errorf("ledger anchor has an unresolved prepared checkpoint")
	}
	if err := state.Valid(); err != nil {
		return err
	}
	if !s.committed.Ledger.Equal(state) {
		return fmt.Errorf("ledger head does not match its external checkpoint")
	}
	return nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var closeErr error
	if s.prepared != nil {
		closeErr = fmt.Errorf("ledger anchor closed with an unresolved prepared checkpoint")
	}
	clear(s.privateKey)
	s.privateKey = nil
	s.poisoned = fmt.Errorf("ledger anchor is closed")
	return closeErr
}

func (s *Store) promote(checkpoint Checkpoint, body []byte) error {
	if err := fileguard.WriteAtomically(s.path, body, 0o600, 0o700); err != nil {
		return err
	}
	if err := removeAndSync(s.pendingPath); err != nil {
		return err
	}
	s.committed, s.committedBytes = checkpoint, append([]byte(nil), body...)
	return nil
}

func Read(path, installationID string, publicKey ed25519.PublicKey) (Checkpoint, []byte, error) {
	return readCheckpoint(path, installationID, publicKey)
}

func readCheckpoint(path, installationID string, publicKey ed25519.PublicKey) (Checkpoint, []byte, error) {
	body, err := readPrivateRegularFile(path)
	if err != nil {
		return Checkpoint{}, nil, err
	}
	var checkpoint Checkpoint
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&checkpoint); err != nil {
		return Checkpoint{}, nil, fmt.Errorf("decode checkpoint: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Checkpoint{}, nil, fmt.Errorf("checkpoint has trailing content")
	}
	canonical, err := marshalCheckpoint(checkpoint)
	if err != nil || !bytes.Equal(canonical, body) {
		return Checkpoint{}, nil, fmt.Errorf("checkpoint is not in canonical form")
	}
	if err := verifyCheckpoint(checkpoint, installationID, publicKey); err != nil {
		return Checkpoint{}, nil, err
	}
	return checkpoint, body, nil
}

func newCheckpoint(installationID string, generation int64, state LedgerState, observedAt time.Time, previous string, privateKey ed25519.PrivateKey) (Checkpoint, []byte, error) {
	if !validInstallationID(installationID) || generation < 0 || len(privateKey) != ed25519.PrivateKeySize || observedAt.IsZero() || observedAt.Location() != time.UTC {
		return Checkpoint{}, nil, fmt.Errorf("ledger anchor checkpoint identity, generation, key, or time is invalid")
	}
	if err := state.Valid(); err != nil {
		return Checkpoint{}, nil, err
	}
	if generation == 0 && previous != "" || generation > 0 && !validSHA256(previous) {
		return Checkpoint{}, nil, fmt.Errorf("ledger anchor predecessor is invalid")
	}
	publicKey, err := PublicKeyFromPrivate(privateKey)
	if err != nil {
		return Checkpoint{}, nil, err
	}
	keyID, _ := PublicKeyID(publicKey)
	checkpoint := Checkpoint{
		SchemaVersion: SchemaVersion, InstallationID: installationID, Generation: generation,
		Ledger: state, ObservedAt: observedAt, TimeEvidence: TimeEvidence,
		PreviousCheckpointSHA256: previous, SignatureAlgorithm: SignatureAlgorithm, KeyID: keyID,
	}
	checkpoint.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, signaturePayload(checkpoint)))
	body, err := marshalCheckpoint(checkpoint)
	return checkpoint, body, err
}

func verifyCheckpoint(checkpoint Checkpoint, installationID string, publicKey ed25519.PublicKey) error {
	keyID, err := PublicKeyID(publicKey)
	if err != nil {
		return err
	}
	if checkpoint.SchemaVersion != SchemaVersion || checkpoint.InstallationID != installationID || checkpoint.Generation < 0 || checkpoint.ObservedAt.IsZero() || checkpoint.ObservedAt.Location() != time.UTC || checkpoint.TimeEvidence != TimeEvidence || checkpoint.SignatureAlgorithm != SignatureAlgorithm || checkpoint.KeyID != keyID {
		return fmt.Errorf("ledger anchor checkpoint envelope is invalid")
	}
	if err := checkpoint.Ledger.Valid(); err != nil {
		return err
	}
	if checkpoint.Generation == 0 && checkpoint.PreviousCheckpointSHA256 != "" || checkpoint.Generation > 0 && !validSHA256(checkpoint.PreviousCheckpointSHA256) {
		return fmt.Errorf("ledger anchor checkpoint predecessor is invalid")
	}
	signature, err := base64.StdEncoding.DecodeString(checkpoint.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize || !ed25519.Verify(publicKey, signaturePayload(checkpoint), signature) {
		return fmt.Errorf("ledger anchor checkpoint signature is invalid")
	}
	return nil
}

func validateSuccessor(committed Checkpoint, committedBytes []byte, pending Checkpoint) error {
	if pending.Generation != committed.Generation+1 || pending.PreviousCheckpointSHA256 != checkpointSHA256(committedBytes) || pending.ObservedAt.Before(committed.ObservedAt) || !pending.Ledger.ForwardFrom(committed.Ledger) {
		return fmt.Errorf("pending ledger anchor is not the exact committed successor")
	}
	return nil
}

func (s LedgerState) Valid() error {
	if s.ApplicationID <= 0 || s.StorageVersion < 1 || s.EventSchemaVersion < 1 || s.EventCount < 0 || s.Sequence < 0 || s.EventCount != s.Sequence || s.ChainAlgorithm != "SHA-256" || s.AuthorityCount < 0 || s.AuthorityAlgorithm != "SHA-256" || !validSHA256(s.AuthoritySHA256) {
		return fmt.Errorf("ledger anchor state is invalid")
	}
	if s.EventCount == 0 {
		if s.EventID != "" || s.ChainHead != "" {
			return fmt.Errorf("empty ledger anchor state has a non-empty head")
		}
		return nil
	}
	if s.EventID == "" || !validSHA256(s.ChainHead) {
		return fmt.Errorf("non-empty ledger anchor state lacks its exact head")
	}
	return nil
}

func (s LedgerState) Equal(other LedgerState) bool { return s == other }

func (s LedgerState) ForwardFrom(previous LedgerState) bool {
	return s.ApplicationID == previous.ApplicationID && s.StorageVersion == previous.StorageVersion && s.EventSchemaVersion == previous.EventSchemaVersion && s.EventCount > previous.EventCount && s.Sequence > previous.Sequence && s.AuthorityCount >= previous.AuthorityCount
}

func signaturePayload(checkpoint Checkpoint) []byte {
	digest := sha256.New()
	writeField(digest, []byte(signatureDomain))
	writeInt(digest, int64(checkpoint.SchemaVersion))
	writeField(digest, []byte(checkpoint.InstallationID))
	writeInt(digest, checkpoint.Generation)
	writeInt(digest, int64(checkpoint.Ledger.ApplicationID))
	writeInt(digest, int64(checkpoint.Ledger.StorageVersion))
	writeInt(digest, int64(checkpoint.Ledger.EventSchemaVersion))
	writeInt(digest, checkpoint.Ledger.EventCount)
	writeInt(digest, checkpoint.Ledger.Sequence)
	writeField(digest, []byte(checkpoint.Ledger.EventID))
	writeField(digest, []byte(checkpoint.Ledger.ChainAlgorithm))
	writeField(digest, []byte(checkpoint.Ledger.ChainHead))
	writeInt(digest, checkpoint.Ledger.AuthorityCount)
	writeField(digest, []byte(checkpoint.Ledger.AuthorityAlgorithm))
	writeField(digest, []byte(checkpoint.Ledger.AuthoritySHA256))
	writeField(digest, []byte(checkpoint.ObservedAt.Format(time.RFC3339Nano)))
	writeField(digest, []byte(checkpoint.TimeEvidence))
	writeField(digest, []byte(checkpoint.PreviousCheckpointSHA256))
	writeField(digest, []byte(checkpoint.SignatureAlgorithm))
	writeField(digest, []byte(checkpoint.KeyID))
	return digest.Sum(nil)
}

func marshalCheckpoint(checkpoint Checkpoint) ([]byte, error) {
	body, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(body, '\n'), nil
}

func checkpointSHA256(body []byte) string {
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}

func pendingPath(path string) string { return path + ".pending" }

func validatePath(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || filepath.Base(path) == "." || filepath.Base(path) == string(filepath.Separator) {
		return fmt.Errorf("ledger anchor path must be a canonical absolute file path")
	}
	return nil
}

func readPrivateRegularFile(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > MaximumFileBytes || runtime.GOOS == "linux" && info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("ledger anchor must be a private bounded regular file")
	}
	return fileguard.ReadUnchangedBoundedFile(path, info, MaximumFileBytes, "ledger anchor")
}

func removeAndSync(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if runtime.GOOS == "windows" {
		return nil
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func validInstallationID(value string) bool {
	if len(value) != len("install-")+32*2 || value[:len("install-")] != "install-" {
		return false
	}
	_, err := hex.DecodeString(value[len("install-"):])
	return err == nil
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == value
}

func writeField(digest hash.Hash, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = digest.Write(length[:])
	_, _ = digest.Write(value)
}

func writeInt(digest hash.Hash, value int64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(value))
	_, _ = digest.Write(encoded[:])
}

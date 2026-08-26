package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/dominicnunez/agentos/internal/bootstrap"
	"github.com/dominicnunez/agentos/internal/fileguard"
	"github.com/dominicnunez/agentos/internal/ledger"
	ledgeranchor "github.com/dominicnunez/agentos/internal/ledger/anchor"
)

const (
	integrityRotateKey      = "rotate-key"
	integrityRecoverKey     = "recover-key"
	integrityResolvePending = "resolve-pending"
)

func runIntegrityMaintenance(ctx context.Context, args []string, input *os.File, output io.Writer) error {
	if len(args) == 0 || (args[0] != integrityRotateKey && args[0] != integrityRecoverKey && args[0] != integrityResolvePending) {
		return fmt.Errorf("use agentos integrity rotate-key, agentos integrity recover-key, or agentos integrity resolve-pending")
	}
	action := args[0]
	configPath, err := parseConfigPath("integrity "+action, args[1:])
	if err != nil {
		return err
	}
	config, err := bootstrap.LoadConfig(configPath)
	if err != nil {
		return err
	}
	if err := config.ValidateReady(); err != nil {
		return fmt.Errorf("installation configuration is invalid: %w", err)
	}
	ui := newTerminalUI(input, output)
	completed, err := ensureIntegrityMaintenancePrivileges(ctx, config, ui, action)
	if err != nil || completed {
		return err
	}
	authorizedBy, err := integrityMaintenanceAuthority(ctx, config)
	if err != nil {
		return err
	}
	if err := requireIntegrityServiceStopped(ctx, config); err != nil {
		return err
	}
	if action == integrityResolvePending {
		return resolveAmbiguousPendingCheckpoint(ctx, config, authorizedBy, ui, output)
	}
	if resumed, err := resumePendingKeyTransition(ctx, configPath, config, action); err != nil {
		return err
	} else if resumed {
		_, err = fmt.Fprintln(output, "Ledger anchor key transition recovered and completed. The service remains stopped.")
		return err
	}
	if _, err := os.Lstat(config.Integrity.CheckpointFile + ".pending"); err == nil {
		return fmt.Errorf("resolve the pending ledger checkpoint before changing its signing key")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect pending ledger checkpoint: %w", err)
	}

	state, err := currentIntegrityState(ctx, config.Paths.Database)
	if err != nil {
		return err
	}
	previousPublicKey, err := ledgeranchor.DecodePublicKey(config.Integrity.PublicKey)
	if err != nil {
		return err
	}
	previous, previousBody, err := ledgeranchor.Read(config.Integrity.CheckpointFile, config.Integrity.InstallationID, previousPublicKey)
	if err != nil {
		return fmt.Errorf("verify current external ledger checkpoint: %w", err)
	}
	if !previous.Ledger.Equal(state) {
		return fmt.Errorf("current SQLite ledger does not match its external checkpoint")
	}

	previousPrivateKey, previousKeyErr := loadLedgerAnchorPrivateKey(ctx, config, config.Integrity.SecretRef)
	if action == integrityRotateKey && previousKeyErr != nil {
		return fmt.Errorf("rotation requires the current signing key; use reviewed key recovery only after investigating its loss: %w", previousKeyErr)
	}
	if action == integrityRecoverKey && previousKeyErr == nil {
		clear(previousPrivateKey)
		return fmt.Errorf("current signing key is available; use continuity-preserving rotation")
	}
	defer clear(previousPrivateKey)

	confirmation := "ROTATE LEDGER ANCHOR KEY"
	if action == integrityRecoverKey {
		confirmation = "RESET LEDGER ANCHOR TRUST"
	}
	answer, err := ui.line("Type "+confirmation+":", true)
	if err != nil {
		return err
	}
	if answer != confirmation {
		return fmt.Errorf("ledger anchor key transition was not approved")
	}

	_, nextPrivateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return fmt.Errorf("generate replacement ledger anchor signing key: %w", err)
	}
	defer clear(nextPrivateKey)
	observedAt := nowUTC()
	if observedAt.Before(config.UpdatedAt) {
		return fmt.Errorf("system wall clock moved behind the installation configuration")
	}
	var record ledgeranchor.KeyTransition
	var nextBody, recordBody []byte
	if action == integrityRotateKey {
		record, nextBody, recordBody, err = ledgeranchor.NewAuthorizedRotation(previous, previousBody, previousPrivateKey, nextPrivateKey, authorizedBy, observedAt)
	} else {
		record, nextBody, recordBody, err = ledgeranchor.NewReviewedTrustReset(previous, previousBody, previousPublicKey, nextPrivateKey, authorizedBy, observedAt)
	}
	if err != nil {
		return err
	}
	nextPublicKey := nextPrivateKey.Public().(ed25519.PublicKey)
	nextKeyID, _ := ledgeranchor.PublicKeyID(nextPublicKey)
	nextSecretRef := "ledger-anchor-key-" + nextKeyID
	credential := ledgerAnchorCredential{
		Version: 1, InstallationID: config.Integrity.InstallationID,
		PrivateKey: base64.StdEncoding.EncodeToString(nextPrivateKey),
	}
	credentialBody, err := json.Marshal(credential)
	if err != nil {
		return err
	}
	defer clear(credentialBody)
	if err := storeEncryptedCredentialNew(ctx, config, nextSecretRef, credentialBody); err != nil {
		return fmt.Errorf("store replacement ledger anchor signing key: %w", err)
	}
	pendingPath, finalPath := transitionPaths(config.Paths.StateDir, record)
	if err := writeExclusiveSynced(pendingPath, recordBody); err != nil {
		return fmt.Errorf("persist pending ledger anchor key transition: %w", err)
	}
	if err := applyKeyTransition(ctx, configPath, config, record, nextBody, pendingPath, finalPath, nextSecretRef); err != nil {
		return fmt.Errorf("complete ledger anchor key transition: %w", err)
	}
	_, err = fmt.Fprintln(output, "Ledger anchor key updated. The service remains stopped.")
	return err
}

func resolveAmbiguousPendingCheckpoint(ctx context.Context, config bootstrap.Config, authorizedBy string, ui *terminalUI, output io.Writer) error {
	state, err := currentIntegrityState(ctx, config.Paths.Database)
	if err != nil {
		return err
	}
	publicKey, err := ledgeranchor.DecodePublicKey(config.Integrity.PublicKey)
	if err != nil {
		return err
	}
	privateKey, err := loadLedgerAnchorPrivateKey(ctx, config, config.Integrity.SecretRef)
	if err != nil {
		return fmt.Errorf("pending checkpoint recovery requires the current signing key: %w", err)
	}
	defer clear(privateKey)
	committed, committedBody, err := ledgeranchor.Read(config.Integrity.CheckpointFile, config.Integrity.InstallationID, publicKey)
	if err != nil {
		return err
	}
	pendingPath := config.Integrity.CheckpointFile + ".pending"
	pending, pendingBody, err := ledgeranchor.Read(pendingPath, config.Integrity.InstallationID, publicKey)
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("no pending ledger checkpoint requires recovery")
	}
	if err != nil {
		return fmt.Errorf("verify pending ledger checkpoint: %w", err)
	}
	if pending.Ledger.Equal(state) {
		store, openErr := ledgeranchor.Open(config.Integrity.CheckpointFile, config.Integrity.InstallationID, publicKey, privateKey, state, time.Now)
		if openErr != nil {
			return openErr
		}
		if err := store.Close(); err != nil {
			return err
		}
		_, err = fmt.Fprintln(output, "Committed pending ledger checkpoint promotion completed. The service remains stopped.")
		return err
	}
	if !committed.Ledger.Equal(state) {
		return fmt.Errorf("SQLite matches neither the committed nor pending external checkpoint")
	}
	answer, err := ui.line("Type RETAIN CURRENT LEDGER HEAD:", true)
	if err != nil {
		return err
	}
	if answer != "RETAIN CURRENT LEDGER HEAD" {
		return fmt.Errorf("pending ledger checkpoint recovery was not approved")
	}
	record, body, err := ledgeranchor.NewPendingResolution(committed, committedBody, pending, pendingBody, state, privateKey, authorizedBy, nowUTC())
	if err != nil {
		return err
	}
	directory := filepath.Join(config.Paths.StateDir, "ledger-anchor-resolutions")
	path := filepath.Join(directory, fmt.Sprintf("%020d-retain-committed-%s.json", record.DiscardedCheckpoint.Generation, record.DiscardedCheckpoint.KeyID))
	if err := writeExclusiveOrSame(path, body); err != nil {
		return fmt.Errorf("preserve pending checkpoint resolution evidence: %w", err)
	}
	if err := os.Remove(pendingPath); err != nil {
		return fmt.Errorf("remove resolved pending checkpoint: %w", err)
	}
	if err := syncDirectory(filepath.Dir(pendingPath)); err != nil {
		return err
	}
	_, err = fmt.Fprintln(output, "Pending checkpoint preserved as signed recovery evidence and current SQLite head retained. The service remains stopped.")
	return err
}

func resumePendingKeyTransition(ctx context.Context, configPath string, config bootstrap.Config, action string) (bool, error) {
	directory := filepath.Join(config.Paths.StateDir, "ledger-anchor-transitions")
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect pending ledger anchor key transitions: %w", err)
	}
	var pendingPath string
	for _, entry := range entries {
		if entry.Type().IsRegular() && strings.HasSuffix(entry.Name(), ".pending.json") {
			if pendingPath != "" {
				return false, fmt.Errorf("multiple pending ledger anchor key transitions require manual investigation")
			}
			pendingPath = filepath.Join(directory, entry.Name())
		}
	}
	if pendingPath == "" {
		return false, nil
	}
	body, err := readBoundedPrivateFile(pendingPath, ledgeranchor.MaximumFileBytes)
	if err != nil {
		return false, err
	}
	var untrusted ledgeranchor.KeyTransition
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&untrusted); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return false, fmt.Errorf("pending ledger anchor key transition is malformed")
	}
	previousPublicKey, err := ledgeranchor.DecodePublicKey(untrusted.PreviousPublicKey)
	if err != nil {
		return false, err
	}
	record, err := ledgeranchor.VerifyKeyTransition(body, previousPublicKey)
	if err != nil {
		return false, err
	}
	wantContinuity := ledgeranchor.TransitionAuthorizedRotation
	if action == integrityRecoverKey {
		wantContinuity = ledgeranchor.TransitionReviewedTrustReset
	}
	if record.Continuity != wantContinuity || record.InstallationID != config.Integrity.InstallationID {
		return false, fmt.Errorf("pending ledger anchor key transition does not match the requested operation")
	}
	oldKeyID, _ := ledgeranchor.PublicKeyID(previousPublicKey)
	nextPublicKey, err := ledgeranchor.DecodePublicKey(record.NextPublicKey)
	if err != nil {
		return false, err
	}
	nextKeyID, _ := ledgeranchor.PublicKeyID(nextPublicKey)
	if config.Integrity.KeyID != oldKeyID && config.Integrity.KeyID != nextKeyID {
		return false, fmt.Errorf("pending ledger anchor key transition is unrelated to the configured trust root")
	}
	nextSecretRef := "ledger-anchor-key-" + nextKeyID
	nextPrivateKey, err := loadLedgerAnchorPrivateKey(ctx, config, nextSecretRef)
	if err != nil {
		return false, fmt.Errorf("load pending replacement ledger anchor key: %w", err)
	}
	if !bytes.Equal(nextPrivateKey.Public().(ed25519.PublicKey), nextPublicKey) {
		clear(nextPrivateKey)
		return false, fmt.Errorf("pending replacement credential does not match its transition")
	}
	clear(nextPrivateKey)
	nextBody, err := ledgeranchor.EncodeCheckpoint(record.NextCheckpoint)
	if err != nil {
		return false, err
	}
	finalPath := strings.TrimSuffix(pendingPath, ".pending.json") + ".json"
	if err := applyKeyTransition(ctx, configPath, config, record, nextBody, pendingPath, finalPath, nextSecretRef); err != nil {
		return false, err
	}
	return true, nil
}

func applyKeyTransition(ctx context.Context, configPath string, config bootstrap.Config, record ledgeranchor.KeyTransition, nextBody []byte, pendingPath, finalPath, nextSecretRef string) error {
	state, err := currentIntegrityState(ctx, config.Paths.Database)
	if err != nil {
		return err
	}
	if !record.NextCheckpoint.Ledger.Equal(state) {
		return fmt.Errorf("SQLite ledger changed while the key transition was pending")
	}
	previousPublicKey, _ := ledgeranchor.DecodePublicKey(record.PreviousPublicKey)
	nextPublicKey, _ := ledgeranchor.DecodePublicKey(record.NextPublicKey)
	current, _, previousErr := ledgeranchor.Read(config.Integrity.CheckpointFile, record.InstallationID, previousPublicKey)
	if previousErr == nil && current == record.PreviousCheckpoint {
		if err := fileguard.WriteAtomically(config.Integrity.CheckpointFile, nextBody, 0o600, 0o700); err != nil {
			return err
		}
	} else {
		current, _, nextErr := ledgeranchor.Read(config.Integrity.CheckpointFile, record.InstallationID, nextPublicKey)
		if nextErr != nil || current != record.NextCheckpoint {
			return fmt.Errorf("external checkpoint matches neither side of the pending key transition")
		}
	}
	if err := prepareIntegrityCheckpointAccess(ctx, config); err != nil {
		return fmt.Errorf("prepare replacement checkpoint for the service: %w", err)
	}
	nextKeyID, _ := ledgeranchor.PublicKeyID(nextPublicKey)
	nextEncoded, _ := ledgeranchor.EncodePublicKey(nextPublicKey)
	oldSecretRef := config.Integrity.SecretRef
	previousKeyID, _ := ledgeranchor.PublicKeyID(previousPublicKey)
	switch config.Integrity.KeyID {
	case previousKeyID:
		previousEncoded, _ := ledgeranchor.EncodePublicKey(previousPublicKey)
		if config.Integrity.PublicKey != previousEncoded || record.ObservedAt.Before(config.UpdatedAt) {
			return fmt.Errorf("configured prior trust root or time does not match the pending transition")
		}
		config.Integrity.PublicKey = nextEncoded
		config.Integrity.KeyID = nextKeyID
		config.Integrity.SecretRef = nextSecretRef
		config.UpdatedAt = record.ObservedAt
		if err := bootstrap.SaveConfig(configPath, config); err != nil {
			return err
		}
		if config.Mode == bootstrap.ModeSystem {
			if err := os.Chmod(configPath, 0o644); err != nil {
				return err
			}
		}
	case nextKeyID:
		if config.Integrity.PublicKey != nextEncoded || config.Integrity.SecretRef != nextSecretRef || config.UpdatedAt.Before(record.ObservedAt) {
			return fmt.Errorf("configured replacement trust root does not match the pending transition")
		}
	default:
		return fmt.Errorf("configured trust root matches neither side of the pending transition")
	}
	if err := applyProviderRuntime(ctx, config); err != nil {
		return fmt.Errorf("refresh service credential binding: %w", err)
	}
	if err := revokePriorAnchorCredential(ctx, config, record, oldSecretRef, nextSecretRef); err != nil {
		return fmt.Errorf("revoke prior ledger anchor credential: %w", err)
	}
	if err := promoteExclusive(pendingPath, finalPath); err != nil {
		return fmt.Errorf("finalize ledger anchor transition evidence: %w", err)
	}
	return nil
}

func revokePriorAnchorCredential(ctx context.Context, config bootstrap.Config, record ledgeranchor.KeyTransition, configuredRef, nextSecretRef string) error {
	previousPublicKey, err := ledgeranchor.DecodePublicKey(record.PreviousPublicKey)
	if err != nil {
		return err
	}
	refs := []string{configuredRef, "ledger-anchor-signing-key", "ledger-anchor-key-" + record.PreviousCheckpoint.KeyID}
	seen := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		if ref == "" || ref == nextSecretRef {
			continue
		}
		if _, duplicate := seen[ref]; duplicate {
			continue
		}
		seen[ref] = struct{}{}
		path := filepath.Join(config.Paths.ConfigDir, "credentials", ref+".cred")
		if _, statErr := os.Lstat(path); errors.Is(statErr, os.ErrNotExist) {
			continue
		} else if statErr != nil {
			return statErr
		}
		privateKey, loadErr := loadLedgerAnchorPrivateKey(ctx, config, ref)
		if loadErr == nil {
			matches := bytes.Equal(privateKey.Public().(ed25519.PublicKey), previousPublicKey)
			clear(privateKey)
			if !matches {
				return fmt.Errorf("candidate prior credential %s does not match the retired key", ref)
			}
		} else if record.Continuity != ledgeranchor.TransitionReviewedTrustReset {
			return fmt.Errorf("cannot verify prior credential %s before removal: %w", ref, loadErr)
		}
		if err := removePrivateRegularFile(path); err != nil {
			return err
		}
	}
	return nil
}

func loadLedgerAnchorPrivateKey(ctx context.Context, config bootstrap.Config, secretRef string) (ed25519.PrivateKey, error) {
	path := filepath.Join(config.Paths.ConfigDir, "credentials", secretRef+".cred")
	body, err := decryptProviderCredential(ctx, config.Mode, path, secretRef)
	if err != nil {
		return nil, err
	}
	defer clear(body)
	var credential ledgerAnchorCredential
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&credential); err != nil || decoder.Decode(&struct{}{}) != io.EOF || credential.Version != 1 || credential.InstallationID != config.Integrity.InstallationID {
		return nil, fmt.Errorf("ledger anchor signing credential is invalid")
	}
	privateKey, err := base64.StdEncoding.DecodeString(credential.PrivateKey)
	if err != nil || len(privateKey) != ed25519.PrivateKeySize {
		clear(privateKey)
		return nil, fmt.Errorf("ledger anchor signing credential is invalid")
	}
	return ed25519.PrivateKey(privateKey), nil
}

func currentIntegrityState(ctx context.Context, databasePath string) (ledgeranchor.LedgerState, error) {
	store, err := ledger.OpenCurrent(databasePath)
	if err != nil {
		return ledgeranchor.LedgerState{}, fmt.Errorf("open SQLite ledger for integrity maintenance: %w", err)
	}
	state, stateErr := store.IntegrityAnchorState(ctx)
	closeErr := store.Close()
	if err := errors.Join(stateErr, closeErr); err != nil {
		return ledgeranchor.LedgerState{}, err
	}
	return state, nil
}

func transitionPaths(stateDir string, record ledgeranchor.KeyTransition) (string, string) {
	keyID := record.NextCheckpoint.KeyID
	kind := strings.ToLower(strings.ReplaceAll(record.Continuity, "_", "-"))
	name := fmt.Sprintf("%020d-%s-%s", record.NextCheckpoint.Generation, kind, keyID)
	directory := filepath.Join(stateDir, "ledger-anchor-transitions")
	return filepath.Join(directory, name+".pending.json"), filepath.Join(directory, name+".json")
}

func writeExclusiveSynced(path string, body []byte) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || len(body) == 0 || len(body) > ledgeranchor.MaximumFileBytes {
		return fmt.Errorf("exclusive evidence path or body is invalid")
	}
	if err := ensurePrivateEvidenceDirectory(filepath.Dir(path)); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	complete := false
	defer func() {
		if !complete {
			_ = os.Remove(path)
		}
	}()
	written, writeErr := file.Write(body)
	if writeErr == nil && written != len(body) {
		writeErr = io.ErrShortWrite
	}
	closeErr := errors.Join(file.Sync(), file.Close())
	if err := errors.Join(writeErr, closeErr); err != nil {
		return err
	}
	complete = true
	return syncDirectory(filepath.Dir(path))
}

func ensurePrivateEvidenceDirectory(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
		return fmt.Errorf("evidence directory path is invalid")
	}
	parent := filepath.Dir(path)
	parentInfo, err := os.Lstat(parent)
	if err != nil || parentInfo.Mode()&os.ModeSymlink != 0 || !parentInfo.IsDir() {
		return fmt.Errorf("evidence directory parent must be a directory, not a link")
	}
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm() != 0o700 {
		return fmt.Errorf("evidence directory must be a private directory, not a link")
	}
	return nil
}

func writeExclusiveOrSame(path string, body []byte) error {
	if err := writeExclusiveSynced(path, body); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrExist) {
		return err
	}
	existing, err := readBoundedPrivateFile(path, ledgeranchor.MaximumFileBytes)
	if err != nil || !bytes.Equal(existing, body) {
		return fmt.Errorf("existing evidence differs from the reviewed recovery record")
	}
	return nil
}

func readBoundedPrivateFile(path string, maximum int64) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, fmt.Errorf("ledger anchor transition path is invalid")
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximum || info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("ledger anchor transition must be a private bounded regular file")
	}
	return readUnchangedBoundedFile(path, info, maximum, "ledger anchor transition")
}

func promoteExclusive(pendingPath, finalPath string) error {
	if err := os.Link(pendingPath, finalPath); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		pendingBody, pendingErr := readBoundedPrivateFile(pendingPath, ledgeranchor.MaximumFileBytes)
		finalBody, finalErr := readBoundedPrivateFile(finalPath, ledgeranchor.MaximumFileBytes)
		if pendingErr != nil || finalErr != nil || !bytes.Equal(pendingBody, finalBody) {
			return fmt.Errorf("final ledger anchor transition evidence already exists with different content")
		}
	}
	if err := syncDirectory(filepath.Dir(finalPath)); err != nil {
		return err
	}
	if err := os.Remove(pendingPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDirectory(filepath.Dir(finalPath))
}

func removePrivateRegularFile(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("refuse to remove unsafe prior credential")
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func syncDirectory(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

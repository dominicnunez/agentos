package recovery

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"modernc.org/sqlite"
)

var requiredColumns = map[string][]string{
	"consumed_approvals": {"approval_id", "effect_fingerprint", "consumed_at"},
	"events":             {"sequence", "event_id", "organization_id", "event_type", "source_actor_id", "source_execution_id", "recipient_scope", "recipient_id", "task_id", "authorization_refs", "artifact_refs", "payload", "correlation_id", "created_at", "schema_version"},
	"external_tasks":     {"organization_id", "task_id", "correlation_id"},
	"external_work":      {"organization_id", "request_id", "correlation_id", "intent_id"},
	"inbox":              {"recipient_scope", "recipient_id", "event_id", "organization_id", "task_id", "available_at", "observed_at", "observation_event_id"},
	"records":            {"kind", "record_id", "version", "body", "created_at"},
}
var requiredTables = []string{"consumed_approvals", "events", "external_tasks", "external_work", "inbox", "records"}

type Result struct {
	Path        string `json:"path"`
	SHA256      string `json:"sha256"`
	SizeBytes   int64  `json:"size_bytes"`
	EventCount  int64  `json:"event_count"`
	MaxSequence int64  `json:"max_sequence"`
}

type backuper interface {
	NewBackup(string) (*sqlite.Backup, error)
}

// Backup creates and verifies an online SQLite snapshot. Destination must not
// exist; publication uses a same-directory hard link so a concurrent creator
// cannot be overwritten between validation and publication.
func Backup(ctx context.Context, source, destination string) (Result, error) {
	return clone(ctx, source, destination)
}

// Restore verifies a backup and materializes it at a new path. It never
// replaces an existing database; the operator switches AGENTOS_DB only after
// stopping the runtime, leaving the prior database available for rollback.
func Restore(ctx context.Context, backup, destination string) (Result, error) {
	if _, err := Verify(ctx, backup); err != nil {
		return Result{}, fmt.Errorf("verify restore source: %w", err)
	}
	return clone(ctx, backup, destination)
}

// Verify checks SQLite integrity and the minimum Agent OS ledger schema, then
// returns a content checksum for an offline backup or restore candidate.
func Verify(ctx context.Context, path string) (result Result, finalErr error) {
	if ctx == nil {
		return Result{}, fmt.Errorf("context is required")
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	resolved, err := sourcePath(path)
	if err != nil {
		return Result{}, err
	}
	db, err := sql.Open("sqlite", resolved)
	if err != nil {
		return Result{}, fmt.Errorf("open recovery database: %w", err)
	}
	db.SetMaxOpenConns(1)
	defer func() {
		if db != nil {
			finalErr = errors.Join(finalErr, db.Close())
		}
	}()
	if _, err := db.ExecContext(ctx, `PRAGMA query_only=ON`); err != nil {
		return Result{}, fmt.Errorf("make recovery verification read-only: %w", err)
	}
	if err := verifyIntegrity(ctx, db); err != nil {
		return Result{}, err
	}

	var tableCount int
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(requiredTables)), ",")
	arguments := make([]any, len(requiredTables))
	for index, table := range requiredTables {
		arguments[index] = table
	}
	query := `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN (` + placeholders + `)`
	if err := db.QueryRowContext(ctx, query, arguments...).Scan(&tableCount); err != nil {
		return Result{}, fmt.Errorf("inspect Agent OS ledger schema: %w", err)
	}
	if tableCount != len(requiredColumns) {
		return Result{}, fmt.Errorf("database is not a complete Agent OS ledger")
	}
	for table, columns := range requiredColumns {
		if err := verifyColumns(ctx, db, table, columns); err != nil {
			return Result{}, err
		}
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(MAX(sequence), 0) FROM events`).Scan(&result.EventCount, &result.MaxSequence); err != nil {
		return Result{}, fmt.Errorf("inspect Agent OS event ledger: %w", err)
	}
	if err := db.Close(); err != nil {
		return Result{}, fmt.Errorf("close verified database before checksum: %w", err)
	}
	db = nil
	result, err = fileResult(resolved, result.EventCount, result.MaxSequence)
	return result, err
}

func verifyIntegrity(ctx context.Context, db *sql.DB) (finalErr error) {
	rows, err := db.QueryContext(ctx, `PRAGMA integrity_check`)
	if err != nil {
		return fmt.Errorf("check SQLite integrity: %w", err)
	}
	defer func() {
		finalErr = errors.Join(finalErr, rows.Close())
	}()
	integrityOK := false
	for rows.Next() {
		var finding string
		if err := rows.Scan(&finding); err != nil {
			return fmt.Errorf("read SQLite integrity result: %w", err)
		}
		if finding != "ok" {
			return fmt.Errorf("SQLite integrity check failed: %s", finding)
		}
		integrityOK = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate SQLite integrity results: %w", err)
	}
	if !integrityOK {
		return fmt.Errorf("SQLite integrity check returned no result")
	}
	return nil
}

func verifyColumns(ctx context.Context, db *sql.DB, table string, required []string) (finalErr error) {
	rows, err := db.QueryContext(ctx, `SELECT name FROM pragma_table_info(?)`, table)
	if err != nil {
		return fmt.Errorf("inspect Agent OS table %s: %w", table, err)
	}
	defer func() {
		finalErr = errors.Join(finalErr, rows.Close())
	}()
	found := make(map[string]struct{}, len(required))
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("read Agent OS table %s: %w", table, err)
		}
		found[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate Agent OS table %s: %w", table, err)
	}
	for _, column := range required {
		if _, ok := found[column]; !ok {
			return fmt.Errorf("agent OS table %s is missing column %s", table, column)
		}
	}
	return nil
}

func clone(ctx context.Context, source, destination string) (result Result, finalErr error) {
	if ctx == nil {
		return Result{}, fmt.Errorf("context is required")
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	resolvedSource, err := sourcePath(source)
	if err != nil {
		return Result{}, err
	}
	resolvedDestination, err := destinationPath(destination)
	if err != nil {
		return Result{}, err
	}
	if samePath(resolvedSource, resolvedDestination) {
		return Result{}, fmt.Errorf("source and destination must be different files")
	}

	temporary, err := os.CreateTemp(filepath.Dir(resolvedDestination), ".agentos-recovery-*")
	if err != nil {
		return Result{}, fmt.Errorf("create recovery staging file: %w", err)
	}
	temporaryPath := temporary.Name()
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return Result{}, fmt.Errorf("close recovery staging file: %w", err)
	}
	defer func() {
		if err := os.Remove(temporaryPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			finalErr = errors.Join(finalErr, fmt.Errorf("remove recovery staging file: %w", err))
		}
	}()

	db, err := sql.Open("sqlite", resolvedSource)
	if err != nil {
		return Result{}, fmt.Errorf("open backup source: %w", err)
	}
	db.SetMaxOpenConns(1)
	defer func() {
		if db != nil {
			finalErr = errors.Join(finalErr, db.Close())
		}
	}()
	if _, err := db.ExecContext(ctx, `PRAGMA query_only=ON`); err != nil {
		return Result{}, fmt.Errorf("make backup source read-only: %w", err)
	}
	connection, err := db.Conn(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("acquire backup source connection: %w", err)
	}
	if err := connection.Raw(func(driverConnection any) error {
		provider, ok := driverConnection.(backuper)
		if !ok {
			return fmt.Errorf("SQLite driver does not support online backup")
		}
		backup, err := provider.NewBackup(temporaryPath)
		if err != nil {
			return err
		}
		for more := true; more; {
			if err := ctx.Err(); err != nil {
				return errors.Join(err, backup.Finish())
			}
			more, err = backup.Step(128)
			if err != nil {
				return errors.Join(err, backup.Finish())
			}
		}
		return backup.Finish()
	}); err != nil {
		_ = connection.Close()
		return Result{}, fmt.Errorf("create online SQLite backup: %w", err)
	}
	if err := connection.Close(); err != nil {
		return Result{}, fmt.Errorf("close backup source connection: %w", err)
	}
	if err := db.Close(); err != nil {
		return Result{}, fmt.Errorf("close backup source: %w", err)
	}
	db = nil

	result, err = Verify(ctx, temporaryPath)
	if err != nil {
		return Result{}, fmt.Errorf("verify recovery staging database: %w", err)
	}
	if err := os.Chmod(temporaryPath, 0o600); err != nil {
		return Result{}, fmt.Errorf("restrict recovery file permissions: %w", err)
	}
	if err := syncFile(temporaryPath); err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if err := requireNoSidecars(resolvedDestination); err != nil {
		return Result{}, err
	}
	if err := os.Link(temporaryPath, resolvedDestination); err != nil {
		return Result{}, fmt.Errorf("publish recovery file without overwrite: %w", err)
	}
	if err := requireNoSidecars(resolvedDestination); err != nil {
		return Result{}, errors.Join(err, os.Remove(resolvedDestination))
	}
	if err := syncDirectory(filepath.Dir(resolvedDestination)); err != nil {
		return Result{}, errors.Join(err, os.Remove(resolvedDestination))
	}
	result.Path = resolvedDestination
	return result, nil
}

func sourcePath(path string) (string, error) {
	if path == "" || path == ":memory:" {
		return "", fmt.Errorf("a file-backed SQLite database is required")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve source path: %w", err)
	}
	resolved := filepath.Clean(absolute)
	info, err := os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf("inspect source database: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("source database must be a regular file")
	}
	return resolved, nil
}

func destinationPath(path string) (string, error) {
	if path == "" || path == ":memory:" {
		return "", fmt.Errorf("a new file-backed destination is required")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve destination path: %w", err)
	}
	parent := filepath.Clean(filepath.Dir(absolute))
	info, err := os.Stat(parent)
	if err != nil {
		return "", fmt.Errorf("inspect destination directory: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("destination parent must be a directory")
	}
	resolved := filepath.Join(parent, filepath.Base(absolute))
	if _, err := os.Lstat(resolved); err == nil {
		return "", fmt.Errorf("destination already exists; recovery never overwrites files")
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("inspect destination: %w", err)
	}
	return resolved, nil
}

func requireNoSidecars(path string) error {
	for _, suffix := range []string{"-journal", "-shm", "-wal"} {
		sidecar := path + suffix
		if _, err := os.Lstat(sidecar); err == nil {
			return fmt.Errorf("destination SQLite sidecar already exists: %s", filepath.Base(sidecar))
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect destination SQLite sidecar: %w", err)
		}
	}
	return nil
}

func samePath(left, right string) bool {
	if runtime.GOOS == "windows" {
		return strings.EqualFold(filepath.Clean(left), filepath.Clean(right))
	}
	return filepath.Clean(left) == filepath.Clean(right)
}

func fileResult(path string, eventCount, maxSequence int64) (Result, error) {
	file, err := os.Open(path)
	if err != nil {
		return Result{}, fmt.Errorf("open recovery file for checksum: %w", err)
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		_ = file.Close()
		return Result{}, fmt.Errorf("checksum recovery file: %w", err)
	}
	info, err := file.Stat()
	closeErr := file.Close()
	if err != nil {
		return Result{}, fmt.Errorf("inspect recovery file: %w", err)
	}
	if closeErr != nil {
		return Result{}, fmt.Errorf("close recovery file after checksum: %w", closeErr)
	}
	return Result{Path: path, SHA256: hex.EncodeToString(hash.Sum(nil)), SizeBytes: info.Size(), EventCount: eventCount, MaxSequence: maxSequence}, nil
}

func syncFile(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open recovery file for sync: %w", err)
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	return errors.Join(syncErr, closeErr)
}

func syncDirectory(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open recovery directory for sync: %w", err)
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	return errors.Join(syncErr, closeErr)
}

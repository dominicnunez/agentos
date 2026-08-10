# SQLite recovery

Agent OS V1 includes a local, operator-controlled recovery utility. This is a
small operational safeguard, not the deferred signed-checkpoint, scheduled
off-site backup, or tamper-evident ledger system.

## Backup

An online backup uses SQLite's native backup API, so committed WAL state is
included without copying live database files directly:

```sh
go run ./cmd/agentos-recovery backup \
  --database ./agentos.db \
  --output ./agentos-backup.db
```

The output path and its parent directory must already be selected by the
operator, and the output file must not exist. The utility writes a private
staging file, verifies SQLite integrity and the Agent OS ledger tables, syncs
it, then publishes it without an overwrite race. It returns JSON containing
the resolved path, SHA-256 checksum, size, event count, and maximum sequence.

Backups contain the full event ledger and may contain sensitive organizational
data. Moving one to a new storage or trust boundary requires the established
sensitive-data-boundary approval. This utility does not upload or transmit it.

## Verify

Verify an offline backup or restore candidate before use or transfer:

```sh
go run ./cmd/agentos-recovery verify --database ./agentos-backup.db
```

Verification is read-only. It rejects corruption and valid SQLite databases
that do not contain the required Agent OS ledger tables and columns.

## Restore without overwrite

Restore materializes a validated backup at a new path:

```sh
go run ./cmd/agentos-recovery restore \
  --backup ./agentos-backup.db \
  --output ./agentos-restored.db
```

It never modifies the backup and never overwrites an existing destination.
The operational switch remains explicit:

1. Stop Agent OS and preserve the current database and any journal files.
2. Run `restore` to a new path and retain its JSON checksum record.
3. Set `AGENTOS_DB` to the restored path.
4. Start Agent OS and run the Human and A2A loopback checks.
5. Keep the prior database untouched until the restored runtime is accepted.

Rollback is the reverse pointer switch: stop the runtime, restore the previous
`AGENTOS_DB` value, and restart. Do not copy one database file over another or
swap paths while Agent OS is running.

## Evidence

Unit tests cover online WAL backup, integrity and schema rejection,
cancellation, destination no-overwrite behavior, restore continuity, and
checksum output. CI additionally runs a disposable process-level pilot:

```text
live Human/A2A work
  -> online backup and verification
  -> stop runtime
  -> restore to a new database
  -> restart
  -> read prior results
  -> continue blocked Human/A2A work
  -> verify expired and revoked credentials fail closed
```

# SQLite recovery

Agent OS V1 includes a local, operator-controlled recovery utility. This is a
small operational safeguard, not the deferred signed-checkpoint, scheduled
off-site backup, or tamper-evident ledger system.

## Backup

An online backup uses SQLite's native backup API, so committed WAL state is
included without copying live database files directly:

```sh
agentos-recovery backup \
  --database /var/lib/agentos/agentos.db \
  --output ./agentos-backup.db
```

The output path and its parent directory must already be selected by the
operator, and the output file must not exist. The utility writes a private
staging file, verifies SQLite integrity, the Agent OS ledger schema, and every
event-coupled projection admission, syncs it, then publishes it without an
overwrite race. Verification requires a one-to-one match between each
organizational projection record and its exact sealed event envelope; orphaned,
copied, malformed, cross-organization, or otherwise mismatched state is
rejected. Existing SQLite journal,
WAL, or shared-memory sidecars at the destination are rejected so stale state
cannot be applied to a recovered ledger. The utility returns JSON containing
the resolved path, SHA-256 checksum, size, event count, and maximum sequence.

Backups contain the full event ledger and may contain sensitive organizational
data. Moving one to a new storage or trust boundary requires the established
sensitive-data-boundary approval. This utility does not upload or transmit it.

## Verify

Verify an offline backup or restore candidate before use or transfer:

```sh
agentos-recovery verify --database ./agentos-backup.db
```

Verification is read-only. It rejects corruption, valid SQLite databases that
do not contain the required Agent OS ledger tables and columns, unsupported
pre-admission projection state, projection lifecycle events without typed
admission, and materialized projections that do not match their authorizing
event or durable Organization/Intent/Work relationship exactly. Task history
must also preserve its immutable execution contract and correlation boundary;
every lifecycle event must match the exact prior and resulting Task status.

## Restore without overwrite

Restore materializes a validated backup at a new path:

```sh
agentos-recovery restore \
  --backup ./agentos-backup.db \
  --output ./agentos-restored.db
```

It never modifies the backup and never overwrites an existing destination.
The operational switch remains explicit:

1. Stop Agent OS and preserve the current database and any journal files.
2. Run `restore` to a new path and retain its JSON checksum record.
3. Update `paths.database` in `/etc/agentos/config.json` to the restored path.
4. Start Agent OS and run `agentos doctor` before accepting new work.
5. Keep the prior database untouched until the restored runtime is accepted.

Rollback is the reverse pointer switch: stop the runtime, restore the previous
`paths.database` value, and restart. Do not copy one database file over another or
swap paths while Agent OS is running.

## Evidence

Unit and race tests cover online WAL backup, integrity, schema and projection
admission rejection, cancellation, destination no-overwrite behavior, restore
continuity, checksum output, restart recovery, A2A continuation, and structured
user completion.

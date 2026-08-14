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
the resolved path, SHA-256 checksum, size, event count, maximum sequence,
storage-schema version, and Event Contract schema version.

Backups contain the full event ledger and may contain sensitive organizational
data. Moving one to a new storage or trust boundary requires the established
sensitive-data-boundary approval. This utility does not upload or transmit it.

## Verify

Verify an offline backup or restore candidate before use or transfer:

```sh
agentos-recovery verify --database ./agentos-backup.db
```

Verification is read-only. It rejects corruption, valid SQLite databases that
do not carry the Agent OS SQLite application ID, unsupported or ambiguous
storage versions, layouts that do not match their exact version, schema
fingerprint drift, Event Contract version mismatch, unsupported pre-admission
projection state, projection lifecycle events without typed
admission, and materialized projections that do not match their authorizing
event or durable Organization/Intent/Work relationship exactly. Task
dependencies must resolve within one Work boundary and remain acyclic. Agent,
Mission, Goal, Work, and Task lifecycle events must match their exact prior and
resulting status, and Agent history must begin with version-one creation.
Completed Work
must retain the exact verified Task evidence that authorized its transition,
and achieved Goals must retain their exact atomic transition and authoritative
progress evidence. Agent configuration changes remain distinct from activation
changes; Team history cannot change tenant ownership or creation identity;
Task history also preserves its immutable execution contract and correlation
boundary. Adaptive Agent execution starts must retain a one-shot dispatch
binding to the exact active Agent, blueprint, and execution-profile projection
revisions that preceded the start event. Verification rejects missing,
superseded, cross-organization, or rewritten roster references. A later
deactivation does not invalidate an earlier committed start, while interrupted
adaptive work remains uncertain and cannot reuse that admission for a blind
retry.

## Storage and Event Contract versions

SQLite storage versions are independent of the Agent OS binary version. The
current runtime writes storage schema v2 and accepts v1 as the oldest supported
upgrade source. Schema v1 is frozen in
`internal/ledger/testdata/storage-v1.sql`. Schema v2 adds metadata that binds
the storage version, Agent OS application ID, current Event Contract schema,
and a fingerprint of the reviewed SQLite layout.

Startup inspects the application ID and complete source layout before making a
change, then applies each ordered migration in one SQLite transaction. A
missing migration, future version, altered layout, or conflicting metadata
leaves the source untouched and fails with an actionable error. A nonempty
unversioned database is unsupported pre-release state: Agent OS does not infer
authority-bearing identity or silently repair it. Preserve that database and
use only an explicitly reviewed migration.

Event Contract schema v3 is the only Event schema admitted by storage v1 and
v2. Future code that changes durable Event Contract meaning must introduce a
new storage migration and retain an explicit validator for every Event version
it claims to support; permissive decoding is not a migration mechanism.

## Restore without overwrite

Restore materializes a validated backup at a new path:

```sh
agentos-recovery restore \
  --backup ./agentos-backup.db \
  --output ./agentos-restored.db
```

It never modifies the backup and never overwrites an existing destination. A
supported older backup remains at its original storage version during the
copy. The first Agent OS startup against the restored path performs the atomic
upgrade before serving work.
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

Unit and race tests cover fresh initialization, exact storage headers, atomic
v1-to-v2 migration, unsupported and corrupt-layout rejection, Event Contract
binding, online WAL backup, oldest-supported verify/backup/restore/migrate,
schema and projection admission rejection, cancellation, destination
no-overwrite behavior, restore continuity, checksum output, restart recovery,
A2A continuation, and structured user completion.

# SQLite recovery

`agentos-recovery` provides local, operator-controlled verification, backup,
and no-overwrite restore for Agent OS SQLite state. Every operation requires a
ready installation configuration so the database can be checked against its
independent signed checkpoint and pinned public key.

Stop Agent OS before restore or an operational database-pointer change. Online
backup is supported, but the command aborts if the external checkpoint changes
while the SQLite snapshot is being produced.

## Backup

```sh
sudo agentos-recovery backup \
  --config /etc/agentos/config.json \
  --output ./agentos-backup.db
```

User-mode installations pass their absolute
`~/.config/agentos/config.json` path without `sudo`.

The destination database and its parent directory must already be selected by
the operator, and neither destination may exist. The command:

1. verifies the current signed checkpoint;
2. creates an online SQLite snapshot that includes committed WAL state;
3. verifies SQLite integrity, application identity, exact schema, Event
   Contract state, event-chain coverage, and event-coupled projections;
4. verifies the snapshot against the same checkpoint;
5. re-reads the live checkpoint and rejects a concurrent change; and
6. publishes the database and `agentos-backup.db.anchor.json` without
   overwriting either destination.

The JSON result reports both paths, the database SHA-256 and size, storage and
Event Contract versions, event count and terminal sequence/event ID, chain
algorithm, and chain head.

Backups can contain sensitive organizational data. Moving them across a data
boundary requires the established approval. The utility does not transmit or
upload them.

## Verify

Verify the active installation:

```sh
sudo agentos-recovery verify \
  --config /etc/agentos/config.json \
  --database /var/lib/agentos/agentos.db
```

Verify an offline backup using its default paired checkpoint:

```sh
sudo agentos-recovery verify \
  --config /etc/agentos/config.json \
  --database ./agentos-backup.db
```

Use `--anchor <absolute-path>` only when the paired checkpoint has a different
name. Verification is read-only and rejects corruption, a wrong Agent OS
application ID, unsupported or drifted storage, incomplete event-chain
coverage, malformed Event Contracts, projection or tenant mismatches, and a
checkpoint that does not bind the exact database head.

## Restore without overwrite

```sh
sudo agentos-recovery restore \
  --config /etc/agentos/config.json \
  --backup ./agentos-backup.db \
  --output /var/lib/agentos/restores/incident-42.db \
  --output-anchor /var/lib/agentos/state/ledger-anchor-restore-incident-42.json
```

The default source checkpoint is `./agentos-backup.db.anchor.json`. An omitted
`--output-anchor` uses `<output>.anchor.json`, which is suitable for offline
verification. A pair intended for activation must place the checkpoint
directly in the installation state directory under the reviewed
`ledger-anchor-restore-<id>.json` naming rule, as above.

Restore verifies the source pair before and after snapshotting, materializes a
private temporary database, verifies the restored database against the signed
checkpoint, and then publishes both destinations without overwriting either.
If publication is interrupted after one file is created, preserve it and
choose new destination paths after investigation; do not overwrite it.

The operational switch remains explicit:

1. Stop Agent OS and preserve the current database, checkpoint, and any SQLite
   journal files.
2. Restore to new database and checkpoint paths inside the service's approved
   data and state directories.
3. Verify the restored pair.
4. In system mode, set the restored database and checkpoint owner to the
   `agentos` service account and mode `0600`; user mode retains the configured
   user's ownership.
5. Update both `paths.database` and `integrity_anchor.checkpoint_file` in the
   installation configuration under the trusted-core approval boundary.
6. Start Agent OS and run `agentos doctor` before accepting work.
7. Retain the prior pair until the restored installation is accepted.

A database pointer cannot be rolled back independently of its exact signed
checkpoint. Swapping only the database intentionally fails closed.

## Credentials and disaster recovery

Database backups do not contain the installation configuration or encrypted
ledger-signing credential. Preserve those through a separately reviewed,
access-controlled system backup or escrow process appropriate to systemd
credentials and the host protection model. Copying an encrypted credential to
another machine does not guarantee it can be decrypted there.

If the private signing key is unavailable but the retained public key still
verifies the database/checkpoint pair, follow the explicit
[`recover-key`](EVENT_LEDGER_INTEGRITY.md#signing-key-lifecycle) procedure. It
records a trust discontinuity; it does not recreate lost cryptographic
continuity.

## Evidence and limits

Tests cover online WAL backup, concurrent checkpoint change, no-overwrite
publication, signed-checkpoint mismatch, restart, restore, rollback and
substitution rejection, oldest supported storage, Event Contract validation,
and projection admission.

Database and checkpoint hashes are integrity evidence, not trusted timestamps,
event truth, third-party nonrepudiation, control-effectiveness conclusions,
ISO/IEC 42001 conformity, or certification.

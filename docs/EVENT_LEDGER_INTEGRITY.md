# Event ledger integrity

Agent OS protects durable event evidence with two distinct layers:

1. every event is part of a domain-separated SHA-256 chain inside SQLite; and
2. the verified chain head and exact ordered durable record projection are
   bound to an Ed25519-signed checkpoint outside SQLite.

Runtime startup, diagnostics, backup, restore, and explicit verification fail
closed unless the complete event chain and the expected external checkpoint
agree.

## In-database chain

Storage version 6 maintains one `event_integrity` record per event sequence.
The first event has an empty previous hash. Every later record stores the prior
event hash and a lowercase SHA-256 digest over:

- the domain `agentos.event-integrity.v1` and algorithm identifier;
- the prior hash and event sequence;
- every event-envelope identity and routing field;
- the exact stored authorization-reference, artifact-reference, and payload
  bytes; and
- the exact stored creation timestamp and Event Contract schema version.

Values are length-prefixed before hashing, and integers use fixed-width
big-endian encoding. This prevents ambiguous field concatenation. A missing,
orphaned, malformed, or noncontiguous integrity record fails verification.

## External checkpoint

The canonical checkpoint binds all of the following under Ed25519:

- checkpoint schema and generation;
- a random 256-bit installation identity;
- SQLite application ID and storage version;
- Event Contract schema version;
- exact event count and sequence;
- exact terminal event ID;
- chain algorithm and head;
- exact record count, domain-separated record algorithm, and digest over every
  stored record identity, version, body, admission binding, and timestamp;
- ordinary host wall-clock evidence;
- prior checkpoint SHA-256; and
- signature algorithm and key ID.

The public verification key is pinned in the installation configuration. The
private key is never stored in that configuration. Setup generates it with the
operating system cryptographic random source and stores it as a systemd
encrypted credential. In system mode only the restricted `agentos` service
receives that one credential at process start. In user mode the credential is
scoped to the configured Linux user service. Provider, A2A, and ledger-signing
credentials use distinct references.

The checkpoint is not stored in SQLite and is never inferred from a supplied
database. Replacing, truncating, or rolling back only the database therefore
causes the externally expected head to disagree and startup to stop.

The record digest closes a separate substitution path: changing an
authority-bearing projection without changing its admitting event also makes
the database disagree with the external checkpoint. It does not make record
content true or authoritative by itself; Event Contract and recovery
validation remain required.

## Atomic append and crash recovery

Every event-writing transaction follows this order:

1. validate the Event Contract and update the SQLite transaction;
2. derive the exact in-transaction ledger head;
3. durably write a signed `.pending` checkpoint successor;
4. commit SQLite;
5. atomically promote the pending checkpoint; and
6. sync the containing directory.

Agent OS also holds a kernel-backed exclusive writer lock for the runtime
lifetime. A second `serve` process cannot attach another checkpoint writer to
the same installation. Provider configuration and checkpoint-key maintenance
share a separate exclusive configuration lock and reload configuration after
acquiring it, preventing stale saves from restoring retired trust material.

Failures before checkpoint preparation roll SQLite back without creating a
pending checkpoint. Any error returned by SQLite commit is treated as
uncertain: the pending checkpoint is retained and the live writer cannot
continue. After such an error or a process or machine interruption:

- SQLite matching the pending successor proves the database commit was
  retained, so startup completes checkpoint promotion;
- SQLite matching the committed checkpoint while a different valid successor
  remains is ambiguous and startup fails closed; and
- SQLite matching neither checkpoint is rejected.

For the ambiguous case, stop the service, investigate the interruption and
available host or storage evidence, then run:

```sh
agentos integrity resolve-pending
```

If SQLite retained the successor, the command completes promotion without a
policy decision. If SQLite retained only the prior head, the configured local
authority must type the exact confirmation before Agent OS retains that head.
The complete discarded signed successor is preserved in a separately signed
resolution record before the pending file is removed. The record proves the
reviewed recovery decision; it does not prove that the discarded event never
occurred in another system.

## Signing-key lifecycle

Key maintenance is Linux-only, requires the service to be stopped, and is
restricted to root or the configured installation owner. It never starts the
service afterward.

Normal rotation:

```sh
agentos integrity rotate-key
```

Rotation generates a new Ed25519 key, preserves the exact ledger head in a
successor checkpoint, and records a dual-key transition. The retiring key
authorizes the exact replacement public key and checkpoint; the replacement
key signs that checkpoint. The service credential binding and configured
public trust root are then changed, and the retired encrypted credential is
removed. The durable transition record preserves verification evidence.

If the current private key is unavailable, normal rotation is refused. After
investigation and an explicit trusted-core approval, use:

```sh
agentos integrity recover-key
```

Recovery first verifies the existing checkpoint with the configured public
key and verifies the exact SQLite chain. It then requires the exact local
confirmation, generates a replacement key, and records
`REVIEWED_TRUST_RESET` with reason `SIGNING_KEY_UNAVAILABLE`. It deliberately
does not claim that the unavailable key authorized its replacement. From that
point forward the new key is the configured trust root. Any recoverable prior
credential is revoked and removed.

Both operations persist a resumable pending transition before changing the
checkpoint or configuration. A retry of the same command verifies and
finishes an interrupted transition instead of creating another one. Multiple,
malformed, unrelated, or inconsistent pending transitions require manual
investigation and fail closed.

Revocation in V1 means switching the explicit local trust root and deleting
the retired encrypted service credential. There is no network certificate
authority, online revocation service, or remote key-recovery authority.

## Backup, restore, and migration boundary

Every database backup is paired with the exact signed checkpoint observed
before and after the SQLite snapshot. A changed checkpoint or any unresolved
pending successor aborts backup, verification, and restore. Restore publishes
a new database and its paired checkpoint without overwriting either
destination; a database file alone is insufficient.

Finalized key-transition evidence authenticates older backup keys back to the
currently pinned trust root. Retired private credentials remain revoked.
Activating a restored backup under an ancestor key therefore requires the
explicit reviewed `recover-key` procedure, which records the discontinuity and
re-anchors that restored ledger head under a fresh key.

Encrypted signing credentials and configuration are not embedded in database
backups. Operators need a separately reviewed secret-recovery or escrow plan
appropriate to their systemd credential protection. If no private key can be
recovered but the public checkpoint remains verifiable, the explicit trust
reset above restores write availability while recording the loss of signing
continuity.

Future storage or Event Contract migrations must update the external
checkpoint under a reviewed maintenance procedure. A migration must not
silently treat a changed storage version as an ordinary event append or infer
a new trust root from the migrated database. Normal runtime and integrity-key
maintenance open only the current storage and Event Contract versions and
reject older layouts without modifying them.

## Assurance limits

These controls provide database-substitution and rollback evidence relative to
the retained installation configuration, public key, checkpoint, and
transition records. A compromised running Agent OS process receives the
signing key and can write both SQLite and the checkpoint, so runtime compromise
can forge new locally valid state until the key is revoked. These controls also
do not defend against a compromised kernel or an operating-system
administrator that can replace the binary, configuration, checkpoint,
encrypted credentials, and database together.

The recorded time is explicitly `SYSTEM_WALL_CLOCK_UNTRUSTED`. It is not a
trusted timestamp. Ed25519 signing here does not establish legal identity,
third-party nonrepudiation, event truth, control effectiveness, ISO/IEC 42001
conformity, or certification. The checkpoint does not grant authority,
approval, capability, completion status, or effect permission and does not
expose raw events through A2A or the dashboard.

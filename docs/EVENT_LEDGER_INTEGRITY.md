# Event ledger integrity

Agent OS cryptographically chains every durable event in SQLite. The chain
detects mutations that make the retained snapshot inconsistent, including a
changed event, a missing integrity record, a noncontiguous deletion, an
insertion, or reordering. Runtime startup, explicit recovery verification,
backup, and restore fail closed when the chain does not match the exact stored
event bytes.

## Chain contract

Storage version 6 adds a one-to-one `event_integrity` record for each event
sequence. The first event has an empty previous hash. Each later record stores
the prior event hash and a lowercase SHA-256 digest over:

- the domain `agentos.event-integrity.v1` and algorithm identifier;
- the prior hash and event sequence;
- every event-envelope identity and routing field;
- the exact stored authorization-reference, artifact-reference, and payload
  bytes;
- the exact stored creation timestamp and Event Contract schema version.

Every value is length-prefixed before hashing, and integers use fixed-width
big-endian encoding. This avoids ambiguous field concatenation. Migration from
an older supported layout validates and reseals earlier migrations first, then
backfills the chain in the same storage transaction. A missing or noncontiguous
event prevents migration.

`agentos doctor` and the SQLite recovery commands report the verified chain
algorithm and head hash. Offline recovery results also retain the SHA-256
checksum of the complete database file. Live diagnostics instead checksum one
native online-backup snapshot and label that scope explicitly; they never call
the live WAL main-file bytes a logical ledger identity. The snapshot checksum
and event-chain head answer different questions and neither replaces the
other.

## Security boundary

This is tamper-evidence, not tamper-prevention. The chain detects partial or
uncoordinated modification of the database. It is not a digital signature,
trusted timestamp, write-once log, or external checkpoint. Without an expected
head outside the database, deletion of the final event and its matching
integrity record leaves a shorter internally consistent snapshot. An attacker
with sufficient host privilege to replace the entire database can also
recompute an internally consistent chain. Operators must protect the database
and backups with the documented ownership and permission controls and retain
independent backup evidence. Signed or externally anchored checkpoints require
a separate reviewed trust and key-management design.

The chain does not expose raw ledger events through the dashboard or A2A, grant
authority, prove that a recorded statement is true, demonstrate control
effectiveness, or establish ISO/IEC 42001 conformity or certification.

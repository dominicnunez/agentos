# Operational and Data Evolution

## V1

- SQLite;
- single authoritative writer;
- append-only event API;
- versioned schemas/migrations;
- projection rebuild tests;
- idempotency records for consequential actions;
- explicit conformance profile.

## Event evolution

Every event stores `schema_version`.

Do not reinterpret historical events under a new schema silently. Upcasters/migrations must be deterministic and tested.

## Restart/recovery

V1 may replay from genesis. Add checkpoints only when replay cost becomes measurable.

## Future storage/distribution

Postgres, NATS/JetStream, distributed workers, multi-writer consensus, stronger ledger integrity/signing, encrypted artifact stores, backups, and multi-tenant isolation are future considerations with explicit prerequisites.

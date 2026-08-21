# User approval baseline

The runtime requires appropriately scoped user approval for:

- financial actions;
- physical-world effects;
- public or external communication or action;
- destructive or irreversible effects;
- expansion of a sensitive-data boundary;
- privilege, capability, or trust expansion;
- legal or binding commitments;
- ordinary Agent OS deployment;
- trusted-core or security changes.

Unanswered decisions fail closed. Approval binds the exact effect fingerprint,
arguments, task, expiry, and single-use status where applicable. Approval of
one effect never grants broader authority.

Before requesting attention, the runtime persists a replay-complete
EffectObligation in `PENDING`. Single-use approval consumption and transition
to `ATTEMPTED` commit atomically. A confirmed obligation is idempotent on
duplicate delivery. An interrupted `ATTEMPTED` obligation remains explicitly
uncertain and is never blindly replayed.

At startup, Agent OS discovers interrupted attempts and uses only a configured
read-only destination status check. Evidence-backed observations may close an
obligation as `CONFIRMED` or `FAILED`. Missing reconciliation support, lookup
failure, unknown status, missing evidence, or a changed attempt remains
`ATTEMPTED` for operator resolution.

No natural-language work channel can decide an approval. The local dashboard uses
the separate exact-effect control described in [Approval control](APPROVAL_CONTROL.md).

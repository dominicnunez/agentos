# Human approval baseline

The runtime must require appropriately scoped human approval for:

- financial actions;
- physical-world effects;
- public or external communication/action;
- destructive or irreversible effects;
- expansion of a sensitive-data boundary;
- privilege or trust expansion;
- legal or binding commitments;
- ordinary Agent OS deployment; and
- trusted-core or security changes.

Unanswered decisions fail closed. Approval must bind the exact effect fingerprint, arguments, task, expiry, and single-use status where applicable. Approval of an effect never grants broader authority.

The V1 lifecycle is durable: `PENDING -> NOTIFIED -> ACKNOWLEDGED ->
PENDING_DECISION -> APPROVED | DENIED`. Acknowledgement records attention only.
Decision authority is matched exactly to human identity, organization,
consequence boundary, and risk. Urgency controls attention only. Unknown
boundaries and unavailable authority fail closed.

Before notification, the runtime persists a replay-complete `EffectObligation`
in `PENDING`. Single-use approval consumption and the transition to `ATTEMPTED`
commit atomically. A confirmed obligation is idempotent on duplicate delivery;
an interrupted `ATTEMPTED` obligation remains explicitly uncertain for later
reconciliation rather than being blindly replayed.

At startup, Agent OS discovers interrupted attempts and uses only a configured
read-only destination status check. Evidence-backed observations may close the
obligation as `CONFIRMED` or `FAILED`. Missing reconciliation support, lookup
failure, unknown status, missing evidence, or a changed attempt remains
`ATTEMPTED` and is surfaced for operator resolution. Reconciliation never
replays the effect-writing adapter.

No production consequential-effect adapter is enabled. The A2A endpoint is
inbound only and ordinary external-Agent identity cannot decide approvals.
The direct Human Gateway is likewise conversational work/input only: even text
from its authenticated human principal cannot decide an exact-effect approval.
Approval requires a separate trusted control bound to the authorized human,
effect fingerprint, scope, and current policy state.

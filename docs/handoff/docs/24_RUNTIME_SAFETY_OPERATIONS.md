# Runtime Safety Operations

V1 needs primitives, not a separate incident-management platform.

Core operations:

- freeze consequential/external actions;
- revoke capability;
- cancel/suspend task;
- isolate actor from new work;
- preserve event/artifact evidence;
- inspect audit/authorization trace;
- resume only after revalidation.

A future incident-response workflow may compose these primitives when real operating experience justifies it.

Configuration rollback does not imply external effects were undone. The UI/audit must distinguish configuration reversibility from effect reversibility.
## v4.2 external-effect recovery

Consequential external effects use EffectObligations.

During restart/recovery:

1. identify PENDING/ATTEMPTED obligations;
2. inspect idempotency/reconciliation capability;
3. query destination status where supported;
4. retry only when policy says duplicate risk is acceptably controlled;
5. otherwise surface uncertainty/require operator resolution.

Configuration rollback does not mark an external effect undone.

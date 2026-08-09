# Approvals, Blocked Tasks, and Notifications

## Authority Non-Solicitation Invariant

> An agent SHALL NOT request expansion of its own permissions, capabilities, or authority. If an assigned task cannot be completed within current information, capabilities, authority, or dependencies, it returns `TASK_BLOCKED` with the unmet requirement.

## Parent remediation

Parent/governing actor may:

- provide information;
- rescope;
- split;
- reassign;
- cancel;
- issue a new separately authorized assignment.

The worker may state “Y cannot be completed without X.” It does not say “grant me X.”

## Approval states

```text
PENDING -> NOTIFIED -> ACKNOWLEDGED -> PENDING_DECISION
                                      -> APPROVED / DENIED
```

Acknowledgement is not approval.

## Risk vs urgency

- consequence risk determines approval authority;
- urgency determines notification behavior.

## V1 notification

Persist risk/urgency/acknowledgement. Use a simple `Notifier` interface and UI/log/email adapter if needed.

Do not build a multi-channel escalation platform in V1.

Principle remains:

> **Escalate attention, never authority.**

An unanswered protected action waits.

## Protective actions

While waiting, already-authorized harm-reducing actions may continue if they do not themselves cross a human boundary.
## v4.2 — effect-bound approval

A human approval should authorize an exact effect, not a general privilege expansion.

Bind approval to:

```text
EffectFingerprint
TaskID
Action
Resource/Destination
ArgumentsHash
ExpiresAt?
SingleUse?
```

If material parameters change, recalculate the fingerprint and re-evaluate whether a new approval is required.

A2A/Hermes cannot approve a human-required effect unless the external actor is itself the configured authorized human identity through an approved human-authentication path; ordinary Hermes operator identity is not sufficient.

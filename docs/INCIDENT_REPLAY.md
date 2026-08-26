# Deterministic incident replay

Agent OS can reconstruct the durable history of one accepted Work conversation
without re-executing it. The authenticated local user requests:

```text
GET /v1/user/incidents/replay?conversation_id=<public conversation ID>
```

The private user gateway resolves the public conversation to its tenant-scoped
internal ledger correlation. A2A actors cannot call this organization-wide
inspection route.

## Evidence boundary

The ledger reads the selected stream and verifies the complete cryptographic
event chain inside one read transaction. The report is deterministic for that
exact SQLite snapshot and contains:

- the public conversation and organization identities;
- an opaque verified ledger-chain algorithm and head hash;
- stream-local event order, Event Contract type, timestamp, and event ID;
- source actor, execution, recipient, and Task identities when present;
- authorization and artifact references;
- a SHA-256 digest of each exact stored payload; and
- explicit stream, Task, and execution predecessor links.

The report excludes raw payloads, prompts, results, artifact contents, the
private ledger correlation, global event counts, the global head event ID, and
global sequence positions. A stream larger than 256 events, an event with more
than 1,024 authorization or artifact references, an exposed envelope value
larger than 4 KiB, or a JSON report larger than 2 MiB fails closed rather than
returning an incomplete reconstruction.

Predecessor links express recorded ordering only. They are not a root-cause
finding, policy judgment, proof that an event was true, or proof that a control
was effective. The global chain head can change when any later event is added,
including an event outside the selected conversation; that does not rewrite the
selected timeline.

## Safety boundary

Incident replay is read-only. It cannot schedule or resume Work, repeat a model
or tool call, resend an effect, alter policy or capability, approve a request,
or certify completion. It supports investigation and evidence preservation but
does not replace operator-owned incident classification, containment, root
cause, corrective action, effectiveness review, or ISO/IEC 42001 assessment.

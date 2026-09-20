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

The ledger reads the selected Work stream, the organization's complete hold and
release history, and effect histories bound to that Work's admitted Tasks. It
verifies the complete cryptographic event chain and the selected records inside
one read transaction. The report is deterministic for that
exact SQLite snapshot and contains:

- the public conversation and organization identities;
- the complete-ledger-chain verification method, without its global head;
- stream-local event order, Event Contract type, timestamp, and event ID;
- source actor, execution, recipient, and Task identities when present;
- authorization and artifact references;
- a SHA-256 digest of each exact stored payload; and
- explicit stream, Task, and execution predecessor links;
- validated hold/release references and the last recorded execution start,
  inference reservation, and effect attempt before each hold;
- stop requests, uncertainty, and later local completion with accounting links;
  and
- Task-bound effect transitions, confirmation references, and reconciliation
  references.

The containment summary declares `admission_scope: CONTAINMENT_BOUNDARIES`.
These admissions identify durable local boundaries checked against retained
hold, stop, and suspension evidence. They do not establish remote dispatch time,
successful external action, or a complete historical authorization judgment.
Coordination and output actions remain explicitly unclassified. Effect linkage
is to a Task, not an inferred individual execution. An uncertain stop remains
in the history when later local completion arrives; local completion does not
prove remote termination.

The report excludes raw payloads, prompts, results, artifact contents, the
private ledger correlation, global event counts, the global head event ID or
hash, and global sequence positions. Combined Work and related evidence larger
than 256 events, an event with more
than 1,024 authorization or artifact references, an exposed envelope value
larger than 4 KiB, or a JSON report larger than 2 MiB fails closed rather than
returning an incomplete reconstruction.
Supporting records are also bounded before loading; oversized or inconsistent
supporting evidence fails the request even when the selected event count fits.

Predecessor links express recorded ordering within the combined selected
evidence, using durable ledger order rather than wall-clock timestamps.
They are not a root-cause
finding, policy judgment, proof that an event was true, or proof that a control
was effective. Complete-chain verification occurs inside the private read
snapshot, but the global head is withheld so activity in another organization
cannot be inferred by polling this tenant-scoped report.

## Safety boundary

Incident replay is read-only. It cannot schedule or resume Work, repeat a model
or tool call, resend an effect, alter policy or capability, approve a request,
or certify completion. It supports investigation and evidence preservation but
does not replace operator-owned incident classification, containment, root
cause, corrective action, effectiveness review, or ISO/IEC 42001 assessment.

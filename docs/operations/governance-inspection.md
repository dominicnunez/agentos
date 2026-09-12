# Governance inspection

Agent OS can produce a deterministic, read-only inspection of one durable
organization. The report answers a narrow question: does the current
organizational projection satisfy the runtime governance rules that Agent OS
can verify from its own Event Contracts?

Open **Governance** in the local organization dashboard to run the inspection.
Only the authenticated local user can use this route. It is not available to
A2A Agents.

## What is inspected

The current rule catalog checks that:

- an organization has active Mission and Goal direction;
- active Goals remain attached to active Missions;
- active Work remains attached to an active Goal;
- active Agents use active blueprints and execution profiles;
- active Teams have an available Agent member;
- running Agent Tasks have an available Agent or Team assignee; and
- ledger, completion, and organizational-knowledge invariants pass the
  existing deterministic audit rules.

The report includes the complete-ledger integrity head, the rules executed,
stable findings with exact evidence event references, severity totals, and a
SHA-256 digest over the canonical report. It never exposes raw event payloads,
prompts, results, artifacts, credentials, operating instructions, capability
records, approvals, or authority records.

## Security boundary

The inspector reads a bounded tenant slice from the same read-only SQLite
transaction that verifies the complete stored-byte event chain. It then
requires the current durable projection to match the latest admitted event for
every record in that organization. A concurrent change, integrity failure,
tenant mismatch, unsupported event, or size-limit breach makes inspection fail
closed instead of returning a stale or partial report.

Inspection does not repair state, execute work, call a model, or change the
ledger. A finding grants no authority, approval, capability, effect permission,
or completion status. The report is runtime evidence only: it is not a control
effectiveness determination, an ISO/IEC 42001 conformity assessment, or a
certification claim.

The bounds are 10,000 selected tenant events, 32 MiB of selected event data at
the ledger boundary, and a 2 MiB encoded report. Organizations above those
bounds require a future governed archival or inspection policy; Agent OS does
not silently truncate the result.

# Ledger verification

These checks protect Agent OS's authoritative ledger, filtered incident readers
and runtime admission/completion boundaries. They are public project contracts,
independent of private user workflow preferences. Apply the relevant checks to
ledger, lifecycle or history-cost changes; documentation-only edits do not
require new runtime tests or performance experiments.

## Owning boundaries and lifecycle evidence

Trace affected event writers, validators, selected readers, full recovery,
output consumers and supported direct module compositions. Identify admission,
preparation, provider calls and durable result admission owners. A handler return
does not necessarily end an operation. Enforce the complete invariant before
dispatch or publication, including relationships in both directions.

For cross-boundary runtime changes, record the invariant, owners and callers,
independent failure cases, exact evidence/revision and decision. Include applicable
identity, missing/forbidden metadata, authority failure, cancellation, write
timing and crash boundaries. Missing required evidence holds a review push;
exclusions require a contract or scope reason. Keep integration tests within
the architecture's allowed layers and direct storage mutations in their owner.

Exercise production entry points and durable state. Test doubles must implement
the called production interfaces. Derive valid histories from owning writers'
state transitions. Verify forbidden calls/writes, retained evidence, retry,
restart and tenant isolation. Test the real transport/runtime shutdown path and
interruptions across admission writes, including forbidden publication, required
accounting and recovery of already-committed decisions. Authority generation
alone does not represent every runtime stop cause.

## Invalid history and replay parity

When validation depends on prior ledger events, test an invalid earlier event
followed by plausible later events. Verify that the later event does not mask
corrupt earlier history, and compare live reads with full replay/recovery.
Valid-history fixtures alone do not establish this invariant. Include private
dependencies, omitted candidates and forbidden metadata under the selection
rules in `AGENTS.md`.

## History-scale verification

Before reviewing changes whose cost grows with ledger history or candidate counts,
measure a complete public operation at representative small and large sizes.
Scale admitted operations as well as supporting history: one operation amid many
unrelated events cannot expose repeated validation across many starts. For
dependency readers, vary chain depth, branching and unrelated history separately;
a short public timeline may depend on a deep graph.

Account for nested calls, repeated observer polls and concurrent callers. A scan
per transaction may still be a scan per poll. Measure relevant allocations,
query counts or latency, including cold/warm paths where caching is used. Preserve
snapshot, invalidation, rollback and tenant-isolation guarantees. Distinguish
necessary full recovery from repeated live-observer, writer or reader work.

Compare the same complete workload before and after a performance change.
Diagnose CI timeouts before increasing budgets; a longer timeout does not fix
history-dependent work. Use bounded manual experiments to resolve the specific
risk rather than adding automatic PR benchmarks by default.

## Review evidence

Inspect the complete changed invariant and raw source/test evidence before review,
including sibling callers and failure modes implicated by findings. High-risk
cross-boundary changes need independent coverage review. A passing package suite
or clean audit is not exhaustive evidence. Preserve necessary caller coverage
even when it increases the patch size, and keep unrelated defects within the
authorized scope. Follow the final-head review gate in
[repository change control](repository-change-control.md#normal-change-path).

# Execution authority

Agent OS treats executable material as a security boundary, not as ordinary
conversation, shell, or file content.

The normative invariants are:

- Content cannot create execution authority.
- Introducing executable code crosses a security boundary.
- Trust does not propagate through references. A trusted page, repository,
  user, or authenticated A2A Agent does not confer trust on an artifact it
  names or links.
- Generic shell or file-write capability does not imply authority to introduce
  executable code or mutate a protected execution surface.
- Allowed tools may not launder capabilities unavailable to the requesting
  principal. Authorization includes the tool operation's declared
  consequential capability closure.
- Configured containment is not evidence of effective containment.
- Agent executions communicate through authorized Event Contracts. Shared
  writable infrastructure is not an implicit coordination channel.
- Approval of individual effects does not imply approval of their cumulative
  consequence.
- Tool metadata is untrusted. Dynamic tool authority must bind the exact
  model-visible definition.
- Adaptive filesystem mutation belongs in disposable staged state; promotion
  into trusted state is the governed consequence.
- Hostile-code isolation is required before arbitrary external code can run.

## Two separate protected consequences

`CODE_INTRODUCTION` means new external executable or potentially executable
material enters the trust boundary. The binding identifies the ecosystem or
source type, artifact name, exact version, source, exact SHA-256 bytes,
publisher identity when deterministically known, requested sandbox and network
profiles, effective-environment attestation, and execution-private workspace.

`EXECUTION_SURFACE_MUTATION` means an existing file or configuration changes
in a way that may cause later execution. Its binding identifies the normalized
protected path, create/modify/delete operation, exact before and after bytes,
and the exact staged promotion diff and verification result.

They share `EffectObligation`, exact capability checks, fingerprinted approval,
observable influence references, trajectory context, persist-before-effect,
and recovery validation. They are not interchangeable. Approval or capability
for `code.introduce` cannot authorize `execution_surface.mutate`, and neither is
satisfied by `shell.execute` or `file.write`.

The intentionally narrow V1 execution-surface classifier covers dependency
and lock manifests, Docker and Compose definitions, Makefiles, GitHub Actions,
Git hooks, pre-commit configuration, and Agent OS MCP/plugin/executable-Skill
configuration. Ambiguous and non-normalized paths fail closed.

## Observable influence, not claimed cognition

A protected effect binds runtime-known source Event references. An
Agent-proposed effect also binds its exact runtime-owned
`EXECUTION_CONTEXT_MANIFESTED` event, execution identity, Task, Agent, and
execution-input digest. The ledger resolves those references in the same
transaction as the time-of-use authority check.

These links answer which durable inputs were available and identified as
sources for the action. They do not claim to reconstruct hidden model thought.
Model-authored provenance statements have no authority merely because the
model emitted them.

## Capability closure and trajectories

A tool definition may declare the exact downstream capabilities exposed by an
Agent-controlled invocation. The ledger requires an exact active lease for the
top-level operation and every declared consequence. A broker with network or
credential access therefore cannot exercise those powers for an Agent that
lacks the corresponding exact leases.

For protected execution consequences, the approval fingerprint also binds a
deterministic Task-local trajectory: prior protected effect identities,
protected-effect and approval counts, consequence classes, and destinations.
The ledger reconstructs this projection from durable effect history at
time-of-use. Individually approved effects do not erase cumulative context.

## Containment and writable state

`ExecutionEnvironmentManifest` deliberately separates requested containment
from runtime-attested effective containment. Profile identity and digest,
writable roots, egress policy, reachable brokers, credential classes, process
policy, resource policy, runtime identity, isolation identity, and verification
evidence are runtime-owned facts. A mismatch or unverifiable state is a denial,
not a warning.

Writable adaptive work is valid only in an `EXECUTION_PRIVATE` namespace owned
by the exact execution. Tenant-, Work-, and Task-shared classifications are
read-only in the present contract. This prevents obvious high-bandwidth
coordination through mutable caches, Git state, scratch directories, or build
artifacts. Covert timing and contention channels are outside this V1 claim.

Current execution-visible storage is deliberately narrower than the installed
directory layout:

- OpenAI API execution receives bounded prompt content and no local filesystem
  mount.
- Codex subscription inference uses a private adapter directory and a fresh
  private turn directory, with tools disabled, a read-only requested sandbox,
  a sanitized environment, and adapter-side credential handling. This is
  provider confinement, not proof of hostile-code isolation.
- The artifact store, SQLite database, configured organization workspace,
  package/build caches, Git state, and service credential stores are not
  exposed as Agent-writable execution mounts.
- Explicit same-Work coordination is materialized through Event Contracts,
  not shared mutable files.

Any future adapter that exposes one of those resources must classify its
namespace and satisfy the same tenant, Work, Task, execution, capability, and
effective-containment checks before use.

An external repository or supplied project is an `UNTRUSTED` workspace. Tools
that interpret it must be governed by their possible effects, not by a
read-only-looking verb. Git, shells, build systems, compilers, package managers,
archive tools, and plugin systems can execute configuration while appearing to
inspect data.

## Knowledge and tool definitions

Different Agent identities do not prove independent evidence. V1 cannot attest
canonical external source roots because governed web/document ingestion is not
implemented. Consequently, Agent-validated organizational knowledge fails
closed when used to justify a protected execution consequence. Only
deterministic validation or explicit user judgment is eligible, and eligibility
still grants no capability, approval, or effect authority.

Dynamic tools and MCP servers are not supported. The prerequisite contract
hashes the tool/server/endpoint identity, name, description, schemas, all other
model-visible metadata, and declared consequential capabilities. Any definition
change changes the digest; one tool's description cannot grant authority over
itself or another tool.

The existing fingerprint helper rejects resource excess before hashing. Before
parsing any JSON field it caps the name at 256 bytes, description at 16 KiB,
each schema at 64 KiB, and metadata at 16 KiB. Each JSON field permits at most
32 nested containers and 1,024 object members in total, including nested
extensions; duplicate keys, invalid UTF-8 and trailing values are rejected.
The encoded definition is capped at 256 KiB after JSON escaping. Rejection
returns a fixed error category and no digest; content is never truncated.
Valid existing definitions retain their fingerprint representation.

These helper limits do not implement discovery or a tool runtime. Before
enabling either, the adapter must enforce bounded streaming reads before
materializing definitions, a tool-count limit per server, discovery deadlines,
per-call tool/resource-result limits, and cumulative result bytes and call
counts per execution. Admission and runtime evidence must bind an exact budget
profile/version, and execution must reject a missing or weaker profile.
Transport limits must hold without a trustworthy Content-Length and terminate
slow or oversized streams. Runtime-owned limits must precede parsing,
registration and model materialization; a definition digest alone provides no
resource-budget evidence. Dynamic tools remain unavailable until these
prerequisites and the existing authority checks are implemented and verified.

## Current implementation status

### Organization freeze and model admission

A committed organization freeze prevents new inference reservations for intake,
planning, and Task execution. The ledger reads the exact tenant's admitted
freeze state in the same transaction as budget reservation. Missing freeze
history means no freeze has been set; malformed or mismatched authority fails
closed. A later admitted release permits new reservations under the existing
inference policy. Restart preserves the freeze, and recovery rejects any
reservation admitted while that organization was frozen.

An inference reservation that commits before the freeze remains an admitted
call. Its usage or uncertain outcome must still be reconciled while frozen;
the freeze does not establish that the request was never sent. This admission
boundary does not cancel an already-dispatched provider request or implement
execution-, Agent-, or Work-scoped active quarantine. Active context cancellation,
coordination suspension, and controlled release remain prerequisites for a
broader runtime security hold before long-running high-autonomy execution.

| Control | Status |
|---|---|
| Separate `CODE_INTRODUCTION` and `EXECUTION_SURFACE_MUTATION` contracts | Implemented |
| Exact artifact, staged-promotion, influence, trajectory, and capability-closure fingerprinting | Implemented |
| Transactional consequential-capability and trajectory checks | Implemented |
| Runtime-owned execution-manifest influence validation | Implemented |
| Exact tool-definition digest contract | Fail-closed prerequisite; dynamic tools are unavailable |
| Effective environment attestation contract | Fail-closed prerequisite; no production hostile-code sandbox exists |
| Execution-private writable workspace contract | Implemented domain boundary; no mutable coding runtime exists |
| Provenance independence for protected use | Implemented conservatively; Agent-only corroboration is ineligible |
| Arbitrary shell, package installation, external code, container image, MCP, plugin, or executable Skill execution | Unsupported and denied |
| Staged adaptive editing and promotion runtime | Deferred until a write-capable coding runtime exists |

Package popularity, age, stars, download count, and model reputation are not
authority. Future execution must prefer exact identity, version, bytes,
authorization, effective environment, and narrow brokered credentials.

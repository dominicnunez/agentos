# V1 threat model

## Purpose and scope

This document defines the security model for the Linux-only Agent OS V1
vertical slice. It covers the runtime, SQLite ledger and recovery command,
Human and A2A work intake, completion review, exact-effect approval control,
model-provider adapters, read-only effect reconciliation, and the release
artifact pipeline.

The model is updated whenever a trust boundary or externally reachable surface
changes. A passing test is evidence for a control, not proof that a deployment
is safe in every environment.

## Protected assets

- authoritative Event Contracts and durable organization/work state;
- capability leases, freezes, approvals, completion decisions, and effect
  obligations;
- bearer credentials, provider credentials, and deployment configuration;
- tenant-confined work, results, model context, and sensitive data;
- effect idempotency, reconciliation evidence, and audit history; and
- release binaries, checksums, SBOMs, provenance, and source identity.

## Trust boundaries

1. **Network to work intake.** Human and A2A requests are untrusted until a
   reviewed, unexpired credential establishes a principal and exact tenant.
2. **Work to authority.** Authenticated conversation text remains untrusted
   work or input; it cannot mint capabilities, approvals, freezes, verified
   completion, or effect state.
3. **Completion review.** A dedicated reviewer credential can decide only the
   exact fingerprinted candidate and evidence presented by the runtime.
4. **Effect approval.** A separate disabled-by-default listener and principal
   registry authorize only an exact organization, boundary, risk, and complete
   ledger-backed effect fingerprint.
5. **Runtime to persistence.** SQLite is the authoritative local ledger.
   Security-sensitive time-of-use checks and the attempted-effect transition
   share one transaction.
6. **Runtime to providers and destinations.** Model providers and read-only
   reconcilers are explicit adapters with server-owned credentials. Production
   effect-writing adapters are absent in V1.
7. **Source to release artifact.** Pinned builders produce checksummed,
   reproducible Linux artifacts with SBOM and unsigned provenance. Publication
   and signing remain separate approval boundaries.

## Threat actors and assumptions

The controls address unauthenticated network callers, holders of a stolen or
overprivileged intake credential, malicious external Agents, prompt-injected or
compromised model output, hostile task content, replay and concurrency, tenant
enumeration, stale approvals, revoked authority, crash uncertainty, and build
or dependency drift.

An attacker with administrative control of the host, process memory, secret
source, trusted registry files, or SQLite file can subvert the V1 process. Host
hardening, secret rotation, filesystem permissions, network policy, TLS key
custody, monitoring, and backup protection are deployment responsibilities.
Agent OS does not claim to defend against a compromised operating-system
administrator.

## Attack surfaces and controls

| Surface | Principal threats | V1 controls | Residual risk |
|---|---|---|---|
| Human and A2A intake | forged identity, tenant traversal, authority-shaped payloads, oversized input, replay, denial of service | separate reviewed registries; exact roles and work scopes; expiry, rate, and concurrency limits; strict size-limited JSON; canonical identifiers; tenant-scoped opaque bindings; durable replay handling; recursive authority-field rejection | bearer credentials are replayable until expiry/revocation; edge-scale traffic filtering is outside the process |
| A2A protocol | method confusion, result disclosure, compatibility shims that widen access | official A2A v1.0 Go types and JSON-RPC transport behind an Agent OS-owned strict boundary; capability-separated status/result access; generic not-found behavior across tenant boundaries; no legacy aliases or SDK-owned task state | protocol evolution requires a new review and interoperability evidence |
| Model execution | prompt injection, self-approval, provider tools, unbounded cost or data egress | explicit task inference policy; bounded manifests; model output remains candidate data; provider tools and storage disabled for OpenAI API; no automatic billable retry; real providers disabled until separate gates | a provider receives approved context during an enabled live test; semantic output may still be malicious |
| Completion review | ordinary operator or Agent finalizes model output, stale decision, cached sensitive review | dedicated reviewer role; exact candidate/evidence fingerprint; no-store responses; durable decision and recovery; stale fingerprints rejected | reviewer judgment can be wrong and must be scoped operationally |
| Approval control | chat-based approval, credential reuse, cross-tenant grant, stale/changed effect, expired decision, route probing | separate listener and credentials; loopback and disabled defaults; TLS 1.3 for remote binding; exact organization/boundary/risk grants; full ledger-sourced effect view; strict non-language operations; canonical immutable-effect fingerprint; revalidation on every transition and at transactional time of use; generic not-found response | a stolen approval credential retains its reviewed grants until expiry or restart with revoked configuration |
| SQLite and recovery | partial state, corrupt restore, rollback overwrite, unauthorized file access | append-only versioned records; transactional projections and effect authorization; integrity/schema verification; no-overwrite restore; explicit rollback procedure | local file access can reveal or alter data; storage encryption and host access control are external |
| Consequential effects | duplicate or unauthorized action, crash after send, false success | persist-before-effect obligation; exact lease/freeze/approval check in the attempt transaction; single-use consumption; idempotency key; evidence-required confirmation; read-only reconciliation; no blind resend | production effect-writing adapter is intentionally disabled |
| Release pipeline | untracked source, dependency substitution, missing dependency source or license evidence, non-reproducible output, artifact mix-up | exact clean commit; pinned Go/Python and actions; module hash verification; compiled-module license discovery; embedded AGPL and source identity; deterministic corresponding source with vendored external module source; offline Linux source test; CGO/VCS stripping; two independent builds and byte comparison; checksums, target SBOMs, provenance; packaged-binary pilot | provenance is unsigned, final software-licensing review remains required, and artifacts are not published until a separately approved release workflow exists |

## Security invariants

- Model output and operator text never become trusted Event Contracts directly.
- A principal cannot expand its own capabilities or inherit authority merely
  through Task DAG relationships.
- Work, completion review, and effect approval use distinct identities and
  credentials, even when one person operates each role.
- Unknown roles, grants, boundaries, states, fields, and execution mechanisms
  fail closed.
- Acknowledgement is not approval; an approval binds one complete immutable
  effect intent and is rechecked immediately before an attempt.
- Tool output is not proof of an effect, and nonempty model text is not proof of
  completion.
- Interrupted effects with uncertain outcomes are reconciled without blind
  resend.
- Remote listeners require explicit enablement and TLS; release publication,
  provider activation, deployment, and consequential effects remain separate
  decisions.

## Verification expectations

Required CI covers formatting, module consistency, build, vet, lint, race
tests, bounded operator-input fuzzing, vulnerability scanning, A2A
interoperability, architecture boundaries, and advisory clone/dead-code
analysis. The release workflow independently builds twice, verifies checksums
and byte reproducibility, unpacks the Linux amd64 archive, and runs the complete
intake, completion-review, approval-isolation, backup, restore, restart,
expiry, and revocation pilot from the packaged binaries.

A security change must add or update an adversarial regression at the boundary
it changes. A finding that weakens an acceptance row returns that row to
partial until both the runtime path and regression are corrected.

## Deferred or explicitly absent

V1 does not include federation, a workflow DSL, semantic/vector memory, broad
tool ecosystems, production effect-writing adapters, automatic uncertain-effect
retries, a multi-host database, or an internet-edge denial-of-service layer.
Real-provider testing, deployment, release signing/publication, and the first
reversible external effect each retain their separate approval gates.

Report suspected vulnerabilities using [SECURITY.md](../SECURITY.md).

# V1 threat model

## Scope

This model covers the Linux-only Agent OS V1 runtime, resumable setup, embedded
web dashboard, private user gateway, A2A intake, provider adapters, artifact store,
SQLite ledger and recovery command, exact-effect approvals, completion review,
and release pipeline.

A passing test is evidence for a control, not proof that every deployment is
safe. A user with root access already controls the machine and can replace the
binary, configuration, service, ledger, or kernel identity result. Agent OS
does not claim to defend against a compromised operating-system administrator.

## Protected assets

- Event Contracts and durable organization, Mission, Goal, Work, and Task state;
- capabilities, freezes, approvals, completion decisions, and effect obligations;
- provider, A2A, and ephemeral dashboard bearer credentials;
- tenant-confined work, results, artifacts, and model context;
- effect idempotency, reconciliation evidence, and audit history;
- release binaries, source identity, checksums, SBOMs, and provenance.

## Trust boundaries

1. **Setup to installed authority.** The account that starts setup becomes the
   local owner. Elevation must preserve and verify that account rather than
   accepting a typed username.
2. **Local process to user gateway.** Linux peer credentials on a mode-`0600`
   Unix socket establish the configured owner. No local bearer file exists.
3. **Browser to local process.** An owner-launched, one-time credential
   establishes an expiring dashboard session on an ephemeral IPv4 loopback
   bridge. The bridge is not the private user gateway.
4. **Network to A2A.** A reviewed Agent record and unique server-owned bearer
   establish the exact principal, tenant, role, scope, expiry, and limits.
5. **Content to authority.** Conversation, model, and artifact content remain
   untrusted. They cannot create approval, capability, policy, or completion
   authority.
6. **User decision to effect.** Approval and subjective completion bind exact
   ledger evidence and are separate from natural-language work.
7. **Runtime to provider.** Only the configured model adapter receives bounded
   execution context and its service-managed credential.
8. **Runtime to persistence.** SQLite is authoritative. Security-sensitive
   time-of-use checks and attempted-effect state share a transaction.
9. **Source to release.** Pinned builders create reproducible, checksummed Linux
   artifacts with corresponding source and dependency-license evidence.

## Attack surfaces and controls

| Surface | Main threats | V1 controls | Residual risk |
|---|---|---|---|
| Setup and elevation | binding the wrong owner, PATH substitution, partial setup, symlink overwrite | system mode is the resumable default; `sudo` origin is verified with `getent`; direct root is allowed; privileged tools use fixed system paths; configuration writes are bounded, atomic, and reject symlinks | root or a compromised system utility can subvert setup |
| Private user gateway | remote exposure, local impersonation, service-account self-approval | Unix socket only; socket activation; owner UID and mode `0600`; kernel `SO_PEERCRED`; the restricted service account cannot connect as the owner; request limits | compromise of the owner account or kernel defeats the boundary |
| Web dashboard | loopback impersonation, DNS rebinding, CSRF, XSS, session theft, lost-response replay, direct ledger mutation | exact IPv4 loopback Host; one-time 256-bit bootstrap in a mode-`0600` temporary page; no credential in terminal output or launcher arguments; expiring in-memory bearer; exact Origin on bootstrap and cross-origin rejection thereafter; no CORS grants or cookies; allowlisted routes; response limits; server-owned recovery from authenticated durable intake, confirmation, input, approval, and review records; hash-bound CSP, frame denial, and no direct persistence access | compromise of the owner account or browser session can act within that owner's V1 authority |
| A2A | stolen bearer, tenant traversal, replay, authority-shaped input, method confusion, substituted trust files | official A2A v1.0 types behind strict authentication and decoding; exact roles/scopes; expiry, rate, and concurrency limits; opaque tenant-scoped IDs; recursive authority-field rejection; only `SendMessage` and `GetTask`; registry, TLS material, and encrypted token sources are confined, ownership-checked, mode-checked, and imported through systemd credentials | bearers remain replayable until rotation or revocation; internet-edge filtering is external |
| Provider setup and use | plaintext secrets, ambient credentials, wrong model, hidden tools, cost or data egress | no `.env` requirement; OpenAI keys use systemd encrypted credentials; rotating Codex credentials use an authenticated encrypted state file with a separately protected systemd key and a private runtime copy; exact tested provider required; dated OpenAI snapshots; provider tools, redirects, storage, and automatic billable retries disabled | credentials and approved prompts exist in process memory; providers receive approved context |
| Semantic intake | invented operator choices, hidden Goal, replacement, or Lab-mode substitution, confirmation replay, lifecycle race | strict bounded output; explicit `STANDARD`/`EXPERIMENT` mode in the complete draft fingerprint; runtime-owned Lab containment only; unsupported adaptive experiments rejected before confirmation; explicit source-message provenance and exact-ID presence checks for Goal and replacement Work IDs; active same-tenant Goal and failed same-Goal predecessor admission in the confirmation transaction; immutable Intent, Goal, and replacement binding | an authorized operator can explicitly confirm the wrong eligible Goal, predecessor, or reviewed mode |
| Controlled replanning | in-place Plan mutation, reopened Tasks, hidden predecessor selection, replacement forks or cycles, cross-tenant lineage, authority or evidence inheritance | authenticated Work-ID disclosure; one explicit predecessor displayed in the reviewed Intent; deterministic predecessor Goal binding; prior failed state rechecked at confirmation sequence during write and recovery; same organization and Goal; one direct successor; fresh Intent, Work, Plan, and Task DAG; atomic admission and replay validation; no inherited approval, capability, effect permission, artifacts, completion, or execution state; Lab replacement rejected at application, durable admission, and replay boundaries | replacement lineage records deliberate recovery but cannot guarantee that the new plan is effective |
| Task-DAG planning | prompt injection, authority invention, graph bombs, dependency cycles, strategic-context substitution, partial persistence, mislabeled lifecycle state, internal-task disclosure | exact accepted-Intent fingerprint; exact same-tenant Mission/Goal events and versions in Goal-bound model input and Plan fingerprint; bounded closed-schema output; runtime-owned root; execution-kind allowlist; 16-Task ceiling; deterministic handler registry; cycle and dependency validation; atomic graph commit; immutable Task contracts; exact event/status transitions at write, replay, and recovery; root-only A2A lookup | a valid but poor Plan can waste bounded model work or require independent review |
| Execution admission | strategic time-of-check/time-of-use races, oversized strategic context, deactivation race, stale or substituted Agent configuration, cross-tenant roster reference, replayed start authority | one typed SQLite start transaction requires the accepted Intent fingerprint and current Mission/Goal revisions to match the immutable Plan for Agent, deterministic, and user-operated work; Agent input size is checked before start; Agent starts also reload the exact active Agent, blueprint, execution profile, pending Task revision, and bounded Team history; sealed admission and replay reject stale, superseded, malformed, or cross-organization bindings; these bindings grant no capability or effect authority | a strategic or roster change committed after dispatch may affect only future dispatches; interrupted adaptive provider contact remains uncertain |
| Structured user completion | self-reported completion, missing documents, duplicate evidence, media spoofing, oversized files, lost-response identity conflict | durable CompletionContract; exact fields, roles, counts, and media types; duplicate-reference rejection; 16 MiB file and 32 MiB request totals; content sniffing; SHA-256 private storage; authenticated origin binding; explicit recovery from the existing durable submission without re-upload | files may still contain malicious content and remain untrusted to later consumers |
| Model completion review | Agent self-certification, stale review, result disclosure, ephemeral-origin loss, unbounded history scans | owner-only private control; exact candidate/evidence fingerprint; no-store responses; durable idempotent decision and recovery for root and locally reviewable child Tasks; pending requests and terminal decisions selected by tenant-scoped, cursor/limit-bounded SQLite queries before exact stream validation | the user's subjective judgment can be wrong |
| Goal progress | worker self-certification, forged, stale, or causally reordered Goal, Mission, or Work evidence, tenant crossing, duplicate terminal transitions, unbounded continuous history | ledger-selected current Work witnesses bounded by Goal criteria; exact active Goal revision and active-at-evaluation Mission binding; causal sequence checks; deterministic criterion coverage; fingerprinted evaluation; atomic target achievement; continuous Goals remain non-terminal; generic writers reject terminal evidence names; event-only rebuild revalidates the chain | exact criterion equality may require a reviewed Goal refinement when independently valid evidence uses different wording or provenance |
| Exact-effect approval | approval through chat, changed effect, stale or expired decision, clock-rollback ordering, ephemeral-origin loss, unbounded terminal-history scans | owner-only private control; full ledger-sourced effect view; typed confirmation; immutable fingerprint; revalidation on every transition and at transactional use; transactionally maintained tenant-scoped pending projection; bounded terminal decisions ordered by ledger commit sequence; explicit expired-binding reconciliation | a compromised owner can approve within that account's V1 authority |
| SQLite and recovery | corruption, wrong-database confusion, unsupported or partial migration, Event Contract drift, forged projection-shaped events, copied or orphaned admission, identity or tenant substitution, mislabeled Agent, Mission, Goal, Work, or Task state, missing terminal evidence, lost input response, unsafe overwrite, unauthorized access | Agent OS SQLite application ID; exact versioned layouts; source validation before one-transaction ordered migration; storage/Event schema metadata and layout fingerprint; frozen oldest-supported fixture; no inferred unversioned compatibility; append-only events and versioned records; one-to-one event-coupled projection fingerprints; exact identity, parent, correlation, prior/resulting state, and terminal Work/Goal evidence validation during write, replay, startup, backup, restore, and explicit bodyless recovery from durable completion or user-input events; read-only verification; no-overwrite backup and restore; private data paths; service umask `0077` | host file access can reveal or alter data; storage encryption and cryptographic ledger attestation are external |
| Consequential effects | duplicate action, revoked authority, crash after send, false success | persist-before-effect obligation; exact lease/freeze/approval checks in the attempt transaction; single-use consumption; idempotency key; evidence-required confirmation; no blind resend | production effect-writing adapters remain absent |
| Release pipeline | dependency substitution, missing source/license, unreproducible archive, artifact mix-up | pinned Go, Node, pnpm, Python, and actions; lockfile, module hash, and compiled Go/browser license checks; reproducible embedded dashboard; embedded AGPL and source identity; vendored Go corresponding source; offline Linux source tests; independent byte comparison; checksums, SBOMs, and provenance | provenance is unsigned and publication remains separately approved |

## Security invariants

- Model, user, Agent, and artifact content never become trusted state directly.
- A model-proposed Plan is coordination data only; it cannot grant authority,
  approve an effect, add an execution mechanism, or make its children public.
- A principal cannot expand its own capability or inherit authority through a
  Task relationship.
- A dispatch binding authorizes one exact Agent invocation only; it cannot be
  reused, substituted, or treated as approval, capability, or effect authority.
- Acknowledgement is not approval; approval binds one immutable effect.
- A ToolOutcome is not proof of an external effect, and nonempty model text is
  not proof of completion.
- Work may join a Goal only through an explicit, reviewed, tenant-checked
  Intent reference. Work completion supplies evidence but cannot certify Goal
  achievement; only the atomic Goal evaluator may admit the terminal state.
- Replacement Work requires a new reviewed Intent naming one exact failed
  predecessor. Its lineage grants no authority or evidence, and the predecessor
  cannot be mutated, reopened, forked, or replaced across tenant or Goal bounds.
- User-originated `HUMAN` Tasks require their structured CompletionContract;
  an A2A Agent cannot complete them through a text continuation.
- Unknown roles, fields, states, grants, boundaries, and execution mechanisms
  fail closed.
- Interrupted uncertain effects are reconciled without automatic resend.
- Remote A2A requires explicit enablement and TLS. The user gateway never binds
  TCP. The owner-launched dashboard bridge is a separate, ephemeral IPv4
  loopback process and exposes only an allowlist of user-gateway operations.
- Runtime and credential directories reject symlink traversal, unexpected
  ownership, and broader-than-required permissions before the service opens
  provider credentials or the ledger.

## Verification

CI covers reproducible SvelteKit generation, compiled frontend license
evidence, dependency audit, type checks, formatting, module consistency, builds, vet, lint, race tests,
bounded gateway fuzzing, vulnerability scanning, official A2A client tests,
architecture boundaries, deterministic release construction, dependency
licenses, corresponding-source offline tests, and packaged Linux binary smoke
checks. Recovery, restart, approval, completion review, provider confinement,
UID authentication, and structured artifact behavior have adversarial unit or
integration tests.

Real-provider testing, deployment, release publication/signing, and the first
reversible external effect remain separate approval gates.

Report suspected vulnerabilities using [SECURITY.md](../SECURITY.md).

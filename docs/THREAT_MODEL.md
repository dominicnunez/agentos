# V1 threat model

## Scope

This model covers the Linux-only Agent OS V1 runtime, resumable setup, terminal
console, private user gateway, A2A intake, provider adapters, artifact store,
SQLite ledger and recovery command, exact-effect approvals, completion review,
and release pipeline.

A passing test is evidence for a control, not proof that every deployment is
safe. A user with root access already controls the machine and can replace the
binary, configuration, service, ledger, or kernel identity result. Agent OS
does not claim to defend against a compromised operating-system administrator.

## Protected assets

- Event Contracts and durable organization, goal, and Task state;
- capabilities, freezes, approvals, completion decisions, and effect obligations;
- provider and A2A bearer credentials;
- tenant-confined work, results, artifacts, and model context;
- effect idempotency, reconciliation evidence, and audit history;
- release binaries, source identity, checksums, SBOMs, and provenance.

## Trust boundaries

1. **Setup to installed authority.** The account that starts setup becomes the
   local owner. Elevation must preserve and verify that account rather than
   accepting a typed username.
2. **Local process to user gateway.** Linux peer credentials on a mode-`0600`
   Unix socket establish the configured owner. No local bearer file exists.
3. **Network to A2A.** A reviewed Agent record and unique server-owned bearer
   establish the exact principal, tenant, role, scope, expiry, and limits.
4. **Content to authority.** Conversation, model, and artifact content remain
   untrusted. They cannot create approval, capability, policy, or completion
   authority.
5. **User decision to effect.** Approval and subjective completion bind exact
   ledger evidence and are separate from natural-language work.
6. **Runtime to provider.** Only the configured model adapter receives bounded
   execution context and its service-managed credential.
7. **Runtime to persistence.** SQLite is authoritative. Security-sensitive
   time-of-use checks and attempted-effect state share a transaction.
8. **Source to release.** Pinned builders create reproducible, checksummed Linux
   artifacts with corresponding source and dependency-license evidence.

## Attack surfaces and controls

| Surface | Main threats | V1 controls | Residual risk |
|---|---|---|---|
| Setup and elevation | binding the wrong owner, PATH substitution, partial setup, symlink overwrite | system mode is the resumable default; `sudo` origin is verified with `getent`; direct root is allowed; privileged tools use fixed system paths; configuration writes are bounded, atomic, and reject symlinks | root or a compromised system utility can subvert setup |
| Private user gateway | remote exposure, local impersonation, service-account self-approval | Unix socket only; socket activation; owner UID and mode `0600`; kernel `SO_PEERCRED`; the restricted service account cannot connect as the owner; request limits | compromise of the owner account or kernel defeats the boundary |
| Terminal console | forged status, terminal escape injection, direct ledger mutation | console uses only the private HTTP-shaped gateway; strict response decoding; untrusted display text has control and direction-format characters removed | visually misleading ordinary Unicode or incorrect user judgment remains possible |
| A2A | stolen bearer, tenant traversal, replay, authority-shaped input, method confusion, substituted trust files | official A2A v1.0 types behind strict authentication and decoding; exact roles/scopes; expiry, rate, and concurrency limits; opaque tenant-scoped IDs; recursive authority-field rejection; only `SendMessage` and `GetTask`; registry, TLS material, and encrypted token sources are confined, ownership-checked, mode-checked, and imported through systemd credentials | bearers remain replayable until rotation or revocation; internet-edge filtering is external |
| Provider setup and use | plaintext secrets, ambient credentials, wrong model, hidden tools, cost or data egress | no `.env` requirement; OpenAI keys use systemd encrypted credentials; rotating Codex credentials use an authenticated encrypted state file with a separately protected systemd key and a private runtime copy; exact tested provider required; dated OpenAI snapshots; provider tools, redirects, storage, and automatic billable retries disabled | credentials and approved prompts exist in process memory; providers receive approved context |
| Structured user completion | self-reported completion, missing documents, duplicate evidence, media spoofing, oversized files | durable CompletionContract; exact fields, roles, counts, and media types; duplicate-reference rejection; 16 MiB file and 32 MiB request totals; content sniffing; SHA-256 private storage; authenticated origin binding | files may still contain malicious content and remain untrusted to later consumers |
| Model completion review | Agent self-certification, stale review, result disclosure | owner-only private control; exact candidate/evidence fingerprint; no-store responses; durable idempotent decision and recovery | the user's subjective judgment can be wrong |
| Exact-effect approval | approval through chat, changed effect, stale or expired decision | owner-only private control; full ledger-sourced effect view; typed confirmation; immutable fingerprint; revalidation on every transition and at transactional use | a compromised owner can approve within that account's V1 authority |
| SQLite and recovery | corruption, partial state, unsafe overwrite, unauthorized access | append-only events and versioned records; transactional projections; read-only integrity/schema verification; no-overwrite backup and restore; private data paths; service umask `0077` | host file access can reveal or alter data; storage encryption is external |
| Consequential effects | duplicate action, revoked authority, crash after send, false success | persist-before-effect obligation; exact lease/freeze/approval checks in the attempt transaction; single-use consumption; idempotency key; evidence-required confirmation; no blind resend | production effect-writing adapters remain absent |
| Release pipeline | dependency substitution, missing source/license, unreproducible archive, artifact mix-up | pinned Go/Python/actions; module hash and license checks; embedded AGPL and source identity; vendored corresponding source; offline Linux source tests; independent byte comparison; checksums, SBOMs, and provenance | provenance is unsigned and publication remains separately approved |

## Security invariants

- Model, user, Agent, and artifact content never become trusted state directly.
- A principal cannot expand its own capability or inherit authority through a
  Task relationship.
- Acknowledgement is not approval; approval binds one immutable effect.
- A ToolOutcome is not proof of an external effect, and nonempty model text is
  not proof of completion.
- User-originated `HUMAN` Tasks require their structured CompletionContract;
  an A2A Agent cannot complete them through a text continuation.
- Unknown roles, fields, states, grants, boundaries, and execution mechanisms
  fail closed.
- Interrupted uncertain effects are reconciled without automatic resend.
- Remote A2A requires explicit enablement and TLS. The user gateway never binds
  TCP.
- Runtime and credential directories reject symlink traversal, unexpected
  ownership, and broader-than-required permissions before the service opens
  provider credentials or the ledger.

## Verification

CI covers formatting, module consistency, builds, vet, lint, race tests,
bounded gateway fuzzing, vulnerability scanning, official A2A client tests,
architecture boundaries, deterministic release construction, dependency
licenses, corresponding-source offline tests, and packaged Linux binary smoke
checks. Recovery, restart, approval, completion review, provider confinement,
UID authentication, and structured artifact behavior have adversarial unit or
integration tests.

Real-provider testing, deployment, release publication/signing, and the first
reversible external effect remain separate approval gates.

Report suspected vulnerabilities using [SECURITY.md](../SECURITY.md).

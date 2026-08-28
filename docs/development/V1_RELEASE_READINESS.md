# V1 release readiness

This operational gate complements the architecture evidence in
[V1_ACCEPTANCE_STATUS.md](V1_ACCEPTANCE_STATUS.md). Passing the architecture
checklist does not authorize deployment or publication.

| Gate | Status | Evidence or next action |
|---|---|---|
| Architecture acceptance | PASS | All 26 current V1 requirements have linked automated evidence. |
| Required CI | PARTIAL | Active repository ruleset [`Protect main`](REPOSITORY_CHANGE_CONTROL.md) requires pull requests, a branch current with `main`, resolved review conversations, and the exact `CI verification (pull_request)`, `Dashboard frontend`, and `Release artifact verification` checks; it blocks deletion and force pushes. [PR #138](https://github.com/dominicnunez/agentos/pull/138) records a conforming exact-head merge. A controlled rejection was not performed because the available identity has an administrator bypass; the repository owner accepted that residual uncertainty and closed [Issue #127](https://github.com/dominicnunez/agentos/issues/127) as `not planned`. |
| Resumable initialization | PASS | System-default and current-account user modes, stable paths, setup checkpoints, provider discovery, and service installation are implemented. Release CI now begins from clean system and user state, resumes the persisted service-stage checkpoint, installs the packaged binary, and verifies the ready checkpoint and exact layout. |
| Controlled startup | PASS | The runtime refuses incomplete configuration and requires exactly one validated real provider. A2A remains disabled without a reviewed Agent registry. |
| Private user access | PASS | The HTTP-shaped user and approval boundaries run only over a mode-`0600` Unix socket and verify kernel peer UID. Unit tests cover `SO_PEERCRED`, client validation, and fail-closed ownership, type, mode, and symlink checks. The installed Linux acceptance test additionally proves an ordinary user owns and can use its installed gateway, root cannot impersonate that user at the HTTP boundary, a different UID cannot reach a root-owned system gateway, and identity-shaped headers cannot replace the kernel-derived durable actor. |
| Graceful shutdown | PASS | Process signals cancel the runtime context, stop intake, drain active requests within a bounded timeout, and close SQLite. |
| Input robustness | PASS | Strict size-limited decoding, authority-field rejection, adversarial tests, and bounded fuzzing cover local user and A2A work content. |
| Binary version identity | PASS | Both commands receive the repository `VERSION` at build time and are checked in CI. |
| Durable storage migration | PASS | SQLite application ID, ordered storage versions, atomic v1-to-v2 migration, exact layout and Event Contract binding, a frozen oldest-supported fixture, and fail-closed unsupported-state diagnostics are tested. |
| Backup and restore | PASS | Native online SQLite backup, integrity and schema verification, no-overwrite restore, and pointer-switch rollback are tested and documented. |
| Real model provider implementation | PASS | Codex subscription and official OpenAI Responses adapters use bounded model-only contracts. One durable SQLite gate uses deterministic reserve-aware selection for subscription, metered API, and local policies, then reserves and reconciles every normalization, planning, and Agent-execution call. Definite pre-send rejection releases quota; ambiguous provider contact remains conservatively charged. OpenAI requires current reviewed pricing, an exact dated snapshot, and encrypted service credential; rotating Codex credentials use authenticated encrypted state with a separately protected key. |
| First-provider setup | PARTIAL | Setup authenticates, enumerates available models, and verifies selected or manually entered models through non-inference provider metadata requests before readiness. Complete a bounded live setup test for each provider after the separate financial and sensitive-data approvals. |
| Structured user completion | PASS | Required fields and files are enforced by a durable CompletionContract; content-addressed artifacts are bounded, sniffed, origin-bound, and treated as untrusted. |
| Completion review | PASS | The configured local owner can approve, reject, or revise an exact fingerprinted model candidate through the private control boundary; Agent and A2A content cannot decide completion. |
| Approval control | PASS | The local owner sees ledger-sourced exact-effect context; acknowledgement, decision, expiry, and time-of-use fingerprint checks remain separate and fail closed. |
| Release artifact pipeline | READY | A read-only workflow builds Linux amd64 and arm64 artifacts twice, checks reproducibility, embeds license/source/dependency evidence, tests corresponding source offline, and emits checksums, SBOMs, and provenance. Nothing is published. |
| Event ledger integrity | PASS | Current storage chains exact event bytes with SHA-256 and verifies complete coverage at startup, backup, restore, and diagnostics. The chain is deliberately described as unsigned and not externally anchored. |
| Deterministic incident replay | PASS | The local-user boundary reconstructs one tenant-scoped public Work conversation from a single chain-verified SQLite snapshot, fails closed above 256 events, exposes payload hashes rather than content, and cannot execute or mutate Work. |
| Distribution license | PASS | Agent OS is `Apache-2.0`. Relicensing was authorized by initial rights holder Dominic Nunez while external code contributions remained closed. |
| Packaged Linux smoke test | PASS | CI unpacks the Linux amd64 archive, verifies both binary versions, and confirms diagnostics reject an uninitialized machine. |
| Five-minute governed workflow | PARTIAL | The user-facing [quickstart](../QUICKSTART.md) and Linux dashboard-loop test cover durable direction, reviewed work, bounded execution, result recovery, and organization projection. Validate the released binary on a clean user-mode Linux installation after provider live-test approval. |
| Full installed Linux test | PARTIAL | Release CI installs the packaged Linux amd64 binary into clean system and ordinary-user layouts on a disposable host; verifies exact paths, modes, ownership, resumable state, diagnostics, private UID access, header-impersonation resistance, restart continuity, and packaged backup/verify/restore; and proves the production binary rejects an unsupported provider before storage creation. System mode starts the generated systemd units with a production-encrypted credential. The hosted Ubuntu 24.04 runner has systemd 255, below Agent OS's documented systemd 258 minimum for complete per-user service credential support, so it exercises the installed user process offline but does not claim to validate user-unit credential decryption. A disposable systemd 258-or-newer host must start and restart the generated user unit before this row can pass. Live provider replacement remains under its separate approval gate. |
| Full real-provider live test | BLOCKED | Wait for requirements to be met and a bounded test plan with the required financial and sensitive-data-boundary approvals. |
| Published release artifacts | BLOCKED | Requires final software-licensing review, public-release approval, immutable tag, signed attestations, and checksummed asset upload. |
| Consequential effects | DISABLED | No production effect-writing adapter is enabled. Keep disabled through the initial release testing. |

## Next release sequence

1. Keep CI and architecture evidence green.
2. Keep the disposable installed-Linux lifecycle evidence green.
3. Test each real provider only after its separate approvals and prerequisites
   are recorded.
4. Perform final security and licensing reviews before approving and publishing
   an immutable release candidate.

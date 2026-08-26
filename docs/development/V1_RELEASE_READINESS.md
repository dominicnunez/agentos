# V1 release readiness

This operational gate complements the architecture evidence in
[V1_ACCEPTANCE_STATUS.md](V1_ACCEPTANCE_STATUS.md). Passing the architecture
checklist does not authorize deployment or publication.

| Gate | Status | Evidence or next action |
|---|---|---|
| Architecture acceptance | PASS | All 26 current V1 requirements have linked automated evidence. |
| Required CI | PASS | Build, vet, lint, race tests, vulnerability scanning, architecture checks, interoperability, and advisory dead-code analysis run on pushes and pull requests. |
| Resumable initialization | PARTIAL | System-default and current-account user modes, stable paths, setup checkpoints, provider discovery, and service installation are implemented. Complete a clean Linux VM installation test before a release candidate. |
| Controlled startup | PASS | The runtime refuses incomplete configuration and requires exactly one validated real provider. A2A remains disabled without a reviewed Agent registry. |
| Private user access | PARTIAL | The HTTP-shaped user and approval boundaries run only over a mode-`0600` Unix socket and verify kernel peer UID. [`TestLocalUserSocketDerivesOwnerFromKernelPeerCredentials`](../../cmd/agentos/local_listener_linux_test.go) exercises the real socket, `SO_PEERCRED`, HTTP context, client validation, and header-impersonation rejection in-process as the CI runner's ordinary Linux user; [`TestLocalHTTPClientRejectsUnsafeUserSocket`](../../cmd/agentos/local_listener_linux_test.go) covers fail-closed ownership, type, mode, and symlink rejection. Runtime and credential directories enforce exact ownership, modes, and symlink-safe paths; systemd services use umask `0077`. Complete installed process-level ordinary-user and root-owner tests on Linux. |
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
| Distribution license | PASS | Agent OS is `AGPL-3.0-only`, with Dominic Nunez as the initial copyright holder. |
| Packaged Linux smoke test | PASS | CI unpacks the Linux amd64 archive, verifies both binary versions, and confirms diagnostics reject an uninitialized machine. |
| Five-minute governed workflow | PARTIAL | The user-facing [quickstart](../QUICKSTART.md) and Linux dashboard-loop test cover durable direction, reviewed work, bounded execution, result recovery, and organization projection. Validate the released binary on a clean user-mode Linux installation after provider live-test approval. |
| Full installed Linux test | TODO | Exercise system and user setup, restart, private UID access, provider replacement, diagnostics, backup/restore, and service persistence on a disposable Linux host. |
| Full real-provider live test | BLOCKED | Wait for requirements to be met and a bounded test plan with the required financial and sensitive-data-boundary approvals. |
| Published release artifacts | BLOCKED | Requires final software-licensing review, public-release approval, immutable tag, signed attestations, and checksummed asset upload. |
| Consequential effects | DISABLED | No production effect-writing adapter is enabled. Keep disabled through the initial release testing. |

## Next release sequence

1. Keep CI and architecture evidence green.
2. Complete disposable Linux installation tests for system mode, user mode, and
   direct-root ownership.
3. Test each real provider only after its separate approvals and prerequisites
   are recorded.
4. Perform final security and licensing reviews before approving and publishing
   an immutable release candidate.

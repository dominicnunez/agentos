# V1 release readiness

This is the operational release gate for Agent OS V1. It complements the
architecture evidence in [V1_ACCEPTANCE_STATUS.md](V1_ACCEPTANCE_STATUS.md).
Passing the architecture checklist does not by itself authorize deployment.

| Gate | Status | Evidence or next action |
|---|---|---|
| Architecture acceptance | PASS | All 20 V1 requirements have linked automated evidence. |
| Required CI | PASS | Build, vet, lint, race tests, vulnerability scan, architecture checks, interoperability, and advisory dead-code analysis run on pushes and pull requests. |
| Controlled startup | PASS | At least one reviewed operator registry is required; remote exposure fails closed without explicit TLS configuration. |
| Graceful shutdown | PASS | Process signals cancel the runtime context, stop HTTP intake, drain active requests within a bounded timeout, and close SQLite. |
| Operator input robustness | PASS | Strict size-limited decoding, authority-field rejection, adversarial tests, and bounded fuzzing cover the Human and A2A shared work-content boundary. |
| Binary version identity | PASS | Both `agentos --version` and `agentos-recovery --version` are linked from the repository `VERSION` value and checked in CI. |
| Backup and restore | PASS | Native online SQLite backup, integrity/schema verification, no-overwrite restore, and pointer-switch rollback are tested and documented. |
| Real model provider implementation | PASS | The disabled-by-default Codex subscription adapter confines the reviewed SDK. The separate official OpenAI Responses adapter pins egress, disables provider tools and storage, bounds requests, and resolves a server-owned credential per call. Both record provider tokens with unknown cost. |
| Human completion review | PASS | Dedicated `REVIEWER` credentials approve, reject, or revise an exact fingerprinted model candidate. Agent/A2A and ordinary Human work paths cannot decide completion; recovery resumes durable decisions. |
| Fake-provider release gate | PASS | Process-level CI keeps a no-network model candidate pending across verified backup/restore, denies Agent and ordinary Operator review authority, rejects a stale fingerprint, and accepts plus idempotently replays the exact dedicated-reviewer decision. |
| Full real-provider live test | BLOCKED | Keep real providers disabled until `v1.0.0-rc.1` artifacts exist, the fake-provider gate above stays green, a bounded live-test plan is approved, and financial plus sensitive-data-boundary approvals are recorded. |
| Approval control | PASS | A separate disabled-by-default listener uses distinct reviewed credentials, exact organization/boundary/risk grants, strict non-language operations, complete ledger-sourced effect context, current-effect revalidation, and fail-closed expiry and fingerprint checks. Chat and A2A remain untrusted work content. |
| Release artifact pipeline | READY | A separate read-only workflow builds Linux amd64 and arm64 artifacts twice, checks byte reproducibility, verifies both commands on the native Linux runner, and emits checksums, per-target CycloneDX SBOMs, plus deterministic in-toto provenance. Nothing is uploaded or published. |
| Distribution license | BLOCKED | The repository has no `LICENSE` file. Record the legal distribution policy before publishing V1 binaries. |
| Published release artifacts | BLOCKED | Requires the distribution decision, explicit public-release approval, immutable `v1.0.0-rc.1` tag, GitHub-signed attestations, and checksummed asset upload. |
| Loopback pilot | PASS | CI restores a live disposable ledger, restarts the runtime, retrieves Human/A2A results, continues blocked work, and checks expiry and revocation. |
| Consequential effects | DISABLED | No production effect-writing adapter is enabled. Keep disabled through the initial pilot. |

## Release sequence

1. Keep the fake-provider loopback, recovery, and security release gate green.
2. Record the distribution license and approve publication of the immutable
   `v1.0.0-rc.1` artifacts and signed attestations.
3. Approve a bounded real-provider live-test plan and record its financial and
   sensitive-data-boundary decisions before enabling a provider.
4. Consider one reversible external effect only after the release candidate is
   stable and reconciliation evidence is demonstrated.

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
| Binary version identity | PASS | `agentos --version` is linked from the repository `VERSION` value and checked in CI. |
| Backup and restore | PASS | Native online SQLite backup, integrity/schema verification, no-overwrite restore, and pointer-switch rollback are tested and documented. |
| Real model provider | PASS | The disabled-by-default Codex subscription adapter pins the reviewed SDK, isolates process and turn state, denies approvals and side effects, bounds execution, and records provider token usage with unknown cost. |
| Approval control | BLOCKED | Requires a separately authenticated exact-effect control and an approved trusted-control design. Chat remains untrusted work content. |
| Release artifacts | TODO | Produce pinned cross-platform binaries, checksums, SBOMs, and build provenance. |
| Loopback pilot | PASS | CI restores a live disposable ledger, restarts the runtime, retrieves Human/A2A results, continues blocked work, and checks expiry and revocation. |
| Consequential effects | DISABLED | No production effect-writing adapter is enabled. Keep disabled through the initial pilot. |

## Release sequence

1. Produce reproducible `v1.0.0-rc.1` artifacts.
2. Review the trusted approval control before implementing it.
3. Consider one reversible external effect only after the release candidate is
   stable and reconciliation evidence is demonstrated.

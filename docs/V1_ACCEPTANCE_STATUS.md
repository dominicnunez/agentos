# V1 acceptance status

This matrix tracks the normative checklist in
`docs/handoff/docs/08_IMPLEMENTATION_ROADMAP_AND_ACCEPTANCE.md`. “Covered” means
the current bounded behavior has an automated regression test. “Partial” means
only a supporting seam or narrower case exists. It is not a production-readiness
claim.

| # | Acceptance criterion | Status | Current evidence / remaining gap |
|---:|---|---|---|
| 1 | Team/Agent identity survives restart | Covered | `TestDurableObjectsSurviveRestartAndRebuildFromEvents` reopens SQLite and compares both the stored projection and event replay. |
| 2 | Lateral message without planner relay | Open | `MESSAGE` is an allowed EventDraft type; inbox/routing behavior is not implemented. |
| 3 | Recipient sees message at an action boundary | Open | Action-boundary inbox surfacing is not implemented. |
| 4 | Failed persistence never exposes a message | Open | Task availability has this guarantee; the equivalent recipient-inbox failure test is still required. |
| 5 | Blocked worker returns control without authority expansion | Partial | Unsupported execution becomes a durable `TASK_BLOCKED`; parent remediation and adversarial self-escalation coverage remain. |
| 6 | Child assignment has no unintended positive capability inheritance | Partial | Exact lease matching rejects broader resources; explicit child-assignment coverage remains. |
| 7 | Human-required action waits for a decision | Open | Approval/effect records exist, but durable pending wait/resume orchestration is not implemented. |
| 8 | Acknowledgement cannot approve | Open | Acknowledgement/decision lifecycle behavior is not implemented. |
| 9 | Freeze/revoke prevents time-of-use action | Partial | `TestCheckRequiresExactUnfrozenLease` covers freeze denial; an integrated effect-time revoke race remains. |
| 10 | Agent text cannot forge trusted state | Partial | `TestAgentCannotMintTrustedControlEvent` covers control-event minting; spoofed identity/completion/attestation cases remain. |
| 11 | Candidate completion cannot bypass Completion Engine | Covered | Vertical-slice event ordering and `TestEvaluateRequiresVerifiedSuccess` require verified structured outcome before terminal completion. |
| 12 | Duplicate delivery cannot duplicate a consequential effect | Partial | Single-use approval consumption blocks reuse; full idempotency/reconciliation after uncertain attempts remains. |
| 13 | Restart preserves pending work, approval, and inbox | Partial | Pending/blocked tasks and DAG dependencies survive and recover; approvals and inbox availability still need restart tests/runtime paths. |
| 14 | Complete operational telemetry | Partial | Inference usage snapshots exist; unified per-run outcome/cost/time/message/block/retry/human telemetry does not. |
| 15 | Accurate manifest for every model execution | Partial | The fake AgentExecution persists a manifest and wire fields round-trip; exact materialized-reference assertions remain. |
| 16 | ToolOutcome failure cannot hide behind success text | Covered | Completion requires both successful status and verified postcondition. |
| 17 | Safe deterministic recovery precedes cognitive recovery | Covered | `TestRecoverRetriesDeterministicWorkAndBlocksUncertainAgentWork` retries deterministic work and refuses blind adaptive replay. |
| 18 | Hermes discovers, submits, and continues through A2A | Partial | Agent Card, authenticated submission, status, and input persistence are tested; pinned real Hermes interoperability and post-input completion remain. |
| 19 | A2A identity cannot bypass capability/human approval | Partial | Submission/status/input capabilities and organization binding fail closed; protected-effect integration remains. |
| 20 | Protected effects use exact approval and durable obligation/reconciliation | Partial | Exact fingerprint and persist-before-effect transitions are tested; restart reconciliation and production adapters are deliberately not enabled. |

V1 remains incomplete until every row is covered by the required runtime path and
adversarial regression test.

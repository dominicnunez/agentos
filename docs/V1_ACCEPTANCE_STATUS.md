# V1 acceptance status

This matrix tracks the normative checklist in
`docs/handoff/docs/08_IMPLEMENTATION_ROADMAP_AND_ACCEPTANCE.md`. “Covered” means
the current bounded behavior has an automated regression test. “Partial” means
only a supporting seam or narrower case exists. It is not a production-readiness
claim.

| # | Acceptance criterion | Status | Current evidence / remaining gap |
|---:|---|---|---|
| 1 | Team/Agent identity survives restart | Covered | `TestDurableObjectsSurviveRestartAndRebuildFromEvents` reopens SQLite and compares both the stored projection and event replay. |
| 2 | Lateral message without planner relay | Covered | `TestLateralMessagesSurviveRestartAndSurfaceAtAgentActionBoundary` sends Agent-, Team-, and Task-addressed EventDrafts directly through the Event Gateway and durable inbox projection. |
| 3 | Recipient sees message at an action boundary | Covered | The same restart test proves chronologically ordered messages enter the next AgentExecution context and exact manifest event refs before the fake model call. |
| 4 | Failed persistence never exposes a message | Covered | `TestInboxProjectionFailureRollsBackMessage` forces the inbox write to fail and verifies the transaction leaves neither a ledger event nor inbox availability. |
| 5 | Blocked worker returns control without authority expansion | Covered | `TestBlockedChildReturnsControlToParentWithoutAuthorityExpansion` proves unsupported child work becomes a typed, durably addressed `TASK_BLOCKED` contract in the parent Task inbox while emitting no capability or approval transition. |
| 6 | Child assignment has no unintended positive capability inheritance | Covered | `TestChildAssignmentDoesNotInheritParentCapability` proves that assigning a child Task to the same Agent does not transfer the parent Task's lease; only a lease explicitly originating from the child authorizes its action. |
| 7 | Human-required action waits for a decision | Covered | `TestProtectedEffectWaitsAcrossRestartForExactAuthorizedDecision` proves a prepared effect remains unavailable through notification, acknowledgement, restart, unauthorized identity, and mismatched fingerprint until the exact authorized decision is durable. |
| 8 | Acknowledgement cannot approve | Covered | The same lifecycle test persists `APPROVAL_ACKNOWLEDGED`, verifies no decision timestamp exists, and confirms the effect adapter remains unreachable. |
| 9 | Freeze/revoke prevents time-of-use action | Covered | `TestFreezeAndRevokePreventEffectAtTimeOfUse` proves durable freeze and revocation prevent `ATTEMPTED`; `TestApprovalExpiryIsRecheckedInsideAttemptTransaction` proves stale preflight approval state cannot bypass transaction-time expiry. Both keep the adapter unreachable. |
| 10 | Agent text cannot forge trusted state | Covered | `TestAgentCannotMintTrustedStateEvents` rejects identity, approval, authority, completion, and runtime-attestation event types before persistence; `TestMessageEnvelopeUsesAuthenticatedIdentity` proves forged control text and metadata remain content under a runtime-stamped envelope; `TestCandidateCompletionCannotMintVerifiedCompletion` preserves the candidate/verified boundary. |
| 11 | Candidate completion cannot bypass Completion Engine | Covered | Vertical-slice event ordering and `TestEvaluateRequiresVerifiedSuccess` require verified structured outcome before terminal completion. |
| 12 | Duplicate delivery cannot duplicate a consequential effect | Covered | `TestSingleUseApprovalIsConsumedBeforeAdapter` proves confirmed-effect redelivery is idempotent and a single-use decision cannot authorize a second effect; the ledger atomically couples consumption with `ATTEMPTED`. |
| 13 | Restart preserves pending work, approval, and inbox | Covered | Runtime recovery tests preserve pending/blocked tasks and dependencies, inbox tests preserve availability/observation, and approval tests reopen SQLite with pending or acknowledged decisions and replay-complete effect obligations intact. |
| 14 | Complete operational telemetry | Partial | Inference usage snapshots exist; unified per-run outcome/cost/time/message/block/retry/human telemetry does not. |
| 15 | Accurate manifest for every model execution | Covered | Every current AgentExecution persists a manifest before inference; the lateral-message test asserts the exact materialized event references and matching context. |
| 16 | ToolOutcome failure cannot hide behind success text | Covered | Completion requires both successful status and verified postcondition. |
| 17 | Safe deterministic recovery precedes cognitive recovery | Covered | `TestRecoverRetriesDeterministicWorkAndBlocksUncertainAgentWork` retries deterministic work and refuses blind adaptive replay. |
| 18 | Hermes discovers, submits, and continues through A2A | Partial | Agent Card, authenticated submission, status, and input persistence are tested; `TestA2AStatusAndInputContinuation` proves authorized input deterministically resumes and completes a HUMAN task through the Completion Engine without duplicate completion. Pinned real Hermes interoperability and result Artifact mapping remain. |
| 19 | A2A identity cannot bypass capability/human approval | Partial | Submission/status/input capabilities and organization binding fail closed; protected-effect integration remains. |
| 20 | Protected effects use exact approval and durable obligation/reconciliation | Partial | Exact durable approval is reloaded and revalidated in the attempt transaction, with replay-complete persist-before-effect transitions and atomic single-use consumption/attempt; automated reconciliation and production adapters remain deliberately disabled. |

V1 remains incomplete until every row is covered by the required runtime path and
adversarial regression test.

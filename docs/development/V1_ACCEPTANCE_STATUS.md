# V1 acceptance status

This compact index maps the normative checklist in
`docs/handoff/docs/08_IMPLEMENTATION_ROADMAP_AND_ACCEPTANCE.md` to current
automated evidence. A checked row is not a production-readiness or deployment
authorization claim.

| # | Requirement | Evidence |
|---:|---|---|
| 1 | Durable identity | PASS — [`TestDurableObjectsSurviveRestartAndRebuildFromEvents`](../../internal/projections/repository_test.go) |
| 2 | Direct lateral messaging | PASS — [`TestLateralMessagesAtActionBoundary`](../../internal/app/service_test.go) |
| 3 | Action-boundary delivery | PASS — [`TestLateralMessagesAtActionBoundary`](../../internal/app/service_test.go) |
| 4 | Atomic message delivery | PASS — [`TestMessageRollbackOnInboxFailure`](../../internal/ledger/sqlite_test.go) |
| 5 | Blocked work returns control | PASS — [`TestBlockedChildReturnsToParent`](../../internal/app/service_test.go) |
| 6 | No implicit authority inheritance | PASS — [`TestChildAssignmentDoesNotInheritParentCapability`](../../internal/authority/authority_test.go) |
| 7 | Durable approval wait | PASS — [`TestProtectedEffectWaitsAcrossRestartForExactAuthorizedDecision`](../../internal/approvals/service_test.go) |
| 8 | Acknowledgement is not approval | PASS — [`TestProtectedEffectWaitsAcrossRestartForExactAuthorizedDecision`](../../internal/approvals/service_test.go) |
| 9 | Time-of-use revocation | PASS — [`TestRevocationBlocksEffect`](../../internal/effects/coordinator_test.go), [`TestApprovalExpiryAtAttempt`](../../internal/effects/coordinator_test.go) |
| 10 | Untrusted text stays untrusted | PASS — [`TestAgentCannotMintTrustedStateEvents`](../../internal/events/events_test.go), [`TestMessageEnvelopeUsesAuthenticatedIdentity`](../../internal/events/events_test.go), [`TestCandidateCompletionCannotMintVerifiedCompletion`](../../internal/events/events_test.go) |
| 11 | Verified completion only | PASS — [`TestVerifierOwnsPostconditionTrust`](../../internal/completion/verifier_test.go), [`TestEvaluateRequiresVerifiedSuccess`](../../internal/completion/engine_test.go) |
| 12 | Consequential effects are idempotent | PASS — [`TestSingleUseApprovalBeforeEffect`](../../internal/effects/coordinator_test.go) |
| 13 | Restart continuity | PASS — [`TestRecoverExecutesPersistedPendingWorkAndPreservesIdentity`](../../internal/app/service_test.go), [`TestProtectedEffectWaitsAcrossRestartForExactAuthorizedDecision`](../../internal/approvals/service_test.go), [`TestMessageInboxSurvivesReopenAndObservation`](../../internal/ledger/sqlite_test.go) |
| 14 | Complete run telemetry | PASS — [`TestProjectBuildsCompleteReplayableRunSummary`](../../internal/telemetry/telemetry_test.go), [`TestRunTelemetryCoversDAG`](../../internal/app/service_test.go) |
| 15 | Model executions have manifests | PASS — [`TestAgentExecutionUsesFakeAdapter`](../../internal/app/service_test.go), [`TestAgentExecutionReturnsSeparateUsageContract`](../../internal/execution/execution_test.go) |
| 16 | Tool failures remain failures | PASS — [`TestRejectedRunRecordsTelemetryAndFailsGoal`](../../internal/app/service_test.go), [`TestEvaluateRequiresVerifiedSuccess`](../../internal/completion/engine_test.go) |
| 17 | Deterministic-first recovery | PASS — [`TestRecoveryIsDeterministicFirst`](../../internal/app/service_test.go) |
| 18 | Authorized A2A operation | PASS — live A2A v1.0 CI, [`TestA2ASendGetAndContinueUseV1TaskContracts`](../../internal/gateway/a2a_test.go), [`TestOfficialA2AClientUsesDurableAgentOSState`](../../internal/gateway/a2a_official_test.go) |
| 19 | A2A cannot approve effects | PASS — [`TestA2ACannotApproveEffects`](../../internal/gateway/a2a_test.go) |
| 20 | Durable effect recovery | PASS — [`TestEffectSuccessNeedsEvidence`](../../internal/effects/coordinator_test.go), [`TestRecoveryConfirmsAttemptedEffectAfterRestartWithoutResend`](../../internal/effects/reconciliation_test.go) |

## Evidence scope

The verified-completion and restart-continuity rows additionally include
`TestHumanReviewerFinalizesExactModelCandidate`,
`TestDedicatedReviewerCanFinalizeButOperatorCannot`, and
`TestRecoveryFinishesDurableCompletionReviewDecision`. These tests bind human
judgment to exact evidence, deny ordinary Operator and Agent authority, retain
unchecked postcondition status, and resume a persisted review decision after
restart.

- Identity and inbox tests reopen SQLite and compare stored state with replay.
- Approval and effect tests cover exact fingerprints, acknowledgement versus
  decision, expiry, freeze, revocation, single use, attempt-time authorization,
  crash uncertainty, and evidence-backed reconciliation without replay.
- Telemetry tests cover every task in a DAG, execution mechanism, timing,
  provider usage, cost, tool calls, messages, blocks, retries, interventions,
  denials, and completion evidence.
- Operator tests cover authenticated intake, replay, capability roles,
  visibility, lifecycle and request limits, tenant isolation, and rejection of
  authority-shaped content.

Production consequential-effect adapters remain disabled. A security finding
that weakens a row returns it to partial until the runtime path and adversarial
regression are corrected.

# V1 acceptance status

This compact index maps the normative checklist in
`docs/handoff/docs/08_IMPLEMENTATION_ROADMAP_AND_ACCEPTANCE.md` to current
automated evidence. A passing row is architecture evidence, not authorization
to deploy or publish.

| # | Requirement | Evidence |
|---:|---|---|
| 1 | Durable identity | PASS - [`TestDurableObjectsSurviveRestartAndRebuildFromEvents`](../../internal/projections/repository_test.go), [`TestMissionGoalWorkHierarchyIsTenantBounded`](../../internal/projections/repository_test.go), [`TestHierarchyRevisionsPreserveIdentityAndDirectionBoundaries`](../../internal/projections/repository_test.go), [`TestSubmitBindsOnlyActiveGoalFromAcceptedIntent`](../../internal/app/service_test.go), [`TestCompletedWorkDrivesEvidenceBackedGoalProgress`](../../internal/app/service_test.go), [`TestSnapshotRejectsMalformedDurableRoster`](../../internal/projections/repository_test.go), [`TestRosterRevisionsPreserveConfigurationAndAgentIdentity`](../../internal/projections/repository_test.go), [`TestSnapshotRejectsMalformedPinnedAgentConfiguration`](../../internal/projections/repository_test.go), [`TestAgentIdentitySurvivesExecutionProfileUpdate`](../../internal/app/service_test.go) |
| 2 | Direct lateral messaging | PASS - [`TestLateralMessagesAtActionBoundary`](../../internal/app/service_test.go) |
| 3 | Action-boundary delivery | PASS - [`TestLateralMessagesAtActionBoundary`](../../internal/app/service_test.go) |
| 4 | Atomic message delivery | PASS - [`TestMessageRollbackOnInboxFailure`](../../internal/ledger/sqlite_test.go) |
| 5 | Blocked work returns control | PASS - [`TestBlockedChildReturnsToParent`](../../internal/app/service_test.go) |
| 6 | No implicit authority inheritance | PASS - [`TestChildAssignmentDoesNotInheritParentCapability`](../../internal/authority/authority_test.go), [`TestSelectUsesStableEligibleAgentWithoutGrantingRequirements`](../../internal/assignment/selector_test.go), [`TestDispatchFailsClosedWhenDurableRosterEligibilityChanges`](../../internal/app/service_test.go) |
| 7 | Durable approval wait | PASS - [`TestProtectedEffectWaitsAcrossRestartForExactAuthorizedDecision`](../../internal/approvals/service_test.go) |
| 8 | Acknowledgement is not approval | PASS - [`TestProtectedEffectWaitsAcrossRestartForExactAuthorizedDecision`](../../internal/approvals/service_test.go) |
| 9 | Time-of-use revocation | PASS - [`TestRevocationBlocksEffect`](../../internal/effects/coordinator_test.go), [`TestApprovalExpiryAtAttempt`](../../internal/effects/coordinator_test.go) |
| 10 | Untrusted text stays untrusted | PASS - [`TestAgentCannotMintTrustedStateEvents`](../../internal/events/events_test.go), [`TestMessageEnvelopeUsesAuthenticatedIdentity`](../../internal/events/events_test.go), [`TestCandidateCompletionCannotMintVerifiedCompletion`](../../internal/events/events_test.go) |
| 11 | Verified completion only | PASS - [`TestVerifierOwnsPostconditionTrust`](../../internal/completion/verifier_test.go), [`TestEvaluateRequiresVerifiedSuccess`](../../internal/completion/engine_test.go), [`TestWorkEvidenceBindsAcceptedIntentPlanAndVerifiedTasks`](../../internal/completion/work_test.go), [`TestGoalProgressEvaluationUsesExactCompletedWorkCriteria`](../../internal/events/goal_progress_test.go), [`TestCompletedWorkDrivesEvidenceBackedGoalProgress`](../../internal/app/service_test.go), [`TestVerticalSlice`](../../internal/app/service_test.go) |
| 12 | Consequential effects are idempotent | PASS - [`TestSingleUseApprovalBeforeEffect`](../../internal/effects/coordinator_test.go) |
| 13 | Restart continuity | PASS - [`TestRecoverExecutesPersistedPendingWorkAndPreservesIdentity`](../../internal/app/service_test.go), [`TestRecoverResumesOnlyRevalidatedAssignmentBlock`](../../internal/app/service_test.go), [`TestCompletedWorkDrivesEvidenceBackedGoalProgress`](../../internal/app/service_test.go), [`TestProtectedEffectWaitsAcrossRestartForExactAuthorizedDecision`](../../internal/approvals/service_test.go), [`TestMessageInboxSurvivesReopenAndObservation`](../../internal/ledger/sqlite_test.go) |
| 14 | Complete run telemetry | PASS - [`TestProjectBuildsCompleteReplayableRunSummary`](../../internal/telemetry/telemetry_test.go), [`TestRunTelemetryCoversDAG`](../../internal/app/service_test.go) |
| 15 | Model executions have manifests | PASS - [`TestAgentExecutionManifestUsesConfiguredModelDescriptor`](../../internal/app/service_test.go), [`TestAgentExecutionReturnsSeparateUsageContract`](../../internal/execution/execution_test.go) |
| 16 | Tool failures remain failures | PASS - [`TestAgentExecutionRejectsMismatchedProviderUsageIdentity`](../../internal/execution/execution_test.go), [`TestUnavailableDeterministicWorkIsRejectedBeforeExecution`](../../internal/app/service_test.go), [`TestEvaluateRequiresVerifiedSuccess`](../../internal/completion/engine_test.go) |
| 17 | Deterministic-first recovery | PASS - [`TestRecoveryIsDeterministicFirst`](../../internal/app/service_test.go) |
| 18 | Authorized A2A operation | PASS - [`TestA2ASendGetAndContinueUseV1TaskContracts`](../../internal/gateway/a2a_test.go), [`TestOfficialA2AClientUsesDurableAgentOSState`](../../internal/gateway/a2a_official_test.go) |
| 19 | A2A cannot approve effects | PASS - [`TestA2ACannotApproveEffects`](../../internal/gateway/a2a_test.go) |
| 20 | Durable effect recovery | PASS - [`TestEffectSuccessNeedsEvidence`](../../internal/effects/coordinator_test.go), [`TestRecoveryConfirmsAttemptedEffectAfterRestartWithoutResend`](../../internal/effects/reconciliation_test.go) |
| 21 | Confirmed semantic intake | PASS - [`TestModelNormalizerRequiresCompleteStrictIntent`](../../internal/intake/normalizer_test.go), [`TestModelNormalizerBindsOnlyExplicitGoalReference`](../../internal/intake/normalizer_test.go), [`TestInvalidGoalCannotStrandIntentConfirmation`](../../internal/intake/service_test.go), [`TestIntentNormalizationManifestsModelUseAndReplaysWithoutInference`](../../internal/intake/service_test.go), [`TestIntentNormalizationRetryCompletesAnInterruptedDraftOnce`](../../internal/intake/service_test.go), [`TestIntentConversationLimitsRejectBeforeAppending`](../../internal/intake/service_test.go), [`TestIntentConfirmationCannotRacePastANewerMessage`](../../internal/intake/service_test.go), [`TestUnconfirmedIntentResumesAfterServiceRestart`](../../internal/intake/service_test.go), [`TestOfficialA2AClientUsesDurableAgentOSState`](../../internal/gateway/a2a_official_test.go) |
| 22 | Bounded durable planning | PASS - [`TestModelPlannerRejectsUntrustedGraphExpansion`](../../internal/planning/planner_test.go), [`TestAcceptedIntentBecomesDurableTaskDAGWithDependencyEvidence`](../../internal/app/service_test.go), [`TestTaskPersistenceFailurePreventsExecutionVisibility`](../../internal/app/service_test.go), [`TestProjectionBatchRollsBackCompleteTaskGraph`](../../internal/ledger/sqlite_test.go), [`TestChildTaskIsNotExternallyAddressable`](../../internal/ledger/sqlite_test.go) |
| 23 | Event-coupled projection authority | PASS - [`TestProjectionAdmissionBindsExactEventBoundary`](../../internal/events/events_test.go), [`TestGenericTrustedPublicationCannotMintProjectionAuthority`](../../internal/events/events_test.go), [`TestTrustedPublicationRequiresObjectPayload`](../../internal/events/events_test.go), [`TestProjectionRecordLoadsRequireExactEventAdmission`](../../internal/ledger/sqlite_test.go), [`TestGenericSQLiteAppendCannotMintProjectionAuthority`](../../internal/ledger/sqlite_test.go), [`TestProjectionWriterRejectsInvalidBoundaryBeforePersistence`](../../internal/ledger/sqlite_test.go), [`TestProjectionWriterRejectsMalformedSealedJSONBeforePersistence`](../../internal/ledger/sqlite_test.go), [`TestOpenRejectsNonemptyPreAdmissionProjectionSchema`](../../internal/ledger/sqlite_test.go), [`TestRebuildRejectsProjectionShapedOrdinaryEvents`](../../internal/projections/repository_test.go), [`TestFullAuditRejectsProjectionEventWithoutMaterializedRecord`](../../internal/projections/repository_test.go), [`TestRecoveryRejectsProjectionAdmissionCorruption`](../../internal/ledger/recovery/sqlite_test.go), [`TestRecoveryRejectsProjectionOrganizationMismatch`](../../internal/ledger/recovery/sqlite_test.go), [`TestRecoveryRejectsMissingProjectionOrganization`](../../internal/ledger/recovery/sqlite_test.go) |

## Evidence scope

Completion and restart coverage also binds user judgment to exact evidence,
denies Agent authority, retains unchecked postcondition status, and resumes a
persisted review decision after restart.

- Identity and inbox tests reopen SQLite and compare stored state with replay.
- Projection-admission tests bind each materialized organizational record to
  one exact runtime-owned Event Contract and reject generic publication,
  copied seals, non-object, malformed, or duplicate JSON, invalid lifecycle or
  organization boundaries, corrupt linkage, permissive pre-release migration,
  event-only replay forgery, and backup/restore drift.
- Approval and effect tests cover exact fingerprints, acknowledgement versus
  decision, expiry, freeze, revocation, single use, attempt-time authorization,
  crash uncertainty, and evidence-backed reconciliation without replay.
- Telemetry tests cover every Task in a DAG, execution mechanism, timing,
  provider usage, cost, tool calls, messages, blocks, retries, interventions,
  denials, and completion evidence.
- Gateway tests cover UID-authenticated local intake, authenticated A2A intake,
  replay, capability roles, visibility, lifecycle and request limits, tenant
  isolation, and rejection of authority-shaped content.
- Planning and assignment tests cover no-inference exact work, closed schemas, task ceilings,
  unsupported execution, unknown dependencies, cycles, atomic graph admission,
  durable replay, stable same-organization Agent selection, exact profile and
  capability-prerequisite checks, dependency evidence selection, and A2A root
  isolation.

## Operational supplement

| Capability | Current evidence |
|---|---|
| Stable Linux paths and resumable setup | Unit-tested configuration and strict checkpoint loading; live Linux installation remains a release-readiness test |
| Setup caller is owner | `sudo` origin verification, direct-root support, exact UID socket checks, and fail-closed configuration validation |
| Provider required before ready | Typed Codex subscription and OpenAI API setup with discovery, connection tests, and production rejection of the fake adapter |
| Private user access | Mode-`0600` Unix socket plus kernel `SO_PEERCRED`; no local bearer registry or TCP listener |
| Runtime and credential confinement | Exact ownership and modes, symlink-safe path validation, systemd private credential imports, and service umask `0077` fail closed before runtime state is opened |
| Required user deliverables | Durable structured fields and content-addressed artifact evidence; plain-text self-report cannot complete the Task |
| Terminal safety | Control and direction-format characters are stripped from untrusted console text |

Production consequential-effect adapters remain disabled. A security finding
that weakens a row returns it to partial until the runtime path and adversarial
regression are corrected.

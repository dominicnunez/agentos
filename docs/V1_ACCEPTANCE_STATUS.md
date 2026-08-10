# V1 acceptance status

This compact index maps the normative checklist in
`docs/handoff/docs/08_IMPLEMENTATION_ROADMAP_AND_ACCEPTANCE.md` to current
automated evidence. A checked row is not a production-readiness or deployment
authorization claim.

| # | Requirement | Evidence |
|---:|---|---|
| 1 | Durable identity | ✅ `projections: TestIdentitySurvivesRestart` |
| 2 | Direct lateral messaging | ✅ `app: TestLateralMessagesAtActionBoundary` |
| 3 | Action-boundary delivery | ✅ `app: TestLateralMessagesAtActionBoundary` |
| 4 | Atomic message delivery | ✅ `ledger: TestMessageRollbackOnInboxFailure` |
| 5 | Blocked work returns control | ✅ `app: TestBlockedChildReturnsToParent` |
| 6 | No implicit authority inheritance | ✅ `authority: TestChildGetsNoParentAuthority` |
| 7 | Durable approval wait | ✅ `approvals: TestApprovalWaitsAcrossRestart` |
| 8 | Acknowledgement is not approval | ✅ `approvals: TestApprovalWaitsAcrossRestart` |
| 9 | Time-of-use revocation | ✅ `effects: TestRevocationBlocksEffect`, `TestApprovalExpiryAtAttempt` |
| 10 | Untrusted text stays untrusted | ✅ `events: TestAgentCannotForgeState`, `TestEnvelopeUsesAuthenticatedIdentity`, `TestCandidateIsNotVerified` |
| 11 | Verified completion only | ✅ `completion: TestVerifierOwnsPostconditionTrust`, `TestCompletionRequiresVerification` |
| 12 | Consequential effects are idempotent | ✅ `effects: TestSingleUseApprovalBeforeEffect` |
| 13 | Restart continuity | ✅ recovery, approval, and inbox restart suites |
| 14 | Complete run telemetry | ✅ `telemetry: TestRunTelemetryIsComplete`; `app: TestRunTelemetryCoversDAG` |
| 15 | Model executions have manifests | ✅ action-boundary manifest assertions |
| 16 | Tool failures remain failures | ✅ completion postcondition tests |
| 17 | Deterministic-first recovery | ✅ `app: TestRecoveryIsDeterministicFirst` |
| 18 | Authorized A2A operation | ✅ live A2A v1.0 CI and gateway conformance suite |
| 19 | A2A cannot approve effects | ✅ `gateway: TestA2ACannotApproveEffects` |
| 20 | Durable effect recovery | ✅ `effects: TestEffectSuccessNeedsEvidence` and reconciliation recovery suite |

## Evidence scope

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

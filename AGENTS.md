# Agent OS repository guidance

Agent OS is a Go modular monolith for operating persistent AI-assisted organizations.

## Authoritative sources

- Start with [`docs/handoff/QUICK_START.md`](docs/handoff/QUICK_START.md).
- Treat [`docs/handoff/IMPLEMENTATION_SCOPE.yaml`](docs/handoff/IMPLEMENTATION_SCOPE.yaml) as the scope authority.
- Use [`docs/handoff/docs/29_V1_BUILD_CONTRACT.md`](docs/handoff/docs/29_V1_BUILD_CONTRACT.md) for implementation requirements.
- Consult the remaining preserved handoff only when the current work needs its detail.
- Preserve `docs/handoff/` as the immutable architecture source. Publish any
  strengthened active runtime contract outside that tree instead of silently
  rewriting the handoff snapshot.

## Project-wide boundaries

- Internal coordination uses runtime-owned Event Contracts over one authoritative ledger; model-generated content is untrusted.
- Actual work determines the Task DAG and execution structure. Use model inference only where adaptive intelligence is justified.
- Authority and completion fail closed: workers cannot expand their own capabilities or certify their own completion.
- A2A is an external operator boundary, not internal IPC or implicit administrative authority.
- Keep deferred architecture deferred unless a human explicitly promotes it from the handoff scope.

Run the appropriate repository checks before a review push; hooks and CI provide
independent enforcement rather than replacing local verification.

## Names in code

Prefer familiar, plain-language names over technical jargon for code identifiers.
Aim for 30 characters or fewer. Use a longer or technical name when needed for
clarity, accuracy, or an established interface.

## Scope security work clearly

Follow OpenAI's [additional safety-check guidance](https://help.openai.com/en/articles/20001326-additional-safety-checks-for-biological-and-cybersecurity-requests-in-chatgpt-codex-and-the-api):

- From the outset, state the authorized defensive outcome: identifying,
  preventing, or remediating a security issue in Agent OS.
- Keep each request focused on that outcome and include only relevant context.
  Retain the technical details needed to diagnose, fix, and verify the issue;
  omit exploit details unnecessary to that work.
- Check that the request complies with OpenAI's Usage Policies. Clear scope
  should explain the legitimate work, not conceal its purpose or effects.
- Changing wording does not change whether a request is allowed or guarantee a
  response. Do not disguise a request or attempt to bypass safeguards.

## Engineering quality workflow

1. Before editing, identify the intended behavior and scope. Trace affected
   entry points, alternate compositions, durable writers and readers, output
   consumers, and retry, recovery, and replay paths.
2. Maintain a compact local evidence matrix for relevant identity, missing or
   invalid metadata, cancellation, authority failure, timing between writes,
   and crash boundaries. Distinguish proven, contradicted, missing, and
   inapplicable coverage; a passing package suite is not exhaustive evidence.
3. Enforce invariants at their authoritative boundary before dispatch or durable
   publication. Validate relationships in both directions and cover supported
   direct composition as well as the production setup.
4. Implement a complete, cohesive invariant across its callers. Prefer an
   existing shared boundary that owns the rule over multiple partial fixes.
5. Exercise realistic failure paths through actual entry points and durable
   state. Verify forbidden calls and writes do not occur, necessary evidence
   survives, retry and restart behave correctly, and other tenants are unaffected.
   Test doubles must implement the interfaces production calls. Where feasible,
   verify regressions fail for the intended defect against the prior behavior.
6. Inspect query and loop costs when history or candidate counts grow. Avoid
   repeated full-history validation per item without a demonstrated need.
7. Before each review push, audit the whole changed invariant and diff, address
   confirmed related gaps, and run the required checks. A review finding triggers
   a class-wide audit of sibling callers and failure modes before the next push.
   Record concrete evidence; external review is an independent gate.
8. Keep PRs cohesive and bounded without omitting necessary callers merely to
   reduce diff size. Track independent confirmed defects under the issue policy
   below and resolve them before proceeding to later goal parts.
9. Keep `docs/THREAT_MODEL.md` aligned with code changes affecting architecture,
   trust boundaries, attack surfaces, security controls, prerequisites, residual
   risks, or severity. Update it in the same PR as relevant code and keep its
   stated baseline accurate. Distinguish implemented controls from incomplete
   controls and future prerequisites. Preserve the supplied draft's four numbered
   sections: Overview; Threat model, Trust boundaries and assumptions; Attack
   surface, mitigations and attacker stories; and Criticality calibration. Retain
   its subsection structure, explanatory prose, and severity lists when updating
   the corresponding content.

## Standing user instructions and goal continuity

- These instructions persist across tasks and context compaction. Later explicit
  user decisions supersede earlier conflicting instructions; do not revive a
  superseded policy from an old handoff or task summary.
- Replace contradictory active instructions at their source when adopting a
  new workflow. Do not stack competing overrides. Clearly label historical
  records; preserve immutable handoff evidence.
- Keep user-facing messages minimal. Report meaningful outcomes, blockers, and
  decisions rather than routine local commits or repeated compliance statements.
- Use the project Markdown documents to guide task work and maintain the
  user-requested goal and evidence of remaining work. Completing one PR does not
  complete the broader goal; do not silently narrow it to the current PR.
- Incorporate `multi-llm-provider-architecture-prompt.md` into the provider goal.
  Agent OS should support all providers Hermes offers where feasible, and use
  multiple configured providers/models for different agent tasks according to
  what the user has available. Do not constrain an organization to one provider.
  Record feasibility limits and unfinished coverage explicitly.
- Assess `AGENT_OS_ARCHITECTURAL_IMPROVEMENTS.md` for merit after the current
  task/PR, preserving the architecture and scope boundaries above. Treat
  proposals as proposals to evaluate, not automatically approved implementation.
- These project planning files may be supplied in the parent workspace rather
  than tracked in a checkout. Consult the supplied originals and maintain
  traceable status; do not modify the immutable `docs/handoff/` snapshot.

## Commits, PRs, and review sequencing

- Make local commits at logical, verified checkpoints instead of accumulating
  all work until an entire issue or goal is complete. A checkpoint commit does
  not imply PR readiness; record remaining work and obtain the required review
  coverage before merge.
- Choose PR opening time based on cohesion and readiness. An issue does not
  require an immediate PR; implementation may fully resolve it before opening.
  Local checkpoint commits continue throughout either approach.
- For all future commits, use the default configured Git author and committer.
  Do not substitute a Codex identity through command-line configuration,
  environment variables, or explicit author/committer overrides. If no default
  identity is available, resolve that setup before committing instead of
  inventing an identity.
- Use [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/#specification)
  for every commit, including squash commit titles. Use
  `type(optional-scope): description`; use `feat` for features, `fix` for fixes,
  and an appropriate type such as `docs`, `test`, or `refactor` for other work.
  Mark breaking changes with `!` before the colon or a `BREAKING CHANGE:` footer.
- Every commit needs review. Review and check evidence must cover the final PR
  head; an approval for an earlier head does not cover subsequent changes.
- General code reviews run automatically on pushes to ready PRs. Move a draft
  PR to ready before expecting automatic review. Observe the automatic review
  before considering a manual request. If it demonstrably
  did not start and a manual general review is necessary, comment `@codex review`.
  Do not request a redundant general review for a push already under review.
- Once a review is requested or running, wait for its reply before continuing
  implementation or pushing further changes. Do not request additional reviews
  while waiting for the current review. Read-only review/CI status checks are OK.
- Track review start/completion times, review type, and change size in a local
  project file outside the repository. Estimate the first status check from
  comparable observations and adjust as evidence changes; avoid frequent fixed
  polling and never treat an estimated completion time as an actual reply.
- Security-sensitive code and any PR changing `docs/THREAT_MODEL.md` require a
  security review. Request it by commenting `@codex security review` only after
  general review has returned with no outstanding issues. This supersedes the
  earlier permission to request security review before general review finishes.
  Applicable checks need not finish before requesting security review, but must
  pass before merge.
- If automation has already started a security review, do not duplicate it.
  Wait for running reviews and verify their coverage of the final head.
- Address review findings, run appropriate checks, and obtain review of each
  follow-up commit. Resolve findings with evidence rather than treating silence
  or an unfinished review as approval.
- Merge only after required general/security reviews are clean, review threads
  are resolved, and all applicable checks for the final head pass. For code
  changes, this includes both push and PR CI where configured. Finish relevant
  edge-case checks before presenting a branch as ready for merge.
- Documentation-only PRs may skip code-related workflows, such as builds, code
  tests, and release-artifact checks. Run applicable documentation checks and
  obtain all required reviews, including security review for threat-model changes.
  In mixed workflows, retain document validation when skipping code checks.
  Changes to executable scripts, CI workflows, dependencies, or runtime
  configuration are code-related changes, not documentation-only changes.
- Use PRs for normal work. The user explicitly authorized the 2026-09-07
  `AGENTS.md` rules consolidation directly on `main` without a PR. That is a
  one-time exception, not standing permission to bypass PRs or review rules.

## Findings outside the current PR

- Open GitHub issues are the authoritative security-gap backlog. Local gap
  Markdown files are historical/reference material; reconcile them with current
  issues and code rather than treating them as a separate active backlog.
- Create GitHub issues for confirmed code problems found outside the current PR
  scope. Track them separately, then work them to resolution and close them
  before proceeding to the next part of the broader goal.
- GitHub issues are for code-related work only. Do not create issues for deferred
  certification administration, organizational paperwork, or other non-code
  certification tasks.
- Issue #126 is excluded from active work and has been closed as not planned
  following the certification decision. Do not reopen or resume its work unless
  the user explicitly changes that decision.

## ISO/IEC 42001 decision

- Pursuit of official ISO/IEC 42001 certification is canceled for now.
- All code must continue to meet applicable ISO/IEC 42001 requirements. Preserve
  and implement the relevant software controls and their verification; canceling
  certification does not authorize removing code safeguards.
- Non-code ISO/IEC 42001 work is indefinitely deferred. Preserve its status and
  supporting documentation outside GitHub issues so certification can resume
  later by addressing the outstanding non-code obligations. Do not claim formal
  certification from code compliance or completed engineering checks.

## Branch cleanup and operational boundaries

- Delete only local branches verified merged into remote main. Prefer Git over
  browser automation. Remote branch deletion requires new explicit authorization.
  Preserve checked-out branches, clones, worktrees, unfinished work, and unmerged
  branches. A squash merge alone is not proof of Git ancestry into remote main.
- Use the authenticated `gh` CLI for GitHub operations; it replaces the former
  GitHub desktop plugins/apps in this workflow.
- Existing GitHub authentication may be used for authorized repository actions.
  Never print, commit, or ask the user to paste a PAT into conversation.
- Do not infer authorization for live provider calls/spending, releases,
  deployments, or deferred governed ingestion from the engineering goal.

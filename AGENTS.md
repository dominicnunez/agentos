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

Repository hooks and CI own routine formatting, lint, test, vet, and build enforcement.

## Standing user instructions and goal continuity

- These instructions persist across tasks and context compaction. Later explicit
  user decisions supersede earlier conflicting instructions; do not revive a
  superseded policy from an old handoff or task summary.
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

- Use [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/#specification)
  for every commit, including squash commit titles. Use
  `type(optional-scope): description`; use `feat` for features, `fix` for fixes,
  and an appropriate type such as `docs`, `test`, or `refactor` for other work.
  Mark breaking changes with `!` before the colon or a `BREAKING CHANGE:` footer.
- Every commit needs review. Review and check evidence must cover the final PR
  head; an approval for an earlier head does not cover subsequent changes.
- General code reviews are configured to run automatically on pushes. Observe
  the automatic review before considering a manual request. If it demonstrably
  did not start and a manual general review is necessary, comment `@codex review`.
  Do not request a redundant general review for a push already under review.
- Once a review is requested or running, wait for its reply before continuing
  implementation or pushing further changes. Do not request additional reviews
  while waiting for the current review. Read-only review/CI status checks are OK.
- Security-sensitive code requires a security review. Request it by commenting
  `@codex security review` only after general review has returned with no
  outstanding issues. This supersedes the earlier permission to request security
  review before general review finishes. CI need not finish before requesting
  security review, but must pass before merge.
- If automation has already started a security review, do not duplicate it.
  Wait for running reviews and verify their coverage of the final head.
- Address review findings, run appropriate checks, and obtain review of each
  follow-up commit. Resolve findings with evidence rather than treating silence
  or an unfinished review as approval.
- Merge only after required general/security reviews are clean, review threads
  are resolved, and all required checks for the final head pass, including both
  push and PR CI where configured. Finish relevant edge-case checks before
  presenting a branch as ready for merge.
- Use PRs for normal work. The user explicitly authorized the 2026-09-07
  `AGENTS.md` rules consolidation directly on `main` without a PR. That is a
  one-time exception, not standing permission to bypass PRs or review rules.

## Findings outside the current PR

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

- After verifying a PR is merged, merged remote branches may be deleted. Prefer
  Git or an authenticated API over browser automation; the browser is an
  authorized fallback. Verify the branch and merged head before deleting it.
- Preserve local clones, worktrees, and local branches, including unfinished
  work. Remote branch cleanup is not permission to remove local work or delete
  unmerged branches.
- Existing GitHub authentication may be used for authorized repository actions.
  Never print, commit, or ask the user to paste a PAT into conversation.
- Do not infer authorization for live provider calls/spending, releases,
  deployments, or deferred governed ingestion from the engineering goal.

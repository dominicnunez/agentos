# Applicable GitHub checks

Ordinary documentation changes retain document validation and skip code builds,
code tests, dependency audits, frontend checks, and release-artifact checks.
Required jobs still report a result so branch protection can complete. The
workflow itself is not excluded with a path filter.

The shared `scripts/ci_scope.py` classifier examines the complete Git diff for
each run. Pull requests compare the base commit with the tested merge commit.
Branch pushes compare the previous and new commits. A new branch uses its unique
common ancestor with the fetched default branch, when available.

Documentation-only changes can include:

- Non-executable `.md`, `.txt`, `.png`, `.jpg`, `.jpeg`, `.gif`, `.webp`, `.svg`,
  and `.pdf` files under `docs/`.
- Root `AGENTS.md`, `CHANGELOG.md`, `CONTRIBUTING.md`, `README.md`, and `SECURITY.md`.
- Markdown files under `governance/aims/`.

All other paths run full code checks. In particular, schemas, scripts, workflow
definitions, dependencies, runtime configuration, `LICENSE`, and `NOTICE` are
code-related inputs even when a file is under `docs/`. Deleting the packaged
root `README.md`, changing a document into an executable or symbolic link, or
moving code into a document path also runs full checks. When adding a new code
consumer of document files, update the classifier and its tests in the same PR.
Filenames must be valid UTF-8, as required by the source archive builder;
otherwise full checks run even for a path with a documentation extension.

Missing or invalid revisions, failed Git inspection, an empty diff, and unknown
events require full checks. Tag pushes and manual release-artifact verification
also run full checks. The classifier reads local Git objects without fetching
additional history or running external diff helpers.

The required `CI verification (pull_request)`, `Dashboard frontend`, and
`Release artifact verification` job names remain unchanged. Code steps run unless
the classifier explicitly returns `false`. Release verification fails if its
frontend prerequisite fails or is cancelled. Document verification, document
history and assessment-bundle checks, and classifier regressions remain active.
The existing history-verification rules are unchanged.

Skipping code checks does not waive reviews. Changes to `docs/threat-model.md`
still require security review after a clean general review. Workflow changes
themselves require full code checks and security review.

## Race-test duration

CI runs the complete normal Go suite and all race tests. It discovers the app
and ledger tests, examples, and fuzz seed targets from compiled race binaries,
then executes their groups on independent runners in parallel. App uses up to
two nonempty groups. Ledger keeps the complete source-gate differential test
alone and distributes every other target round-robin across up to sixteen nonempty
groups. All other packages run under the race detector in one additional job.
Each package or group retains its twenty-minute timeout and uncached test run.
No tests, subtests, generated cases or history sizes are omitted from race testing.

Run `python3 scripts/race_tests.py` locally to execute the same race corpus
sequentially. It also runs the complete normal app and ledger packages.
`--suite normal` runs the full normal Go suite; `--suite plan` prints the CI
matrix discovered from the current source. `--suite app --group N` and
`--suite ledger --group N` execute exactly one discovered group, and
`--suite other` race-tests every package outside those two exact packages.
Groups retain every subtest of their top-level targets; completion must match
fresh discovery. App additionally requires both completion-growth sizes and
both unrelated-history sizes to pass in their owning group. Invalid group
numbers, missing targets, failed processes, race reports and timeouts fail.

The required `CI verification (pull_request)` result depends on discovery,
normal/code/document checks and the entire race matrix. A failed, cancelled or
unexpectedly skipped prerequisite cannot yield a successful required result.
Documentation-only changes still run document checks and skip the race matrix;
their final result requires successful classification and document checks.

Parallel runners reduce elapsed CI time by sharing independent groups across
more runners; they do not reduce the test workload or speed up individual reads.
Runner availability and compilation/cache costs affect the actual duration.
The complete ungrouped normal suite retains shared-process interaction checks.
As with the previous sequential race groups, race interactions must be exercised
within one top-level test; grouping does not provide cross-group race detection.
All groups run on the same workflow revision with the Go version in `go.mod`.

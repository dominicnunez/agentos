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

Skipping code checks does not waive reviews. Follow `AGENTS.md` for review
sequencing; changes to `docs/threat-model.md` still require security review after
a clean general review. Workflow changes themselves require full code checks
and security review.

## Race-test duration

The race suite has a twenty-minute timeout per Go package. Large SQLite history
tests incur substantial overhead under the race detector, and hosted runners
vary in speed. Run `go test -race -timeout=20m ./...` to match CI. Assertion
failures, data races, and expiry of this timeout fail the check.

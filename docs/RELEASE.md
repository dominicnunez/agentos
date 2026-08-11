# V1 release artifacts

Agent OS builds release-candidate artifacts without enabling a real model
provider or publishing a GitHub release. The repository `VERSION` is the single
binary version source.

## Reproducible builder v1

Run from an exact Git commit with the Go toolchain pinned by `go.mod` and the
CPython runtime pinned by `.python-version`:

```sh
python3 scripts/build-release.py --output work/release
```

The output directory must not already exist. The source checkout must contain
only files tracked by its Git commit: tracked changes, untracked files, and
ignored files all fail closed. The builder requires the exact Go version named
by `go.mod`, ignores ambient Go workspace and user settings, and derives
`SOURCE_DATE_EPOCH` from the source commit. A caller may supply only that same
epoch. It disables CGO and VCS embedding, pins baseline target architecture
levels, downloads the committed module graph, verifies cached module content
against its recorded hashes before compilation, removes local paths and link
build IDs, and embeds `VERSION` into both commands.

The V1 target matrix is intentionally Linux-only:

| Operating system | Architectures | Package |
|---|---|---|
| Linux | amd64, arm64 | `.tar.gz` |

Each package contains:

- `agentos`;
- `agentos-recovery`;
- `README.md` and `VERSION`; and
- the target's CycloneDX 1.6 module SBOM.

The output directory also contains every target SBOM, `SHA256SUMS`, and one
deterministic in-toto/SLSA provenance statement. The statement records the
source commit, build parameters, target matrix, exact Go and Python builder
versions, and subjects. It is build metadata, not a signature or GitHub
attestation.

The dedicated release-artifact workflow builds the complete output twice in
parallel with normal CI, compares it byte for byte, verifies every checksum,
inspects archive content and metadata, and runs both native binaries' version
commands. It then unpacks the Linux amd64 archive and runs the complete
loopback intake, completion-review, approval-isolation, backup, restore,
restart, expiry, and revocation pilot from those packaged binaries. It has
read-only repository permission and does not upload or publish artifacts. Each
of the two builds uses its own fresh Go module and build caches; no release
cache is restored, shared between builds, or saved.
macOS and Windows packages are outside the V1 support boundary.

## Publication boundary

The builder does not create tags, releases, attestations, or deployments.
Publishing `v1.0.0-rc.1` is a separate public/external and trusted-release
decision. Before publication:

1. record a distribution-license decision; this repository currently has no
   `LICENSE` file;
2. confirm required CI is green on the exact `main` commit;
3. approve the immutable `v1.0.0-rc.1` tag and public GitHub release;
4. rebuild from that tag and compare it with the approved commit output;
5. generate GitHub-signed artifact attestations with minimal workflow
   permissions; and
6. upload only the checksummed archives, SBOMs, provenance, and checksum file.

A released SemVer identifier is immutable. Any change after publication uses a
new release candidate such as `v1.0.0-rc.2`; do not move or replace a published
tag.

Release publication does not authorize deployment, consequential effects, or a
real model provider. Those gates remain separate in
[V1_RELEASE_READINESS.md](V1_RELEASE_READINESS.md).

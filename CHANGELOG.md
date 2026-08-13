# Changelog

Notable user-facing and operational changes are recorded here using a Keep a
Changelog structure and Semantic Versioning.

## [Unreleased]

Target: `1.0.0-rc.1`

### Added

- Durable organizations, Missions, Goals, intents, bounded Work, Task DAGs, Agent identities,
  executions, and append-only Event Contracts over SQLite.
- Private Linux user and authenticated A2A v1.0 work, status, result, and
  continuation boundaries with tenant-scoped capability roles.
- Resumable Linux setup with system mode by default, stable filesystem paths,
  caller-bound ownership, required provider discovery and testing, encrypted
  OpenAI credentials, a private Unix-socket console, and read-only diagnostics.
- Structured user-task completion with required fields, bounded
  content-addressed artifact evidence, and terminal-safe rendering.
- Deterministic execution, fake Agent execution, structured ToolOutcomes,
  ExecutionContextManifests, and explicit CompletionContracts.
- Dedicated reviewer decisions for unverified model candidates, including
  approve, reject, revise, stale-evidence protection, and restart recovery.
- Disabled-by-default Codex subscription and official OpenAI Responses model
  adapters with bounded model-only contracts.
- Backup, verification, no-overwrite restore, and fail-closed effect
  reconciliation tooling.

### Security

- Exact-effect approval, expiry, revocation, freeze, single-use, and
  attempt-time authorization checks.
- Kernel-provided local peer identity, owner-only exact-effect controls, strict
  Agent registries, role isolation, rate and concurrency limits, remote TLS
  requirements, bounded decoding, and authority-shaped input rejection.

### Operations

- Linux amd64/arm64 reproducible release builder for both commands, per-target
  CycloneDX SBOMs, SHA-256 checksums, and deterministic in-toto provenance.
- CI enforcement for formatting, build, vet, lint, race tests, fuzzing,
  vulnerability scanning, interoperability, recovery, and architecture.

# API Scope — v4.2

V1 API covers organization/team/task/event/control plus minimal versioned knowledge, instruction/reference Skills, deterministic audit findings/runs, and inference resource status/telemetry.

Lab/Experiment orchestration remains `VALIDATE NEXT`; no public federation/A2A API is required for V1.
## v4.2 A2A boundary

The REST/OpenAPI surface is not the A2A protocol definition.

V1 separately exposes a minimal A2A v1.0 Operator Gateway/Agent Card for Hermes. A2A wire handling belongs in `internal/operator/a2a` and translates to internal application commands/domain objects.

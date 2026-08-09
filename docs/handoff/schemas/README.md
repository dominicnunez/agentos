# Schemas

`SCOPE.json` classifies schema implementation status.

V1 communication is defined by:

- `event-envelope.schema.json`
- `event-draft.schema.json`
- typed payload schemas for V1 agent-proposable events

Older `semantic-message.schema.json` is removed from active schemas and retained only under `history/` as superseded provenance.

Schemas marked `VALIDATE_NEXT` or `FUTURE_IF_EARNED` are design references, not V1 implementation requirements.


## v4.2 active schemas

- `knowledge-record.schema.json`
- `skill.schema.json`
- `audit-finding.schema.json`
- `inference-pool.schema.json`

`experiment.validate-next.schema.json` documents a future-compatible Lab object but is not V1 CORE.

- `inference-usage-snapshot.schema.json` — normalized usage telemetry source/time/confidence.
- `event-knowledge-proposed-payload.schema.json` — repeated-pattern proposals require at least three occurrence refs by default.
- `event-skill-proposed-payload.schema.json` — V1 instruction/reference Skill proposal payload.
## v4.2 active boundary/runtime schemas

- `external-actor.schema.json`
- `a2a-task-mapping.schema.json`
- `execution-context-manifest.schema.json`
- `tool-outcome.schema.json`
- `effect-obligation.schema.json`

These keep A2A at the external boundary while preserving internal Event Contracts and add auditable runtime/effect evidence.

# Structured model input and source evidence

Production intake, planning, and Agent execution use the `model-input-v1`
request contract. Runtime instructions and blueprint instructions are system
messages. Operator messages and accepted task context are user messages.
Strategy, knowledge, coordination, and dependency evidence are separately
classified `low_privilege_data`. Prior model output, when supported by the
adapter, uses the assistant role. Source kind and role must agree; a payload
cannot choose its own instruction privilege.

Each message includes a runtime-selected source reference and a SHA-256 digest
of its text. The request has closed, case-sensitive JSON fields, valid UTF-8,
at most 256 messages, and a 256 KiB canonical serialization limit. Planning
also retains its 128 KiB complete-input limit. Invalid input fails before a
provider call. An adapter without structured-input support is rejected rather
than silently receiving a flattened prompt.

Derived Intent/Task IDs and compound blueprint, Goal, and knowledge revision
references use bounded SHA-256 identity components. Prefixes and child-task
suffixes therefore do not invalidate an admitted identifier, and full identities
are hashed without truncation. Blueprint ID and version are hashed separately
so delimiter-containing components cannot alias another revision.

## Invocation binding

The runtime issues a `src_` handle for each selected source. Handles bind the
organization, execution ID, complete input, and message position. The inference
guard validates that binding against its admitted scope before reserving the
existing durable inference budget. Canonical request fingerprints include
roles, source references, content digests, and handles. Billing and usage
reconciliation continue through the same inference boundary.

Handles are deterministic evidence identifiers, not secrets or capabilities.
They grant no approval, authority, or completion status. Resolution requires
membership in the current invocation's runtime binding; raw IDs and handles
embedded inside content do not create sources. Exact replays retain the same
handles. Another organization, execution, or input produces different handles.

The `intent-normalizer-v4` output retains the field name `source_message_id`,
but its value must be an issued operator-message handle. After strict output
validation, intake resolves the handle to the original durable message ID.
This applies to context, deliverables, completion criteria, constraints,
missing inputs, resolved decisions, Goal references, and replacement Work
references. Unknown handles and handles for non-operator sources fail closed.
Explicit and confirmed provenance still require a source; Goal and replacement
Work IDs must still occur exactly in that source message. The deterministic
literal intake path does not invoke a model and retains its direct provenance.

## Transport and replay

OpenAI Responses receives separate system, user, and assistant messages. Data
messages use a user message with a quoted `LOW_PRIVILEGE_DATA` envelope. Codex
receives leading system messages as developer instructions and user/data
messages as envelopes in a fresh user turn. Its current adapter rejects prior
assistant messages and system messages placed after user/data messages.
Envelopes expose the source handle, class, and content; internal source
references and digests are not added as transport metadata. Original payloads
can contain IDs or metadata claims as untrusted text.

Neither transport provides a native role below user for these data messages.
Quoting and explicit classification preserve the runtime representation;
they do not prove that a model will resist prompt injection. Runtime authority,
effect admission, independent verification, and completion controls remain
necessary even when model output follows the requested schema.

New Agent executions require context-builder `v4`, whose manifest fingerprints
the canonical, invocation-bound request. Replay reconstructs those bytes from
the selected durable sources. Historical `v1`, `v2`, and `v3` manifests retain
their original materialization rules; new executions cannot select them.
Planning uses prompt contract `task-planner-v2` with separate accepted-Intent
and strategy messages.

This contract is a prerequisite for broader provider routing. It does not
itself add provider connections, simultaneous route selection, or new Hermes
provider adapters. Tests use synthetic adapters and local transport fixtures;
they do not establish live-provider availability or behavior.

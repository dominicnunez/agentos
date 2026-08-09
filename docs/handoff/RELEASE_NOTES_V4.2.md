# Agent OS v4.2 Release Notes

v4.2 supersedes v4.1.2.

## Why this is v4.2

The core Event Contract architecture is unchanged, but V1 gains a concrete external-operator boundary and several runtime-evidence/durability primitives learned from studying Hermes Agent v0.19.0, v0.19.1, and v0.20.0.

## V1 additions

- minimal inbound A2A v1.0 Operator Gateway;
- Hermes v0.20.0 initial interoperability profile;
- ExternalActor identity/capability mapping;
- A2A external-task to internal Intent/Goal/Task correlation;
- ExecutionContextManifest for every model execution;
- ToolOutcome with postconditions/retryability/recovery;
- deterministic recovery before cognitive recovery;
- effect-bound human approval fingerprint;
- durable EffectObligation for consequential external effects;
- simple SecretSource seam;
- new audit/adversarial cases covering these boundaries.

## Still not V1

- outbound A2A remote-agent delegation/federation;
- Hermes as an internal Agent OS RuntimeAdapter;
- source-grounded knowledge validation;
- context compaction/hibernation;
- additional secret-manager integrations;
- full federation/marketplace.

## Architecture unchanged

- Event Contracts remain internal IPC;
- real work defines workflows/agents/teams;
- LLMs are used only when justified;
- human consequence boundaries remain human-controlled;
- knowledge/Skills remain evidence-backed and auditable, not authority.

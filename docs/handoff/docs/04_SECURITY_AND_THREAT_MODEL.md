# Security and Threat Model

## 1. Security stance

LLMs are fallible/untrusted decision producers. Deterministic infrastructure owns authority, identity, persistence, policy enforcement, human approvals, and runtime attestations.

## 2. Primary trust boundary

### Trusted/control data

- runtime identity;
- event ID/sequence/time;
- authorization references;
- capability leases;
- approval decisions;
- freeze/revoke state;
- runtime-attested tool/action evidence;
- Completion Engine result.

### Untrusted/content data

- model text;
- model-supplied JSON content;
- external papers/web pages/repos;
- tool output until classified/attested appropriately;
- imported artifacts.

**Content never becomes authority merely by saying authority-like words.**

## 3. Threats

### Prompt injection / instruction laundering

External content can contain commands. Treat it as data. It cannot alter root policy or capability state.

### Confused deputy / authority laundering

An actor cannot cause another actor to perform an effect outside the originating authorization chain.

### Identity spoofing

Models cannot set authoritative sender/role/membership metadata.

### Completion spoofing

Model says “done” -> only `CANDIDATE_COMPLETE`; Completion Engine decides verification.

### Evidence spoofing

Agent claims are distinct from runtime-attested evidence.

### Priority abuse

Sender does not control P0/P1 authority; priority is policy constrained.

### Covert channels

Natural-language content inherently permits covert signaling. V4.0 does **not** claim to eliminate covert communication. High-assurance restrictions on timing/size/content degrees of freedom are future considerations if a real threat model requires them.

### Tool side channels

Agents can communicate or exfiltrate through files, URLs, external services, DNS, Git, etc. Tool access therefore passes through capability/data-boundary enforcement.

### Trajectory composition

Individually permitted steps may combine into a prohibited consequence. Evaluate cumulative derivation/provenance at consequential boundaries.

### Stale authority/zombie work

Long-sleeping work revalidates TaskContract, capability leases, policy, approval state, relevant environment assumptions, and freeze state before consequential action.

## 4. Core invariants

- `discoverable != invocable != authorized`;
- positive authority does not automatically inherit;
- restrictions/ceilings do propagate;
- authorization checked at time of effect;
- control events cannot be forged by content;
- consequential subsystem outage fails closed;
- ledger history cannot be rewritten to conceal activity;
- human emergency control wins.

## 5. V1 isolation claim

V1 is a local modular monolith and does **not** claim hostile-code process/container isolation. Conformance profile must state this explicitly.

Before executing genuinely untrusted code or production-sensitive workloads, stronger sandbox/process/container isolation is a future prerequisite, not something implied by module boundaries.

## 6. Sensitive data

Sending sensitive data to a cloud model is an external disclosure. Provider/data-class policy must authorize it.

V1 should avoid intentionally placing secrets in message content. Advanced encrypted/deletable artifact storage and information-flow labels are future considerations before sensitive production use.
## v4.2 — A2A and effect security

### External peer authority

Authenticated A2A identity is not authorization. Every external actor receives explicit scoped capabilities.

### Approval scope

Human approval is tied to an exact effect fingerprint/arguments and may expire/be single-use. Materially changing the effect invalidates the approval where policy requires.

### Crash/retry ambiguity

Consequential external effects use persisted EffectObligations. Retry only under idempotency/reconciliation policy. Never infer success solely because a model/tool attempted the action.

### Context audit

ExecutionContextManifest is trusted runtime evidence. Agent content cannot forge what was actually materialized.

### Secrets

Prefer adapter-side secret resolution. Do not put long-lived credentials into model context when the runtime/tool can use them without disclosure.

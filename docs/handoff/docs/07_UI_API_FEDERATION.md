# Human UI, API, and External Integration

## 1. V1 UI

A basic CLI/web UI is sufficient. Show:

- organizations/teams/agents;
- tasks and dependencies;
- event/message timeline;
- blocked work;
- pending approvals;
- completion status/evidence;
- freeze/revoke controls;
- audit metadata.

## 2. Plain-language labels

Default examples:

| UI | Internal |
|---|---|
| Can't continue | `TASK_BLOCKED` |
| Needs your decision | `APPROVAL_PENDING` |
| Work submitted for checking | `CANDIDATE_COMPLETE` |
| Work verified | `TASK_VERIFIED_COMPLETE` |
| Work history | Event ledger/audit projection |
| Put agent on hold | Actor status/dormancy (future if needed) |

## 3. Event inspection

Human view should clearly separate:

- trusted runtime envelope;
- agent-generated content;
- artifacts/provenance;
- authorization references;
- runtime attestation.

There is no separate semantic-language inspector.

## 4. API

REST + SSE is sufficient for V1 control/read/event streaming.

WebSockets may be added later if bidirectional UI needs justify them.

## 5. External integrations

V1 has no **general federation** requirement; v4.2 requires only the minimal inbound A2A Operator Gateway for Hermes/external operator use.

The V1 inbound A2A Operator Gateway maps external task/session transport to internal work. Outbound A2A discovery/delegation remains later. Do not define an ANL/ASM federation payload.

MCP may be used behind the tool/capability gateway for compatible tools/resources; it is not the internal communication substrate.


## v4.1 UI additions

Default human views should include:

- Team/Agent knowledge with version history and “Why do we believe this?” provenance;
- active Skills with version, last verified, dependencies/capabilities required;
- open Audit Findings;
- inference resource status: subscription estimate/reset, metered budget, local capacity, reserve state;
- simple explanation of why a model/pool was selected when useful.

Keep exact IDs/evidence/profile details in Advanced/Audit views.
## v4.2 — A2A Operator Gateway is V1

Prior deferral of all A2A/federation no longer applies to the **minimal inbound operator interface**.

V1 includes:

- A2A v1.0 Agent Card;
- authenticated external actor mapping;
- work submission/continuation;
- task status/progress;
- blocked/input-needed mapping;
- result Artifact mapping;
- correlation to internal Intent/Goal/Task IDs;
- Hermes interoperability/conformance tests.

Still deferred:

- arbitrary outbound remote-agent discovery/delegation;
- federation marketplace;
- cross-organization trust federation;
- dynamic remote capability negotiation.

Do not expose root control operations through A2A merely because the protocol can carry messages.

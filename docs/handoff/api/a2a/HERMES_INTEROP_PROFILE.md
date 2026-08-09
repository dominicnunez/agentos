# Hermes Interoperability Profile — Initial v4.2 Target

## Baseline

Initial reference target from the design study:

```text
Hermes Agent: v0.20.0
Protocol: A2A v1.0
Role: external operator/manager of Agent OS
```

Hermes source/release reference:

`https://github.com/NousResearch/hermes-agent/releases`

This is a **tested compatibility baseline**, not a permanent lock.

Before upgrading the supported Hermes release:

1. run A2A protocol/conformance tests;
2. run real Hermes discovery/work/progress/blocked/input/result integration tests;
3. verify identity mapping and authorization behavior;
4. verify no human-boundary regression;
5. record the newly supported version.

If a Hermes release has an A2A plugin/registration issue in a particular CLI/TUI path, use a known supported gateway/integration path or fix the adapter/integration. Do not redesign internal Agent OS semantics around a transient Hermes bug.

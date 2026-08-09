
# Research — Non-Normative

Research material may contain ANL/Agent Semantic Model terminology from earlier design stages. Those communication architectures are superseded by v4.0 Event Contracts. Research remains useful for collaboration, safety, runtime, evaluation, and organizational ideas but cannot override normative v4.0 docs.

---

# Historical Research Documents

These two documents predate the current Agent OS + ANL specification and are preserved unchanged as design history.

They contain valuable research synthesis on AgentRadio, task graphs, blackboards, context/memory management, skill learning, verification, planning, compute allocation, and failure modes.

When they conflict with `../docs/00_START_HERE.md` through `../docs/10_AI_CODING_AGENT_INSTRUCTIONS.md`, the newer normative documents win.

Important evolution since these were written:

- Team is now explicitly a first-class durable actor.
- The architecture is explicitly an Agent OS, not only a multi-agent framework.
- ANL is the native semantic communication IR.
- Canonical machine messages are the source of truth and human views are decoded on demand.
- Security design now includes a semantic firewall and covert-channel-reduction objectives.
- The Organization Plane supports both building and operating artificial organizations.

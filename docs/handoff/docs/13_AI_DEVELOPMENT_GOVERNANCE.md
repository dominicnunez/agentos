# AI Development Governance

## 1. Purpose

The codebase itself will likely be developed with coding agents. Repository controls therefore need to prevent architecture erosion and unreviewed scope growth.

## 2. Authoritative sources

Order:

1. `IMPLEMENTATION_SCOPE.yaml`
2. `docs/29_V1_BUILD_CONTRACT.md`
3. v4 normative docs
4. schemas/API contracts/tests
5. `future/` only after explicit promotion
6. `research/` and `history/` are non-normative

## 3. Scope discipline

A coding agent must not interpret “future consideration” as implementation permission.

Every new package/subsystem should map to a `V1_CORE` scope item or an explicit human promotion.

## 4. Architectural changes

Changes to these require explicit review:

- Event Contract set/trust boundary;
- ledger/event ordering semantics;
- capability/policy enforcement;
- human consequence boundaries;
- Completion Engine authority;
- root invariants;
- provider/tool trust boundaries.

## 5. Removal-first maintenance

Prefer deleting dead/redundant code and dependencies over hiding them behind abstraction/suppression.

Archguard protects structural boundaries. Gallow is advisory anti-entropy until proven useful enough for stronger enforcement.

## 6. AI-generated patches

Require:

- tests;
- architecture checks;
- adversarial regression where relevant;
- no silent scope-tier promotion;
- no generated authority bypass;
- no implementation of superseded ANL/ASM concepts.

# Safety, Integrity, and Constitution

## 1. Safety is infrastructure

Safety is not a special agent department. Deterministic infrastructure constrains every intelligent actor.

## 2. Trusted computing base — V1

Keep small:

- actor/runtime identity;
- Event Gateway and ledger integrity interfaces;
- root typed policy;
- consequence classifier;
- capability gate;
- authorization trace logic;
- approval state;
- freeze/revoke;
- Completion Engine integrity;
- runtime attestation.

## 3. Authority hierarchy

```text
ROOT INVARIANTS
  > HUMAN/TENANT POLICY
  > ORGANIZATION POLICY
  > GOAL/TASK CONTRACT
  > TEAM/ASSIGNMENT RESTRICTIONS
  > CAPABILITY LEASE
  > ACTION
```

Lower layers may narrow but not expand an upper-layer ceiling.

## 4. Root invariants

1. no self-granting authority;
2. no positive authority inheritance by default;
3. no ledger rewriting to conceal history;
4. no content-forged identity/approval/attestation/completion;
5. no bypass of human consequence boundary;
6. no disabling human freeze/revoke through model action;
7. no worker modification of its own completion criteria;
8. no consequential action when required safety authority/state is unavailable;
9. persist communication before recipient availability;
10. unknown control semantics fail closed.

## 5. Human consequence boundaries

Human approval required for:

- financial;
- physical-world;
- public/external write;
- privilege/trust expansion;
- sensitive-data boundary expansion;
- destructive/effectively irreversible;
- legal/binding;
- trusted-core/security deployment.

Already-approved spending/resource use inside an envelope can remain autonomous.

## 6. Reversible internal autonomy

Agents may autonomously plan, research, code, test, communicate, reorganize bounded internal work, and use pre-approved resources so long as the effect remains inside the autonomy envelope.

Organizational optimization machinery itself is future, but this policy principle remains.
## v4.2 external operator invariant

> **External protocol access does not create authority.**

An authenticated A2A/Hermes peer has only the capabilities assigned to its `ExternalActor` identity. It cannot inherit root/human/organization authority from being the primary operator interface.

Protected human consequence boundaries remain unchanged.

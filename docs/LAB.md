# Governed Lab

The Lab lets the organization try a bounded approach without treating the
result as trusted operating knowledge. It composes the ordinary
Intent -> Work -> Task path with explicit containment, resource ceilings, and
an `EXPERIMENTAL_UNVERIFIED` trust label.

The Work scheduler and completion engine remain authoritative. The Lab is not
a second workflow engine and cannot grant capabilities, approve effects, or
activate a result.

## Current V1 slice

The first V1 slice is deliberately narrow:

- one experiment is bound to one same-organization Work and its immutable
  objective;
- the experiment records a disposable sandbox reference, the fixed
  `lab-no-effects-v1` capability profile, and explicit execution, child,
  usage, cost, wall-time, and inference-pool ceilings;
- execution is deterministic, uses no model inference, allows no metered cost,
  and passes through the existing Task completion boundary;
- execution-kind, inference, execution-count, and child-count ceilings are
  checked before Task admission and checked again from durable state;
- terminal experiment state is derived only from the authoritative Work
  transition and exact completion evidence;
- a new Intent, its Work, and experimental containment are admitted in one
  transaction, so restart or retry cannot silently downgrade the request;
- a crash after Work becomes terminal is closed by startup reconciliation;
- completion retains `EXPERIMENTAL_UNVERIFIED` and cannot activate anything.

Adaptive Agent execution remains fail closed until its token, cost, wall-time,
sandbox, and capability usage can be enforced at the same transactional
boundary. Recording a budget without enforcing it is not accepted as Lab
support.

## Promotion candidates

A completed experiment may be nominated as a candidate for knowledge, a
reference-based skill, or configuration. A nomination must name the exact
experiment revision and result, plus a distinct, earlier, same-organization
Work completion from another correlation stream as reproduction evidence.

The commissioning actor may nominate a result but cannot certify it. The
candidate has only the `CANDIDATE` state. There is intentionally no Lab method
that activates knowledge, skills, configuration, policy, authority, or
effects; those remain behind their normal validation and approval boundaries.

# AI incident, nonconformity, and corrective action

Status: **DRAFT — process not approved or operated**

## Classification and intake

An AI incident is an event or condition that caused or could cause harm,
unauthorized action, loss of control, misleading evidence, material service
failure, or violation of an approved AI policy, obligation, or impact boundary.
A nonconformity is failure to satisfy an applicable AIMS, policy, process,
control, legal, contractual, or other approved requirement. A security issue
may be both.

Reports are accepted through the approved security or project reporting path.
The incident owner records an immutable identifier, reporter channel, discovery
and occurrence times, affected scope, systems and parties, information
classification, initial severity, known facts, uncertainty, and evidence
preservation actions. Public issue trackers must not receive secrets, personal
data, exploit details that increase harm, confidential supplier information, or
restricted incident evidence.

## Response

1. Protect people and contain ongoing harm within delegated authority.
2. Preserve exact evidence and chain of custody without treating model output
   or ledger presence as proof that a claim is true.
3. Determine notification and escalation duties; consequential external,
   legal, sensitive-data, destructive, or deployment decisions require their
   established approval.
4. Correct the immediate condition and decide whether related operation,
   release, approval, capability, provider, or access must pause or be revoked.
5. Analyze contributing and root causes across technology, data, people,
   process, supplier, governance, incentives, and organizational context.
6. Assess whether similar conditions exist elsewhere and update AI risk and
   impact records.
7. Define corrective actions with owner, due condition, verification method,
   and expected risk reduction.
8. Independently review implementation and effectiveness after enough evidence
   exists; closing a code change alone does not prove effectiveness.
9. Retain lessons learned, required communication, and management-review input.

## Severity and timing proposal

| Severity | Meaning | Proposed response expectation |
|---|---|---|
| Critical | Ongoing or imminent catastrophic harm, broad unauthorized consequential control, systemic evidence compromise, or severe legal/safety exposure | Immediate containment and accountable escalation; affected operation remains stopped until explicit authorization |
| High | Material harm or control failure with serious plausible consequences or meaningful spread | Prompt containment, accountable review within two business days, and release block until treated or explicitly decided |
| Medium | Limited impact, contained nonconformity, or important weakness without current serious harm | Assigned action and review date; trend and scope analysis |
| Low | Minor isolated weakness or improvement opportunity | Recorded disposition and periodic trend review |

## Required record

Each incident or nonconformity record contains: ID; classification; severity and
rationale; scope and affected parties; facts and uncertainty; evidence
references and access class; containment; notifications and decisions; cause
analysis; related risks/impacts; correction; corrective actions, owners and due
conditions; verification; effectiveness result; residual decision; lessons;
closure identity and time; and management-review escalation.

An action is not closed by the same automated system that proposed or executed
it. Missing evidence, an unanswered required decision, an expired exception, or
an ineffective correction leaves the record open and fails closed at any gate
that depends on closure.

# AIMS objectives and measures

Status: **DRAFT**

These proposed objectives apply to the Agent OS project AIMS. Baselines begin
only after approval. A metric is evidence, not proof by itself that a control is
effective or that Agent OS conforms to ISO/IEC 42001.

| ID | Objective | Measure and proposed target | Owner role | Frequency | First evaluation |
|---|---|---|---|---|---|
| OBJ-01 | Prevent unauthorized consequential action | 100% of supported consequential-effect classes pass authenticated, tenant-bound, capability, approval, expiry, and transaction-boundary tests; zero known bypasses at release | Security owner | Every change and release candidate | Before first public V1 binary |
| OBJ-02 | Preserve durable evidence integrity | 100% of admitted Event Contracts have verified contiguous integrity records; backup/restore and startup tamper tests pass | Technical owner | Every change and quarterly recovery exercise | Within 30 days after policy approval |
| OBJ-03 | Make organizational work reproducible | 100% of supported execution paths bind exact execution context and completion evidence; deterministic replay tests pass | Technical owner | Every change and release candidate | Before first public V1 binary |
| OBJ-04 | Protect tenant and sensitive data boundaries | Zero known cross-tenant disclosure paths; all public exports remain bounded and exclude credentials, private prompts, unrelated tenant data, and internal payloads | Security owner | Every change and quarterly review | Within 30 days after policy approval |
| OBJ-05 | Govern AI risk and impact | 100% of material changes receive recorded risk and impact screening; every Critical or High risk has an owner, treatment, due condition, and explicit residual-risk decision before release | AIMS manager | Every material change and quarterly | Before the next material feature merge after approval |
| OBJ-06 | Maintain response and learning | Every confirmed AI or security incident receives containment, cause analysis, corrective action, and effectiveness review; Critical incidents begin containment immediately and receive accountable review within two business days | Incident owner | Per incident and quarterly trend review | At first qualifying incident after approval |
| OBJ-07 | Control suppliers and dependencies | 100% of compiled dependencies retain discoverable license evidence and vulnerability checks; critical providers receive documented risk, data, availability, monitoring, and exit review before production use | Supplier owner | Every dependency change and annually | Before first public V1 binary |
| OBJ-08 | Keep claims and user information accurate | Zero known unsupported conformity or certification claims; release documentation states current capabilities, material limitations, approval boundaries, and intended-use constraints | Product owner | Every documentation or release change | Before first public V1 binary |
| OBJ-09 | Close assurance findings | Critical findings block affected work; High findings block release unless explicitly treated; Medium and Low findings receive recorded disposition and target review | AIMS manager | Continuous and monthly | Within 30 days after policy approval |
| OBJ-10 | Operate the AIMS review cycle | Complete scheduled internal audit and management review with evidence, decisions, owners, and follow-up before seeking certification assessment | Accountable executive | At least annually | Before certification assessment |

## Measurement rules

- Evidence must identify its source, scope, observation time, owner, and exact
  result. Missing or ambiguous evidence does not count as a pass.
- CI success shows that automated checks passed for one revision; it does not
  replace operating evidence, impact assessment, internal audit, or management
  review.
- Metric failures create a nonconformity or risk-treatment decision rather than
  being silently excluded from the denominator.
- Targets and evaluation dates require approval together with the AIMS scope
  and policy. Changes follow controlled-document and change-management rules.

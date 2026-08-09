# Human Language and UI Terminology

Default UI uses normal workplace language. Advanced/Audit exposes exact event/state terminology.

| Human-facing | Internal |
|---|---|
| Can't continue | `TASK_BLOCKED` |
| Needs your decision | `APPROVAL_PENDING` |
| I saw this | `APPROVAL_ACKNOWLEDGED` |
| Work submitted for checking | `CANDIDATE_COMPLETE` |
| Work verified | `TASK_VERIFIED_COMPLETE` |
| Doesn't have access | capability check denied |
| Waiting on another task | dependency waiting state |
| Work history | event-ledger audit projection |
| Team messages | `MESSAGE` events |
| Evidence | `EVIDENCE_PUBLISHED` + artifact refs |

Human UI should explain action, reason, consequence, reversibility, evidence, and what is waiting. Do not expose only opaque enum names.


## v4.1 terminology

| Human-facing | Internal |
|---|---|
| What we learned | Versioned institutional knowledge |
| Why we believe this | Provenance/evidence history |
| Previous versions | Knowledge/Skill version history |
| Team procedure / reusable skill | Skill |
| Needs re-checking | STALE / audit revalidation finding |
| Audit issue | AUDIT_FINDING |
| AI/model capacity | InferencePool resource status |
| Experimental / not trusted yet | EXPERIMENTAL_UNVERIFIED |

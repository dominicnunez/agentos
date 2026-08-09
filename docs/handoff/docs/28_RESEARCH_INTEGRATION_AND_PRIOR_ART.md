# Research Integration and Prior Art

The August 2026 landscape study is included under `../research/landscape-2026-08-08/`.

## Adopted lessons already reflected in v4

- preserve originating human intent;
- make authorization ancestry inspectable;
- capability/policy feasibility before quality/cost selection;
- separate durable Agent identity, ExecutionProfile, and RuntimeAdapter;
- `discoverable != invocable != authorized`;
- declare conformance/unsupported guarantees;
- use passive async communication for the main collaboration hypothesis;
- use deterministic enforcement below models;
- evaluate before optimizing.

## Research ideas intentionally deferred

See `../future/FUTURE_CONSIDERATIONS.md` for topology selection, organization optimization, Goodhart monitoring, memory/SOP/skills, research self-improvement, runtime scheduling enhancements, federation, and other mature-system ideas.

## Communication revision

The landscape work originally recommended a smaller semantic model. v4 goes further: it removes the general semantic model entirely and uses Event Contracts + ordinary content.

This should itself be validated by observing whether concrete runtime needs force new typed event distinctions.


## v4.1 practical skill-learning influence

The handoff now explicitly borrows the useful external-agent pattern of durable, progressively loaded procedural skills while changing the trust model: proposed procedures are versioned/auditable, do not grant authority, and cannot directly become trusted executable runtime code. This is inspiration/adaptation rather than a claim that skill externalization is novel to Agent OS.

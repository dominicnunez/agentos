# Future / Historical Design Note — Codec And Toon Historical Notes

**Status:** Non-normative. Preserved from v3.2 because useful portions may be revisited after prerequisites are met.  
**Important:** Any ANL or Agent Semantic Model assumptions in this preserved note are superseded by v4.0 Event Contracts and MUST NOT be implemented.  
**Authority:** `../FUTURE_CONSIDERATIONS.md` determines whether/when an idea may be revisited.

---

# Semantic Codec Policy and TOON

## 1. Layering decision

Do not confuse **semantic meaning** with **serialization**.

```text
Agent Semantic Model
        |
        +-- JSON       V1 canonical persistence/API/model structured IO
        +-- human text deterministic authoritative rendering
        +-- TOON       later context codec experiment
        +-- CBOR/etc.  possible future wire/storage profiles
```

A codec may change without changing the semantic object.

## 2. TOON decision

TOON is **not** a replacement for the Agent Semantic Model.

TOON is a candidate compact representation for model context, particularly for large uniform repeated records.

It is classified `VALIDATE NEXT`.

V1 does not require:

- TOON generation by models;
- TOON ledger storage;
- adaptive codec selection;
- a TOON dependency in core semantic validation.

## 3. V1 rule

Use JSON everywhere first:

- canonical stored message;
- REST API;
- constrained model output;
- model input unless the benchmark explicitly tests a codec.

This gives a stable baseline.

## 4. TOON benchmark

When promoted to `VALIDATE NEXT`, compare JSON vs TOON on Agent OS payloads such as:

- TeamSync batches;
- evidence lists;
- task-state batches;
- memory retrieval results;
- large uniform observations.

Measure:

- prompt tokens;
- completion tokens;
- parse failure;
- semantic field loss;
- task accuracy;
- latency;
- cost.

A token reduction is not sufficient if semantic or task accuracy falls materially.

## 5. Runtime-owned encoding

If TOON is adopted for context:

```text
model structured output
   -> validated semantic object
   -> canonical persistence
   -> runtime ContextBuilder
   -> TOON projection when selected
   -> receiving model
```

Prefer runtime encoding over asking models to invent/canonicalize TOON themselves.

## 6. No early adaptive codec optimizer

Do not immediately build per-model/payload automatic codec choice.

First collect benchmark evidence. If different models/payload shapes have meaningfully different winners, then a simple `ContextCodec` interface may be justified.

## 7. Source references

- User-provided overview: `https://www.digitalocean.com/community/tutorials/toon-vs-json`
- TOON specification/project: `https://github.com/toon-format/spec`

External format versions are dependencies, not Agent OS semantic authority.

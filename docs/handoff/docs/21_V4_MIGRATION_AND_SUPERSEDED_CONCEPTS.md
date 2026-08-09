# v4.0 Migration and Superseded Concepts

## From v3.2 to v4.0

### Replace

```text
Agent Semantic Model / SemanticMessage
        ->
EventDraft + Event + typed Event Contracts + ordinary content
```

### Remove from active code/spec

- semantic ontology package;
- observation/assertion/question/answer/contradiction message-type taxonomy;
- semantic parser/validator beyond ordinary event payload validation;
- authoritative semantic human renderer;
- ANL/ASM federation concepts.

### Keep

- canonical JSON;
- structured payloads where runtime behavior needs them;
- artifact/evidence references;
- runtime-owned identity/provenance;
- asynchronous lateral delivery;
- event sourcing;
- action-boundary passive awareness;
- deterministic safety/completion.

## Serialization

JSON is V1. TOON may later be a context codec. No semantic layer depends on a particular codec.

## Historical material

Research/history files may still contain ANL/ASM terminology. They are explicitly non-normative and should not be copied into implementation.


## v4.1 correction to v4.0 scope

v4.0 correctly removed ANL/ASM but overcorrected by pushing all durable memory/skills outside V1. v4.1 restores a minimal evidence-backed versioned knowledge layer and instruction/reference Skills. This does **not** revive ANL/ASM or the old broad memory ontology. The Event Contract architecture remains authoritative.

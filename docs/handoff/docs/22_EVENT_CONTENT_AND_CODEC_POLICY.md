# Event Content and Codec Policy

## V1

- Persist events as canonical JSON.
- API uses JSON.
- Model context uses JSON/natural language.
- Agent content may be text or structured JSON plus artifact refs.

## Codec boundary

If later needed:

```go
type ContextCodec interface {
    Encode(ContextView) ([]byte, error)
}
```

Candidate codecs may include JSON, TOON, CSV-like table views, or others.

The codec is a projection for model consumption. It never changes authoritative Event meaning/history.

## TOON

TOON is `VALIDATE_NEXT`, not V1. Revisit only after:

- representative JSON context traces exist;
- token/context cost is meaningful;
- benchmark includes accuracy/parsing failure, not token count alone.

## Structured domain content

Applications may define their own structured payload/content schemas. Do not promote those schemas into global Agent OS event kinds unless runtime control behavior requires it.

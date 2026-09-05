# Exact JSON at security boundaries

The `internal/boundaryjson` package validates JSON before decoding it into a
closed runtime schema. It rejects duplicate object members, including escaped
spellings of the same key and duplicates inside raw or open data. Struct field
names must match their declared JSON spelling exactly; unknown fields and
case-insensitive aliases fail closed. Invalid UTF-8, multiple top-level values,
more than 64 nested levels, and inputs exceeding 16 MiB are rejected. Transport
and domain limits can be narrower and remain enforced by their callers.

Validation walks the token stream once and caches schema field lookup tables
for that decode. A second decode performs ordinary Go type conversion into a
new value. Failed decoding leaves the caller's destination unchanged; success
replaces it, including clearing omitted fields. Error messages contain fixed
categories, never input field names or values.

String-keyed maps, interfaces, and `json.RawMessage` deliberately permit open
data keys. These keys remain case-sensitive, and duplicate detection still
applies throughout their contents. Typed map values retain their closed schemas.
The numeric-preserving entry point retains `json.Number` in open data. Domain
validation must still reject missing fields, invalid enum values, forbidden
nulls, and values outside their permitted ranges. Custom object decoders are
not an escape from the declared struct schema.

The shared decoder is used by structured model output, A2A protocol DTOs and
content, bootstrap and trust configuration, effect obligations and status
responses, closed ledger and recovery contracts, projection records, and audit
evidence. It also replaces the event admission duplicate-key scanner. Existing
partial event views and third-party provider/SDK schemas are not automatically
converted into closed schemas by this change. This contract does not claim
that every `encoding/json` call in the repository has been replaced.

Existing valid canonical records retain their representation and fingerprints;
no schema version or on-disk rewrite is introduced. Previously tolerated
ambiguous input can now fail at admission or recovery instead of silently
selecting one interpretation.

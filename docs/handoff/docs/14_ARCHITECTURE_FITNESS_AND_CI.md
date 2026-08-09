# Architecture Fitness and CI

## Required CI gates

V1 should enforce:

- `gofmt`;
- `go test ./...`;
- `go vet ./...`;
- race tests for concurrency-critical modules;
- Archguard module/dependency rules;
- schema validation tests;
- migrations/replay tests;
- adversarial safety tests;
- Gallow advisory anti-entropy if retained.

## Event architecture tests

Required examples:

- EventDraft cannot set authoritative source/sequence/time;
- model-proposed `APPROVAL_DECIDED` is rejected;
- message is unavailable if persistence fails;
- stored event canonical JSON/hash is stable under the chosen canonicalizer;
- duplicate delivery cannot duplicate consequential action;
- projection rebuild from ledger yields same state;
- agent text claiming another sender does not change envelope identity;
- agent text claiming completion does not set verified completion.

## Architectural fitness

Prevent:

- direct agent-to-agent semantic bypass around Event Gateway;
- policy module importing model provider implementation;
- completion depending on worker self-verdict;
- future packages becoming runtime dependencies before promotion;
- separate shadow state stores becoming alternative truth sources.
## v4.2 A2A/Hermes test gates

The A2A adapter is a boundary and should be testable without a live Hermes process.

CI should include:

- unit tests for A2A -> internal command translation;
- ExternalActor authentication/authorization tests;
- task/status/artifact mapping tests;
- blocked/input-needed continuation tests;
- architecture guard proving core domain modules do not import A2A protocol packages.

A separate integration/release profile should run against a pinned supported Hermes release/configuration before declaring Hermes interoperability working.

Do not weaken the protocol/domain boundary to make the integration test easier.
